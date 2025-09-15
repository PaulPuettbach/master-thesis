from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
import random
import re
from threading import Thread, RLock
from time import sleep
from collections import deque

config.load_incluster_config()

# this is basically how this works:
# look at all the nodes
# keep watch for all the pods
# match them to the right node
#in part inspired by https://sebgoa.medium.com/kubernetes-scheduling-in-python-3588f4928b13

"""Init"""
v1=client.CoreV1Api()

"""global"""
#list of all pod names to not double check
pod_name_array = []
#list of touples with pod name and the resource name
pod_name_list_scheduled = []
node_id = 0 
ready_nodes = {}
thread_lock = RLock()
recent_deletions = []
deletion_lock = RLock()

class NodeMeta:
    def __init__(self, id, name, memory, status):
        self.id = id
        self.name = name
        self.memory = memory
        allowed_status = ["Available", "NotAvailable"]
        if not(isinstance(status, str) and status in allowed_status):
            raise ValueError(f"status should be one of \"Available\", \"NotAvailable\", got {status}")
        self.status = status
        self.queue = deque()
    def __str__(self):
        return f'Node({self.id}:{self.name},{self.memory},{self.status})'
    
    def __repr__(self): 
        return f'Node({self.id}:{self.name},{self.memory},{self.status})'


def byte_unit_conversion(input):
    # 16.213.536Ki
    # 1408Mi
    # 1 Mi is 1024 Ki
    #first capture group is 1 or more digits and then 0 or more whitespace then  the other capture group with 1 or more letters
    p = re.compile(r'(\d+)\s*(\w+)')
    number_str, unit = p.match(input).groups()
    number = int(number_str)
    if unit == "Ki":
        return number * 1024
    elif unit == "Mi":
        return number * 1024 * 1024
    elif unit == "K":
        return number * 1000
    elif unit == "M":
        return number * 1000 * 1000
    else:
        raise ValueError(f"Unknown unit: {unit}")


"""Logic"""
def nodes_available():
    global node_id
    global ready_nodes
    global thread_lock
    count = 0
    with thread_lock:
        for n in v1.list_node().items:

            eligable = True
            for condition in n.status.conditions:
                if (condition.type == 'Ready' and condition.status != 'True') or \
                   (condition.type == 'MemoryPressure' and condition.status == 'True') or \
                   (condition.type == 'DiskPressure' and condition.status == 'True') or \
                   (condition.type == 'PIDPressure' and condition.status == 'True') or \
                   (condition.type == 'NetworkUnavailable' and condition.status == 'True'):
                    eligable = False
                    break
            if eligable:
                node_name = n.metadata.name
                node_memory = int(byte_unit_conversion(n.status.allocatable["memory"]) * 0.75)
                # Get all pods in all namespaces
                all_pods = v1.list_pod_for_all_namespaces().items

                # Filter those scheduled to the target node
                requested_memory = 0
                for pod in all_pods:
                    if pod.spec.node_name == node_name:
                        for container in pod.spec.containers:
                            # print(f"this is one container with this request {container.resources.requests}", flush=True)
                            if container.resources.requests and "memory" in container.resources.requests:
                                requested_memory += byte_unit_conversion(container.resources.requests["memory"])
                node_memory -= requested_memory
                ready_nodes[count + node_id] = NodeMeta(id=count + node_id, name=node_name, memory=node_memory, status="Available")

        node_id = count + node_id

def watch_node_conditions():
    global node_id
    global ready_nodes
    global thread_lock
    w = watch.Watch()

    for event in w.stream(v1.list_node):
        #print(f"this is the node event {event['type']}", flush=True)
        node = event['object']
        node_name = node.metadata.name

        eligable = True
        for condition in node.status.conditions:
            if (condition.type == 'Ready' and condition.status != 'True') or \
               (condition.type == 'MemoryPressure' and condition.status == 'True') or \
               (condition.type == 'DiskPressure' and condition.status == 'True') or \
               (condition.type == 'PIDPressure' and condition.status == 'True') or \
               (condition.type == 'NetworkUnavailable' and condition.status == 'True'):
                eligable = False
                break
        with thread_lock:
            if eligable and all(node.name != node_name for node in ready_nodes.values()):
                node_memory = int(byte_unit_conversion(node.status.allocatable["memory"]) * 0.75)
                node_id +=1
                ready_nodes[node_id] =  NodeMeta(id=node_id, name=node_name, memory=node_memory, status="Available")
                continue
        with thread_lock:
            match = next((node.id for node in ready_nodes.values() if node.name == node_name), None)
        if not eligable and match:
            with thread_lock:
                ready_nodes[match].status="NotAvailable"

#metadata is the V1objectmeta of the pod https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ObjectMeta.md
#node is the name of the node n.metadata.name
def schedule(meta, node, namespace="spark-namespace"):

    global pod_name_list_scheduled

    if not node:
        print("no usuable nodes", flush=True)
        raise Exception("cannot schedule no available nodes")
    target=client.V1ObjectReference()
    target.kind="Node"
    target.apiVersion="v1"
    target.name= node

    body=client.V1Binding(target = target, metadata = meta)
    pod_name_list_scheduled.append((meta.name, node))

    #there is an issue with the kuebrentes api package it does not matter much the pod will be shceduled correctly so we just ignore it
    # see https://github.com/kubernetes-client/python/issues/825
    try:
        v1.create_namespaced_binding(namespace = namespace, body = body, _preload_content=False)
        return True
    except:
        return True

def schedule_after_add(meta, node):

    if node.status == "NotAvailable":
        #put the pod in a retry queue
        node.queue.append(meta)
        return True
    try:
        spec_pod = v1.read_namespaced_pod(meta.name, "spark-namespace").spec
    except ApiException as e:
        if e.status == 404:
            #print(f"Pod {pod_meta.name} already deleted. Skipping scheduling.", flush=True)
            return True
        else:
            raise  # re-raise if it's a different error
        
    requested_memory = 0
    for container in spec_pod.containers:
        # print(f"this is one container with this request {container.resources.requests}", flush=True)
        requested_memory += byte_unit_conversion(container.resources.requests["memory"])


    if node.memory <= requested_memory:
        #put pod into retry queue
        # print("not scheduled", flush=True)
        node.queue.append(meta)
        return True
    # print(f"this is the available node memory {available_node_memory} and the requested memory {requested_memory} for pod name {pod_meta.name} scheduling from daemon", flush=True)
    node.memory = node.memory - requested_memory
    schedule(meta, node.name)

def schedule_from_queue(node):
    global pod_name_array
    # print(f"get into the schedule from queue", flush=True)
    if  node.status == "NotAvailable":
        return True
    node_name = node.name
    # print(f"attempting to schedule from queue", flush=True)
    #https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md#read_node_status i think the documentation is wrong and the return type should be v1NodeStatus:
    #https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1NodeStatus.md
    while node.queue:
        available_node_memory = node.memory
        pod_meta = node.queue[0]
        if not node_name:
            #dont do anything try again once another pod finishes
            break
        if pod_meta.name not in pod_name_array:
            #print(f"this is a pod we deleted before that is still in the queue {pod_meta.name}", flush=True)
            node.queue.popleft()
            #we dont delete out of the queue when the pod gets deleted as that might be a lot of iterations instead of just checking 
            break
        
        try:
            spec_pod = v1.read_namespaced_pod(pod_meta.name, "spark-namespace").spec
        except ApiException as e:
            if e.status == 404:
                #print(f"Pod {pod_meta.name} already deleted. Skipping scheduling.", flush=True)
                return True
            else:
                raise  # re-raise if it's a different error
        #this is the dictonary for the status of allocatable{'cpu': '24', 'ephemeral-storage': '972991057538', 'hugepages-1Gi': '0', 'hugepages-2Mi': '0', 'memory': '16213536Ki', 'pods': '110'}
        
        requested_memory = 0
        for container in spec_pod.containers:
            # print(f"this is one container with this request {container.resources.requests}", flush=True)
            requested_memory += byte_unit_conversion(container.resources.requests["memory"])

        if available_node_memory <= requested_memory:
            #dont do anything try again once another pod finishes
            # print("not scheduled", flush=True)
            break
        # print(f"this is the available node memory {available_node_memory} and the requested memory {requested_memory} for pod name {pod_meta.name} scheduling from queue", flush=True)
        node.queue.popleft()
        node.memory = available_node_memory - requested_memory
        schedule(pod_meta, node_name)

def flush_retry_queue_periodically():
    global recent_deletions
    global deletion_lock
    global thread_lock
    while True:
        sleep(2)
        with deletion_lock:
            deleted = recent_deletions.copy()
            recent_deletions.clear()

        with thread_lock:
            for node in deleted:
                schedule_from_queue(node)

def watch_pod():
#every new spark task comes with multiple pods they come in at the same time so we buffer for time
    global pod_name_array
    global pod_name_list_scheduled
    global ready_nodes
    global recent_deletions
    global deletion_lock
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):
        if event['type'] == 'ADDED':
            if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "random-scheduler":

                if event['object'].metadata.name not in pod_name_array:
                    pod_name_array.append(event['object'].metadata.name)
                    meta = client.V1ObjectMeta()
                    meta.name = event['object'].metadata.name
                    meta.uid = event['object'].metadata.uid
                    with thread_lock:
                        random_node = random.choice(list(ready_nodes.values()))
                    schedule_after_add(meta, random_node)
        elif event['type'] == 'MODIFIED' or event['type'] == 'DELETED':

            if (event['object'].status.phase == "Succeeded" or event['object'].status.phase == "Failed" or event['type'] == 'DELETED') and event['object'].spec.scheduler_name == "random-scheduler":
                #print(f"this the pod name after check for phase or deleted {event['object'].metadata.name}", flush=True)
                if event['object'].metadata.name in pod_name_array:
                    # print(f"this the pod name after check for pod_name_list_running {event['object'].metadata.name}", flush=True)
                    for pod_name in pod_name_array:
                        if pod_name == event['object'].metadata.name:
                            # print(f"this the pod name after check if in pod_dic {event['object'].metadata.name}", flush=True)
                            pod_name_array.remove(pod_name)
                            spec_pod = event['object'].spec
                            for element in pod_name_list_scheduled:
                                #element is touple with (pod_name, node_name)
                                # if the pod was deleted and not scheduled before nothing is done
                                if element[0] == pod_name:
                                    pod_name_list_scheduled.remove(element)
                                    # print(f"this the pod name after check if in pod_name_list_scheduled {event['object'].metadata.name}", flush=True)
                                    requested_memory = 0
                                    #bad form but easiest
                                    with thread_lock:
                                        node = [node for node in ready_nodes.values() if node.name == element[1]][0]
                                    for container in spec_pod.containers:
                                        requested_memory += byte_unit_conversion(container.resources.requests["memory"])
                                    with deletion_lock:
                                        # print(f"success this pod: {pod_meta.name}, and getting this much memory back {requested_memory}", flush=True)
                                        node.memory += requested_memory
                                        if node.name not in [deleted.name for deleted in recent_deletions]: recent_deletions.append(node)
                                    break
                                
                            break
        else:
            #this event['type'] == 'UNKNOWN':
            print("something went wrong", flush=True)
            print(f"this is the pod event {event['type']}", flush=True)
            print(f"this pod did not work {event['object']}", flush=True)

def main():
    node_thread = Thread(target=watch_node_conditions)
    node_thread.start()
    retry_thread = Thread(target=flush_retry_queue_periodically)
    retry_thread.start()
    nodes_available()
    watch_pod()

if __name__ == '__main__':
    main()