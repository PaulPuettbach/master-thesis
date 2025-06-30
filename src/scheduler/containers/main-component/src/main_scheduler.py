from kubernetes import client, config, watch
import requests
import os
import re
import json
from kubernetes.client.rest import ApiException
from deepdiff import DeepDiff
from collections import deque
from flask import Flask, request
from threading import Thread, RLock
from time import sleep

config.load_incluster_config()

# this is basically how this works:
# look at all the nodes
# keep watch for all the pods
# match them to the right node
#in part inspired by https://sebgoa.medium.com/kubernetes-scheduling-in-python-3588f4928b13
#take the resource dic add a queue to it every time a pod finishes we see if we can take the first thing from the fifo and add it
"""
The phase of a Pod is a simple, high-level summary of where the Pod is in its lifecycle. The conditions array, the reason and message
fields, and the individual container status arrays contain more detail about the pod's status. There are five possible phase values:

Pending: 
    The pod has been accepted by the Kubernetes system, but one or more of the container images has not been created. This includes time
    before being scheduled as well as time spent downloading images over the network, which could take a while.
Running:
    The pod has been bound to a node, and all of the containers have been created. At least one container is still running, or is in the
    process of starting or restarting.
Succeeded:
    All containers in the pod have terminated in success, and will not be restarted.
Failed:
    All containers in the pod have terminated, and at least one container has terminated in failure.
    The container either exited with non-zero status or was terminated by the system.
Unknown:
    For some reason the state of the pod could not be obtained, typically due to an error in communicating with the host of the pod.

"""
"""Init"""
v1=client.CoreV1Api()
app = Flask(__name__)
worker_service = os.environ['DAEMON-SERVICE']
worker_service_port = os.environ['DAEMON-SERVICE-PORT']

"""global variables"""
#make dictonary 
node_id = 0
pod_name_list_running = []
pod_name_list_scheduled = []
pod_id = 0
pod_dic = {}
resource_dic = {}
best_fitness = 0
thread_lock = RLock()
deletion_lock = RLock()
recent_deletions = []


last_pod = None

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

"""util"""

def compare_pod_changes(current_pod):
    global last_pod

    if not last_pod:
        last_pod = current_pod
        return

    pod_name = current_pod.metadata.name\
    
    # Convert to dicts for DeepDiff
    current_pod_dict = current_pod.to_dict()
    last_pod_dict = last_pod.to_dict()

    
    # Use DeepDiff to find differences
    diff = DeepDiff(last_pod_dict, current_pod_dict, ignore_order=True)

    if diff:
        print(f"-------------------------------------------------------------- Changes in pod '{pod_name}':--------------------------------------------------------------")
        print(diff.to_json(indent=2))
    else:
        print(f"No changes in pod '{pod_name}'")

    # Update stored state
    last_pod = current_pod

def byte_unit_conversion(input):
    # 16.213.536Ki
    # 1408Mi
    # 1 Mi is 1024 Ki
    #first capture group is 1 or more digits and then 0 or more whitespace then  the other capture group with 1 or more letters
    p = re.compile(r'(\d+)\s*(\w+)')
    number_str, unit = p.match(input).groups()
    number = int(number_str)
    match unit:
        case "Ki":
            return number * 1024
        case "Mi":
            return number * 1024 * 1024
        case "K":
            return number * 1000
        case "M":
            return number * 1000 * 1000
        case _:
            raise ValueError(f"Unknown unit: {unit}")
        
        
"""Logic"""

def watch_node_conditions():
    global node_id
    global resource_dic
    w = watch.Watch()

    for event in w.stream(v1.list_node):
        #print(f"this is the node event {event['type']}", flush=True)
        node = event['object']
        node_name = node.metadata.name

        eligable = True
        for condition in node.status.conditions:
            if condition.type == 'Ready' and condition.status != 'True':
                eligable = False
            if condition.type == 'MemoryPressure' and condition.status == 'True':
                eligable = False
            if condition.type == 'DiskPressure' and condition.status == 'True':
                eligable = False
            if condition.type == 'PIDPressure' and condition.status == 'True':
                eligable = False
            if condition.type == 'NetworkUnavailable' and condition.status == 'True':
                eligable = False
        if eligable and all(node.name != node_name for node in resource_dic.values()):
            node_memory = int(byte_unit_conversion(node.status.allocatable["memory"]) * 0.75)
            with thread_lock:
                node_id +=1
                resource_dic[node_id] =  NodeMeta(id=node_id, name=node_name, memory=node_memory, status="Available")
            node_change(node_id, "add")

        if not eligable and any(node.name == node_name for node in resource_dic.values()):
            resource_dic[node_id].status="NotAvailable"
            node_change(node_id, "delete")

def nodes_available():
    ready_nodes = []
    for n in v1.list_node().items:
        eligable = True
        for condition in n.status.conditions:
            if condition.type == 'Ready' and condition.status != 'True':
                eligable = False
            if condition.type == 'MemoryPressure' and condition.status == 'True':
                eligable = False
            if condition.type == 'DiskPressure' and condition.status == 'True':
                eligable = False
            if condition.type == 'PIDPressure' and condition.status == 'True':
                eligable = False
            if condition.type == 'NetworkUnavailable' and condition.status == 'True':
                eligable = False
        if eligable:
            ready_nodes.append(n)
    return ready_nodes

#metadata is the V1objectmeta of the pod https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ObjectMeta.md
#node is the name of the node n.metadata.name
def schedule(pod_meta, node_name, resource_id, namespace="spark-namespace"):
    # print(f"this is the name of the pod being scheduled {pod_meta.name}", flush=True)
    global pod_name_list_scheduled
    
    try:
        target=client.V1ObjectReference()
        target.kind="Node"
        target.apiVersion="v1"
        target.name= node_name

        body=client.V1Binding(target = target, metadata = pod_meta)

        #there is an issue with the kuebrentes api package it does not matter much the pod will be shceduled correctly so we just ignore it
        # see https://github.com/kubernetes-client/python/issues/825
        pod_name_list_scheduled.append((pod_meta.name, resource_id))
        v1.create_namespaced_binding(namespace = namespace, body = body, _preload_content=False)
        return True
    except Exception as e:
        print(f"this is where it goes wrong {e}", flush=True)
        return True

def schedule_from_EA(pod_meta, resource_id):
    global pod_name_list_scheduled
    global resource_dic
    node = resource_dic[int(resource_id)]
    if  node.status == "NotAvailable":
        #put the pod in a retry queue
        node.queue.append(pod_meta)
        return True
    node_name = node.name
    available_node_memory = node.memory
    # print(f"attempting to schedule from EA", flush=True)
    #https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md#read_node_status i think the documentation is wrong and the return type should be v1NodeStatus:
    #https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1NodeStatus.md
    if any(pod_meta.name == pod[0] for pod in pod_name_list_scheduled):
        return True
    
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

    print(f"this is the available node memory {available_node_memory} and the requested memory {requested_memory} for pod name {pod_meta.name} scheduling from daemon", flush=True)

    if available_node_memory <= requested_memory:
        #put pod into retry queue
        print("not scheduled", flush=True)
        node.queue.append(pod_meta)
        return True
    node.memory = available_node_memory - requested_memory
    schedule(pod_meta, node_name, resource_id)

def schedule_from_queue(resource_id):
    global pod_name_list_scheduled
    global resource_dic
    node = resource_dic[int(resource_id)]
    if  node.status == "NotAvailable":
        return True
    node_name = node.name
    # print(f"attempting to schedule from queue", flush=True)
    #https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md#read_node_status i think the documentation is wrong and the return type should be v1NodeStatus:
    #https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1NodeStatus.md
    while node.queue:
        available_node_memory = node.memory
        pod_meta = node.queue[0]
        if any(pod_meta.name == pod[0] for pod in pod_name_list_scheduled):
            break
        if not node_name:
            #dont do anything try again once another pod finishes
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
        print(f"this is the available node memory {available_node_memory} and the requested memory {requested_memory} for pod name {pod_meta.name} scheduling from queue", flush=True)

        if available_node_memory <= requested_memory:
            #dont do anything try again once another pod finishes
            print("not scheduled", flush=True)
            break
        node.queue.popleft()
        node.memory = available_node_memory - requested_memory
        schedule(pod_meta, node_name, resource_id)

def flush_retry_queue_periodically():
    global recent_deletions
    global deletion_lock
    global resource_dic
    while True:
        sleep(2)
        with deletion_lock:
            deleted = recent_deletions
            recent_deletions.clear()

        with thread_lock:
            for resource_id in deleted:
                if resource_id in resource_dic:
                    schedule_from_queue(resource_id)

def watch_pod():
#every new spark task comes with multiple pods they come in at the same time so we buffer for time
    w = watch.Watch()
    global pod_id
    global pod_dic
    global pod_name_list_running
    global pod_name_list_scheduled
    global resource_dic
    global recent_deletions
    global deletion_lock
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):      
        #print(f"this is the pod event {event['type']}", flush=True)
        if event['type'] == 'ADDED':
            if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "custom-scheduler":
                #tenantname =  [x.value for x in event['object'].spec.containers[0].env if x.name == "SPARK_USER_MANUEL"][0]
                #update the worker nodes
                if event['object'].metadata.name not in pod_name_list_running:
                    tenantname = [x.value for x in event['object'].spec.containers[0].env if x.name == "SPARK_USER_MANUEL"][0]
                    #print(f"this is the tenantname: {tenantname}", flush=True)
                    #print(f"this is the pod id to add {pod_id} and the name {event['object'].metadata.name}", flush=True)
                    update_worker(pod_id, tenantname, "Pending")
                    pod_name_list_running.append(event['object'].metadata.name)
                    meta = client.V1ObjectMeta()
                    meta.name = event['object'].metadata.name
                    meta.uid = event['object'].metadata.uid
                    pod_dic[pod_id] = meta
                    pod_id += 1
        elif event['type'] == 'MODIFIED':
            if (event['object'].status.phase == "Succeeded" or event['object'].status.phase == "Failed") and event['object'].spec.scheduler_name == "custom-scheduler":
                if event['object'].metadata.name in pod_name_list_running:
                    for pod_meta in list(pod_dic.values()):
                        if pod_meta.name == event['object'].metadata.name:
                            pod_id_to_remove = list(pod_dic.keys())[list(pod_dic.values()).index(pod_meta)]
                            update_worker(pod_id_to_remove, tenantname, "Succeeded")
                            pod_name_list_running.remove(pod_meta.name)
                            spec_pod = event['object'].spec
                            for element in pod_name_list_scheduled:
                                #element is touple with (pod_name, resource_id)
                                # if the pod was deleted and not scheduled before nothing is done
                                if element[0] == pod_meta.name:
                                    requested_memory = 0
                                    for container in spec_pod.containers:
                                        requested_memory += byte_unit_conversion(container.resources.requests["memory"])
                                    node = resource_dic[int(element[1])]
                                    if event['object'].status.phase == "Failed":
                                        if pod_meta in node.queue: node.queue.remove(pod_meta)
                                        with deletion_lock:
                                            if element[1] not in recent_deletions:
                                                recent_deletions.append(element[1])
                                                print(f"deleting this pod: {pod_meta.name}, and getting this much memory back {requested_memory}", flush=True)
                                                node.memory += requested_memory
                                            break
                                    with thread_lock:
                                        print(f"success this pod: {pod_meta.name}, and getting this much memory back {requested_memory}", flush=True)
                                        node.memory += requested_memory
                                        schedule_from_queue(element[1])
                                    break
                                
                            break
        elif event['type'] == 'DELETED':
            #do nothing already handeled in the modified when the status rolls over to failed
            continue
        else:
            #this event['type'] == 'UNKNOWN':
            print("something went wrong", flush=True)
            print(f"this is the pod event {event['type']}", flush=True)
            print(f"this pod did not work {event['object']}", flush=True)
def schedule_on_node(resource_id, ids):
    #new solution found we clear all queues (from old solution) schedule as many as we can and fill up the queues
    global pod_dic
    global resource_dic
    with thread_lock:
        for resource in resource_dic.values():
            resource.queue.clear()
        for id in ids:
            # print(f"attempting to schedule id {id} from schedule on node", flush=True)
            schedule_from_EA(pod_dic[id], resource_id)


                    
"""Ingress"""

@app.route('/update-solution', methods=['POST'])
def update():
    # print("got the update", flush=True)
    global best_fitness
    update = request.get_json()
    if update[list(update)[0]] < best_fitness:
        return "OK", 200
    else:
        best_fitness = update[list(update)[0]]
    for key, value in update.items():
        if key == "fitness":
            continue
        worker = Thread(target=schedule_on_node, args=[key, value])
        worker.start()
    return "OK", 200


"""Egress"""
        
def init_worker():
    url = f"http://{worker_service}/init"
    global node_id
    global resource_dic
    to_send = {}
    count = 0
    for count, node in enumerate(nodes_available()):
        node_name = node.metadata.name
        node_memory = int(byte_unit_conversion(node.status.allocatable["memory"]) * 0.75)
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
            
        with thread_lock:
            resource_dic[count + node_id] = NodeMeta(id=node_id, name=node_name, memory=node_memory, status="Available")
            to_send[count + node_id] = node_name
    node_id = count + node_id
    response = requests.post(url, json = to_send)
    if response.status_code < 400:
        return response
    else:
        print(f"Request failed with status code {response.status_code}", flush=True)

def update_worker(id, tenant, status):
    global best_fitness
    with thread_lock:
        best_fitness = 0
    url = f"http://{worker_service}/update"
    json_obj = {"id": id, "tenant": tenant, "status": status}
    #print(f"updating daemon with this id {id}", flush=True)
    response = requests.post(url, json = json_obj)
    if response.status_code < 400:
        return response
    else:
        print(f"Request failed with status code {response.status_code}", flush=True)

def node_change(node, operation):
    url = f"http://{worker_service}/node-change"
    json_obj = {"node_id" : node}
    json_obj["operation"] = operation
    response = requests.post(url, json = json_obj)
    if response.status_code < 400:
        return response
    else:
        print(f"Request failed with status code {response.status_code}", flush=True)

"""main """

def main():
    flask_thread = Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': '80'})
    flask_thread.start()
    init_worker()
    node_thread = Thread(target=watch_node_conditions)
    node_thread.start()
    retry_thread = Thread(target=flush_retry_queue_periodically)
    retry_thread.start()
    watch_pod()

if __name__ == '__main__':
    main()