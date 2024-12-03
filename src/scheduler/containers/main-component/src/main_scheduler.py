from kubernetes import client, config, watch
import requests
import os
from flask import Flask, request
from threading import Thread, RLock
from time import sleep

config.load_incluster_config()

# this is basically how this works:
# look at all the nodes
# keep watch for all the pods
# match them to the right node
#in part inspired by https://sebgoa.medium.com/kubernetes-scheduling-in-python-3588f4928b13

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

def watch_node_conditions():
    global node_id
    global resource_dic
    w = watch.Watch()

    for event in w.stream(v1.list_node):
        node = event['object']
        event_type = event['type']
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
        if eligable and node_name not in resource_dic.values():
            with thread_lock:
                node_id +=1
                resource_dic[node_id] = node_name
            node_change(node_id, "add")

        if not eligable and node_name in resource_dic.values():
            with thread_lock:
                resource_dic = {key:val for key, val in resource_dic.items() if val != node_name}
            node_change(node_id, "delete")

"""Logic"""
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
            ready_nodes.append(n.metadata.name)
    return ready_nodes

#metadata is the V1objectmeta of the pod https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ObjectMeta.md
#node is the name of the node n.metadata.name

def schedule(meta, node, namespace="spark-namespace"):
    global pod_name_list_scheduled
    if not node:
        print("no usuable nodes", flush=True)
        raise Exception("cannot schedule no available nodes") 
    
    if meta.name not in pod_name_list_scheduled:
        #print(f"this is the name of the pod being scheduled {meta.name}", flush=True)
        
        target=client.V1ObjectReference()
        target.kind="Node"
        target.apiVersion="v1"
        target.name= node

        body=client.V1Binding(target = target, metadata = meta)

        #there is an issue with the kuebrentes api package it does not matter much the pod will be shceduled correctly so we just ignore it
        # see https://github.com/kubernetes-client/python/issues/825
        pod_name_list_scheduled.append(meta.name)
        try:
            v1.create_namespaced_binding(namespace = namespace, body = body, _preload_content=False)
            return True
        except:
            return True

def watch_pod():
#every new spark task comes with multiple pods they come in at the same time so we buffer for time
    w = watch.Watch()
    global pod_id
    global pod_dic
    global pod_name_list_running
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):
        tenantname =  [x.value for x in event['object'].spec.containers[0].env if x.name == "SPARK_USER"][0]
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "custom-scheduler":
            #update the worker nodes
            if event['object'].metadata.name not in pod_name_list_running:
                # print(f"this is the podname: {event['object'].metadata.name}", flush=True)
                # print(f"this is the pod id {pod_id}", flush=True)
                update_worker(pod_id, tenantname, "Pending")
                pod_name_list_running.append(event['object'].metadata.name)
                meta = client.V1ObjectMeta()
                meta.name = event['object'].metadata.name
                meta.uid = event['object'].metadata.uid
                pod_dic[pod_id] = meta
                pod_id += 1
        if (event['object'].status.phase == "Succeeded" or event['object'].status.phase == "Failed") and event['object'].spec.scheduler_name == "custom-scheduler":
            if event['object'].metadata.name in pod_name_list_running:
                for pod_meta in list(pod_dic.values()):
                    if pod_meta.name == event['object'].metadata.name:
                        pod_id_to_remove = list(pod_dic.keys())[list(pod_dic.values()).index(pod_meta)]
                        update_worker(pod_id_to_remove, tenantname, "Succeeded")
                        pod_name_list_running.remove(pod_meta.name)
                        break

def schedule_on_node(resource_id, ids):
    global resource_dic
    global pod_dic
    for id in ids:
        # print(f"attempting to schedule id {id}", flush=True)
        with thread_lock:
            schedule(pod_dic[id], resource_dic[int(resource_id)])


                    
"""Ingress"""

@app.route('/update-solution', methods=['POST'])
def update():
    global best_fitness
    update = request.get_json()
    if update[list(update)[0]] < best_fitness:
        return
    else:
        best_fitness = update[list(update)[0]]
    threads = []
    for key, value in update.items():
        if key == "fitness":
            continue
        worker = Thread(target=schedule_on_node, args=[key, value])
        worker.start()
    for thread in threads:
        thread.join()
    return "OK", 200


"""Egress"""
        
def init_worker():
    url = f"http://{worker_service}/init"
    global node_id
    global resource_dic
    count = 0
    for count, node in enumerate(nodes_available()):
        with thread_lock:
            resource_dic[count + node_id] = node
    node_id = count + node_id
    response = requests.post(url, json = resource_dic)
    if response.status_code < 400:
        return response
    else:
        print(f"Request failed with status code {response.status_code}", flush=True)

def update_worker(id, tenant, status):
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
    watch_pod()

if __name__ == '__main__':
    main()