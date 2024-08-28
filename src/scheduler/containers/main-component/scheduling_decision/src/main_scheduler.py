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
            node_change({node_id: node_name}, "add")

        if not eligable and node_name in resource_dic.values():
            with thread_lock:
                resource_dic = {key:val for key, val in resource_dic.items() if val != node_name}
            node_change({node_id: node_name}, "delete")

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
    print("enter schedule", flush=True)
        
    target=client.V1ObjectReference()
    target.kind="Node"
    target.apiVersion="v1"
    target.name= node

    body=client.V1Binding(target = target, metadata = meta)

    #there is an issue with the kuebrentes api package it does not matter much the pod will be shceduled correctly so we just ignore it
    # see https://github.com/kubernetes-client/python/issues/825
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
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):
        tenantname =  [x.value for x in event['object'].spec.containers[0].env if x.name == "SPARK_USER"][0]
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "custom-scheduler":
            #update the worker nodes
            update_worker(pod_id, tenantname, "Pending")
            pod_dic[pod_id] = event['object'].metadata
            pod_id += 1
        if event['object'].status.phase == "Succeeded" and event['object'].spec.scheduler_name == "custom-scheduler":
            print(f"got a pod with status succeded ")
            pod_id = list(pod_dic.keys())[list(pod_dic.values()).index(event['object'].metadata)]
            update_worker(pod_id, tenantname, "Succeeded")

def schedule_on_node(resource_id, ids):
    global resource_dic
    global pod_dic
    print("enter schedule on node", flush=True)
    for id in ids:
        with thread_lock:
            schedule(pod_dic[id], resource_dic[int(resource_id)])


                    
"""Ingress"""
@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200

@app.route('/update-solution', methods=['POST'])
def update():
    global best_fitness
    update = request.get_json()
    print(f"update = {update}", flush = True)
    if update[list(update)[0]] < best_fitness:
        return
    else:
        best_fitness = update[list(update)[0]]
    for key, value in update.items():
        if key == "fitness":
            continue
        worker = Thread(target=schedule_on_node, args=[key, value])
        worker.start()
    return "OK", 200


"""Egress"""
#check status of server
# def test_worker():
#     url = f"http://{worker_service}/test"
#     print(f"this is the url that it is pinging {url}")
#     response = requests.get(url)
#     if response.status_code < 400:
#         print(f"Request successful with status code: {response.status_code}")
#         print(response.text)
#         return response
#     else:
#         print(f"Request unsuccessful with status code: {response.status_code}")
        
def init_worker():
    url = f"http://{worker_service}/init"
    print(f"this is the url that it is pinging {url}")
    global node_id
    global resource_dic
    count = 0
    for count, node in enumerate(nodes_available()):
        with thread_lock:
            resource_dic[count + node_id] = node
    print(f"this is the resource dic in the init {resource_dic}", flush=True)
    node_id = count + node_id
    response = requests.post(url, json = resource_dic)
    if response.status_code < 400:
        print(f"Request successful with status code: {response.status_code}")
        print(response.text)
        return response
    else:
        print(f"Request failed with status code {response.status_code}")

def update_worker(id, tenant, status):
    url = f"http://{worker_service}/update"
    json_obj = {"id": id, "tenant": tenant, "status": status}
    response = requests.post(url, json = json_obj)
    if response.status_code < 400:
        print("Request successful")
        print(response.text)
        return response
    else:
        print(f"Request failed with status code {response.status_code}")

def node_change(node, operation):
    url = f"http://{worker_service}/node-change"
    json_obj = node
    json_obj += {"operation": operation}
    response = requests.post(url, json = json_obj)
    if response.status_code < 400:
        print("Request successful")
        print(response.text)
        return response
    else:
        print(f"Request failed with status code {response.status_code}")

"""main """

def main():
    #only need this if the number fo 
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     n = executor.submit(watch_node)
    #     p = executor.submit(watch_pod)
    flask_thread = Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': '80'})
    flask_thread.start()
    node_thread = Thread(target=watch_node_conditions)
    node_thread.start()
    init_worker()
    watch_pod()

if __name__ == '__main__':
    main()