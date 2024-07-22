from kubernetes import client, config, watch
import requests
import os
from flask import Flask, request
from threading import Thread
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

"""Ingress"""
@app.route('/test', methods=['POST'])
def test():
    print("test")


"""Logic"""
def nodes_available():
    ready_nodes = []
    for n in v1.list_node().items:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready":
                    ready_nodes.append(n.metadata.name)
    return ready_nodes

#metadata is the V1objectmeta of the pod https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ObjectMeta.md
#node is the name of the node n.metadata.name

def schedule(meta, node, namespace="spark-namespace"):
        
    target=client.V1ObjectReference()
    target.kind="Node"
    target.apiVersion="v1"
    target.name= node

    body=client.V1Binding(target = target, metadata = meta)

    #there is an issue with the kuebrentes api package it does not matter much the pod will be shceduled correctly so we just ignore it
    # see https://github.com/kubernetes-client/python/issues/825
    try:
        v1.create_namespaced_binding(namespace = namespace, body = body)
        return True
    except:
        return True

def watch_pod():
#every new spark task comes with multiple pods they come in at the same time so we buffer for time
    w = watch.Watch()
    global pod_id
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "custom-scheduler":
            tenantname =  [x.value for x in event['object'].spec.containers[0].env if x.name == "SPARK_USER"][0]
            #update the worker nodes
            update_worker(pod_id, tenantname, "Pending")
            schedule(event['object'].metadata, nodes_available()[0])

# only need this if the number of nodes changes
# def watch_node():
#     w = watch.Watch()
#     # still in the default namespace
#     for event in w.stream(v1.list_namespaced_pod, "default"):
#         if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == scheduler_name:
#             # do something

                    


"""Egress"""
#check status of server
def test_worker():
    url = f"http://{worker_service}/test"
    print(f"this is the url that it is pinging {url}")
    response = requests.get(url)
    if response.status_code < 400:
        print(f"Request successful with status code: {response.status_code}")
        print(response.text)
        return response
    else:
        print("server not ready")
        sleep(2)
        test_worker()
        
def init_worker():
    url = f"http://{worker_service}/init"
    print(f"this is the url that it is pinging {url}")
    json_obj = {}
    global node_id
    for count, node in enumerate(nodes_available()):
        json_obj[count + node_id] = node
    node_id = count + node_id
    response = requests.post(url, json = json_obj)
    if response.status_code < 400:
        print(f"Request successful with status code: {response.status_code}")
        print(response.text)
        return response
    else:
        print(f"Request failed with status code {response.status_code}")
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

"""main """

def main():
    #only need this if the number fo 
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     n = executor.submit(watch_node)
    #     p = executor.submit(watch_pod)
    flask_thread = Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': '80'})
    flask_thread.start()
    test_worker()
    init_worker()
    watch_pod()

if __name__ == '__main__':
    main()