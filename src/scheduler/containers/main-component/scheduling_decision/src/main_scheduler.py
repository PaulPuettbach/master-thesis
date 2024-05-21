from kubernetes import client, config, watch
import requests
import os
from flask import Flask, request
from threading import Thread

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
taks_dic = []
id_counter = 0

"""Ingress"""
@app.route('/', methods=['POST'])
def result():
    print(request.form['foo']) # should display 'bar'
    return 'Received !' # response to your request


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

def schedule(metadata, node, namespace="spark-scheduler"):
    body=client.V1Binding()
        
    target=client.V1ObjectReference()
    target.kind="Node"
    target.apiVersion="v1"
    target.name= node
    
    meta=metadata
    
    body.target=target
    body.metadata=meta
    
    return v1.create_namespaced_binding(namespace, body)

def watch_pod():
    #event['object']  is going to be the pod
    #tenantname =  [x for x in event['object'].spec.containers[0].env if x["name"] == "SPARK_USER"]
    
    #------------------- important ----------------------
    #event['object'].metadata is all that is needed
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "spark-scheduler":
            #update the worker nodes
              #update_worker()
            print(event['object'].metadata.name)

# only need this if the number of nodes changes
# def watch_node():
#     w = watch.Watch()
#     # still in the default namespace
#     for event in w.stream(v1.list_namespaced_pod, "default"):
#         if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == scheduler_name:
#             # do something

def main():
    #only need this if the number fo 
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     n = executor.submit(watch_node)
    #     p = executor.submit(watch_pod)
    flask_thread = Thread(target=app.run, kwargs={'host': '0.0.0.0'})
    flask_thread.start()
    print("get to the main")
    print(nodes_available(), flush=True)
    print("should have printed the nodes", flush=True)
    watch_pod()
                    

if __name__ == '__main__':
    main()

"""Egress"""

# def update_worker():
#     response = requests.get(f"http://{worker_service}:{worker_service_port}/path")
#     if response.status_code == 200:
#         print("Request successful")
#         print(response.text)
#         return response
#     else:
#         print(f"Request failed with status code {response.status_code}")