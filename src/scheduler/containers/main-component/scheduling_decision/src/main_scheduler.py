from kubernetes import client, config, watch
import time
import json
import requests
import concurrent.futures
from flask import Flask, request

config.load_incluster_config()

# this is basically how this works:
# look at all the nodes
# keep watch for all the pods
# match them to the right node
#in part inspired by https://sebgoa.medium.com/kubernetes-scheduling-in-python-3588f4928b13


v1=client.CoreV1Api()

scheduler_name = "EA"
app = Flask(__name__)

# @app.route('/', methods=['POST'])
# def result():
#     print(request.form['foo']) # should display 'bar'
#     return 'Received !' # response to your request

# def nodes_available():
#     ready_nodes = []
#     for n in v1.list_node().items:
#             for status in n.status.conditions:
#                 if status.status == "True" and status.type == "Ready":
#                     ready_nodes.append(n.metadata.name)
#     return ready_nodes

# def scheduler(name, node, namespace="default"):
#     body=client.V1Binding()
        
#     target=client.V1ObjectReference()
#     target.kind="Node"
#     target.apiVersion="v1"
#     target.name= node
    
#     meta=client.V1ObjectMeta()
#     meta.name=name
    
#     body.target=target
#     body.metadata=meta
    
#     return v1.create_namespaced_binding(name, namespace, body)

def watch_pod():
    w = watch.Watch()
    # still in the default namespace
    for event in w.stream(v1.list_namespaced_pod, "default"):
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == scheduler_name:
            # do something

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
    watch_pod()
                    
if __name__ == '__main__':
    main()
