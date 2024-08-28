from kubernetes import client, config, watch
import random

config.load_incluster_config()

# this is basically how this works:
# look at all the nodes
# keep watch for all the pods
# match them to the right node
#in part inspired by https://sebgoa.medium.com/kubernetes-scheduling-in-python-3588f4928b13

"""Init"""
v1=client.CoreV1Api()




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
        v1.create_namespaced_binding(namespace = namespace, body = body, _preload_content=False)
        return True
    except:
        return True

def watch_pod():
#every new spark task comes with multiple pods they come in at the same time so we buffer for time
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "random-scheduler":
            schedule(event['object'].metadata, random.choice(nodes_available()))

def main():
    watch_pod()

if __name__ == '__main__':
    main()