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

"""global"""
pod_name_array = []


"for util"
# def compare_json(dic, dic2):
#     diff = {}
#     for key in dic:
#         if key not in dic2:
#             diff[key] = dic[key]
#         elif dic[key] != dic2[key]:
#             if isinstance(dic[key], dict) and isinstance(dic2[key], dict):
#                 diff.update(compare_json(dic[key], dic2[key]))
#             else:
#                 diff[key] = dic[key]


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

    if not node:
        print("no usuable nodes", flush=True)
        raise Exception("cannot schedule no available nodes") 

    print(f"this is called this many times and this is the meta", flush=True)
        
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
    global pod_name_array
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "spark-namespace"):
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == "random-scheduler":
            # print(f"this is the old name {temp.name}", flush=True)
            # if temp.name != "False":
            #     print(f"this is the diff {compare_json(dict(event['object'].metadata),dict(temp))}", flush=True)
            # temp = event['object'].metadata
            # print(f"this is the new name {temp.name}", flush=True)
            if event['object'].metadata.name not in pod_name_array:
                pod_name_array.append(event['object'].metadata.name)
                meta = client.V1ObjectMeta()
                meta.name = event['object'].metadata.name
                meta.uid = event['object'].metadata.uid
                schedule(meta, random.choice(nodes_available()))

def main():
    watch_pod()

if __name__ == '__main__':
    main()