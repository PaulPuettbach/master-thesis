#!/bin/bash
kubectl create -f spark-master-controller.yaml
kubectl create -f spark-master-service.yaml
kubectl create -f spark-ui-proxy-controller.yaml
kubectl create -f spark-ui-proxy-service.yaml
kubectl create -f spark-worker-controller.yaml
kubectl create -f zeppelin-controller.yaml
kubectl create -f zeppelin-service.yaml
# kubectl get --no-headers -o name pods -l component=zeppelin |  sed "s/^.\{4\}//"
# this is the command for the localhost zeppelin hosting
# kubectl port-forward $(kubectl get --no-headers -o name pods -l component=zeppelin |  sed "s/^.\{4\}//") 8080:8080

# kubectl exec $(kubectl get --no-headers -o name pods -l component=zeppelin |  sed "s/^.\{4\}//") -it pyspark