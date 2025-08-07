#!/bin/bash
# #this is a section for debuging to be taking out at a later
# echo "-----------------------------------------------------------"
# kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n spark-namespace ) -n spark-namespace
echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep '^scheduler' | grep -v 'scheduler-daemon') -n kube-system #-c  init-daemon-service
echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'scheduler-daemon') -n kube-system #-c init-main-service

kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'random-scheduler') -n kube-system 

