#!/bin/bash
# #this is a section for debuging to be taking out at a later
# echo "-----------------------------------------------------------"
# kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n spark-namespace ) -n spark-namespace
echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep '^scheduler' | grep -v 'scheduler-daemon') -n kube-system #-c  init-daemon-service
echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'scheduler-daemon') -n kube-system #-c init-main-service

kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'random-scheduler') -n kube-system 

kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n spark-namespace | grep 'driver') -n spark-namespace

puttbach:x:1062:1062::/home/puttbach/:/bin/bash 
cloud_controller_puttbach:x:1001:1001::/home/cloud_controller_puttbach:/bin/bash