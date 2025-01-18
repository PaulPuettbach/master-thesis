#!/bin/bash
export PATH=$PATH:$HOME/minio-binaries/
cd ../../scheduler/containers/util

./load-repo.sh

cd ../../

helm install scheduler main-helm-chart/ --wait
cd ../spark

kubectl apply -f service.account.yaml
cd ../benchmark/scripts