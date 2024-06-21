#!/bin/bash
helm uninstall scheduler
./cleanup.sh

cd ../scheduler/containers/util

./load-repo.sh

cd ../../
helm install scheduler main-helm-chart/
cd ../spark

kubectl apply -f service.account.yaml

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class ShortestPath \
    --name shortest-path \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.scheduler.name=custom-scheduler \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=docker.io/paulpuettbach/spark_image/spark:test \
    local:///opt/spark/benchmark/my-spark-project-1.0.jar


kubectl get pods -n kube-system