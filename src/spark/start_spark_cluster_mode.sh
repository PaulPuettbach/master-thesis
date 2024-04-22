#!/bin/bash
#./spark-3.5.0-bin-hadoop3/bin/docker-image-tool.sh -r docker.io/paulpuettbach/spark_image -t latest -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
#--conf spark.kubernetes.scheduler.name=spark-scheduler

./spark-3.5.0-bin-hadoop3/bin/docker-image-tool.sh -r docker.io/paulpuettbach/spark_image -t test build
./spark-3.5.0-bin-hadoop3/bin/docker-image-tool.sh -r docker.io/paulpuettbach/spark_image -t test push

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class org.apache.spark.examples.SparkPi \
    --name shortest-path \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=docker.io/paulpuettbach/spark_image/spark:test \
    local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode client \
    --name shortest-path \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=docker.io/paulpuettbach/spark_image/spark:test \
    ../benchmark/algorithms/test.py


# kubectl get pods -n spark-namespace
# kubectl logs -n spark-namespace
kubectl describe pod -n spark-namespace

kubectl get pods -n spark-namespace -o yaml

kubectl get pods -n spark-namespace