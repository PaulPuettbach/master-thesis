#!/bin/bash
./cleanup.sh
#this does not work
#--conf spark.executorEnv.SPARK_USER="Fred" \


kubectl apply -f service.account.yaml

echo "-----------------------------------------------------------"

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class org.apache.spark.examples.SparkPi \
    --name shortest-path \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances=3 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=docker.io/paulpuettbach/spark_image/spark:test \
    local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar
