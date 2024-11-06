#!/bin/bash
./cleanup.sh
#this does not work
#--conf spark.executorEnv.SPARK_USER="Fred" \
# cd ../scheduler

cd ../scheduler/containers/util

./load-repo.sh

cd ../../

helm install scheduler main-helm-chart/ --wait
cd ../spark

kubectl apply -f service.account.yaml

echo "-----------------------------------------------------------"

# ./spark-3.5.0-bin-hadoop3/bin/spark-submit \
#     --master k8s://localhost:6443 \
#     --deploy-mode cluster \
#     --class ShortestPath \
#     --name shortest-path \
#     --conf spark.kubernetes.namespace=spark-namespace \
#     --conf spark.executor.instances=5 \
#     --conf spark.kubernetes.scheduler.name=custom-scheduler \
#     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#     --conf spark.kubernetes.container.image=docker.io/paulpuettbach/spark_image/spark:test \
    
    
#     #local:///opt/spark/benchmark/my-spark-project-1.0.jar

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class ShortestPath \
    --name shortest-path \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances=3 \
    --conf spark.kubernetes.scheduler.name=custom-scheduler \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
    --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    s3a://mybucket/mySparkProject1.0.jar

echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep '^scheduler' | grep -v 'scheduler-daemon') -n kube-system #-c  init-daemon-service
echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'scheduler-daemon') -n kube-system #-c init-main-service