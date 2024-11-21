#!/bin/bash
./cleanup.sh
# #this does not work
# #--conf spark.executorEnv.SPARK_USER="Fred" \
# # cd ../scheduler

cd ../scheduler/containers/util

./load-repo.sh

cd ../../

helm install scheduler main-helm-chart/ --wait
cd ../spark

kubectl apply -f service.account.yaml

echo "-----------------------------------------------------------"

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class science.atlarge.graphalytics.graphx.bfs.BreadthFirstSearchJob \
    --name bfs \
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
    s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
    s3a://mybucket/test-bfs/test-bfs-directed.v\
    s3a://mybucket/test-bfs/test-bfs-directed.e \
    true \
    s3a://mybucket/test-bfs/output \
    1

# ./spark-3.5.0-bin-hadoop3/bin/spark-submit \
#     --master k8s://localhost:6443 \
#     --deploy-mode cluster \
#     --class science.atlarge.graphalytics.graphx.cdlp.CommunityDetectionLPJob \
#     --name cdlp \
#     --conf spark.kubernetes.namespace=spark-namespace \
#     --conf spark.executor.instances=3 \
#     --conf spark.kubernetes.scheduler.name=custom-scheduler \
#     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#     --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
#     --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
#     --conf spark.hadoop.fs.s3a.access.key=minio \
#     --conf spark.hadoop.fs.s3a.secret.key=minio123 \
#     --conf spark.hadoop.fs.s3a.path.style.access=true \
#     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#     s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
#     s3a://mybucket/test-cdlp/test-cdlp-directed.v\
#     s3a://mybucket/test-cdlp/test-cdlp-directed.e \
#     true \
#     s3a://mybucket/test-cdlp/output \
#     5

# ./spark-3.5.0-bin-hadoop3/bin/spark-submit \
#     --master k8s://localhost:6443 \
#     --deploy-mode cluster \
#     --class science.atlarge.graphalytics.graphx.lcc.LocalClusteringCoefficientJob \
#     --name lcc \
#     --conf spark.kubernetes.namespace=spark-namespace \
#     --conf spark.executor.instances=3 \
#     --conf spark.kubernetes.scheduler.name=custom-scheduler \
#     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#     --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
#     --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
#     --conf spark.hadoop.fs.s3a.access.key=minio \
#     --conf spark.hadoop.fs.s3a.secret.key=minio123 \
#     --conf spark.hadoop.fs.s3a.path.style.access=true \
#     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#     s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
#     s3a://mybucket/test-lcc/test-lcc-directed.v\
#     s3a://mybucket/test-lcc/test-lcc-directed.e \
#     true \
#     s3a://mybucket/test-lcc/output

# ./spark-3.5.0-bin-hadoop3/bin/spark-submit \
#     --master k8s://localhost:6443 \
#     --deploy-mode cluster \
#     --class science.atlarge.graphalytics.graphx.pr.PageRankJob \
#     --name pr \
#     --conf spark.kubernetes.namespace=spark-namespace \
#     --conf spark.executor.instances=3 \
#     --conf spark.kubernetes.scheduler.name=custom-scheduler \
#     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#     --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
#     --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
#     --conf spark.hadoop.fs.s3a.access.key=minio \
#     --conf spark.hadoop.fs.s3a.secret.key=minio123 \
#     --conf spark.hadoop.fs.s3a.path.style.access=true \
#     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#     s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
#     s3a://mybucket/test-pr/test-pr-directed.v\
#     s3a://mybucket/test-pr/test-pr-directed.e \
#     true \
#     s3a://mybucket/test-pr/output \
#     0.85 \
#     14

# ./spark-3.5.0-bin-hadoop3/bin/spark-submit \
#     --master k8s://localhost:6443 \
#     --deploy-mode cluster \
#     --class science.atlarge.graphalytics.graphx.sssp.SingleSourceShortestPathJob \
#     --name sssp \
#     --conf spark.kubernetes.namespace=spark-namespace \
#     --conf spark.executor.instances=3 \
#     --conf spark.kubernetes.scheduler.name=custom-scheduler \
#     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#     --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
#     --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
#     --conf spark.hadoop.fs.s3a.access.key=minio \
#     --conf spark.hadoop.fs.s3a.secret.key=minio123 \
#     --conf spark.hadoop.fs.s3a.path.style.access=true \
#     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#     s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
#     s3a://mybucket/test-sssp/test-sssp-directed.v\
#     s3a://mybucket/test-sssp/test-sssp-directed.e \
#     true \
#     s3a://mybucket/test-sssp/output \
#     weight \
#     1

# ./spark-3.5.0-bin-hadoop3/bin/spark-submit \
#     --master k8s://localhost:6443 \
#     --deploy-mode cluster \
#     --class science.atlarge.graphalytics.graphx.wcc.WeaklyConnectedComponentsJob \
#     --name wcc \
#     --conf spark.kubernetes.namespace=spark-namespace \
#     --conf spark.executor.instances=3 \
#     --conf spark.kubernetes.scheduler.name=custom-scheduler \
#     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#     --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
#     --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
#     --conf spark.hadoop.fs.s3a.access.key=minio \
#     --conf spark.hadoop.fs.s3a.secret.key=minio123 \
#     --conf spark.hadoop.fs.s3a.path.style.access=true \
#     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#     s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
#     s3a://mybucket/test-wcc/test-wcc-directed.v\
#     s3a://mybucket/test-wcc/test-wcc-directed.e \
#     true \
#     s3a://mybucket/test-wcc/output

echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep '^scheduler' | grep -v 'scheduler-daemon') -n kube-system #-c  init-daemon-service
echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'scheduler-daemon') -n kube-system #-c init-main-service

mc cp --recursive --insecure myminio/mybucket/test-bfs/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-bfs/output/
mc cp --recursive --insecure myminio/mybucket/test-cdlp/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-cdlp/output/
mc cp --recursive --insecure myminio/mybucket/test-lcc/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-lcc/output/
mc cp --recursive --insecure myminio/mybucket/test-pr/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-pr/output/
mc cp --recursive --insecure myminio/mybucket/test-sssp/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-sssp/output/
mc cp --recursive --insecure myminio/mybucket/test-wcc/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-wcc/output/

cd ../benchmark/results/

cd test-bfs/
./coalasce.sh
cd ..

cd test-cdlp/
./coalasce.sh
cd ..

cd test-lcc/
./coalasce.sh
cd ..

cd test-pr/
./coalasce.sh
cd ..

cd test-sssp/
./coalasce.sh
cd ..

cd test-wcc/
./coalasce.sh
cd ..