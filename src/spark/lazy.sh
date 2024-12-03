#!/bin/bash

# #this does not work
# #--conf spark.executorEnv.SPARK_USER="Fred" \
# # cd ../scheduler
export PATH=$PATH:$HOME/minio-binaries/
if [ $# -ne 4 ]
then
    echo "the 4 arguments provided that are needed are: <user>, <algorithm>, <number of executors>, <graph>" 1>&2
    exit 1
fi
#user input
user=$1
algorithm=$2
n_executor=$3
graph=$4

#buckets dont work well with hyphens in the name for some reason so paramter expansion and replacing 
outputpath="${graph//-/_}"
file="../config-template/graphs/$graph.properties"
vertex_file=$(grep '\.vertex-file =' "../benchmark/config-template/graphs/$graph.properties" | cut -d'=' -f2 | xargs)
edge_file=$(grep '\.edge-file =' "../benchmark/config-template/graphs/$graph.properties" | cut -d '=' -f2 | xargs)
directed=$(grep '\.directed =' "../benchmark/config-template/graphs/$graph.properties" | cut -d'=' -f2 | xargs)

#params=$(awk -F"=" '/# Parameters/ {y=1;next} y {gsub(/^[ \t]+/, "", $2); if (a) print a" \\"; a=$2;} END {gsub(/^[ \t]+/, "", $2); print $2}' ../benchmark/config-template/graphs/$graph.properties)
params=$(awk -F"=" '/# Parameters/ {y=1;next} y {gsub(/^[ \t]+/, "", $2); print $2 ;}' ../benchmark/config-template/graphs/$graph.properties)
for param in $params; do
    spark_args+=("$param")
done


case $algorithm in

    bfs)
        class=science.atlarge.graphalytics.graphx.bfs.BreadthFirstSearchJob
        ;;
    cdlp)
        class=science.atlarge.graphalytics.graphx.cdlp.CommunityDetectionLPJob
        ;;
    lcc)
        class=science.atlarge.graphalytics.graphx.lcc.LocalClusteringCoefficientJob
        ;;
    pr)
        class=science.atlarge.graphalytics.graphx.pr.PageRankJob
        ;;
    sssp)
        class=science.atlarge.graphalytics.graphx.sssp.SingleSourceShortestPathJob
        ;;
    wcc)
        class=science.atlarge.graphalytics.graphx.wcc.WeaklyConnectedComponentsJo
        ;;
    *)
        echo "provided algorithm is not one of: <bfs>, <cdlp>, <lcc>, <pr>, <sssp>, <wcc>" 1>&2
        exit 1
        ;;
esac


./cleanup.sh

cd ../scheduler/containers/util

# ./load-repo.sh

cd ../../

helm install scheduler main-helm-chart/ --wait
cd ../spark

kubectl apply -f service.account.yaml

echo "-----------------------------------------------------------"

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class $class \
    --name $algorithm \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances=$n_executor \
    --conf spark.executorEnv.SPARK_USER=$user \
    --conf spark.kubernetes.scheduler.name=custom-scheduler \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
    --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
    s3a://mybucket/$outputpath/${vertex_file} \
    s3a://mybucket/$outputpath/${edge_file} \
    $directed \
    s3a://mybucket/$outputpath/output \
    "${spark_args[@]}"


echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep '^scheduler' | grep -v 'scheduler-daemon') -n kube-system #-c  init-daemon-service
echo "-----------------------------------------------------------"
kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'scheduler-daemon') -n kube-system #-c init-main-service

# mc cp --recursive --insecure myminio/mybucket/test-bfs/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-bfs/output/
# mc cp --recursive --insecure myminio/mybucket/test-cdlp/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-cdlp/output/
# mc cp --recursive --insecure myminio/mybucket/test-lcc/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-lcc/output/
mc cp --recursive --insecure myminio/mybucket/test_pr_directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-pr/output/
# mc cp --recursive --insecure myminio/mybucket/test-sssp/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-sssp/output/
# mc cp --recursive --insecure myminio/mybucket/test-wcc/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-wcc/output/

cd ../benchmark/results/

# cd test-bfs/
# ./coalasce.sh
# cd ..

# cd test-cdlp/
# ./coalasce.sh
# cd ..

# cd test-lcc/
# ./coalasce.sh
# cd ..

cd test-pr/
./coalasce.sh
# cd ..

# cd test-sssp/
# ./coalasce.sh
# cd ..

# cd test-wcc/
# ./coalasce.sh

# echo "-----------------------------------------------------------"
# kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n spark-namespace ) -n spark-namespace