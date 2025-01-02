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
#only need the right side of the assignment and cut the prefix whitespace
vertex_file=$(grep '\.vertex-file =' "../config-template/graphs/$graph.properties" | cut -d'=' -f2 | xargs)
edge_file=$(grep '\.edge-file =' "../config-template/graphs/$graph.properties" | cut -d '=' -f2 | xargs)
directed=$(grep '\.directed =' "../config-template/graphs/$graph.properties" | cut -d'=' -f2 | xargs)

#could be multiple
params=$(awk -F"=" '/# Parameters/ {y=1;next} y {gsub(/^[ \t]+/, "", $2); print $2 ;}' ../config-template/graphs/$graph.properties)
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
        class=science.atlarge.graphalytics.graphx.wcc.WeaklyConnectedComponentsJob
        ;;
    *)
        echo "provided algorithm is not one of: <bfs>, <cdlp>, <lcc>, <pr>, <sssp>, <wcc>" 1>&2
        exit 1
        ;;
esac

cd ../../spark

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class $class \
    --name $algorithm \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances=$n_executor \
    --conf spark.executorEnv.SPARK_USER_MANUEL=$user \
    --conf spark.kubernetes.driverEnv.SPARK_USER_MANUEL=$user \
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