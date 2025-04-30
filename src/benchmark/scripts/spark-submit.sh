#!/bin/bash

exec > /dev/null 2>&1
export PATH=$PATH:$HOME/minio-binaries/
if [ $# -ne 7 ]
then
    echo "the 7 arguments provided that are needed are: <user>, <algorithm>, <number of executors>, <graph>, <size_of_graph>, <scheduler>, <name>" 1>&2
    exit 1
fi
#user input
user=$1
algorithm=$2
n_executor=$3
graph=$4
size_of_graph=$5
scheduler=$6
name=$7
#buckets dont work well with hyphens in the name for some reason so paramter expansion and replacing 
outputpath="graphs/$size_of_graph/$graph"

file="../config-template/graphs/$graph.properties"
if [ ! -f "$file" ]; then
    echo "the provided graph does not have a corresponding properties file perhaps the graph is not supported or misspelled" 1>&2
    exit 1
fi
#only need the right side of the assignment and cut the prefix whitespace
vertex_file=$(grep '\.vertex-file =' "../config-template/graphs/$graph.properties" | cut -d'=' -f2 | xargs)
edge_file=$(grep '\.edge-file =' "../config-template/graphs/$graph.properties" | cut -d '=' -f2 | xargs)
directed=$(grep '\.directed =' "../config-template/graphs/$graph.properties" | cut -d'=' -f2 | xargs)

supported_algorithms=$(grep '\.algorithms =' "../config-template/graphs/$graph.properties" | cut -d'=' -f2 | xargs)

#the regex means it has to have nothing or a comma preceding and nothing or a comma succiding
if ! [[ $supported_algorithms =~ (^|, )"$algorithm"(, |$) ]]
then
    echo "the algorithm provided is not supported for the graph" 1>&2
    exit 1
fi

if ! [[ $size_of_graph =~ ^(test_graphs|size_S|size_M)$ ]]
then
    echo "<size of graph> has to be one of test_graphs, size_S, size_M" 1>&2
    exit 1
fi

case $scheduler in

    default)
        scheduler_conf=()
        ;;
    custom-scheduler)
        scheduler_conf=(
            "--conf spark.kubernetes.scheduler.name=custom-scheduler"
        )
        ;;
    *)
        echo "provided <scheduler> is not one of: default, custom-scheduler" 1>&2
        exit 1
        ;;
esac

#could be multiple
params=$(awk -F"=" "/# Parameters for ${algorithm^^}/ {y=1;next} y && /# Parameters/ {y=0}  y {gsub(/^[ \t]+/, \"\", \$2); print \$2 ;}" ../config-template/graphs/$graph.properties)

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

mc --insecure rm  myminio/mybucket/${outputpath}/${name}/output/ --recursive --force

./spark-3.5.0-bin-hadoop3/bin/spark-submit \
    --master k8s://localhost:6443 \
    --deploy-mode cluster \
    --class $class \
    --name ${name} \
    --conf spark.kubernetes.executor.podNamePrefix=${name} \
    --conf spark.kubernetes.namespace=spark-namespace \
    --conf spark.executor.instances="$n_executor" \
    --conf spark.executorEnv.SPARK_USER_MANUEL="$user" \
    --conf spark.kubernetes.driverEnv.SPARK_USER_MANUEL="$user" \
    --conf spark.kubernetes.executor.deleteOnTermination=false \
    ${scheduler_conf[@]} \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
    --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
    s3a://mybucket/${outputpath}/${vertex_file} \
    s3a://mybucket/${outputpath}/${edge_file} \
    "$directed" \
    s3a://mybucket/${outputpath}/${name}/output \
    "${spark_args[@]}"