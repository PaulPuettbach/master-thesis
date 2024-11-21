#!/bin/bash

declare -A params_array
# the vairables
user=$1
algorithm=$2
n_executor=$3
graph=$4

file="../config-template/graphs/$graph.properties"
vertex_file=$(grep '\.vertex-file =' "../config-template/graphs/$graph.properties" | cut -d'=' -f2)
edge_file=$(grep '\.edge-file =' "../config-template/graphs/$graph.properties" | cut -d '=' -f2)
directed=$(grep '\.directed =' "../config-template/graphs/$graph.properties" | cut -d'=' -f2)
echo "testing awk"
#(awk -F"=" '/# Parameters/ {y=1;next} y {gsub(/^[ \t]+/, "", $2); print $2, " \\"}' ../config-template/graphs/$graph.properties)

params=$(awk -F"=" '/# Parameters/ {y=1;next} y {gsub(/^[ \t]+/, "", $2); print $2}' ../config-template/graphs/$graph.properties)

# Remove the last two characters

echo "-----------------------------------------------------------"

echo "user"
echo $user
echo "-----------------------------------------------------------"
echo "algorithm"
echo $algorithm
echo "-----------------------------------------------------------"
echo "n_executors"
echo $n_executor
echo "-----------------------------------------------------------"
echo "graph"
echo $graph
echo "-----------------------------------------------------------"
echo "vertex_file"
echo $vertex_file
echo "-----------------------------------------------------------"
echo "edge_file"
echo $edge_file
echo "-----------------------------------------------------------"
echo "directed"
echo $directed
echo "-----------------------------------------------------------"


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

echo "-----------------------------------------------------------"
echo "class"
echo $class

# ./spark-3.5.0-bin-hadoop3/bin/spark-submit \
#     --master k8s://localhost:6443 \
#     --deploy-mode cluster \
#     --class $class \
#     --name $algorithm \
#     --conf spark.kubernetes.namespace=spark-namespace \
#     --conf spark.executor.instances=$n_executor \
#     --conf spark.executorEnv.SPARK_USER=$user \
#     --conf spark.kubernetes.scheduler.name=custom-scheduler \
#     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#     --conf spark.kubernetes.container.image=paulpuettbach/spark_image/spark:test \
#     --conf spark.hadoop.fs.s3a.endpoint=http://myminio-hl.minio-tenant.svc.cluster.local:9000 \
#     --conf spark.hadoop.fs.s3a.access.key=minio \
#     --conf spark.hadoop.fs.s3a.secret.key=minio123 \
#     --conf spark.hadoop.fs.s3a.path.style.access=true \
#     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#     s3a://mybucket/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar \
#     s3a://mybucket/$graph/$vertex_file\
#     s3a://mybucket/$graph/$edge_file \
#     $directed \
#     s3a://mybucket/$graph/output \
#     1

# mc cp --recursive --insecure myminio/mybucket/test-bfs-directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-bfs/output/
# mc cp --recursive --insecure myminio/mybucket/test-cdlp-directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-cdlp/output/
# mc cp --recursive --insecure myminio/mybucket/test-lcc-directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-lcc/output/
# mc cp --recursive --insecure myminio/mybucket/test-pr-directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-pr/output/
# mc cp --recursive --insecure myminio/mybucket/test-sssp-directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-sssp/output/
# mc cp --recursive --insecure myminio/mybucket/test-wcc-directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-wcc/output/

# cd ../benchmark/results/

# cd test-bfs/
# ./coalasce.sh
# cd ..

# cd test-cdlp/
# ./coalasce.sh
# cd ..

# cd test-lcc/
# ./coalasce.sh
# cd ..

# cd test-pr/
# ./coalasce.sh
# cd ..

# cd test-sssp/
# ./coalasce.sh
# cd ..

# cd test-wcc/
# ./coalasce.sh
# cd ..