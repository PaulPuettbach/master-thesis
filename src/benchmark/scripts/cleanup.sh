#!/bin/bash
if [ $# -ne 1 ]
then
    echo "the argument provided that is needed is: <scheduler>" 1>&2
    exit 1
fi

scheduler=$1
case $scheduler in

    default)
        :
        ;;
    custom-scheduler)
        helm uninstall scheduler --wait
        ;;
    *)
        echo "provided <scheduler> is not one of: default, custom-scheduler" 1>&2
        exit 1
        ;;
esac
kubectl delete pods --all -n spark-namespace
kubectl delete svc --all -n spark-namespace
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-cdlp-directed/output/ --recursive --force


# kubectl delete rolebinding pod-create-spark-role-binding -n spark-namespace
# kubectl delete role pod-create-spark -n spark-namespace
# kubectl delete serviceaccount spark -n spark-namespace

#do this in the benchmark itself so i dont have to do it in the cleanup since the cleanup depends on what type of workload i am running 
# mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-bfs-undirected/output/ --recursive --force
# mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-cdlp-undirected/output/ --recursive --force
# mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-lcc-directed/output/ --recursive --force
# mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-directed/output/ --recursive --force
# mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-sssp-undirected/output/ --recursive --force
# mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-wcc-undirected/output/ --recursive --force
#25/02/19 10:07:40 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (10.1.3.68:43646) with ID 2,  ResourceProfileId 0

# cd ../results/

# cd test-bfs/
# rm -r output/*
# rm combined_result
# cd ..

# cd test-cdlp/
# rm -r output/*
# rm combined_result
# cd ..

# cd test-lcc/
# rm -r output/*
# rm combined_result
# cd ..

# cd test-pr/
# rm -r output/*
# rm combined_result
# cd ..

# cd test-sssp/
# rm -r output/*
# rm combined_result
# cd ..

# cd test-wcc/
# rm -r output/*
# rm combined_result
# cd ..

