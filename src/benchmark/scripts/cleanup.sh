#!/bin/bash
helm uninstall scheduler --wait
kubectl delete pods --all -n spark-namespace
kubectl delete svc --all -n spark-namespace


kubectl delete rolebinding pod-create-spark-role-binding -n spark-namespace
kubectl delete role pod-create-spark -n spark-namespace
kubectl delete serviceaccount spark -n spark-namespace

mc --insecure rm  myminio/mybucket/test-bfs-directed/output --recursive --force
mc --insecure rm  myminio/mybucket/test_cdlp_directed/output --recursive --force
mc --insecure rm  myminio/mybucket/test_lcc_directed/output --recursive --force
mc --insecure rm  myminio/mybucket/test_pr_directed/output/ --recursive --force
mc --insecure rm  myminio/mybucket/test_sssp_directed/output --recursive --force
mc --insecure rm  myminio/mybucket/test_wcc_directed/output --recursive --force

cd ../results/

cd test-bfs/
rm -r output/*
rm combined_result
cd ..

cd test-cdlp/
rm -r output/*
rm combined_result
cd ..

cd test-lcc/
rm -r output/*
rm combined_result
cd ..

cd test-pr/
rm -r output/*
rm combined_result
cd ..

cd test-sssp/
rm -r output/*
rm combined_result
cd ..

cd test-wcc/
rm -r output/*
rm combined_result
cd ..

