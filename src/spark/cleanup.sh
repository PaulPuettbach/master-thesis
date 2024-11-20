#!/bin/bash
helm uninstall scheduler --wait
kubectl delete pods --all -n spark-namespace
kubectl delete svc --all -n spark-namespace


kubectl delete rolebinding pod-create-spark-role-binding -n spark-namespace
kubectl delete role pod-create-spark -n spark-namespace
kubectl delete serviceaccount spark -n spark-namespace

mc --insecure rm  myminio/mybucket/test-bfs/output --recursive --force
mc --insecure rm  myminio/mybucket/test-cdlp/output --recursive --force
mc --insecure rm  myminio/mybucket/test-lcc/output --recursive --force
mc --insecure rm  myminio/mybucket/test-pr/output --recursive --force
mc --insecure rm  myminio/mybucket/test-sssp/output --recursive --force
mc --insecure rm  myminio/mybucket/test-wcc/output --recursive --force

cd results/

cd test-bfs/
rm -r output/*
cd ..

cd test-cdlp/
rm -r output/*
cd ..

cd test-lcc/
rm -r output/*
cd ..

cd test-pr/
rm -r output/*
cd ..

cd test-sssp/
rm -r output/*
cd ..

cd test-wcc/
rm -r output/*
cd ..

