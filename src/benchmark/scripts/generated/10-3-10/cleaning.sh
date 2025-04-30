#!/bin/bash
kubectl delete pods --all -n spark-namespace
kubectl delete svc --all -n spark-namespace


kubectl delete rolebinding pod-create-spark-role-binding -n spark-namespace
kubectl delete role pod-create-spark -n spark-namespace
kubectl delete serviceaccount spark -n spark-namespace

rm -f "/tmp/levi-pr-test-pr-undirected"
rm -f "/tmp/levi-pr-test-pr-directed"
rm -f "/tmp/olivia-bfs-test-bfs-directed"
rm -f "/tmp/levi-bfs-test-bfs-directed"
rm -f "/tmp/sophia-bfs-test-bfs-undirected"
rm -f "/tmp/mateo-bfs-test-bfs-undirected"
rm -f "/tmp/elijah-pr-test-pr-directed"
rm -f "/tmp/sophia-pr-test-pr-undirected"
rm -f "/tmp/liam-sssp-test-sssp-undirected"
rm -f "/tmp/liam-sssp-test-sssp-directed"
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-undirected/levi-pr-test-pr-undirected/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-directed/levi-pr-test-pr-directed/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-bfs-directed/olivia-bfs-test-bfs-directed/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-bfs-directed/levi-bfs-test-bfs-directed/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-bfs-undirected/sophia-bfs-test-bfs-undirected/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-bfs-undirected/mateo-bfs-test-bfs-undirected/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-directed/elijah-pr-test-pr-directed/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-undirected/sophia-pr-test-pr-undirected/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-sssp-undirected/liam-sssp-test-sssp-undirected/output/ --recursive --force
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-sssp-directed/liam-sssp-test-sssp-directed/output/ --recursive --force