#!/bin/bash

./init.sh

#figure out kubectl watch to get the fairness for messeaure: wait time error to average wait time with sliding window its called simple moving average and it is the old average plus 1/k *(newest value - oldest value)
# the k (number of values in the sliding window) should be chosen such that it is close to the number of elements currently in the queue which is lamda
kubectl get pod --no-headers -o custom-columns=NODE:.spec.nodeName -n minio | grep 'scheduler-daemon'

#sample from the exponetial ditsribution for the interarrival time
rate=2.5
x=$(python3 generate_exponential.py "$rate")

#pick random tenant
tenants="tenants.txt"
random_tenant=$(shuf -n 1 "$tenants")

#pick random graph

./spark-submit.sh paul bfs 3 test-bfs-directed
./spark-submit.sh paul cdlp 3 test-cdlp-directed
./spark-submit.sh paul lcc 3 test-lcc-directed
./spark-submit.sh paul pr 3 test-pr-directed
./spark-submit.sh paul sssp 3 test-sssp-directed
./spark-submit.sh paul wcc 3 test-wcc-directed



mc cp --recursive --insecure myminio/mybucket/test_bfs_directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-bfs/output/
mc cp --recursive --insecure myminio/mybucket/test_cdlp_directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-cdlp/output/
mc cp --recursive --insecure myminio/mybucket/test_lcc_directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-lcc/output/
mc cp --recursive --insecure myminio/mybucket/test_pr_directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-pr/output/
mc cp --recursive --insecure myminio/mybucket/test_sssp_directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-sssp/output/
mc cp --recursive --insecure myminio/mybucket/test_wcc_directed/output/ /mnt/d/mystuff2/master_thesis/src/benchmark/results/test-wcc/output/

cd ../results

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



./logging

./cleanup