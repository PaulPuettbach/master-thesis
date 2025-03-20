#!/bin/bash

# ./init.sh

# #take the mean squared error per tenant, error from is the average wait time normalized with currently pending pods
# #(queue length) at the time they are meassured
# kubectl get pod --no-headers -o custom-columns=NODE:.spec.nodeName -n minio | grep 'scheduler-daemon'

# #this will get the current number of pod that ar running (change to pending later)
# kubectl get pods --field-selector=status.phase=Running --no-headers | awk ' END {print NR}'

# #sample from the exponetial ditsribution for the interarrival time
# rate=2.5
# x=$(python3 generate_exponential.py "$rate")

# #pick random tenant
# tenants="tenants.txt"
# random_tenant=$(shuf -n 1 "$tenants")

#pick random graph
for algo in bfs cdlp lcc pr sssp wcc; do
    ./spark-submit.sh paul $algo 3 test-$algo-directed test_graphs custom-scheduler
    mc cp --recursive --insecure myminio/mybucket/graphs/test_graphs/test-$algo-directed/output/ /mnt/c/Users/paulp/Documents/my_stuff/comp_sci_master/master-thesis/src/benchmark/results/test-$algo/output/
done

cd ../results

for dir in test-bfs test-cdlp test-lcc test-pr test-sssp test-wcc; do
  cd "$dir"
  ./coalasce.sh
  cd ..
done

cd ../scripts



# ./logging

#./cleanup