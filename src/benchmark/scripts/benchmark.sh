#!/bin/bash

#./cleanup.sh
export PATH=$PATH:$HOME/minio-binaries/
cd ../../scheduler/containers/util

./load-repo.sh

cd ../../

helm install scheduler main-helm-chart/ --wait
cd ../spark

kubectl apply -f service.account.yaml
cd ../benchmark/scripts

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



# #this is a section for debuging to be taking out at a later
# echo "-----------------------------------------------------------"
# kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n spark-namespace ) -n spark-namespace
# echo "-----------------------------------------------------------"
# kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep '^scheduler' | grep -v 'scheduler-daemon') -n kube-system #-c  init-daemon-service
# echo "-----------------------------------------------------------"
# kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n kube-system | grep 'scheduler-daemon') -n kube-system #-c init-main-service