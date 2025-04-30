#!/bin/bash

# This script is generated
if [ $# -ne 1 ]
then
    echo "the argument provided that is needed is: <scheduler>" 1>&2
    exit 1
fi

scheduler=$1
export PATH=$PATH:$HOME/minio-binaries/
cd ../..

./init.sh $scheduler

submit_work () {
  local tenant=$1
  local algorithm=$2
  local number_of_executors=$3
  local graph=$4
  local graphsize=$5
  local scheduler=$6
  local add_to_backoff=$7
  local max_try=$8
  local name=$9
  if (( max_try >= 2 ))
  then
    echo "failed due to congestion"
    return 1
  fi

  local backoff=$((1 + $RANDOM % 10))
  ((add_to_backoff+=3))
  whole_backoff=$((backoff+add_to_backoff))

  # check for duplicate names propagate the name downstream through the recursion
  name_taken=$(kubectl get pods -n spark-namespace -l spark-app-name=$name --output name | wc -l)
  if [[ $name_taken -ne 0 ]]
  then
    if [[ $name =~ _([0-9]+$) ]]
    then
      local base="${name%_*}"
      local num="${name##*_}"
      local new_num=$(( num + 1 ))
      name="${base}_${new_num}"
    else
      name="${name}_2"
    fi
  fi
  pipe=/tmp/$name
  mkfifo $pipe
  (time ./spark-submit.sh $tenant $algorithm $number_of_executors $graph $graphsize $scheduler $name) 2> >(tee $pipe >/dev/null) &

  while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=$name,spark-role=executor --output name | wc -l) -eq 0 ]]
  do
  #poll every 4 seconds
    sleep 4
  done
  #need to redirect the output to nothign lest it override the ttc and name
  kubectl wait --for=condition=Ready -n spark-namespace pods -l spark-app-name=$name,spark-role=executor --timeout=60s >/dev/null
  if [ $? -eq 0 ]
  then
    ttc=$(cat $pipe)
    ttc=$(echo $ttc | awk '/real/ {print $2}')
    rm -f $pipe
    echo $ttc $name
  else
    rm -f $pipe
    kubectl delete pods -l spark-app-name=$name --field-selector=status.phase!=Failed -n spark-namespace
    sleep $whole_backoff
    echo "bakoff worked i think"
    return_values=$(submit_work "$tenant" "$algorithm" "$number_of_executors" "$graph" "$graphsize" "$scheduler" "$add_to_backoff" "$((max_try + 1))" "$name")
    if [[ $? -eq 0 ]]
    then
      echo "$return_values"
      return 0
    else
      return 1
    fi
  fi
}

(
return_values=$(submit_work ellie bfs 3 test-bfs-directed test_graphs $scheduler 0 0 "ellie-bfs-test-bfs-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/6-3-5/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/6-3-5/ellie_times.csv
  echo -n "," >> generated/6-3-5/ellie_times.csv
  echo $timestamp_scheduled_formated >> generated/6-3-5/ellie_times.csv
done
kubectl delete pods $(kubectl get pods -n spark-namespace -l spark-app-name=${name} --field-selector=status.phase!=Failed -o jsonpath='{.items[*].metadata.name}') -n spark-namespace
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-bfs-directed/output/ --recursive --force
) &

(
return_values=$(submit_work liam sssp 3 test-sssp-undirected test_graphs $scheduler 0 0 "liam-sssp-test-sssp-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/6-3-5/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/6-3-5/liam_times.csv
  echo -n "," >> generated/6-3-5/liam_times.csv
  echo $timestamp_scheduled_formated >> generated/6-3-5/liam_times.csv
done
kubectl delete pods $(kubectl get pods -n spark-namespace -l spark-app-name=${name} --field-selector=status.phase!=Failed -o jsonpath='{.items[*].metadata.name}') -n spark-namespace
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-sssp-undirected/output/ --recursive --force
) &

(
return_values=$(submit_work luna pr 3 test-pr-undirected test_graphs $scheduler 0 0 "luna-pr-test-pr-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/6-3-5/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/6-3-5/luna_times.csv
  echo -n "," >> generated/6-3-5/luna_times.csv
  echo $timestamp_scheduled_formated >> generated/6-3-5/luna_times.csv
done
kubectl delete pods $(kubectl get pods -n spark-namespace -l spark-app-name=${name} --field-selector=status.phase!=Failed -o jsonpath='{.items[*].metadata.name}') -n spark-namespace
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-undirected/output/ --recursive --force
) &

(
return_values=$(submit_work luna bfs 3 test-bfs-undirected test_graphs $scheduler 0 0 "luna-bfs-test-bfs-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/6-3-5/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/6-3-5/luna_times.csv
  echo -n "," >> generated/6-3-5/luna_times.csv
  echo $timestamp_scheduled_formated >> generated/6-3-5/luna_times.csv
done
kubectl delete pods $(kubectl get pods -n spark-namespace -l spark-app-name=${name} --field-selector=status.phase!=Failed -o jsonpath='{.items[*].metadata.name}') -n spark-namespace
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-bfs-undirected/output/ --recursive --force
) &

(
return_values=$(submit_work lucas pr 3 test-pr-undirected test_graphs $scheduler 0 0 "lucas-pr-test-pr-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/6-3-5/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/6-3-5/lucas_times.csv
  echo -n "," >> generated/6-3-5/lucas_times.csv
  echo $timestamp_scheduled_formated >> generated/6-3-5/lucas_times.csv
done
kubectl delete pods $(kubectl get pods -n spark-namespace -l spark-app-name=${name} --field-selector=status.phase!=Failed -o jsonpath='{.items[*].metadata.name}') -n spark-namespace
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-undirected/output/ --recursive --force
) &
#----------------------------------------------------------------
wait
sort -o generated/6-3-5/lucas_times.csv -t, -k1,1 generated/6-3-5/lucas_times.csv
./cleanup.sh $scheduler
