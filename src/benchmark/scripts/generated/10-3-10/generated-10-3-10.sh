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

trap 'echo "Interrupt received, stopping jobs..."; kill $(jobs -p); exit 1' SIGINT SIGTERM
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
  if (( max_try >= 5 ))
  then
    echo "failed due to congestion"
    return 1
  fi
  retry() {
    rm -f $pipe
    kubectl delete pods -l spark-app-name=$name -n spark-namespace > /dev/null 2>&1
    sleep $whole_backoff
    return_values=$(submit_work "$tenant" "$algorithm" "$number_of_executors" "$graph" "$graphsize" "$scheduler" "$add_to_backoff" "$((max_try + 1))" "$name")
    if [[ $? -eq 0 ]]
    then
      echo "$return_values"
      return 0
    else
      echo "$return_values"
      return 1
    fi
  }
  local backoff=$((1 + $RANDOM % 20))
  ((add_to_backoff+=3))
  whole_backoff=$((backoff+add_to_backoff))

  # check for duplicate names propagate the name downstream through the recursion
  name_taken=$(kubectl get pods -n spark-namespace -l spark-app-name=$name --output name | wc -l)
  if [[ $name_taken -ne 0 ]]
  then
    if [[ $name =~ -([0-9]+$) ]]
    then
      local base="${name%-*}"
      local num="${name##*-}"
      local new_num=$(( num + 1 ))
      name="${base}-${new_num}"
    else
      name="${name}-2"
    fi
  fi
  local pipe
  pipe=$(mktemp /tmp/${name}.XXXXXX)
  (time ./spark-submit.sh $tenant $algorithm $number_of_executors $graph $graphsize $scheduler $name) 2> >(tee $pipe >/dev/null) &
  local timeout=40
  local elapsed=0
  while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=$name,spark-role=executor --output name | wc -l) -eq 0 ]]
  do
  #poll every 4 seconds
    if (( waited >= wait_timeout )); then
      retry
      return $?
    fi
    sleep 4
    ((waited+=4))
  done
  #need to redirect the output to nothign lest it override the ttc and name
  kubectl wait --for=condition=Ready -n spark-namespace pods -l spark-app-name=$name,spark-role=executor --timeout=30s > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    while [[ ! -s "$pipe" ]] 
    do
      sleep 1
    done
    local ttc
    ttc=$(grep 'real' "$pipe" | awk '{print $2}')
    rm -f $pipe
    echo $ttc $name
    return 0
  else
    retry
  fi
}
(
return_values=$(submit_work levi pr 3 test-pr-undirected test_graphs $scheduler 0 0 "levi-pr-test-pr-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/levi_times.csv
  echo -n "," >> generated/10-3-10/levi_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/levi_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work levi pr 3 test-pr-directed test_graphs $scheduler 0 0 "levi-pr-test-pr-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/levi_times.csv
  echo -n "," >> generated/10-3-10/levi_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/levi_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work olivia bfs 3 test-bfs-directed test_graphs $scheduler 0 0 "olivia-bfs-test-bfs-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/olivia_times.csv
  echo -n "," >> generated/10-3-10/olivia_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/olivia_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work levi bfs 3 test-bfs-directed test_graphs $scheduler 0 0 "levi-bfs-test-bfs-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/levi_times.csv
  echo -n "," >> generated/10-3-10/levi_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/levi_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work sophia bfs 3 test-bfs-undirected test_graphs $scheduler 0 0 "sophia-bfs-test-bfs-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/sophia_times.csv
  echo -n "," >> generated/10-3-10/sophia_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/sophia_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work mateo bfs 3 test-bfs-undirected test_graphs $scheduler 0 0 "mateo-bfs-test-bfs-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/mateo_times.csv
  echo -n "," >> generated/10-3-10/mateo_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/mateo_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work elijah pr 3 test-pr-directed test_graphs $scheduler 0 0 "elijah-pr-test-pr-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/elijah_times.csv
  echo -n "," >> generated/10-3-10/elijah_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/elijah_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work sophia pr 3 test-pr-undirected test_graphs $scheduler 0 0 "sophia-pr-test-pr-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/sophia_times.csv
  echo -n "," >> generated/10-3-10/sophia_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/sophia_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
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
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/liam_times.csv
  echo -n "," >> generated/10-3-10/liam_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/liam_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
(
return_values=$(submit_work liam sssp 3 test-sssp-directed test_graphs $scheduler 0 0 "liam-sssp-test-sssp-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
echo "this is the return values $return_values"
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-3-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo -n $timestamp_created_formated >> generated/10-3-10/liam_times.csv
  echo -n "," >> generated/10-3-10/liam_times.csv
  echo $timestamp_scheduled_formated >> generated/10-3-10/liam_times.csv
done
kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace
) &
#----------------------------------------------------------------

# # Store in array for easier tracking
# pids=($pid1 $pid2 $pid3 $pid4 $pid5 $pid6 $pid7 $pid8 $pid9 $pid10)
# names=("Job1" "Job2" "Job3" "Job4" "Job5" "Job6" "Job7" "Job8" "Job9" "Job10")
# # Periodically check which are still running
# while :; do
#   all_done=true
#   for i in "${!pids[@]}"; do
#     if kill -0 "${pids[i]}" 2>/dev/null; then
#       echo "${names[i]} (PID ${pids[i]}) is still running"
#       all_done=false
#     fi
#   done
#   $all_done && break
#   sleep 20
# done

wait
sort -o generated/10-3-10/liam_times.csv -t, -k1,1 generated/10-3-10/liam_times.csv
#./cleanup.sh $scheduler
