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
  if (( max_try >= 2 ))
  then
    echo "failed due to congestion"
    return 1
  fi

  local backoff=$((1 + $RANDOM % 10))
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

  while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=$name,spark-role=executor --output name | wc -l) -eq 0 ]]
  do
  #poll every 4 seconds
    sleep 4
  done
  #need to redirect the output to nothign lest it override the ttc and name
  kubectl wait --for=condition=Ready -n spark-namespace pods -l spark-app-name=$name,spark-role=executor --timeout=60s >/dev/null 2>&1
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
    rm -f $pipe
    kubectl delete pods -l spark-app-name=$name -n spark-namespace > /dev/null 2>&1
    sleep $whole_backoff
    return_values=$(submit_work "$tenant" "$algorithm" "$number_of_executors" "$graph" "$graphsize" "$scheduler" "$add_to_backoff" "$((max_try + 1))" "$name")
    if [[ $? -eq 0 ]]
    then
      echo "$return_values"
      return 0
    else
      echo "an error ocurred"
      return 1
    fi
  fi
}
sleep 0.2554842207512615
(
return_values=$(submit_work levi cdlp 3 test-cdlp-undirected test_graphs $scheduler 0 0 "levi-cdlp-test-cdlp-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/levi_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.16702609205110286
(
return_values=$(submit_work levi bfs 3 test-bfs-directed test_graphs $scheduler 0 0 "levi-bfs-test-bfs-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/levi_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.2529355109868715
(
return_values=$(submit_work noah bfs 3 test-bfs-directed test_graphs $scheduler 0 0 "noah-bfs-test-bfs-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/noah_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.03032710286657299
(
return_values=$(submit_work levi sssp 3 test-sssp-directed test_graphs $scheduler 0 0 "levi-sssp-test-sssp-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/levi_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.25815279956890963
(
return_values=$(submit_work olivia lcc 3 test-lcc-undirected test_graphs $scheduler 0 0 "olivia-lcc-test-lcc-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/olivia_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.44381065716650653
(
return_values=$(submit_work isabella lcc 3 test-lcc-directed test_graphs $scheduler 0 0 "isabella-lcc-test-lcc-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/isabella_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.12997046700886727
(
return_values=$(submit_work mateo lcc 3 test-lcc-directed test_graphs $scheduler 0 0 "mateo-lcc-test-lcc-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/mateo_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.5068800978070184
(
return_values=$(submit_work charlotte wcc 3 test-wcc-directed test_graphs $scheduler 0 0 "charlotte-wcc-test-wcc-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/charlotte_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.0293293829772121
(
return_values=$(submit_work asher lcc 3 test-lcc-undirected test_graphs $scheduler 0 0 "asher-lcc-test-lcc-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/asher_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.20021897466297578
(
return_values=$(submit_work ava cdlp 3 test-cdlp-directed test_graphs $scheduler 0 0 "ava-cdlp-test-cdlp-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/20-4-10/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions[]? | select(.type=="PodScheduled").lastTransitionTime)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/ava_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/20-4-10/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
#----------------------------------------------------------------
wait
sort -o generated/20-4-10/merged_output.csv -t, -k1,1 generated/20-4-10/merged_output.csv
./cleanup.sh $scheduler
