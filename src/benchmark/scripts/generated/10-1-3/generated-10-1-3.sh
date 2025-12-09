#!/bin/bash

# This script is generated
if [ $# -ne 1 ]
then
    echo "the argument provided that is needed is: <scheduler>" 1>&2
    exit 1
fi

scheduler=$1
tmp_file=$(mktemp)
export PATH=$PATH:$HOME/minio-binaries/
mkdir -p ${scheduler}
cd ../..

./init.sh $scheduler

trap 'echo "Interrupt received, stopping jobs..."; kill $(jobs -p); exit 1; rm -f "$tmp_file"' SIGINT SIGTERM
trap 'rm -f "$tmp_file"' EXIT
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
  if (( max_try >= 4 ))
  then
    echo "failed due to congestion"
    return 1
  fi
  local pipe
  pipe=$(mktemp /tmp/${name}.XXXXXX)
  retry() {
    rm -f $pipe
    timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | (.metadata.creationTimestamp)')
    for timestamp in $timestamps; do   
      timestamp_created_formated=$(date -d "$timestamp" +%s)
      timestamp_scheduled_formated=$(date +%s)  #good estimate right before deletion
      echo 1 >> "$tmp_file"
      echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/10-1-3/${scheduler}/merged_output.csv
    done
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
  local backoff=$((1 + $RANDOM % 30))
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
  (time ./spark-submit.sh $tenant $algorithm $number_of_executors $graph $graphsize $scheduler $name) 2> >(tee $pipe >/dev/null) &
  local timeout=80
  local elapsed=0
  #this really checks if the driver is scheduled since no driver = no executor
  while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=$name,spark-role=executor --output name | wc -l) -eq 0 ]]
  do
  #poll every 4 seconds
    if (( elapsed >= timeout )); then
      retry
      return $?
    fi
    sleep 4
    ((elapsed+=4))
  done
  #while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=$name,spark-role=executor -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | wc -l) -le 1 ]]
  #need to redirect the output to nothign lest it override the ttc and name
  #little annoying since it forces all executors that are created to be running while somtimes just one is fine the issue is it might create more executors that specified further bogging doen the system
  local timeout=80
  local elapsed=0
  while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=$name,spark-role=executor --field-selector status.phase=Running --output name | wc -l) -eq 0 ]]
  do
  #poll every 4 seconds
    if (( elapsed >= timeout )); then
      retry
      return $?
    fi
    sleep 4
    ((elapsed+=4))
  done
  while [[ ! -s "$pipe" ]] 
  do
    sleep 1
  done
  local ttc
  ttc=$(grep 'real' "$pipe" | awk '{print $2}')
  rm -f $pipe
  echo $ttc $name
  return 0
}
sleep 0.0823716524344082
(
return_values=$(submit_work oliver bfs 3 test-bfs-undirected test_graphs $scheduler 0 0 "oliver-bfs-test-bfs-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-1-3/${scheduler}/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions? | if . == null then "missing" elif any(.type == "PodScheduled") then .[] | select(.type=="PodScheduled").lastTransitionTime else "missing" end)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  if [[ "$timestamp_scheduled" == "missing" ]]; then
    echo 0 >> "$tmp_file"
  else
    timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  fi
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/10-1-3/${scheduler}/oliver_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/10-1-3/${scheduler}/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.18488336765968075
(
return_values=$(submit_work isabella wcc 3 test-wcc-directed test_graphs $scheduler 0 0 "isabella-wcc-test-wcc-directed")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-1-3/${scheduler}/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions? | if . == null then "missing" elif any(.type == "PodScheduled") then .[] | select(.type=="PodScheduled").lastTransitionTime else "missing" end)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  if [[ "$timestamp_scheduled" == "missing" ]]; then
    echo 0 >> "$tmp_file"
  else
    timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  fi
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/10-1-3/${scheduler}/isabella_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/10-1-3/${scheduler}/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
sleep 0.538258144541232
(
return_values=$(submit_work lucas sssp 3 test-sssp-undirected test_graphs $scheduler 0 0 "lucas-sssp-test-sssp-undirected")
if [[ $? -ne 0 ]]
then
  echo "an error occured this is the return value"
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
IFS=' ' read -r ttc name <<< "$return_values"
echo "$ttc" >> generated/10-1-3/${scheduler}/time.txt
timestamps=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o json | jq -r '.items[] | "\(.metadata.creationTimestamp),\(.status.conditions? | if . == null then "missing" elif any(.type == "PodScheduled") then .[] | select(.type=="PodScheduled").lastTransitionTime else "missing" end)"')
for timestamp in $timestamps; do
  IFS=',' read -r timestamp_created timestamp_scheduled <<< "$timestamp"
  timestamp_created_formated=$(date -d "$timestamp_created" +%s)
  if [[ "$timestamp_scheduled" == "missing" ]]; then
    echo 0 >> "$tmp_file"
  else
    timestamp_scheduled_formated=$(date -d "$timestamp_scheduled" +%s)
  fi
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/10-1-3/${scheduler}/lucas_times.csv
  echo ${timestamp_created_formated},${timestamp_scheduled_formated} >> generated/10-1-3/${scheduler}/merged_output.csv
done
kubectl delete pods -l spark-app-name=${name},spark-role=driver -n spark-namespace
) &
#----------------------------------------------------------------
wait
sort -o generated/10-1-3/${scheduler}/merged_output.csv -t, -k1,1 generated/10-1-3/${scheduler}/merged_output.csv

failed=$(grep -c '^1$' "$tmp_file")
not_scheduled=$(grep -c '^0$' "$tmp_file")


printf "========================================\n\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "             RESULT SUMMARY             \n\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "========================================\n\n\n\n" >> generated/10-1-3/${scheduler}/result_summary.txt

printf "Generated by Script: ${0}\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "Timestamp: %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "Parameters:\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "\tnumber of tenants = 10\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "\tarrival rate of spark jobs = 1\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "\tnumber of runs = 3\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "\tscheduler = ${scheduler}\n\n\n\n" >> generated/10-1-3/${scheduler}/result_summary.txt

printf -- "----------------------------------------\n\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf " FAILURE RATE \n\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf -- "----------------------------------------\n\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "Failed Pods (retried): ${failed}\n" >> generated/10-1-3/${scheduler}/result_summary.txt
printf "Not Scheduled (not retried): ${not_scheduled}\n\n\n\n" >> generated/10-1-3/${scheduler}/result_summary.txt

./cleanup.sh $scheduler
python3 generate-results.py ./generated/10-1-3/${scheduler}

