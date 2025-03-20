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
  errsv=$?
  if [ $errsv -ne 0 ]
  then
    echo "found error with error number $errsv"
  else
    echo "no issues"
  fi
  set +m
  ( { time ./spark-submit.sh $tenant $algorithm $number_of_executors $graph $graphsize $scheduler $name > /dev/null 2>&1 ; }  2>$pipe ) &
  ( { time ./spark-submit.sh melia cdlp 3 test-cdlp-directed test_graphs default melia-cdlp-test-cdlp-directed >/dev/null 2>&1 ; }  2>/tmp/melia-cdlp-test-cdlp-directed ) &
  #why does this work
  ( { time ./spark-submit.sh melia cdlp 3 test-cdlp-directed test_graphs default melia-cdlp-test-cdlp-directed >/dev/null 2>&1 ; } ) &
  
  while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=$name,spark-role=executor --output name | wc -l) -eq 0 ]]
  do
  #poll every 4 seconds
    sleep 4
  done
  #need to redirect the output to nothign lest it override the ttc and name
  kubectl wait --for=condition=Ready -n spark-namespace pods -l spark-app-name=$name,spark-role=executor --timeout=60s >/dev/null
  if [ $? -eq 0 ]
  then
    ttc=$(<$pipe)
    errsv=$?
    if [ $errsv -ne 0 ]
    then
      echo "found error with error number $errsv"
    else
      echo "no issues"
      echo "this should be the ttc $ttc"
    fi
    rm -f $pipe
    echo "this is the final print $ttc $name"
  else
    rm -f $pipe
    kubectl delete pods -l spark-app-name=$name --field-selector=status.phase!=Failed -n spark-namespace
    sleep $whole_backoff
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
sleep 0.04432235693447884
(
return_values=$(submit_work melia cdlp 3 test-cdlp-directed test_graphs $scheduler 0 0 "melia-cdlp-test-cdlp-directed" | tee /dev/tty)
if [[ $? -ne 0 ]]
then
  echo "$return_values"
  #exit 77 exit all subshells
  exit 12
fi
exit 12
read ttc name <<< "$return_values"
echo "$ttc" >> generated/6-3-1/time.txt
timestamps_scheduled=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="PodScheduled")].lastTransitionTime},{end}'| tr ',' ", ")
timestamps_scheduled="${timestamps_scheduled%,}"
IFS=',' read -ra timestamps_scheduled_array <<< "$timestamps_scheduled"
timestamps_created=$(kubectl get pods --namespace spark-namespace -l spark-app-name=${name} -o jsonpath='{range .items[*]}{.metadata.creationTimestamp},{end}'| tr ',' ", ")
timestamps_created="${timestamps_created%,}"
IFS=',' read -ra timestamps_created_array <<< "$timestamps_created"
echo timestamps scheduled >> generated/6-3-1/melia_times.txt
for timestamp in "${timestamps_scheduled_array[@]}"; do
  tse_scheduled=$(date -d "$timestamp" +%s)
  echo $tse_scheduled >> generated/6-3-1/melia_times.txt
done
echo timestamps created >> generated/6-3-1/melia_times.txt
for timestamp in "${timestamps_created_array[@]}"; do
  tse_created=$(date -d "$timestamp" +%s)
  echo $tse_created >> generated/6-3-1/melia_times.txt
done
kubectl delete pods $(kubectl get pods -n spark-namespace -l spark-app-name=${name} --field-selector=status.phase!=Failed -o jsonpath='{.items[*].metadata.name}') -n spark-namespace
mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-cdlp-directed/output/ --recursive --force
) &
#----------------------------------------------------------------
wait
./cleanup.sh $scheduler
