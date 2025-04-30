#!/bin/bash

export PATH=$PATH:$HOME/minio-binaries/
if [ $# -ne 3 ]
then
    echo "the 3 arguments provided that are needed are: <n_tenant> at most 20, <rate>, <n_runs>" 1>&2
    exit 1
fi
#user input
n_tenant=$1
rate=$(( $2 ))
n_runs=$(( $3 ))

  cd generated/
 #pick random tenant
  tenants_file="../txt/tenants.txt"
  random_tenants=$(shuf -n $(( n_tenant )) "$tenants_file")
  echo "this is the random tenants: $random_tenants"

mkdir $n_tenant-$rate-$n_runs
benchmark="$n_tenant-$rate-$n_runs/generated-$n_tenant-$rate-$n_runs.sh"

# Start by writing the script header to the file
cat << 'EOF' > $benchmark
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
EOF

# #sample from the exponetial ditsribution for the interarrival time
# x=$(python3 exponential-random.py "$rate")

#Add commands in a loop
#pick graph size
#pick graph
#pick tenant
#pick algo
#time the spark 
for (( i=1; i<=n_runs; i++ )); do
  #pick random graph_size
  x=$(python3 ../exponential-random.py $rate)
  graphsizes="../txt/graph_sizes.txt"
  random_graphsize=$(shuf -n 1 "$graphsizes")
  #random_graphsize=${random_graphsize//$'\r'/}
  random_graphsize="test_graphs"
  echo "this is the random graphsize: $random_graphsize"

  #pick random graph
  graphs="${random_graphsize}.txt"
  random_graph=$(shuf -n 1 "../txt/graph_sizes/$graphs")
  random_graph=${random_graph//$'\r'/}
  echo "this is the random graph: $random_graph"

  #pick random tenant
  random_tenant=$(shuf -n 1 -e $random_tenants)
  random_tenant=${random_tenant//$'\r'/}
  echo "this is the random tenant: $random_tenant"

  supported_algorithms=$(grep '\.algorithms =' "../../config-template/graphs/$random_graph.properties" | cut -d'=' -f2 )
  echo "this is the supported algorithms $supported_algorithms"

  random_algorithm=$(echo $supported_algorithms | tr -d ' '| tr ',' "\n" | shuf -n 1)
  random_algorithm=${random_algorithm//$'\r'/}
  echo "this is the random algorithm $random_algorithm"
  # #--name ${algorithm}-${graph}-${user} \ this is the name in the spark submit
  # #25/02/19 10:07:40 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (10.1.3.68:43646) with ID 2,  ResourceProfileId 0
  # #spark submit
  # #when spark submit finishes take the ttc and the time spend pending
  # # at the end find how many pods were pending based on mc --insecure rm  myminio/mybucket/graphs/test_graphs/test-pr-directed/output/ --recursive --force
  # echo "#----------------------------------------------------------------" >> $benchmark  | shuf -n 1
  echo "sleep $x" >> $benchmark
  echo "(" >> $benchmark
  echo "return_values=\$(submit_work $random_tenant $random_algorithm 3 $random_graph $random_graphsize \$scheduler 0 0 \"${random_tenant}-${random_algorithm}-${random_graph}\")" >> $benchmark
  echo "if [[ \$? -ne 0 ]]" >> $benchmark
  echo "then" >> $benchmark
  echo "  echo \"an error occured this is the return value\"" >> $benchmark
  echo "  echo \"\$return_values\"" >> $benchmark
  echo "  #exit 77 exit all subshells" >> $benchmark
  echo "  exit 12" >> $benchmark
  echo "fi" >> $benchmark
  echo "IFS=' ' read -r ttc name <<< \"\$return_values\"" >> $benchmark
   echo "ttc_formated=\$(date -d \"\$ttc\" +%s)" >> $benchmark
  echo "echo \"\$ttc_formated\" >> generated/$n_tenant-$rate-$n_runs/time.txt" >> $benchmark

  echo "timestamps=\$(kubectl get pods --namespace spark-namespace -l spark-app-name=\${name} -o json | jq -r '.items[] | \"\\(.metadata.creationTimestamp),\\(.status.conditions[]? | select(.type==\"PodScheduled\").lastTransitionTime)\"')" >> $benchmark
  echo "for timestamp in \$timestamps; do" >> $benchmark
  echo "  IFS=',' read -r timestamp_created timestamp_scheduled <<< \"\$timestamp\"" >> $benchmark
  echo "  timestamp_created_formated=\$(date -d \"\$timestamp_created\" +%s)" >> $benchmark
  echo "  timestamp_scheduled_formated=\$(date -d \"\$timestamp_scheduled\" +%s)" >> $benchmark

  echo "  echo -n \$timestamp_created_formated >> generated/$n_tenant-$rate-$n_runs/${random_tenant}_times.csv" >> $benchmark
  echo "  echo -n \",\" >> generated/$n_tenant-$rate-$n_runs/${random_tenant}_times.csv" >> $benchmark
  echo "  echo \$timestamp_scheduled_formated >> generated/$n_tenant-$rate-$n_runs/${random_tenant}_times.csv" >> $benchmark
  echo "done" >> $benchmark
  echo "kubectl delete pods -l "spark-app-name=${name},spark-role=driver" -n spark-namespace" >> $benchmark
  # # loading_bar="{"
  # # progress=$(( (60/n_runs )*i ))
  # # echo "this is the progress $progress"
  # # progress_left=$(( (60/n_runs)*(n_runs-i) ))
  # # echo "this is the progress_left $progress_left"
  # # for (( j=1; j<=progress; j++ ));
  # # do
  # #   loading_bar+="*"
  # # done
  # # for (( k=1; k<=progress_left; k++ ));
  # # do
  # #   loading_bar+="_"
  # # done
  # # loading_bar+="} \r"
  # # echo "echo -ne \""$loading_bar"\"" >> $benchmark

  echo ") &" >> $benchmark
done

#this is for after everything ran
#echo "time_spent_pending=\$((tse_scheduled - tse_created))" >> $benchmark
#echo "echo \$time_spent_pending >> generated/$n_tenant-$rate-$n_runs/$random_tenant-fairness.txt" >> $benchmark
#echo "(time_spent_pending / n_pending) >> fairness.txt" >> $benchmark 
# | tr ',' \"\\n\"
echo "#----------------------------------------------------------------" >> $benchmark
echo "wait" >> $benchmark
echo "sort -o generated/$n_tenant-$rate-$n_runs/${random_tenant}_times.csv -t, -k1,1 generated/$n_tenant-$rate-$n_runs/${random_tenant}_times.csv" >> $benchmark
echo "./cleanup.sh \$scheduler" >> $benchmark 
#take the mean squared error per tenant, error from is the average wait time normalized with currently pending pods
#(queue length) at the time they are meassured

#take the kubectl of the executor pods if they cannot be scheduled due to memory pressure unschedule the driver
#then take the random backoff and try again increase backoff after unsuccsessfull attemp
#how do i increase the backoff i take the number and add the same again 

chmod +x $benchmark

