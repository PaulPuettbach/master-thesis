#!/bin/bash

./init.sh default

pipe="/tmp/my_pipe" 
mkfifo $pipe

(time ./spark-submit.sh melia cdlp 3 test-cdlp-directed test_graphs default melia-cdlp-test-cdlp-directed) 2> >(tee $pipe) &


while [[ $(kubectl get pods -n spark-namespace -l spark-app-name=melia-cdlp-test-cdlp-directed,spark-role=executor --output name | wc -l) -eq 0 ]]
do
#poll every 4 seconds
    jobs -l
    sleep 4
done

ttc=$(cat $pipe)
ttc=$(echo $ttc | awk '/real/ {print $2}')
echo "this is the ttc $ttc"


rm -f $pipe
wait

./cleanup.sh default