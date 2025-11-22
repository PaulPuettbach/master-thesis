#!/bin/bash

if [ $# -ne 1 ]
then
    echo "the argument provided that is needed is: <scheduler>" 1>&2
    exit 1
fi

scheduler=$1
case $scheduler in

    random-scheduler)
        cd ../../random
        #./load-repo.sh
        helm install random-scheduler custom/helm-random --wait
        cd ../spark
        ;;

    default)
        cd ../../spark
        ;;
    custom-scheduler)
        cd ../../scheduler/containers/util
        #./load-repo.sh
        cd ../../
        helm install scheduler custom/scheduler --wait
        cd ../spark
        ;;
    *)
        echo "provided <scheduler> is not one of: default, custom-scheduler, random-scheduler" 1>&2
        exit 1
        ;;
esac

export PATH=$PATH:$HOME/minio-binaries/

kubectl apply -f service.account.yaml
cd ../benchmark/scripts