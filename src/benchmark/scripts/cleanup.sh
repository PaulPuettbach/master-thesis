#!/bin/bash
export VM_TARGET="ssh cloud_controller_puttbach@192.168.164.2 -i /home/puttbach/.ssh/id_rsa_continuum"
if [ $# -ne 1 ]
then
    echo "the argument provided that is needed is: <scheduler>" 1>&2
    exit 1
fi

scheduler=$1
case $scheduler in

    random-scheduler)
        $VM_TARGET "helm uninstall random-scheduler --wait"
        ;;
    default)
        :
        ;;
    custom-scheduler)
        $VM_TARGET "helm uninstall scheduler --wait"
        ;;
    *)
        echo "provided <scheduler> is not one of: default, custom-scheduler, random-scheduler" 1>&2
        exit 1
        ;;
esac
kubectl delete pods --all -n spark-namespace
kubectl delete svc --all -n spark-namespace


kubectl delete rolebinding pod-create-spark-role-binding -n spark-namespace
kubectl delete role pod-create-spark -n spark-namespace
kubectl delete serviceaccount spark -n spark-namespace



