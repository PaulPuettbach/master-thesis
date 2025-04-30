#!/bin/bash
if [ $# -ne 1 ]
then
    echo "the argument provided that is needed is: <scheduler>" 1>&2
    exit 1
fi

scheduler=$1
case $scheduler in

    default)
        :
        ;;
    custom-scheduler)
        helm uninstall scheduler --wait
        ;;
    *)
        echo "provided <scheduler> is not one of: default, custom-scheduler" 1>&2
        exit 1
        ;;
esac
kubectl delete pods --all -n spark-namespace
kubectl delete svc --all -n spark-namespace


kubectl delete rolebinding pod-create-spark-role-binding -n spark-namespace
kubectl delete role pod-create-spark -n spark-namespace
kubectl delete serviceaccount spark -n spark-namespace



