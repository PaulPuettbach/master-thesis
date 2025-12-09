#!/bin/bash

export KUBECONFIG="/mnt/sdc/puttbach/remote-kube-config.yaml"
export K8S_MASTER_URL="k8s://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
export VM_TARGET="ssh cloud_controller_puttbach@192.168.164.2 -i /home/puttbach/.ssh/id_rsa_continuum"

if [ $# -ne 1 ]
then
    echo "the argument provided that is needed is: <scheduler>" 1>&2
    exit 1
fi

scheduler=$1
case $scheduler in

    random-scheduler)
        #cd ../../random
        #./load-repo.sh
        $VM_TARGET "helm install random-scheduler custom/helm-random --wait"
        ;;

    default)
        ;;
    custom-scheduler)
        #cd ../../scheduler/containers/util
        #./load-repo.sh
        $VM_TARGET "helm install scheduler custom/scheduler --wait"
        ;;
    *)
        echo "provided <scheduler> is not one of: default, custom-scheduler, random-scheduler" 1>&2
        exit 1
        ;;
esac

kubectl apply -f service.account.yaml