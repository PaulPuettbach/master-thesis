#!/bin/bash
kubectl delete pods --all -n spark-namespace
kubectl delete svc --all -n spark-namespace


kubectl delete rolebinding pod-create-spark-role-binding -n spark-namespace
kubectl delete role pod-create-spark -n spark-namespace
kubectl delete serviceaccount spark -n spark-namespace