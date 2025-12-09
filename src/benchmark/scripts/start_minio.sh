#!/bin/bash
cd ../../minio/

helm install minio --namespace minio --create-namespace custom/operator
helm install minio-tenant --namespace minio-tenant --create-namespace custom/tenant
