#!/bin/bash
cd ../minio/

helm install minio ./operator --namespace minio --create-namespace
helm install --namespace minio-tenant --create-namespace paul ./tenant