#!/bin/bash

REPO_ROOT="/mnt/d/mystuff2/master_thesis"
DOCS_DIR="$REPO_ROOT/hosting"

mkdir -p "$DOCS_DIR"

cd "$REPO_ROOT/src/scheduler/"

helm package main-helm-chart/ --destination "$DOCS_DIR"

cd "$REPO_ROOT/src/minio/"
helm package operator/ --destination "$DOCS_DIR"
helm package tenant/ --destination "$DOCS_DIR"

cd "$REPO_ROOT/src/random/"
helm package helm-random/ --destination "$DOCS_DIR"

helm repo index "$DOCS_DIR" --url https://paulpuettbach.github.io/master_thesis
