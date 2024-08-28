#!/bin/bash
#build the container image for the random scheduler
cd containers
docker build -t random-scheduler .

#save the images to local image reposotory
docker save -o repo.tar random-scheduler:latest
docker load --input repo.tar