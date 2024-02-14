#!/bin/bash
#build the container image for the main component
cd ../main-component
docker build -t main-component .
cd ..

#built the container image that is going to run on the daemonset
cd daemon
docker build -t daemon .
cd ../util

#save the images to local image reposotory
docker save -o repo.tar main-component:latest daemon:latest
docker load --input repo.tar