#!/bin/bash

# #notes for the continuum config##
# #set up the remote repo for everything
# #how to package spark
# #Hi, hope you are doing well ! I had some time now to look through continuum and I have some questions
# install helm in the vms the way i wanted to do that is add 
#              - name: Install Helm
#                shell: |
#                 curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
#        into the base_install file of the kubernetes directory in the resource_manager directory. is this a valid approach or should i add this to the control_install only ?
# 2.  i havent used a remote kubernetes cluster like this before should i ssh into the cloud controler every time ? that seems laborious 
# ####### ssh #######
# ssh node4
# # 3. ssh into the vm

# ####### initilize #######

# # I need helm images, i need graphs, i need algorithms, i need minio image for both operator and tenant, need spark image

# # 1.ServerAliveInterval 20
# for single file
# scp /path/to/local/file node4:/mnt/sdc/puttbach/

##actual noted##