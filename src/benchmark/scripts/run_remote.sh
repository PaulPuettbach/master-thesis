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

# This guide explains how to configure your local machine to submit jobs
# directly to the remote Kubernetes cluster, avoiding the need to copy files.

# 1. CONFIGURE SSH PROXYJUMP
# --------------------------
# Teach your local SSH client how to connect to the controller VM via the jump host.
#
# 1.1. Securely copy the private key from `node4` to your local machine.
#      This command avoids manual copy-paste errors. Run it in your local WSL terminal:
#      ssh node4 'cat /home/puttbach/.ssh/id_rsa_continuum' > ~/.ssh/id_rsa_continuum
#      Then, set the correct permissions:
#      chmod 600 ~/.ssh/id_rsa_continuum
#
# 1.2. Edit your local `~/.ssh/config` file (e.g., ~/.ssh/config in WSL) and add:
#
#      Host jump-node4
#        HostName <IP_OF_NODE4>
#        User puttbach
#
#      Host vm # Or k8s-controller, your choice
#        HostName 192.168.164.2
#        User cloud_controller_puttbach
#        IdentityFile ~/.ssh/id_rsa_continuum
#        ProxyJump jump-node4
#
# 2. CONFIGURE KUBECTL
# --------------------
# This allows your local `kubectl` to talk to the remote cluster.
#
# 2.1. Copy the remote cluster's configuration to your local machine.
#      This command fetches the config file from the `vm`, replaces the server
#      address with localhost to route traffic through the SSH tunnel, and saves it.
#      The first `sed` command replaces the remote server address with a free local port (6444).
#      The second `sed` command adds `insecure-skip-tls-verify: true` to bypass the hostname
#      mismatch error, which is common and acceptable when using an SSH tunnel for development.
ssh vm 'kubectl config view --raw' \
| sed -E 's|server: https://[0-9\.]+:[0-9]+|server: https://127.0.0.1:6444|' \
| sed '/certificate-authority-data:/c\    insecure-skip-tls-verify: true' > ~/remote-kube-config.yaml
#      in another terminal
#      NOTE: We use local port 6444 to avoid conflicts with Docker Desktop.
#      ssh -L 6444:192.168.164.2:6443 vm
#      If you see "bind: Address already in use", it means another process is using the port.
#      Inside WSL, you can check with `sudo lsof -i :6444` and stop it with `kill <PID>`.
#
# 2.2. Tell kubectl to use both your local and the new remote config.
#      This is temporary for your current terminal session. To make it permanent,
#      add this line to your `~/.bashrc` or `~/.zshrc` file.
#      export KUBECONFIG="$HOME/.kube/config:$HOME/remote-kube-config.yaml"
#
#      Verify it's set correctly by running:
#      echo $KUBECONFIG
#
# 2.3. Switch to the remote cluster's context.
#      First, see the name of the new context:
#      kubectl config get-contexts # Look for a name like 'kubernetes-admin@kubernetes'
#
#      Then, switch to it (replace <your-remote-context-name> with the actual context name):
#      kubectl config use-context <your-remote-context-name>
#
#      You can verify the active configuration with:
#      kubectl config view --minify
#      # The output should show the server as 'https://127.0.0.1:6443' and the
#      # current-context as your remote context.
#
# 2.4. Verify the connection. If this command shows the remote nodes, it worked!
#      kubectl get nodes
#

# 3. PREPARE AND RUN THE BENCHMARK
# --------------------------------
# This section guides you through setting up MinIO, uploading your data, and running the benchmark.

/mnt/sdc/puttbach
scp -r graphs/test/ node4:/mnt/sdc/puttbach/graphs/
# 3.1. Upload Necessary Files to MinIO
#      Your Spark jobs will read the graph data and algorithm JAR from a MinIO S3 bucket inside the cluster.
#      First, install and configure MinIO on the remote cluster.
#
#      a) SSH into the controller VM.
#         ssh vm
#
#      b) Inside the VM, clone your repository and run the MinIO startup script.
#         # Replace <your-repo-url> with the actual URL to your Git repository.
#         git clone <your-repo-url>
#         cd <your-repo-dir>/src/minio/
#         ./start_minio.sh # This uses Helm to deploy MinIO
#         exit # Exit the VM shell
#
#      c) On your LOCAL machine, open a port-forward to the MinIO service in a new terminal. Keep it running.
#         kubectl port-forward svc/myminio-hl 9000:9000 -n minio-tenant
#
#      d) In another LOCAL terminal, configure the MinIO Client (mc).
#         mc alias set myminio http://localhost:9000 minio minio123 --insecure
#
#      e) Create a bucket for your data.
#         mc mb myminio/mybucket --insecure
#
#      f) Upload your graph files and the Graphalytics JAR to the bucket.
#         # Adjust the source paths to where your files are located locally.
#         mc cp --recursive --insecure /mnt/d/mystuff2/master_thesis/src/benchmark/toUpload/graphs myminio/mybucket/
#         mc cp --insecure /path/to/your/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar myminio/mybucket/
#
# 4. RUN THE BENCHMARK
# --------------------
# 4.1. Set the K8S_MASTER_URL environment variable from your active kubectl context.
#      This tells your local spark-submit where to find the remote cluster.
#      export K8S_MASTER_URL="k8s://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
#
# 4.2. You can now run your benchmark scripts locally. They will target the remote cluster.
#      ./benchmark-constructor.sh 10 5 20
#      ./generated/10-5-20/generated-10-5-20.sh custom-scheduler
