# to download spark
download the tar file for spark
extract with tar -zxvf spark-x.x.x-bin-hadoopx.y.tgz
configure

# to make file for the spark cluster
make java code
call javac on te file  
take the resulting class file and any other things you need with it and call
jar cf jar-file input-file(s)

delte the target dir
mvn clean package -DskipTests -Dmaven.buildNumber.skip

#what i changed so far
added new role to service account
# notes


also just wget the datasets and copy the run script and the eniter properites flder from the original repo pick 
random interarrival time from the exponential dis at this time a random user is chosen with a random algorithm and a random graph and spark submitted

find size for the graphs in benchmark/config_template/benchmarks
use "time" the bash command for the ttc "real" user is cpu time spend in the user space and then sys is the time spend in kernal space


!!!!!!!!!!!!!!!!!!!!!!!!!!!!! important have to check if i can keep queing stuff to nodes that are oom should be fine just have them pending n 

also the k for the sliding window i dependent on the current queue length which is the numnber of pending pods divided by the number of nodes

tar --use-compress-program=unzstd -xvf archive.tar.zst

kubectl run waitpod --namespace spark-namespace --image=busybox --restart=Never --overrides='{"apiVersion":"v1","spec":{"schedulerName":"custom-scheduler","containers":[{"name":"waitpod","image":"busybox","command":["/bin/sh","-c","sleep 60"],"env":[{"name":"SPARK_USER_MANUEL","value":"Frank"}],"resources":{"requests":{"memory":"64Mi"}}}],"restartPolicy":"Never"}}'

# Remote Cluster Access

curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

## 1. Configure Local Kubectl

Set the `KUBECONFIG` environment variable to use both local and remote configurations.
```bash
export KUBECONFIG="$HOME/.kube/config:$HOME/remote-kube-config.yaml"
kubectl config use-context kubernetes-admin@kubernetes --kubeconfig $HOME/remote-kube-config.yaml
```

In a separate terminal, create an SSH tunnel to the Kubernetes API server.
```bash
ssh -L 6444:192.168.164.2:6443 vm
```

## 2. Install and Access MinIO

First, install MinIO on the remote cluster by running the installation script on the controller VM.
```bash
ssh vm
helm repo add custom https://paulpuettbach.github.io/master-thesis/
helm install minio --namespace minio --create-namespace custom/operator
helm install minio-tenant --namespace minio-tenant --create-namespace custom/tenant

helm install minio-tenant -n minio-tenant --create-namespace custom/tenant\
  --set tenant.pools[0].servers=4 \
  --set tenant.pools[0].name=pool-0 \
  --set tenant.pools[0].volumesPerServer=1 \
  --set tenant.pools[0].size=100Gi \
  --set tenant.pools[0].storageClassName=local-path


Next, on your local machine, open a port-forward to the MinIO service in a new terminal.
```bash
kubectl port-forward svc/myminio-hl 9000:9000 -n minio-tenant
```

Finally, configure your local MinIO client (`mc`) in another terminal.
```bash
mc alias set myminio http://localhost:9000 minio minio123 --insecure
```



ssh vm
mkdir the correct directory
install helm curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
helm install minio
scp -r /mnt/sdc/puttbach cloud_controller_puttbach@192.168.164.2:/home/cloud_controller_puttbach/
mc the whole shebang
test the spark submit
# Configure your MinIO endpoint
mc alias set myminio http://<vm-ip>:9000 <ACCESS_KEY> <SECRET_KEY>


take the kubectl apply f first
