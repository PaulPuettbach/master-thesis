# to download spark
download the tar file for spark
extract with tar -zxvf spark-x.x.x-bin-hadoopx.y.tgz
configure

# to make file for the spark cluster
make java code
call javac on te file  
take the resulting class file and any other things you need with it and call
jar cf jar-file input-file(s)
mvn clean package -DskipTests -Dmaven.buildNumber.skip

#what i changed so far
added new role to service account
# notes

look up the inversion thing for distributions

look up the state of the pods and try to take them only out if the state is finished

also make sure the node is still available

one issue is the available resources adding that into the calculatons has some problem a a pod does not need to specify that information and it incerases the complexity and overhead like crazy so I try the stop gap messure i simply check the status of the node if i everhave to change that:

for pod in pods.items:
    print(f"Pod Name: {pod.metadata.name}")
    for container in pod.spec.containers:
        print(f"  Container Name: {container.name}")
        
        # Get resource requests (what the container needs to run)
        requests = container.resources.requests
        if requests:
            cpu_request = requests.get('cpu')
            memory_request = requests.get('memory')
            print(f"    CPU Request: {cpu_request}")
            print(f"    Memory Request: {memory_request}")
        else:
            print("    No CPU or Memory requests specified.")
        
        # Get resource limits (the maximum resources the container can use)
        limits = container.resources.limits
        if limits:
            cpu_limit = limits.get('cpu')
            memory_limit = limits.get('memory')
            print(f"    CPU Limit: {cpu_limit}")
            print(f"    Memory Limit: {memory_limit}")
        else:
            print("    No CPU or Memory limits specified.")

status:
  allocatable:
    cpu: "4"
    memory: "16Gi"
    ephemeral-storage: "100Gi"

also just wget the datasets and copy the run script and the eniter properites flder from the original repo pick 
random interarrival time from the exponential dis at this time a random user is chosen with a random algorithm and a random graph and spark submitted

more schedulers
use minio
at least 3
look up continuem

this has what algorithms can be used with what graphs

# notes for minio
step one
export PATH=$PATH:$HOME/minio-binaries/

step two different console
kubectl port-forward svc/myminio-hl 9000 -n minio-tenant

step three
mc alias set myminio http://localhost:9000 minio minio123 --insecure

option after
mc mb myminio/mybucket --insecure

mc cp --recursive --insecure /mnt/d/mystuff2/master_thesis/src/benchmark/toUpload/ myminio/mybucket/

mc rm --insecure myminio/mybucket/

tar --use-compress-program=unzstd -xvf archive.tar.zst
