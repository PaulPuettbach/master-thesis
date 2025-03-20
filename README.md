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

find size for the graphs in benchmark/config_template/benchmarks
use "time" the bash command for the ttc "real" user is cpu time spend in the user space and then sys is the time spend in kernal space


!!!!!!!!!!!!!!!!!!!!!!!!!!!!! important have to check if i can keep queing stuff to nodes that are oom should be fine just have them pending n 

also the k for the sliding window i dependent on the current queue length which is the numnber of pending pods divided by the number of nodes

# notes for minio
step one
export PATH=$PATH:$HOME/minio-binaries/

step two different console
kubectl port-forward svc/myminio-hl 9000 -n minio-tenant

kubectl get pods -n spark-namespace -o jsonpath='{range .items[?(@.status.conditions[*].reason=="Unschedulable")]}{.metadata.name}{"\n"}{end}'


step three
mc alias set myminio http://localhost:9000 minio minio123 --insecure

option after
mc mb myminio/mybucket --insecure

mc cp --recursive --insecure /mnt/d/mystuff2/master_thesis/src/benchmark/toUpload/graphs myminio/mybucket/

or this
mc cp --recursive --insecure /mnt/c/Users/paulp/Documents/my_stuff/comp_sci_master/master-thesis/src/benchmark/toUpload/graphs myminio/mybucket/

mc rm --insecure myminio/mybucket/

tar --use-compress-program=unzstd -xvf archive.tar.zst
