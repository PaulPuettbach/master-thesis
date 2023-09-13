import math
from random import randint
from bitmap import BitMap

class Tenant:
    def __init__(self,id):
        self.id = id
    
    def __str__(self):
        return f'tenant({self.id})'
    
    def __repr__(self): 
        return f'tenant({self.id})'
    
class Task:
    def __init__(self,id, status, tenant, migration = False, migrate_to = ""):
        self.id = id
        self.status = status
        self.tenant = tenant
        self.migration = migration
        self.migrate_to = migrate_to
    
    def __str__(self):
        if not self.migration:
            return f'task({self.id}, status: {self.status}, ({self.tenant}))'
        else:
            return f'migration_task({self.id}, status: {self.status}, ({self.tenant}), migrate_to : {self.migrate_to})'
    
    def __repr__(self): 
        if not self.migration:
            return f'task({self.id}, status: {self.status}, ({self.tenant}))'
        else:
            return f'migration_task({self.id}, status: {self.status}, ({self.tenant}), migrate_to : {self.migrate_to})'
    
class Gene:
    def __init__(self,resource, tasksqueue):
        self.resource = resource
        self.tasksqueue = tasksqueue
    
    def __str__(self):
        return f'gene({self.tasksqueue} on {self.resource})'
    
    def __repr__(self): 
        return f'gene({self.tasksqueue} on {self.resource})' 

class Genotype:
    def __init__(self, gene_array):
        self.gene_array = gene_array

    def __str__(self):
        return f'genotype({self.gene_array})'
    
class Population:
    def __init__(self, population):
        self.population = population

    def __str__(self):
        return f'Population(size: {len(self.population)}, first 10 genotypes: {self.population[0:10]})'
    
class current_resources:
    def __init__(self, resource_array):
        self.resource_array = resource_array

    def __str__(self):
        return f'current_resources(size: {len(self.resource_array)}, list of nodes: {self.resource_array})'

def fairness(genotype):
    # iterate over the tasks and determine the place in queue for all tenants
    #current tenant ids is list of dictonary with the id and a tuple with count of the tasksqueue position and count of number of tasks

    #this is dictonary with tenant id as key and array of size 2. idx0: sum of taskqueue positions idx1: number of tasks the tenant has pending
    current_tenant_ids = {}
    for gene in genotype.gene_array:
        for task_queue_position in range(1,len(gene.tasksqueue)+1):
            if len(current_tenant_ids) == 0:
                current_tenant_ids[gene.tasksqueue[task_queue_position - 1].tenant.id] = [task_queue_position, 1]
            elif gene.tasksqueue[task_queue_position- 1].tenant.id not in current_tenant_ids:
                current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id] = [task_queue_position, 1]
            else:
                current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][0] = current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][0] + task_queue_position
                current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][1] = current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][1] + 1

    #mean squared error from mean taskqueue sum
    #normalizing would require debilitating performance cuts so we dont

    #values tasknumber divided by sum of taskposition list of number between 0 and 1 1 is best
    normalized_fairness = list(map(lambda x: x[1]/x[0], current_tenant_ids.values()))
    
    mean_normalized_fairness = sum(normalized_fairness)/len(current_tenant_ids)
    error = sum(map(lambda x : abs(x - mean_normalized_fairness), normalized_fairness))/len(normalized_fairness)

    #error between 0 and 1 0 being better but we want 1 being better so:
    return 1 - error

def locality(genotype):
    #without node level metrics it might be better to just check how many of the tasks of the same tenant are on the same node
    #calc average amount of ndoes that the tasks of the same tenant are scheduled to should be as low as possible
    #each gene has necessarily a different resource
    
    #this is dictonary with tenant id as key and array of size 3. idx0: number of tasks the tenant has pending, idx1: number of nodes they are scheduled on, idx2: the last resource a pedning task was encountered
    current_tenant_ids = {}
    gene_counter = 0
    for gene in genotype.gene_array:
        for task_queue_position in range(len(gene.tasksqueue)):
            if len(current_tenant_ids) == 0:
                current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id] = [1, 1, gene_counter]
            elif gene.tasksqueue[task_queue_position].tenant.id not in current_tenant_ids:
                current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id] = [1, 1, gene_counter]
            elif gene_counter >= current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id][2]:
                current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id][0] = current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id][0] +1
                if gene_counter > current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id][2]:
                    current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id][1] = current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id][1] +1
                    current_tenant_ids[gene.tasksqueue[task_queue_position].tenant.id][2] = gene_counter
        gene_counter = gene_counter + 1
    
    #number between 0 and 1 0 being better ratio of number of resources and number of tasks  
    noramlized_resources_per_tenant = list(map(lambda x : 0 if x[1]==1 else x[1]/x[0], current_tenant_ids.values()))
    
    #between 0 and 1, closer to 0 being better
    mean_noramlized_resources_per_tenant = sum(noramlized_resources_per_tenant)/ len(current_tenant_ids)

    #should be closer to 1 is better
    return 1 - mean_noramlized_resources_per_tenant



parent1 = Genotype([Gene("node1", [Task(2,"pending", Tenant(1)),Task(3,"pending", Tenant(1)), Task(4,"pending", Tenant(2))]), Gene("node2", [Task(1,"pending", Tenant(3))]), Gene("node3", [Task(8,"pending", Tenant(1)),Task(5,"pending", Tenant(1)), Task(7,"pending", Tenant(1)),Task(6,"pending", Tenant(2))])])
parent2 = Genotype([Gene("node1", [Task(8,"pending", Tenant(1)),Task(4,"pending", Tenant(2))]), Gene("node2", [Task(6,"pending", Tenant(2)), Task(1,"pending", Tenant(3)), Task(2,"pending", Tenant(1))]), Gene("node3", [Task(5,"pending", Tenant(1)),Task(3,"pending", Tenant(1)), Task(7,"pending", Tenant(1))])])


fair1 = fairness(parent1)
fair2 = fairness(parent2)

print(f"this is the parent1 fairness {fair1}")
print(f"this is the parent2 fairness {fair2}")

local1 = locality(parent1)
local2 = locality(parent2)

print(f"this is the parent1 locality {local1}")
print(f"this is the parent2 locality {local2}")

                
