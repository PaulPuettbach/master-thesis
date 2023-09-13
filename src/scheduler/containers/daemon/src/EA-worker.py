from flask import Flask, request
import math
from random import randint, random
from bitmap import BitMap

app = Flask(__name__)
"""
fairness based on queue are all tenants getting teh same resources

locality based on observed message latency is difficult needs to be added to each task

resource utilization dk how to do this one we can get state now and then guestimate i think
enregy efficiency is also resource utilization

order is init crossover mutation fitnesscalc selection crossover etc.



we do island model to preserve diversity no improvement for x generations

over selection: In this method, the population is first ranked by fitness and then divided into two groups, the top x% in
one and the remaining (100 - x)% in the other. When parents are selected, 80% of the selection operations
choose from the first group, and the other 20% from the second.

init: random

fitnesscalc: fitnessfunctions

parent selection: k tournament selection  change k to change selection pressure

crossover: k point two parent crossover

mutation : three adaptive mutation coefficents one for small step 

replacement: elitism 


lambda amount of children and mue number of parents
"""

"""Ingress"""

#-----this is from the main scheduler-----#
#get updates of workqueue from the main scheduler
@app.route('/workqueue', methods=['POST'])
def workqueue():
    print("update from main scheduler to the workqueue")

# # get updates on own resource utilization to change pool size
# @app.route('/change_pool', methods=['POST'])
# def node_status():
#     print("update from main scheduler to the pool")


"""Logic"""
class tenant:
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
        return f'genome({self.tasksqueue} on {self.resource})'
    
    def __repr__(self): 
        return f'genome({self.tasksqueue} on {self.resource})' 

class Genotype:
    def __init__(self, gene_array, fitnessvalue):
        self.gene_array = gene_array
        self.fitnessvalue = fitnessvalue

    def __str__(self):
        return f'genotype(fitness{self.fitnessvalue}, {self.gene_array})'
    
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

"""
gloabl variables
"""
fairness_coef = 0.3
local_coef = 0.7


"""
all the fitness functions
input:  
    - to_eval ([Genotype]): set of Genotypes to eval for fitness
output: 
    - NA the input will be updated with new fitness values
description:
take the input set and recalculate the fitness value. the function is :
fairness_coef  * fairness + local_coef * locality + res_util_coef * res_util + energy_coef * energy - scaling_pen_coef scaling_pen

"""

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





def fitness_eval(to_eval):
    if not isinstance(to_eval[0], Genotype):
        raise TypeError("fitness eval called with wrong parameter type")
    for genotype in to_eval:
        genotype.fitnessvalue = fairness_coef * fairness(genotype) + local_coef * locality(genotype)
    

#iterate n times. N being equal to poolsize


"""
input:  
    - poolsize (int) is the number of genotypes creates
    - inital_taks_queue ([tasks]) the pending tasks at the time of the the init
output: 
    - a full pool of genotypes
constrains:
    - the poolsize has to fit the resource the deamon is scheduled on
description:

Initialize the pool of genotypes with randomized Genotypes
"""
def init(poolsize, inital_taks_queue):
    if type(poolsize) is not int:
        raise TypeError("init called with wrong parameter type")
    if not isinstance(inital_taks_queue[0], Task):
        raise TypeError("init called with wrong parameter type")
    if len(inital_taks_queue) < 1:
        raise Exception("init called with empty task queue")
    
    pool = []

#iterate n times. N being equal to poolsize
    for i in range(poolsize):
        genotype = Genotype([], 0)
        for resource in current_resources.resource_array:
            gene = Gene(resource, [])
            genotype.gene_array.append(gene)
        for task in inital_taks_queue:
            resource_id = randint(0, len(current_resources.resource_array))# including the last number so minus one for index
            genotype.gene_array[resource_id].tasksqueue.append(task)
        pool.append(genotype)
        fitness_eval(pool)
    return pool


"""
input:  
    - n_parents (int): is the number of parents returned
    - k(int): how many parents are considered for each tournament
output: 
    - a set of parents to be taken for crossover
constrains:
    - the poolsize has to fit the resource the deamon is scheduled on
description:

Initialize the pool of genotypes with randomized Genotypes
"""
def parent_selection(n_parents, k):
    if type(n_parents) is not int:
        raise TypeError("parent selection called with wrong parameter type")
    if type(k) is not int:
        raise TypeError("parent selection called with wrong parameter type")

#iterate n times. N being equal to poolsize


    
   
"""
input:  
    - first parent (Genotype): the first parent for the crossover
    - second parent(Genotype): the second parent for the crossover
    - k(int): number of crossoverpoints
output:
    - child (Genotype): a child that is a combination of the parents
constrains:
    - Small changes to a genotype induce small changes in the corresponding phenotypes
    - tasks cannot be duplicated and all tasks have to be assigned
description:
has to be meaningful representation of both parents. Therefore, for each gene for the taskqueue it is important where the task is placed in what order and how many there are placed on a node.
To do this for each child take the number of tasks per node from one parent wholesale. For the order and what node thez are placed on take the taskqueue from both parents decide a crossoverpoint
and for the first tasks up to the crossover point try to take primarly from one parent and for every task after the crossover point try to take primarly from the other. The order is try to take 
from the desired parent if that does not work try to take from the other parent if that does not work take from the tasks that are leftover. The tasks that are leftover are updated after each gene
by taking all the leftover tasks from both parents and adding them to the leftover tasks.

"""
def partially_mapped_crossover(parent1, parent2, k=1):
    if not isinstance(parent1, Genotype) or not isinstance(parent2, Genotype):
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if type(k) is not int:
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if not len(parent1.gene_array) == len(parent2.gene_array):
        raise Exception("k_point_crossover called with two parents of different length")
    
    child = Genotype([])
    for index in range(len(parent1.gene_array)):
        child.gene_array.append(Gene("placeholder",[]))
    
    leftover_tasks = []
    # one more than number of tasks in total because id start at one and constant arithmatic is more wasterful then simply having one more unused bit
    bm = BitMap(9)
    gene_idx = 0
    while gene_idx < len(child.gene_array):
        temp = k
        while True:  
            n_tasks_per_chunk = math.floor(len(parent1.gene_array[gene_idx].tasksqueue) / (temp+1))
            if n_tasks_per_chunk == 0:
                temp = temp-1
            else:
                break

        child.gene_array[gene_idx].resource = parent1.gene_array[gene_idx].resource

        #the taskqueue size is taken from one parent
        for i in range(len(parent1.gene_array[gene_idx].tasksqueue)):
            child.gene_array[gene_idx].tasksqueue.append(0)

        child_task_idx = 0
        chosen_idx = 0
        other_idx = 0
        coin = randint(0,1)
        if coin:
            chosen_parent_tasks = parent1.gene_array[gene_idx].tasksqueue
            other_parent_tasks = parent2.gene_array[gene_idx].tasksqueue
        else:
            chosen_parent_tasks = parent2.gene_array[gene_idx].tasksqueue
            other_parent_tasks = parent1.gene_array[gene_idx].tasksqueue


        while child_task_idx <  len(child.gene_array[gene_idx].tasksqueue):

            task_taken = False
            
            if child_task_idx < n_tasks_per_chunk:
                while not task_taken and chosen_idx < len(chosen_parent_tasks):
                    if not bm.test(chosen_parent_tasks[chosen_idx].id):
                        child.gene_array[gene_idx].tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        bm.set(chosen_parent_tasks[chosen_idx].id)
                        task_taken = True
                        chosen_idx = chosen_idx +1
                    else:
                        chosen_idx = chosen_idx +1
                        continue

                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(other_parent_tasks[other_idx].id):
                        child.gene_array[gene_idx].tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        bm.set(other_parent_tasks[other_idx].id)
                        task_taken = True
                        other_idx = other_idx +1
                    else:
                        other_idx = other_idx +1
                        continue
            else:
                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(other_parent_tasks[other_idx].id):
                        child.gene_array[gene_idx].tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        bm.set(other_parent_tasks[other_idx].id)
                        task_taken = True
                        other_idx = other_idx +1
                    else:
                        other_idx = other_idx +1
                        continue
                
                while not task_taken and chosen_idx < len(chosen_parent_tasks):
                    if not bm.test(chosen_parent_tasks[chosen_idx].id):
                        child.gene_array[gene_idx].tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        bm.set(chosen_parent_tasks[chosen_idx].id)
                        task_taken = True
                        chosen_idx = chosen_idx +1
                    else:
                        chosen_idx = chosen_idx +1
                        continue
            if not task_taken:
                for task_idx in range(len(leftover_tasks)-1):
                    if not bm.test(leftover_tasks[task_idx].id):
                        child.gene_array[gene_idx].tasksqueue[child_task_idx] = leftover_tasks[task_idx]
                        leftover_tasks.pop(task_idx)
                        bm.set(leftover_tasks[task_idx].id)
                        task_taken = True
                        break
            child_task_idx = child_task_idx +1
        #after this point there is guranttedd to be a task in the child queue and any leftover go to leftovers
        while chosen_idx < len(chosen_parent_tasks):
            if not bm.test(chosen_parent_tasks[chosen_idx].id):
                leftover_tasks.append(chosen_parent_tasks[chosen_idx])
            chosen_idx = chosen_idx +1
        
        while other_idx < len(other_parent_tasks):
            if not bm.test(other_parent_tasks[other_idx].id):
                leftover_tasks.append(other_parent_tasks[other_idx])
            other_idx = other_idx +1
        gene_idx = gene_idx +1
        
    return child


"""
input:  
    - input_array([Genotype]): a list of all the Genotypes that need to be mutated
    - mutation_coefficient1(float): between 0 and 1 mutation chance for each of the genes
output:
    - output_array([Genotype]): a list of all the now mutated Genotypes
constrains:
    - same order of input and output array
"""
def mutation(input_array, mutation_coefficient1, mutation_coefficient2, mutation_coefficient3):
    if not isinstance(input_array[0], Genotype):
        raise TypeError("init called with wrong parameter type")
    for genotype in input_array:
        if random() <= mutation_coefficient1:
            


# for i in range(k +1):
#             #on even take parent 1
#             if i % 2 == 0:
#                 #the last bit
#                 if i == k:
#                     child.append(parent1.array[(i * n_genes_per_chunk):])
#                 else:
#                     child.append(parent1.array[(i * n_genes_per_chunk):((i+1) * (n_genes_per_chunk))])
#             #on odd take parent 2
#             if i % 2 == 1:
#                             #the last bit
#                 if i == k:
#                     child.append(parent2.array[(i * n_genes_per_chunk):])
#                 else:
#                     child.append(parent2.array[(i * n_genes_per_chunk):((i+1) * (n_genes_per_chunk))])
#     return child

"""Egress"""
