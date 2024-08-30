from flask import Flask, request
import requests
import math
import random
import time
from queue import Empty
from bitmap import BitMap
import threading
import os
import sys
import multiprocessing
import copy

"""
fairness based on queue are all tenants getting teh same resources

locality based on observed message latency is difficult needs to be added to each task

resource utilization dk how to do this one we can get state now and then guestimate i think
enregy efficiency is also resource utilization

order is init fitnesscalc parent_selection crossover mutation fitnesscalc selection parent_selection etc.



we do island model to preserve diversity no improvement for x generations

over selection: In this method, the population is first ranked by fitness and then divided into two groups, the top x% in
one and the remaining (100 - x)% in the other. When parents are selected, 80% of the selection operations
choose from the first group, and the other 20% from the second.

or we do:
(mu, lambda) Selection The (mu, lamda) strategy used in Evolution Strategies where
typically lamda>mu children are created from a population of mu parents. This
method works on a mixture of age and fitness. The age component means
that all the parents are discarded, so no individual is kept for more than
one generation (although of course copies of it might exist later). The fitness
component comes from the fact that the lamda offspring are ranked according to
the fitness, and the best mu form the next generation
    - this seems better because we have no interest in keeping parents since we want to escape local optima and
    the state of the problem changes with each new pod coming in so parent solutions become stale quick anyway
    it doesnt even safe computational effort as we will have to recalc fitness with each new pod anyway

init: random

fitnesscalc: fitnessfunctions

parent selection: k tournament selection  change k to change selection pressure without replacement and it should be fine

crossover: k point two parent crossover

mutation : three adaptive mutation coefficents one for small step 

replacement: elitism 


lambda amount of children and mue number of parents


notes from book
we have a multimodal problem and we dont want a genetic drift
i would argue we dont even need to exchange the individuals because the problem definition changes somewhat often anyway



"""



"""Init"""


"""Logic"""
class Tenant:

    __tenant_dict = {}
    __id_counter = -1
    def __init__(self,name):
        if not(isinstance(name, str)):
            raise ValueError(f"name str expected, got {name} with type {type(name)}")
        self.name = name
        self.id = self.__find_id()
    
    def __str__(self):
        return f'tenant({self.id}:{self.name})'
    
    def __repr__(self): 
        return f'tenant({self.id}:{self.name})'
    def __eq__(self, other):
        if isinstance(other, Tenant):
            return self.id == other.id
        else:
            raise ValueError(f"cant equate tenant to {type(other)}")
    def __find_id(self):
        if self.name in Tenant.__tenant_dict:
            return Tenant.__tenant_dict[self.name]
        else:
            Tenant.__id_counter += 1
            Tenant.__tenant_dict[self.name] = Tenant.__id_counter
            return Tenant.__id_counter
    
class Task:
    def __init__(self,id, status, tenant, migration=False, migrate_to=None):
        if not(isinstance(id, int) and id >= 0):
            raise ValueError(f"id as integer bigger or equal to 0 expected, got {id}")
        self.id = id

        allowed_status = ["Pending", "Running", "Succeeded"]
        if not(isinstance(status, str) and status in allowed_status):
            raise ValueError(f"status should be one of \"Pending\", \"Running\", \"Succeeded\" expected, got {status}")
        self.status = status

        if not(isinstance(tenant, Tenant)):
            raise ValueError(f"tenant instance expected, got object of type: {type(tenant)}")
        self.tenant = tenant

        if not isinstance(migration, bool):
            raise ValueError(f"migration should be boolean, got {migration}")
        self.migration = migration

        if not ((isinstance(migrate_to, Node) and migration == True) or (isinstance(migrate_to, type(None)) and migration == False)):
            raise ValueError(f"migrate_to should be Node or None if migration is set to false, got {migrate_to}")
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
    def __eq__(self, other):
        if isinstance(other, Task):
            return self.id == other.id and self.tenant == other.tenant
        else:
            raise ValueError(f"cant equate task to {type(other)}")
    
class Gene:
    def __init__(self, resource, tasksqueue):
        if not isinstance(resource, Node):
            raise ValueError(f"resource should be Node, got {resource}")
        self.resource = resource

        if not ((all(isinstance(task, Task) for task in tasksqueue)) or (len(tasksqueue) == 0)) :
            raise ValueError("tasksqueue should be a an empty list or contain Task objects")
        self.tasksqueue = tasksqueue
    
    def __str__(self):
        return f'gene({self.tasksqueue} on {self.resource})'
    
    def __repr__(self): 
        return f'gene({self.tasksqueue} on {self.resource})'
    
    def __eq__(self, other):
        if isinstance(other, Gene):
            return self.resource == other.resource and self.tasksqueue == other.tasksqueue
        else:
            raise ValueError(f"cant equate gene to {type(other)}")

class Genotype:
    def __init__(self, _gene_array, fitnessvalue=0.0):
        if not ((all(isinstance(gene, Gene) for gene in _gene_array)) or (len(_gene_array) == 0)) :
            raise ValueError("_gene_array should be a an empty list or contain Gene objects")
        self._gene_array = _gene_array

        if not isinstance(fitnessvalue, float) or fitnessvalue > 1 or fitnessvalue < 0:
            raise ValueError(f"fitnessvalue should be float between 0.0 and 1.0, got {fitnessvalue} with type {type(fitnessvalue)}")
        self.fitnessvalue = fitnessvalue
    
    #if its unclear if the task is already in the genotype use this
    def append_task(self, new_task, resource):
        if not isinstance(new_task, Task):
            raise ValueError(f"Only elements of type task can be appended, got {type(new_task)}")
        all_task_ids = {task.id for gene in self._gene_array for task in gene.tasksqueue}
        if any(new_task.id == id for id in all_task_ids):
            raise ValueError(f"Tasks need to be unique the task to append: {new_task}, is already in the genotype schedule")
        for gene in self._gene_array:
            if gene.resource == resource:
                gene.tasksqueue.append(new_task)

    def __str__(self):
        return f'genotype(fitness{self.fitnessvalue}, {self._gene_array})'
    def __repr__(self):
        return f'genotype(fitness{self.fitnessvalue}, {self._gene_array})'
    
    def __eq__(self, other):
        if isinstance(other, Genotype):
            return self._gene_array == other._gene_array
        else:
            raise ValueError(f"cant equate Genotype to {type(other)}")
    
class Population:
    def __init__(self, population_array):
        if not ((all(isinstance(genotype, Genotype) for genotype in population_array)) or (len(population_array) == 0)) :
            raise ValueError("population_array should be a an empty list or contain Geneotype objects")
        self.population_array = population_array

    def __str__(self):
        return f'Population(size: {len(self.population_array)}, first 10 genotypes: {self.population_array[0:10]})'
    def __repr__(self):
        return f'Population(size: {len(self.population_array)}, first 10 genotypes: {self.population_array[0:10]})'
    
class Node:
    def __init__(self,id):
        if not(isinstance(id, int) and id >= 0):
            raise ValueError(f"integer bigger or equal to 0 expected, got {id}")
        self.id = id
    
    def __str__(self):
        return f'Node(id: {self.id})'
    def __repr__(self):
        return f'Node(id: {self.id})'
    
    def __eq__(self, other):
        if isinstance(other, Node):
            return self.id == other.id
        else:
            raise ValueError(f"cant equate Node to {type(other)}")

"""
Parameters
"""
#buffer time for batch processing
buffer_time = 2
#how many genotypes in population pool
poolsize = 20
#number of children since ew do mu lambda should be bigger than poolsize
n_children = int(math.ceil(poolsize*1.5))
#how much fairness counts in the fitness
fairness_coef = 0.3
#how much locality counts in the fitness
local_coef = 0.7
#number of parents for the parent selection
n_parents = 5
#k for the parent selection
k = int(math.ceil(poolsize/5))
#hwo many crossover points for the crossover
k_point=1
#see mutation for explenation
mutation_coefficient1 = 0
mutation_coefficient2 = 0
mutation_coefficient3 = 0

#need the default value for the unit tests to work
main_service = os.getenv('MAIN-SERVICE', "did not import")


"""
gloabl variables
"""
# the whole population of the genotypes
population = Population([])
#a queue of the resource nodes arriving in init
resources_queue = multiprocessing.Queue()
#a list of the resource nodes available
current_resources = []
#the number of new tasks
n_new_tasks = multiprocessing.Value('i', 0)
#a queue of the tasks that still need to be added to the genotype
new_tasks = multiprocessing.Queue()
#the number of old tasks
n_old_tasks = multiprocessing.Value('i', 0)
#a array of all tasks that are unscheduled
old_tasks = multiprocessing.Queue()
# these two are for batch processing incoming tasks
update_q = []
# a lock for the threads for the updateq
thread_lock_update_q = threading.RLock()
#event that tasks arrived
tasks_arrived = threading.Event()
#event that init call arrived
http_init = multiprocessing.Event()
#number of nodes in init call
n_init = multiprocessing.Value('i', 0)
#event that init call arrived
node_update = multiprocessing.Event()
#number of nodes in init call
n_node= multiprocessing.Value('i', 0)
# weather the genotype task array is empty
no_task = multiprocessing.Value('b', True)
#the best current fitnessvalue
best_solution = Genotype([])
#weather there is a new best solution
new_best = False

#taken from https://stackoverflow.com/questions/8391411/how-to-block-calls-to-print
# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')


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
    for gene in genotype._gene_array:
        for task_queue_position in range(1,len(gene.tasksqueue)+1):
            if len(current_tenant_ids) == 0:
                current_tenant_ids[gene.tasksqueue[task_queue_position - 1].tenant.id] = [task_queue_position, 1]
            elif gene.tasksqueue[task_queue_position- 1].tenant.id not in current_tenant_ids:
                current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id] = [task_queue_position, 1]
            else:
                current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][0] = current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][0] + task_queue_position
                current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][1] = current_tenant_ids[gene.tasksqueue[task_queue_position- 1].tenant.id][1] + 1

    #mean squared error from mean taskqueue sum

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
    for gene in genotype._gene_array:
        for task_queue_position in range(len(gene.tasksqueue)):
            if len(current_tenant_ids) == 0 or gene.tasksqueue[task_queue_position].tenant.id not in current_tenant_ids:
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
    global fairness_coef
    global local_coef
    global best_solution
    global new_best
    if not isinstance(to_eval[0], Genotype):
        raise TypeError("fitness eval called with wrong parameter type")
    for genotype in to_eval:
        fitness = fairness_coef * fairness(genotype) + local_coef * locality(genotype)
        genotype.fitnessvalue = fitness
        if fitness > best_solution.fitnessvalue:
            best_solution = genotype
            new_best = True
            
    

#iterate n times. N being equal to poolsize


"""
input:  
    - size (int) is the number of genotypes creates
output: 
    - none
constrains:
    - the size has to fit the resource the deamon is scheduled on
description:
    Initialize the pool of genotypes with randomized Genotypes
"""
def init(size):
    global population
    global resources_queue
    global n_init
    global current_resources
    if type(size) is not int:
        raise TypeError("init called with wrong parameter type")


#iterate n times. N being equal to size
#M number of resources N population size means O(M)+O(N*M) is better if M < N
    rec_resource_counter = 0
    while True:
        with n_init.get_lock():
            #done
            if rec_resource_counter == n_init.value:
                break
        try:
            temp = resources_queue.get_nowait()
            resource = temp[0]
        except Empty:
            continue
        current_resources.append(resource)
        rec_resource_counter += 1 

    for i in range(size):
        genotype = Genotype([])
        for resource in current_resources:
            gene = Gene(resource, [])
            genotype._gene_array.append(gene)
        population.population_array.append(genotype)

"""
input:  
    - none
output: 
    - none
constrains:
    - none
description:
    add nodes from the queue to the current resources
"""

def update_current_resources():
    global resources_queue
    global n_node
    global current_resources
    global population
    rec_node_counter = 0

    while True:
        with n_node.get_lock():
            #done
            if rec_node_counter == n_node.value:
                break
        try:
            node, operation = resources_queue.get_nowait()
        except Empty:
            continue
        rec_node_counter += 1 
        if operation == "add":
            current_resources.append(node)
            for genotype in population.population_array:
                genotype._gene_array.append(Gene(node, []))
        elif operation == "delete":
            current_resources.remove(node)
            for genotype in population.population_array:
                to_remove_idx = None
                for idx, gene in  enumerate(genotype._gene_array):
                    if gene.resource == node:
                        to_remove_idx = idx
                        valid_new_gene_idx = [i for i in range(len(genotype._gene_array)) if i != idx]
                        for task in gene.tasksqueue:
                            genotype._gene_array[random.choice(valid_new_gene_idx)].tasksqueue.append(task)
                genotype._gene_array.pop(to_remove_idx)

"""
input:  
    - nothing
output: 
    - nothing
constrains:
    - none
description:
    Add the new not yet considered pending tasks to the population from the new task queue
"""
def add_tasks_to_genotype():
    global population
    global new_tasks

    rec_task_counter = 0

    while True:
        with n_new_tasks.get_lock():
            #done
            if rec_task_counter == n_new_tasks.value:
                break
        try:
            task = new_tasks.get_nowait()
        except Empty:
            continue
        rec_task_counter += 1 
    
    #iterate n times. N being equal to poolsize
        for genotype in population.population_array:
            resource_id = random.randint(0, len(genotype._gene_array) -1 )# including the last number so minus one for index
            genotype._gene_array[resource_id].tasksqueue.append(task)
    with n_new_tasks.get_lock():
        n_new_tasks.value = 0


"""
input:  
    - old_tasks ([tasks]) the old scheduled tasks at the time of the the function call
output: 
    - nothing
constrains:
    - none
description:
    delete the old already scheduled tasks from the genotype
"""
def del_tasks_from_genotype():
    global population
    global old_tasks

    rec_task_counter = 0

    while True:
        with n_old_tasks.get_lock():
            #done
            if rec_task_counter == n_old_tasks.value:
                break
        try:
            task = old_tasks.get_nowait()
        except Empty:
            continue
        rec_task_counter += 1 

        for gene in [gene for genotype in population.population_array for gene in genotype._gene_array]:
            try:
                gene.tasksqueue.remove(task)
            except ValueError:
                pass
    if not [tasks for genes in population.population_array[0]._gene_array for tasks in genes.tasksqueue]:
        with no_task.get_lock():
            no_task.value = True
    with n_old_tasks.get_lock():
        n_old_tasks.value = 0


"""
input:  
    - n: the number of individuals to select(should be the population size of the coming epoch)
    - to_select: the set of individuals from which to select the next population this is an array of genotypes
output: 
    - updated population
description:
    there should be mu parents and lambda children mixed together and we disregardded all the parents and must now choose among the lamda children
    the input shoudl just be the children and we do the rest when calling the function
"""
def selection(n, to_select):

    global population
    if n > len(to_select):
        return "Error: n is greater than the size of the array."
    if n == len(to_select):
        return to_select
    
    """utility functions start ------------------------------------------"""
    #standard quicksort adaption where we sort till we find the exact pivot that is the nth element from the top
    def partition(arr, low, high):
        pivot = arr[high].fitnessvalue
        i = low - 1

        for j in range(low, high):
            if arr[j].fitnessvalue >= pivot:
                i = i + 1
                #for every initial element where j >= i the element is getting switched with itself
                # i am guessing that would get optimized by the compiler but for now i test to save the obsolete
                # instruction
                if i == j:
                    continue
                arr[i], arr[j] = arr[j], arr[i]

        arr[i + 1], arr[high] = arr[high], arr[i + 1]
        return i + 1

    def quicksort(arr, low, high, n):
        pi = partition(arr, low, high)
        
        #found the nth biggest element as the pivot
        if pi == n:
            return arr[:n]
        #the pivot is not in the nth biggest elements we have to check from the pivot till the top of the array
        elif pi < n:
            return quicksort(arr, pi + 1, high, n)
        #the pivot is bigger than the nth element we should check from the end of the array to the pivot
        else:
            return quicksort(arr, low, pi - 1, n)
    """utility functions end ------------------------------------------"""

    return quicksort(to_select, 0, len(to_select) - 1, n)
    



"""
input:  
    - n_parents (int): is the number of parents returned
    - k(int): how many parents are considered for each tournament increase this to increase the
    selection pressure, has to be less than n parents otherwise it is always the top one
    and if k ==1 it is random
output: 
    - a set of parents to be taken for crossover
description:
    find a number of parents acording to tournament selction the higher the k the higher the selection pressure
"""
def parent_selection(population):
    global n_parents
    global k

    if not isinstance(n_parents, int) or not isinstance(k, int):
        raise TypeError("parent selection called with wrong parameter type")
    if k >= len(population) or k<1 :
        raise ValueError(f"parent selection called with k that is bigger or equal to the size of the canidate pool or with k smaller to one got {k}")

    mating_pool = []
    
    for _ in range(n_parents):
        #random.sample is without replacement which is what we want random.choice is with
        potential_mates = random.sample(population, k)
        best_mate = max(potential_mates, key=lambda mate: mate.fitnessvalue)
        mating_pool.append(best_mate)

    return mating_pool
        


# BEGIN
# /* Assume we wish to select lamda members of a pool of mu individuals */
# set current member = 1;
# WHILE ( current member ≤ lamda ) DO
# Pick k individuals randomly, with or without replacement;
# Compare these k individuals and select the best of them;
# Denote this individual as i;
# set mating pool[current member] = i;
# set current member = current member + 1;
# OD
# END

#iterate n times. N being equal to poolsize


    
   
"""
input:  
    - first parent (Genotype): the first parent for the crossover
    - second parent(Genotype): the second parent for the crossover
    - k_point(int): number of crossoverpoints
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
def partially_mapped_crossover(parent1, parent2):
    global k_point

    if not isinstance(parent1, Genotype) or not isinstance(parent2, Genotype):
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if type(k_point) is not int:
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if not len(parent1._gene_array) == len(parent2._gene_array):
        raise Exception("k_point_crossover called with two parents of different length")
    
    child = Genotype([])
    
    leftover_tasks = []
    # one more than number of tasks in total because id start at one and constant arithmatic is more wasterful then simply having one more unused bit
    bm = BitMap(9)
    gene_idx = 0
    while gene_idx < len(parent1._gene_array):
        #in case there are no tasks in the taskqueue for the resource
        if len(parent1._gene_array[gene_idx].tasksqueue) == 0:
            #have to clean up and continue to next gene
            cleanup_idx = 0
            cleanup_taks = parent2._gene_array[gene_idx].tasksqueue
            while cleanup_idx < len(cleanup_taks):
                if not bm.test(cleanup_taks[cleanup_idx].id):
                    leftover_tasks.append(cleanup_taks[cleanup_idx])
                cleanup_idx = cleanup_idx +1


        temp = k_point
        while True:  
            n_tasks_per_chunk = math.floor(len(parent1._gene_array[gene_idx].tasksqueue) / (temp+1))
            if n_tasks_per_chunk == 0:
                temp = temp-1
                if temp == 0:
                    #there is only one task
                    n_tasks_per_chunk = 1
                    break
            else:
                break

        child._gene_array.append(Gene(parent1._gene_array[gene_idx].resource,[]))

        #the taskqueue size is taken from one parent
        for i in range(len(parent1._gene_array[gene_idx].tasksqueue)):
            child._gene_array[gene_idx].tasksqueue.append(0)

        child_task_idx = 0
        chosen_idx = 0
        other_idx = 0
        coin = random.randint(0,1)
        if coin:
            chosen_parent_tasks = parent1._gene_array[gene_idx].tasksqueue
            other_parent_tasks = parent2._gene_array[gene_idx].tasksqueue
        else:
            chosen_parent_tasks = parent2._gene_array[gene_idx].tasksqueue
            other_parent_tasks = parent1._gene_array[gene_idx].tasksqueue


        while child_task_idx <  len(child._gene_array[gene_idx].tasksqueue):

            task_taken = False
            
            if child_task_idx < n_tasks_per_chunk:
                while not task_taken and chosen_idx < len(chosen_parent_tasks):
                    if not bm.test(chosen_parent_tasks[chosen_idx].id):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        bm.set(chosen_parent_tasks[chosen_idx].id)
                        task_taken = True
                        chosen_idx = chosen_idx +1
                    else:
                        chosen_idx = chosen_idx +1
                        continue

                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(other_parent_tasks[other_idx].id):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        bm.set(other_parent_tasks[other_idx].id)
                        task_taken = True
                        other_idx = other_idx +1
                    else:
                        other_idx = other_idx +1
                        continue
            else:
                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(other_parent_tasks[other_idx].id):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        bm.set(other_parent_tasks[other_idx].id)
                        task_taken = True
                        other_idx = other_idx +1
                    else:
                        other_idx = other_idx +1
                        continue
                
                while not task_taken and chosen_idx < len(chosen_parent_tasks):
                    if not bm.test(chosen_parent_tasks[chosen_idx].id):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        bm.set(chosen_parent_tasks[chosen_idx].id)
                        task_taken = True
                        chosen_idx = chosen_idx +1
                    else:
                        chosen_idx = chosen_idx +1
                        continue
            if not task_taken:
                for task_idx in range(len(leftover_tasks)-1):
                    if not bm.test(leftover_tasks[task_idx].id):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = leftover_tasks[task_idx]
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
                leftover_tasks.append(other_parent_tasks[other_idx]   )
            other_idx = other_idx +1
        gene_idx = gene_idx +1
        
    return child


"""
input:  
    - input_array([Genotype]): a list of all the Genotypes that need to be mutated
    - mutation_coefficient1(float): between 0 and 1 mutation chance for an entire taskqueue to be swapped between nodes
    - mutation_coefficient2(float): between 0 and 1 mutation chance for each of the genes that a random task is taken and added to another node/gene
    - mutation_coefficient3(float): between 0 and 1 mutation chance for each task to switch with another task on the same node
output:
    - no return value input array is mutated in place
constrains:
    - same order of input and output array
description:
    for each resource node in the schedule of each individual (genotype) there is a chance to swap one task of that resource node with the task of another
    resource node: important to note if the second chosen node does not have any resources no further attempts are made to find another node to save compution time
    this should be included in the determination of the mutation coefficient 
"""
def mutation(input_array):
    global mutation_coefficient1
    global mutation_coefficient2
    global mutation_coefficient3
    


    if not isinstance(input_array[0], Genotype):
        raise TypeError("mutation called with wrong parameter type")
    
    for genotype in input_array:
        if random.random() <= mutation_coefficient1:
            switch_taskque_gene_1_idx = random.choice(range(len(genotype._gene_array)))
            idx_except_current = [i for i in range(len(genotype._gene_array)) if i != switch_taskque_gene_1_idx]
            if idx_except_current:
                switch_taskque_gene_2_idx = random.choice(idx_except_current)

                temp = genotype._gene_array[switch_taskque_gene_1_idx].tasksqueue
                genotype._gene_array[switch_taskque_gene_1_idx].tasksqueue = genotype._gene_array[switch_taskque_gene_2_idx].tasksqueue 
                genotype._gene_array[switch_taskque_gene_2_idx].tasksqueue = temp

        if mutation_coefficient2>0 or mutation_coefficient3>0:
            for idx, gene in enumerate(genotype._gene_array):
                
                if random.random() <= mutation_coefficient2 and len(gene.tasksqueue) != 0:
                    #need to switch one task of the current gene with another task of a random gene
                    idx_except_current = [i for i in range(len(genotype._gene_array)) if i != idx]
                    if(idx_except_current):
                        gene_other_idx = random.choice(idx_except_current)
                        task = random.choice(gene.tasksqueue)
                        genotype._gene_array[gene_other_idx].tasksqueue.append(task)
                        gene.tasksqueue.remove(task)

                if mutation_coefficient3 > 0 and len(gene.tasksqueue) >=2:
                    for task_idx, task in enumerate(gene.tasksqueue):
                        if random.random() <= mutation_coefficient3:
                            idx_except_current = [i for i in range(len(gene.tasksqueue)) if i != task_idx]
                            if(idx_except_current):
                                switch_task_idx = random.choice(idx_except_current)
                                
                                temp = gene.tasksqueue[task_idx]
                                gene.tasksqueue[task_idx] = gene.tasksqueue[switch_task_idx] 
                                gene.tasksqueue[switch_task_idx] = temp

def epoch():
    global poolsize
    global no_task
    global new_tasks
    global old_tasks
    global n_children
    global population
    global best_solution
    global new_best
    global http_init
    global node_update
    
    http_init.wait()
    init(poolsize)
    while True:
        if no_task.value:
            continue
        if node_update.is_set():
            update_current_resources()
        
        if not current_resources:
            continue

        #add tasks to genotype dont need atomicity here if something is put into new_tasks or old_tasks from the batch update while this runs do it next epocj rather than lock
        add_tasks_to_genotype()
        #del tasks to genotype
        del_tasks_from_genotype()


        
        #fitnesscalc
        fitness_eval(population.population_array)

        #parent_selection
        parents = parent_selection(population.population_array)

        #crossover
        children = []
        for i in range(n_children):
            children.append(partially_mapped_crossover(random.choice(parents), random.choice(parents)))

        #mutation
        mutation(children)

        #fitnesscalc
        fitness_eval(children)

        if new_best:
            new_best = False
            worker_thread = threading.Thread(target=update_solution, args=[best_solution])
            worker_thread.start()

        #selection
        population.population_array = selection(poolsize, children)
        
        #check for best solution and send happens in fitness_eval

        #loop                 


"""Egress"""
def update_solution(solution):
    url = f"http://{main_service}/update-solution"
    json_obj = {"fitness": solution.fitnessvalue}
    for gene in solution._gene_array:
        json_obj[gene.resource.id] = []
        for task in gene.tasksqueue:
            json_obj[gene.resource.id].append(task.id)
    response = requests.post(url, json = json_obj)
    if response.status_code < 400:
        return response
    else:
        print(f"Request failed with status code {response.status_code}")
    

"""Ingress"""
app = Flask(__name__)



def update_batch():
    global tasks_arrived
    global buffer_time
    global thread_lock_update_q
    global new_tasks
    global old_tasks
    global no_task
    global update_q
    while True:
        tasks_arrived.wait()


        #generate batch
        while tasks_arrived.is_set():
            tasks_arrived.clear()
            time.sleep(buffer_time)

        with thread_lock_update_q:
            temp_copy = copy.deepcopy(update_q)
            update_q = []
        counter_new = 0
        counter_old = 0
        for task_raw in temp_copy:
            task_cast = Task(task_raw['id'], task_raw['status'], Tenant(task_raw['tenant']))
            if task_cast.status == "Pending":                
                new_tasks.put_nowait(task_cast)
                counter_new += 1
            if task_cast.status == "Succeeded":
                old_tasks.put_nowait(task_cast)
                counter_old += 1
            if counter_new > 0:
                with n_new_tasks.get_lock():
                    n_new_tasks.value = counter_new
            if counter_old > 0:
                with n_old_tasks.get_lock():
                    n_old_tasks.value = counter_old
            with no_task.get_lock():
                no_task.value = False
    



#-----this is from the main scheduler-----#
#get updates of workqueue from the main scheduler
@app.route('/init', methods=['POST'])
def init_request():
    global resources_queue
    global http_init
    global n_init
    # first time init is called
    counter = 0
    for node_id in request.json.keys():
        resources_queue.put_nowait((Node(int(node_id)), "add"))
        counter += 1
    with n_init.get_lock():
        n_init.value = counter
    http_init.set()
    return('', 204)

@app.route('/node-change', methods=['POST'])
def node_change():
    global resources_queue
    global node_update
    global n_node
    update = request.get_json()
    resources_queue.put_nowait([Node(int(update[list(update)[0]])), update[list(update)[1]]])
    with n_node.get_lock():
        n_node.value += 1
    node_update.set()
    return('', 204)

@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200

@app.route('/update', methods=['POST'])
def update():
    global thread_lock_update_q
    global tasks_arrived
    global update_q
    incoming_task = request.json
    with thread_lock_update_q:
        update_q.append(incoming_task)
        tasks_arrived.set()
    return 'OK', 200

def util_process():
    #IO bound
    flask_thread = threading.Thread(target=app.run, kwargs={'host': '0.0.0.0' , 'port': '80'})
    flask_thread.start()
    #cpu bound
    update_batch()


if __name__ == '__main__':

    #CPU bound
    batch_process = multiprocessing.Process(target=util_process)
    batch_process.start()
    epoch()