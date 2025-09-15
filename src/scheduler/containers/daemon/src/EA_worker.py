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
general overview of the EA
order is init, random-seed, fitnesscalc, parent_selection, crossover, mutation, fitnesscalc, selection, parent_selection, repeat.


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

fitnesscalc: weighted sum of fitnessfunctions

parent selection: k tournament selection  change k to change selection pressure without replacement and it should be fine

crossover: k point two parent crossover

mutation : three adaptive mutation coefficents one for small step 

replacement: elitism 

lambda amount of children and mue number of parents


notes from book
we have a multimodal problem and we dont want a genetic drift
i would argue we dont even need to exchange the individuals because the problem definition changes somewhat often anyway

"""


#-------------------------------------------Logic-------------------------------------------


class Tenant:
    """
    tenant in the cluster

    Each Tenant on the spark cluster gains its own Tenant class

    Attributes:
        name (str): the name of the tenant on the cluster
        id (int): the unique id identifying the tenant
        __tenant_dict (list[Tenant]): a list of all unique Tenants
        __id_counter (int): current highest id

    Methods:
        __find_id: automatically assign the next available id for each new tenant
    """

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
    """
    each Task/Pod 
    
    Each Task or Pod that is needed for the Spark cluster gains its own Task class when it is
    marked for scheduling by the Kubernetes API

    
    Attributes:
        id (int): unique identifying the task/pod
        status (str): one of Pending Running Succeeded the status of the pod
        tenant (Tenant): the Tenant of the cluster that is resposible for submitting the pod
        migration (bool): not implemented yet for migrating the pod to another node
        migrate_to (Node): not implemented yet for migrating the pod to another node
    
    Methods:
        only python specific
    """
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
    """
    Gene class represents a Node and a queue for the pod to be scheduled to the node
    
    In line with the EA terminology this represents a unique part of the Genotype. Each Node with all task that should be scheduled to it is represented by one Gene.
    
    Attributes:
        resource (Node): Node class representing the unique node of the cluster
        taskqueue (list[Task]): the Tasks/Pods that should be scheduled to the Node
    
    Methods:
        the usual python nodes
    """
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
    """
    A compound of all Genes
    
    using the EA terminology for a single element of the population that contains Genes one for each Node in the cluster.
    
    Attributes:
        _gene_array (type): Description of attribute1.
        attribute2 (type): Description of attribute2.
    
    Methods:
        append_task: this method will test if the task is already part of the taskqueue of any Gene in the Genotype and only add it if not
    """
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
                break

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
    """
    All genotypes in the pool for the EA
    
    a class wrapper for the pool of individual genotypes
    
    Attributes:
        population_array (list[Genotype]): The complete list of all Genotypes in the pool
    
    Methods:
        the usual python functions
    """
    def __init__(self, population_array):
        if not ((all(isinstance(genotype, Genotype) for genotype in population_array)) or (len(population_array) == 0)) :
            raise ValueError("population_array should be a an empty list or contain Geneotype objects")
        self.population_array = population_array

    def __str__(self):
        return f'Population(size: {len(self.population_array)}, first 10 genotypes: {self.population_array[0:10]})'
    def __repr__(self):
        return f'Population(size: {len(self.population_array)}, first 10 genotypes: {self.population_array[0:10]})'
    
class Node:
    """
    a class representation for the nodes in the cluster
    
    Each node of the cluster is wrapped as an object of this class.
    
    Attributes:
        id (int): unique identitifyer of a node
    
    Methods:
        the usual python functions
    """
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
mutation_coefficient1 = 0.01
mutation_coefficient2 = 0.1
mutation_coefficient3 = 0.1

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





def fairness(genotype):
    """
    Calculates the fairness of a scheduling solution based on tenant task distribution.

    This function evaluates how equitably tasks from different tenants are
    scheduled across the nodes. It computes a normalized queue position score for
    each tenant, which is the number of their tasks divided by the sum of their
    queue positions (lower positions are better).

    It then calculates the mean absolute deviation of these scores from the
    overall average. The final fairness score is `1 - deviation`, where a value
    closer to 1.0 signifies a more fair distribution among tenants.

    Args:
        genotype (Genotype): The scheduling solution to evaluate, containing
            genes that map tasks to nodes.
    Returns:
        float: A fairness score between 0.0 and 1.0. A higher value indicates
            a more fair schedule. Returns 1.0 if there are no tasks to evaluate.
    Raises:
        NA
    """
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

    # If there are no tasks, the schedule is perfectly "fair" by definition.
    if not current_tenant_ids:
        return 1.0

    #mean error from mean taskqueue sum

    #values tasknumber divided by sum of taskposition list of number between 0 and 1 1 is best
    normalized_fairness = list(map(lambda x: x[1]/x[0], current_tenant_ids.values()))
    
    mean_normalized_fairness = sum(normalized_fairness)/len(current_tenant_ids)
    error = sum(map(lambda x : abs(x - mean_normalized_fairness), normalized_fairness))/len(normalized_fairness)

    #error between 0 and 1 0 being better but we want 1 being better so:
    return 1 - error

def locality(genotype):
    """
    Calculates the locality of a scheduling solution.

    Locality is a measure of how concentrated a tenant's tasks are on a minimal
    number of nodes. A higher locality score is better. The score is calculated
    by first determining a "spread ratio" for each tenant, which is the number
    of nodes their tasks are on divided by the number of tasks they have.

    The final score is `1 - average_spread_ratio`, where a value closer to 1.0
    indicates that tenants' tasks are well-concentrated.

    Args:
        genotype (Genotype): The scheduling solution to evaluate.
    Returns:
        float: A locality score between 0.0 and 1.0. A higher value indicates
            better locality. Returns 1.0 if there are no tasks to evaluate.
    Raises:
        NA
    """
    tenant_stats = {}  # {tenant_id: {'task_count': int, 'nodes': set()}}

    for gene in genotype._gene_array:
        for task in gene.tasksqueue:
            tenant_id = task.tenant.id
            if tenant_id not in tenant_stats:
                tenant_stats[tenant_id] = {'task_count': 0, 'nodes': set()}
            
            tenant_stats[tenant_id]['task_count'] += 1
            tenant_stats[tenant_id]['nodes'].add(gene.resource.id)

    if not tenant_stats:
        return 1.0

    spread_ratios = []
    for stats in tenant_stats.values():
        # If all tasks are on one node, spread is 0 (best).
        # Otherwise, calculate the ratio of nodes to tasks.
        spread_ratios.append(0.0 if len(stats['nodes']) <= 1 else len(stats['nodes']) / stats['task_count'])

    # Average spread across all tenants. Closer to 0 is better.
    mean_spread = sum(spread_ratios) / len(spread_ratios)

    # Invert so that 1.0 is the best score.
    return 1.0 - mean_spread

def fitness_eval(to_eval):
    """
    Calculates and assigns a fitness value to each genotype in a list.

    The fitness is a weighted sum of the `fairness` and `locality` scores.
    This function has a side effect of updating the global `best_solution`
    if a genotype with a higher fitness score is found.

    Args:
        to_eval (list[Genotype]): A list of genotypes to evaluate.
    Returns:
        NA
    Raises:
        TypeError: If the input is not a list of Genotype objects.
    """
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
            
def init(size):
    """
    Initializes the EA population and node resources.

    This function is called once at startup. It populates the list of
    `current_resources` by reading from a queue filled by an initial HTTP
    request. It then creates an initial `population` of `size` genotypes,
    where each genotype contains a gene for every available resource, but with
    empty task queues.

    Args:
        size (int): The number of genotypes to create for the initial population.
    Returns:
        NA
    Raises:
        NA
    """
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

def update_current_resources():
    """
    Updates node resources dynamically during runtime.

    Reads node update operations (add/delete) from a queue.
    - 'add': A new node is added to `current_resources`, and a corresponding
      empty gene is added to every genotype in the population.
    - 'delete': A node is removed. For each genotype, the corresponding gene
      is removed, and any tasks in its queue are randomly redistributed to
      the remaining genes.
    
    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
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
                        break
                genotype._gene_array.pop(to_remove_idx)

def add_tasks_to_genotype():
    """
    Adds new pending tasks to all genotypes in the population.

    Reads tasks from the `new_tasks` queue. For each new task, it is added
    to a randomly selected gene (node) in every genotype of the population.
    This random placement helps maintain diversity. After adding tasks, the
    global `best_solution` is reset as the problem definition has changed.
    
    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    global population
    global new_tasks
    global best_solution
    global n_new_tasks
    global no_task

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
        best_solution = Genotype([])
    
    #iterate n times. N being equal to poolsize
        # print(f"in the add tasks this taks is added test {task.id}", flush=True)
        # print(f"rec task counter in loop {rec_task_counter}, n_new_tasks in loop {n_new_tasks.value}")
        for genotype in population.population_array:
            resource_id = random.randint(0, len(genotype._gene_array) -1 )# including the last number so minus one for index
            genotype._gene_array[resource_id].tasksqueue.append(task)
    if rec_task_counter > 0:
        with n_new_tasks.get_lock():
            n_new_tasks.value -= rec_task_counter
        #     print(f"rec task counter after decrementing in the afetermath {rec_task_counter}, n_new_tasks in loop {n_new_tasks.value}")
        # print("we get in the setting no task value to false again", flush=True)
        with no_task.get_lock():
            no_task.value = False

def del_tasks_from_genotype():
    """
    Removes completed or succeeded tasks from all genotypes.

    Reads tasks from the `old_tasks` queue. For each completed task, it is
    removed from the task queue of the corresponding gene in every genotype.
    The global `best_solution` is reset as the problem has changed. If all
    tasks are removed, the `no_task` flag is updated.

    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    global population
    global old_tasks
    global best_solution
    global no_task
    global n_old_tasks

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
        best_solution = Genotype([])

        #print(f"removing this task {task.id}", flush=True)
        for genotype in population.population_array:
            for gene in genotype._gene_array:
                try:
                    gene.tasksqueue.remove(task)
                except ValueError:
                    pass # Task was not in this gene's queue, which is expected.
    if not [tasks for genes in population.population_array[0]._gene_array for tasks in genes.tasksqueue]:
        with no_task.get_lock():
            no_task.value = True
    with n_old_tasks.get_lock():
        n_old_tasks.value -= rec_task_counter


def selection(n, to_select):
    """
    Selects the top `n` individuals from a given list using (mu, lambda) selection.

    This function implements elitist selection by choosing the `n` individuals
    with the highest fitness scores from the `to_select` list (typically the
    child population). It uses an efficient quickselect-style algorithm to find
    the top `n` elements without fully sorting the entire list.

    Args:
        n (int): The number of individuals to select for the next generation.
        to_select (list[Genotype]): The pool of individuals (e.g., children)
            from which to select.
    Returns:
        list[Genotype]: A list containing the `n` fittest individuals.  
    Raises:
        NA
    """
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
    


def parent_selection(candidate_pool):
    """
    Selects a pool of parents using k-tournament selection.

    This method runs `n_parents` tournaments. In each tournament, `k`
    individuals are randomly sampled from the `candidate_pool`. The individual
    with the highest fitness from that sample is selected as a parent.
    Higher values of `k` increase selection pressure.

    Args:
        candidate_pool (list[Genotype]): The population from which to select parents.
    Returns:
        list[Genotype]: A list of `n_parents` selected for mating.
    Raises:
        NA
    """
    global n_parents
    global k

    if not isinstance(n_parents, int) or not isinstance(k, int):
        raise TypeError("parent selection called with wrong parameter type")
    if k >= len(candidate_pool) or k<1 :
        raise ValueError(f"parent selection called with k that is bigger or equal to the size of the canidate pool or with k smaller to one got {k}")

    mating_pool = []
    
    for _ in range(n_parents):
        #random.sample is without replacement which is what we want random.choice is with
        potential_mates = random.sample(candidate_pool, k)
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


    
   
def partially_mapped_crossover(parent1, parent2):
    """
    Creates a child genotype using a custom partially mapped crossover.

    This crossover operator is designed to combine traits from two parents
    while ensuring all tasks are scheduled exactly once. For each gene (node),
    the size of the task queue is inherited from one parent. The tasks to fill
    that queue are then drawn from both parents' corresponding genes, using a
    crossover point to decide which parent to prioritize. A bitmap is used to
    prevent duplicate tasks, and a `leftover_tasks` list handles tasks that
    could not be placed in their original gene context.

    Args:
        parent1 (Genotype): The first parent for crossover.
        parent2 (Genotype): The second parent for crossover.
    Returns:
        Genotype: The resulting child genotype.
    Raises:
        NA
    """
    global k_point

    if not isinstance(parent1, Genotype) or not isinstance(parent2, Genotype):
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if type(k_point) is not int:
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if not len(parent1._gene_array) == len(parent2._gene_array):
        raise Exception("k_point_crossover called with two parents of different length")
    
    child = Genotype([])
    
    leftover_tasks = []
    #used to just use the id as the index for the bitmap but ids only increase due to some synchronization issues that were fixed and you might have 3 tasks but one of them has an index of 500 which seems
    #wastefull to allocate so much memory in the bitmap this is a quick if hard to parse workaround the index of the id in the task_id_map is what is set and tested for the bitmap
    task_id_map = {task_obj.id: i for i, task_obj in enumerate(task_obj for gene_obj in parent1._gene_array for task_obj in gene_obj.tasksqueue)}
    # one more than number of tasks in total because id start at one and constant arithmatic is more wasterful then simply having one more unused bit
    bm = BitMap(len(task_id_map))
    gene_idx = 0
    while gene_idx < len(parent1._gene_array):
        #in case there are no tasks in the taskqueue for the resource
        if len(parent1._gene_array[gene_idx].tasksqueue) == 0:
            #have to clean up and continue to next gene
            cleanup_idx = 0
            cleanup_taks = parent2._gene_array[gene_idx].tasksqueue
            while cleanup_idx < len(cleanup_taks):
                if not bm.test(task_id_map[cleanup_taks[cleanup_idx].id]):
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
                    if not bm.test(task_id_map[chosen_parent_tasks[chosen_idx].id]):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        bm.set(task_id_map[chosen_parent_tasks[chosen_idx].id])
                        task_taken = True
                        chosen_idx = chosen_idx +1
                    else:
                        chosen_idx = chosen_idx +1
                        continue

                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(task_id_map[other_parent_tasks[other_idx].id]):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        bm.set(task_id_map[other_parent_tasks[other_idx].id])
                        task_taken = True
                        other_idx = other_idx +1
                    else:
                        other_idx = other_idx +1
                        continue
            else:
                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(task_id_map[other_parent_tasks[other_idx].id]):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        bm.set(task_id_map[other_parent_tasks[other_idx].id])
                        task_taken = True
                        other_idx = other_idx +1
                    else:
                        other_idx = other_idx +1
                        continue
                
                while not task_taken and chosen_idx < len(chosen_parent_tasks):
                    if not bm.test(task_id_map[chosen_parent_tasks[chosen_idx].id]):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        bm.set(task_id_map[chosen_parent_tasks[chosen_idx].id])
                        task_taken = True
                        chosen_idx = chosen_idx +1
                    else:
                        chosen_idx = chosen_idx +1
                        continue
            if not task_taken:
                for task_idx in range(len(leftover_tasks)-1):
                    if not bm.test(task_id_map[leftover_tasks[task_idx].id]):
                        child._gene_array[gene_idx].tasksqueue[child_task_idx] = leftover_tasks[task_idx]
                        leftover_tasks.pop(task_idx)
                        bm.set(task_id_map[leftover_tasks[task_idx].id])
                        task_taken = True
                        break
            child_task_idx = child_task_idx +1
        #after this point there is guranttedd to be a task in the child queue and any leftover go to leftovers
        while chosen_idx < len(chosen_parent_tasks):
            if not bm.test(task_id_map[chosen_parent_tasks[chosen_idx].id]):
                leftover_tasks.append(chosen_parent_tasks[chosen_idx])
            chosen_idx = chosen_idx +1
        
        while other_idx < len(other_parent_tasks):
            if not bm.test(task_id_map[other_parent_tasks[other_idx].id]):
                leftover_tasks.append(other_parent_tasks[other_idx]   )
            other_idx = other_idx +1
        gene_idx = gene_idx +1
        
    return child


def mutation(input_array):
    """
    Applies three types of mutation to a list of genotypes in-place.

    1.  **Swap Queues (mutation_coefficient1)**: Swaps the entire task queue
        between two randomly selected genes (nodes).
    2.  **Move Task (mutation_coefficient2)**: Moves a single random task from
        one gene's queue to another randomly selected gene's queue.
    3.  **Swap Tasks (mutation_coefficient3)**: Swaps the position of two
        tasks within the same gene's queue.

    Args:
        input_array (list[Genotype]): The list of genotypes to mutate.
    Raises:
        TypeError: If the input is not a list of Genotype objects.
    Raises:
        NA
    """
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
    """
    The main loop of the evolutionary algorithm.

    This function orchestrates the entire EA lifecycle, including initialization,
    evaluation, selection, crossover, and mutation, in an infinite loop.
    It also handles dynamic updates to tasks and nodes.

    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    global poolsize
    global no_task
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
            best_solution = Genotype([])
        
        if not current_resources:
            continue

        #could be that as we get batch update and then call deltasks overwriting the flag that there are new tasks
        #del tasks to genotype
        del_tasks_from_genotype()
        #add tasks to genotype dont need atomicity here if something is put into new_tasks or old_tasks from the batch update while this runs do it next epocj rather than lock
        add_tasks_to_genotype()

        if no_task.value:
            continue


        
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
            # for gene in best_solution._gene_array:
            #     for task in gene.tasksqueue:
            #         print(f"this is the pod id for all pods that should be scheduled {task.id}", flush=True)
            worker_thread = threading.Thread(target=update_solution, args=[best_solution])
            worker_thread.start()

        #selection
        population.population_array = selection(poolsize, children)
        
        #check for best solution and send happens in fitness_eval

        #loop                 


"""Egress"""
def update_solution(solution):
    """
    Sends the current best solution to the main scheduler service.

    This function is called when a new best solution is found. It formats the
    genotype into a JSON payload and POSTs it to the `/update-solution`
    endpoint of the main service.

    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    # print("sending out an update", flush=True)
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
        print(f"Request failed with status code {response.status_code}", flush=True)
    

"""Ingress"""
app = Flask(__name__)



def update_batch():
    """
    Handles batch processing of incoming task updates.

    This function runs in a separate thread and waits for task update events.
    When an event is received, it waits for a `buffer_time` to collect a
    batch of updates. It then processes the batch, sorting tasks into
    `new_tasks` (for pending pods) and `old_tasks` (for succeeded pods)
    queues, which are then consumed by the main EA loop.

    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    #thread event set by http ingress
    global tasks_arrived
    #global parameter
    global buffer_time
    #lock for the update 1
    global thread_lock_update_q
    #two multiprocessing quese used by the other process
    global new_tasks
    global old_tasks

    #use its lock is also multiprocessing value
    global no_task

    #used between threads also safe
    global update_q
    global n_old_tasks
    global n_new_tasks
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
                n_new_tasks.value = n_new_tasks.value + counter_new
        if counter_old > 0:
            with n_old_tasks.get_lock():
                n_old_tasks.value = n_old_tasks.value + counter_old
        with no_task.get_lock():
            no_task.value = False
    



#-----this is from the main scheduler-----#
#get updates of workqueue from the main scheduler
@app.route('/init', methods=['POST'])
def init_request():
    """
    Flask route to handle the initial cluster resource information.

    Receives a list of nodes at startup and populates the `resources_queue`.

    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
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
    """
    Flask route to handle dynamic changes in cluster nodes.

    Receives add/delete operations for nodes and puts them on the `resources_queue`.
    
    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    global resources_queue
    global node_update
    global n_node
    update = request.json
    resources_queue.put_nowait([Node(int(update[list(update)[0]])), update[list(update)[1]]])
    with n_node.get_lock():
        n_node.value += 1
    node_update.set()
    return('', 204)

@app.route('/health', methods=['GET'])
def health():
    """
    Flask route for a simple health check.
    
    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    required_endpoints = {'/init', '/update', '/health'}
    registered_endpoints = {rule.rule for rule in app.url_map.iter_rules()}

    if required_endpoints.issubset(registered_endpoints):
        return 'OK', 200
    else:
        return 'Service routes not ready', 50

@app.route('/update', methods=['POST'])
def update():
    """
    Flask route to receive task status updates from the main scheduler.

    Appends incoming task JSON objects to a temporary queue and sets an
    event to trigger the batch processing logic.

    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
    global thread_lock_update_q
    global tasks_arrived
    global update_q
    incoming_task = request.json
    # print(f"this is the incoming task {incoming_task}", flush=True)
    with thread_lock_update_q:
        update_q.append(incoming_task)
    tasks_arrived.set()
    return('OK', 200)

def util_process():
    """
    Manages the utility threads for handling HTTP requests and batching.

    This function starts the Flask server in one thread and the `update_batch`
    processor in the main thread of this process.
    Args:
        NA  
    Returns:
        NA
    Raises:
        NA
    """
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