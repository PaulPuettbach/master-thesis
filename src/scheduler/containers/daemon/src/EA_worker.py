from flask import Flask, request
import math
import random
from bitmap import BitMap

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
#get the initalwokqueue


"""Logic"""
class Tenant:
    def __init__(self,id):
        if not(isinstance(id, int) and id >= 0):
            raise ValueError(f"integer bigger or equal to 0 expected, got {id}")
        self.id = id
    
    def __str__(self):
        return f'tenant({self.id})'
    
    def __repr__(self): 
        return f'tenant({self.id})'
    def __eq__(self, other):
        if isinstance(other, Tenant):
            return self.id == other.id
        else:
            raise ValueError(f"cant equate tenant to {type(other)}")
    
class Task:
    def __init__(self,id, status, tenant, migration=False, migrate_to=None):
        if not(isinstance(id, int) and id >= 0):
            raise ValueError(f"id as integer bigger or equal to 0 expected, got {id}")
        self.id = id

        allowed_status = ["Pending", "Schedule", "Finished"]
        if not(isinstance(status, str) and status in allowed_status):
            raise ValueError(f"status should be one of \"Pending\", \"Schedule\", \"Finished\" expected, got {status}")
        self.status = status

        if not(isinstance(tenant, Tenant) and tenant.id >= 0):
            raise ValueError(f"tenant should be integer bigger or equal to 0 expected, got {tenant}")
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
        return f'genome({self.tasksqueue} on {self.resource})'
    
    def __repr__(self): 
        return f'genome({self.tasksqueue} on {self.resource})'
    
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
    
class CurrentResources:
    def __init__(self, resource_array):
        if not ((all(isinstance(node, Node) for node in resource_array)) or (len(resource_array) == 0)) :
            raise ValueError("resource_array should be a an empty list or contain Node objects")
        self.resource_array = resource_array

    def __str__(self):
        return f'current_resources(size: {len(self.resource_array)}, list of nodes: {self.resource_array})'
    def __repr__(self):
        return f'current_resources(size: {len(self.resource_array)}, list of nodes: {self.resource_array})'

class CurrentTaskqueue:
    def __init__(self, task_array):
        if not ((all(isinstance(task, Task) for task in task_array)) or (len(task_array) == 0)) :
            raise ValueError("task_array should be a an empty list or contain Task objects")
        self.task_array = task_array

    def __str__(self):
        return f'current_resources(size: {len(self.resource_array)}, list of nodes: {self.resource_array})'
    def __repr__(self):
        return f'current_resources(size: {len(self.resource_array)}, list of nodes: {self.resource_array})'

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
gloabl variables
"""
fairness_coef = 0.3
local_coef = 0.7
population = Population([])
current_resources = CurrentResources([])


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
    - an array of size poolsize of genotypes
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
        genotype = Genotype([])
        for resource in current_resources.resource_array:
            gene = Gene(resource, [])
            genotype._gene_array.append(gene)
        for task in inital_taks_queue:
            resource_id = random.randint(0, len(current_resources.resource_array) -1 )# including the last number so minus one for index
            genotype._gene_array[resource_id].tasksqueue.append(task)
        pool.append(genotype)
        fitness_eval(pool)
    return pool

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
def parent_selection(n_parents, k, population):
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


        temp = k
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
                leftover_tasks.append(other_parent_tasks[other_idx])
            other_idx = other_idx +1
        gene_idx = gene_idx +1
        
    return child


"""
input:  
    - input_array([Genotype]): a list of all the Genotypes that need to be mutated
    - mutation_coefficient1(float): between 0 and 1 mutation chance for each of the genes that random task is taken and added to another node
    - mutation_coefficient2(float): between 0 and 1 mutation chance for an entire taskqueue to be appended to be swapped between nodes
output:
    - output_array([Genotype]): a list of all the now mutated Genotypes
constrains:
    - same order of input and output array
description:
    for each resource node in the schedule of each individual (genotype) there is a chance to swap one task of that resource node with the task of another
    resource node: important to note if the second chosen node does not have any resources no further attempts are made to find another node to save compution time
    this should be included in the determination of the mutation coefficient 
"""
def mutation(input_array, mutation_coefficient1):#, mutation_coefficient2, mutation_coefficient3):
    if not isinstance(input_array[0], Genotype):
        raise TypeError("init called with wrong parameter type")
    for genotype in input_array:
        for gene in genotype._gene_array:
            if random() <= mutation_coefficient1 and len(gene.tasksqueue) != 0:
                #need to switch one task of the current gene with another task of a random gene
                switch_gene_other_idx = random.choice(range(len(genotype._gene_array)))
                switch_gene_taskqueue_len = len(genotype._gene_array[switch_gene_other_idx].tasksqueue)
                if switch_gene_taskqueue_len == 0:
                    continue
                switch_task_other_idx = random.choice(range(switch_gene_taskqueue_len))
                switch_task_this_idx = random.choice(range(len(gene.tasksqueue)))


                switch_temp = genotype._gene_array[switch_gene_other_idx].tasksqueue[switch_task_other_idx]
                genotype._gene_array[switch_gene_other_idx].tasksqueue[switch_task_other_idx] = gene.tasksqueue[switch_task_this_idx]
                gene.tasksqueue[switch_task_this_idx] = switch_temp

"""Ingress"""
app = Flask(__name__)

#-----this is from the main scheduler-----#
#get updates of workqueue from the main scheduler
@app.route('/workqueue', methods=['POST'])
def workqueue():
    print("update from main scheduler to the workqueue")

# # get updates on own resource utilization to change pool size
# @app.route('/change_pool', methods=['POST'])
# def node_status():
#     print("update from main scheduler to the pool")
"""Egress""" 
