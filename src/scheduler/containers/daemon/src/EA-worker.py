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

parent selection: k tournament selection  change k to change selection pressure

crossover: k point two parent crossover

mutation : three adaptive mutation coefficents one for small step 

fitnesscalc: fitnessfunctions

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
class Task:
    def __init__(self,id, status, migration = False, migrate_to = ""):
        self.id = id
        self.status = status
        self.migration = migration
        self.migrate_to = migrate_to
    
    def __str__(self):
        if not self.migration:
            return f'task({self.id}, status: {self.status})'
        else:
            return f'migration_task({self.id}, status: {self.status}, migrate_to : {self.migrate_to})'
    
    def __repr__(self): 
        if not self.migration:
            return f'task({self.id}, status: {self.status})'
        else:
            return f'migration_task({self.id}, status: {self.status}, migrate_to : {self.migrate_to})'
    
class Gene:
    def __init__(self,resource, tasksqueue):
        self.resource = resource
        self.tasksqueue = tasksqueue
    
    def __str__(self):
        return f'genome({self.tasksqueue} on {self.resource})'
    
    def __repr__(self): 
        return f'genome({self.tasksqueue} on {self.resource})' 

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
    

#iterate n times. N being equal to poolsize
    for i in range(poolsize -1):
        genotype = Genotype([])
        for resource in current_resources.resource_array:
            gene = Gene(resource, [])
            genotype.gene_array.append(gene)
        for task in inital_taks_queue:
            resource_id = randint(0, len(current_resources.resource_array) -1)# including the last number so minus one for index
            genotype.gene_array[resource_id].tasksqueue.append(task)
    return genotype



    
   
"""
input:  
    - first parent (Genotype): the first parent for the crossover
    - second parent(Genotype): the second parent for the crossover
    - minimum_prozent(float): number between 0 and 1 prozent of genes from parent in first step
output:
    - child (Genotype): a child that is a combination of the parents
constrains:
    - Small changes to a genotype induce small changes in the corresponding phenotypes
    - tasks cannot be duplicated and all tasks have to be assigned
description:


maybe try to change which parents queue lengths are used

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
