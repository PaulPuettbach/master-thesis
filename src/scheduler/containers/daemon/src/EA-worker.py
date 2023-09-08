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
    
    child = Genotype([Gene("placeholder",[])] * len(parent1.gene_array))
    
    leftover_tasks = []
    gene_idx = 0
    for gene in child.gene_array:
        while True:  
            n_tasks_per_chunk = math.floor(len(parent1.gene_array[gene_idx].tasksqueue) / (k+1))
            if n_tasks_per_chunk == 0:
                k = k-1
            else:
                break
        if k == 0: #this ensure n tasks per chucnk is one
            gene.tasksqueue = parent1.gene_array[gene_idx].tasksqueue
            continue


        gene.resource = parent1.gene_array[gene_idx].resource

        #the taskqueue size is taken from one parent
        gene.tasksqueue = [0] * len(parent1.gene_array[gene_idx].tasksqueue)


        """for each chuck iterate over teh n tasks per chunck and flip a coin what parent to take from where you go over the tasks till there is one you can use
        """

        """two loops inside one for each parents taskqueue i make the iteration modulo the taskqueue length so we start in the front again if we have to we take number of chunks per task from each parent the 
        other one we mark as leftover if we cant take more from one queue we try the other if we cant take from either we take from leftover
        """
        """

psudocode:
used bitmap = bitmap
gene in child
    taskqueue = parent1 taskqueue
    child_task_idx
    
    for task in child:
        if task_ix < lowerbound and task idx > upper bound
        #take from parent 1 if you can if you cant take from parent two or leftover
            while parent1_task_idx < parent 1 taskquque length
                
                if and  bitmap <<<<<<< taskid == 0
                    take parent 1
                    step parent 1 idx
                    update bitmap to taken
            
            while parent2_task_idx < parent 2 taskqueue length
                if bitmap taskid + 0
                    take parent 2
                    step idx parent 2
                    update bitmap
            for task in leftover
                if bitmap == 0
                    take
                    break
            should not get here
        else
            while parent2_task_idx < parent 1 taskquque length
                
                if and  bitmap <<<<<<< taskid == 0
                    take parent 1
                    step parent 1 idx
                    update bitmap to taken
            
            while parent1_task_idx < parent 2 taskqueue length
                if bitmap taskid + 0
                    take parent 2
                    step idx parent 2
                    update bitmap
            for task in leftover
                if bitmap == 0
                    take
                    break
            should not get here
    step through rest of parent1 and two and update leftover
        while parent1_task_idx < parent 1 taskquque length
            if bitmap == 0
                contiue
            else
                leftover.append(task)

        while parent2_task_idx < parent 1 taskquque length
            if bitmap == 0
                contiue
            else
                leftover.append(task)
"""

        # for i in range(k +1):
        #     #on even take parent 1
        #     if i % 2 == 0:
        #         #the last bit
        #         if i == k:
        #             gene.tasksqueue[(i * n_tasks_per_chunk):] = parent1.gene_array[gene_idx].taskqueue[(i * n_tasks_per_chunk):]
        #         else:
        #             child.append(parent1.array[(i * n_genes_per_chunk):((i+1) * (n_genes_per_chunk))])
        #     #on odd take parent 2
        #     if i % 2 == 1:
        #                     #the last bit
        #         if i == k:
        #             child.append(parent2.array[(i * n_genes_per_chunk):])
        #         else:
        #             child.append(parent2.array[(i * n_genes_per_chunk):((i+1) * (n_genes_per_chunk))])
        # gene_idx = gene_idx +1
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
