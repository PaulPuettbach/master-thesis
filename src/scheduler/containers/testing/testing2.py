import math
from random import randint, choice, random

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

"""
input:  
    - input_array([Genotype]): a list of all the Genotypes that need to be mutated
    - mutation_coefficient1(float): between 0 and 1 mutation chance for each of the genes that random task is taken and added to another node
    - mutation_coefficient2(float): between 0 and 1 mutation chance for an entire taskqueue to be appended to be swapped between nodes
output:
    - output_array([Genotype]): a list of all the now mutated Genotypes
constrains:
    - same order of input and output array
"""
def mutation(input_array, mutation_coefficient1):#, mutation_coefficient2, mutation_coefficient3):
    if not isinstance(input_array[0], Genotype):
        raise TypeError("init called with wrong parameter type")
    for genotype in input_array:
        for gene in genotype.gene_array:
            if random() <= mutation_coefficient1:
                #need to switch one task of the current gene with another task of a random gene
                switch_gene_other_idx = choice(range(len(genotype.gene_array)))
                switch_task_other_idx = choice(range(len(genotype.gene_array[switch_gene_other_idx].tasksqueue)))
                switch_task_this_idx = choice(range(len(gene.tasksqueue)))


                switch_temp = genotype.gene_array[switch_gene_other_idx].tasksqueue[switch_task_other_idx]
                genotype.gene_array[switch_gene_other_idx].tasksqueue[switch_task_other_idx] = gene.tasksqueue[switch_task_this_idx]
                gene.tasksqueue[switch_task_this_idx] = switch_temp



parent1 = Genotype([Gene("node1", [Task(2,"pending", Tenant(1)),Task(3,"pending", Tenant(1)), Task(4,"pending", Tenant(2))]), Gene("node2", [Task(1,"pending", Tenant(3))]), Gene("node3", [Task(8,"pending", Tenant(1)),Task(5,"pending", Tenant(1)), Task(7,"pending", Tenant(1)),Task(6,"pending", Tenant(2))])])
parent2 = Genotype([Gene("node1", [Task(8,"pending", Tenant(1)),Task(4,"pending", Tenant(2))]), Gene("node2", [Task(6,"pending", Tenant(2)), Task(1,"pending", Tenant(3)), Task(2,"pending", Tenant(1))]), Gene("node3", [Task(5,"pending", Tenant(1)),Task(3,"pending", Tenant(1)), Task(7,"pending", Tenant(1))])])


mutation_coefficient1 = 0.4
print("---------before----------\n")
print(f"this is the parent1 {parent1}")
print(f"this is the parent2 {parent2}")

mutation([parent1, parent2], mutation_coefficient1)

print("----------------after---------------\n")
print(f"this is the parent1 {parent1}")
print(f"this is the parent2 {parent2}")

                
