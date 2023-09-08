import math
from random import randint
from bitmap import BitMap

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

# def k_point_crossover(parent1, parent2, k):
#     #is constrained since it cannot leave out or duplicate tasks so iterate over tasks and do n point crossover to what resource they are mapped
#     if not isinstance(parent1, Genotype) or not isinstance(parent2, Genotype):
#         raise TypeError("k_point_crossover called with wrong paramter type")
    
#     if not len(parent1.array) == len(parent2.array):
#         raise Exception("k_point_crossover called with two parents of different length")
    
#     #sort by task
#     parent1.array.sort(key = lambda x: x.task)
#     parent2.array.sort(key = lambda x: x.task)

#     #find crossover point in the array
#     while True:
#         n_chromosomes_per_chunk = math.floor(len(parent1.array) / (k+1))
#         if n_chromosomes_per_chunk == 0:
#             k = k-1
#         else:
#             break

#     child = []
#     for i in range(k +1):
#         #on even take parent 1
#         if i % 2 == 0:
#             #the last bit
#             if i == k:
#                 child.append(parent1.array[(i * n_chromosomes_per_chunk):])
#             else:
#                 child.append(parent1.array[(i * n_chromosomes_per_chunk):((i+1) * (n_chromosomes_per_chunk))])
#         #on odd take parent 2
#         if i % 2 == 1:
#                         #the last bit
#             if i == k:
#                 child.append(parent2.array[(i * n_chromosomes_per_chunk):])
#             else:
#                 child.append(parent2.array[(i * n_chromosomes_per_chunk):((i+1) * (n_chromosomes_per_chunk))])
#     return child

# solution1 = Genotype([Chromosome("node1", "taks1"),Chromosome("node2", "taks2"),Chromosome("node3", "taks3"), Chromosome("node4", "taks4"), Chromosome("node5", "taks5"), Chromosome("node6", "taks6")])
# solution2 = Genotype([Chromosome("node1", "taks5"),Chromosome("node2", "taks3"),Chromosome("node3", "taks4"), Chromosome("node4", "taks6"), Chromosome("node5", "taks2"), Chromosome("node6", "taks1")])

# try:
#     child = k_point_crossover(solution1,solution2,8)
#     print("child after crossover")
#     print(child)
# except Exception as e:
#        # By this way we can know about the type of error occurring
#         print("The error is: ",e)
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
    - if there are less than 10 nodes it does not make sense to take whole node task assignments wholesale
description:
constrains are that the parents should be meaningfully preserved in the child which is non trivial

for a task it is important: what node it is scheduled on with what other tasks it is scheduled and in what order so do k point cross over 

order not as important as what is in front what is in the back
"""
# def order_crossover(parent1, parent2, k):
#     if not isinstance(parent1, Genotype) or not isinstance(parent2, Genotype):
#         raise TypeError("k_point_crossover called with wrong parameter type")
    
#     if not len(parent1.array) == len(parent2.array):
#         raise Exception("k_point_crossover called with two parents of different length")
    
#     #init child as genotype with nulled gene array
#     # child = Genotype([0]*len(parent1.gene_array))

    
#     #find to random points in the first parent should
#     #what if there len parent is 1, 2, 3

#     #idx counter for the gene
#     gene_idx = 0 
#     for gene in parent1.gene_array:
#         #crossoverpoint 1 cannot be last index of the array
#         crossover_point1 = randint(0, len(gene.taskqueue)-2) 
#         crossover_point2 = randint(crossover_point1+1, len(gene.taskqueue)-1)
#         task_idx = 0
#         for task in gene.taskqueue:


#         gene_idx = gene_idx + 1
        


def partially_mapped_crossover(parent1, parent2, k=1):
    if not isinstance(parent1, Genotype) or not isinstance(parent2, Genotype):
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if type(k) is not int:
        raise TypeError("k_point_crossover called with wrong parameter type")
    
    if not len(parent1.gene_array) == len(parent2.gene_array):
        raise Exception("k_point_crossover called with two parents of different length")
    
    child = Genotype([Gene("placeholder",[])] * len(parent1.gene_array))
    
    leftover_tasks = []
    # one more than number of tasks in total because id start at one and constant arithmatic is more wasterful then simply having one more unused bit
    bm = BitMap(9)
    gene_idx = 0
    for gene in child.gene_array:
        print(f"this is how many time gene loop is called {gene_idx}")
        while True:  
            n_tasks_per_chunk = math.floor(len(parent1.gene_array[gene_idx].tasksqueue) / (k+1))
            if n_tasks_per_chunk == 0:
                k = k-1
            else:
                break
        if k == 0: #this ensure n tasks per chucnk is one
            gene.tasksqueue = parent1.gene_array[gene_idx].tasksqueue
            continue
        print(f"this is the n task per chunck for three should be one{n_tasks_per_chunk}")

        print(f"this is the gene_idx {gene_idx}")
        print(f"this is what is put as the resource {parent1.gene_array[gene_idx].resource}")
        gene.resource = parent1.gene_array[gene_idx].resource

        #the taskqueue size is taken from one parent
        gene.tasksqueue = [0] * len(parent1.gene_array[gene_idx].tasksqueue)
        print(f"this is the gene taskqueue at this point {gene.tasksqueue}")
        child_task_idx = 0
        chosen_idx = 0
        other_idx = 0
        coin = randint(0,1)
        if coin:
            print("chosen parent is 1")
            chosen_parent_tasks = parent1.gene_array[gene_idx].tasksqueue
            other_parent_tasks = parent2.gene_array[gene_idx].tasksqueue
        else:
            print("chosen parent is 2")
            chosen_parent_tasks = parent2.gene_array[gene_idx].tasksqueue
            other_parent_tasks = parent1.gene_array[gene_idx].tasksqueue
        while child_task_idx <  len(gene.tasksqueue):
            print(f"this is how many time task loop is called {child_task_idx}")

            task_taken = False
            
            if child_task_idx < n_tasks_per_chunk:
                while not task_taken and chosen_idx < len(chosen_parent_tasks):
                    if not bm.test(chosen_parent_tasks[chosen_idx].id):
                        print("==============this is chosen parent and found ================")
                        print(f"full queue before assignment {gene.tasksqueue}")
                        print(f"this is the task before {gene.tasksqueue[child_task_idx]}")
                        print(f"what is being assigned {chosen_parent_tasks[chosen_idx]}")
                        gene.tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        print(f"full queue afer assignment {gene.tasksqueue}")
                        bm.set(chosen_parent_tasks[chosen_idx].id)
                        task_taken = True
                    chosen_idx = chosen_idx +1
                    break

                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(other_parent_tasks[other_idx].id):
                        print("==============this is chosen parent and other ================")
                        print(f"full queue before assignment {gene.tasksqueue}")
                        print(f"this is the task before {gene.tasksqueue[child_task_idx]}")
                        print(f"what is being assigned {other_parent_tasks[chosen_idx]}")
                        gene.tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        print(f"full queue afer assignment {gene.tasksqueue}")
                        bm.set(other_parent_tasks[other_idx].id)
                        task_taken = True
                    other_idx = other_idx +1
                    break
            else:
                while not task_taken and other_idx < len(other_parent_tasks):
                    if not bm.test(other_parent_tasks[other_idx].id):
                        print("==============this is chosen parent and found ================")
                        print(f"full queue before assignment {gene.tasksqueue}")
                        print(f"this is the task before {gene.tasksqueue[child_task_idx]}")
                        print(f"what is being assigned {other_parent_tasks[chosen_idx]}")
                        gene.tasksqueue[child_task_idx] = other_parent_tasks[other_idx]
                        print(f"full queue afer assignment {gene.tasksqueue}")
                        bm.set(other_parent_tasks[other_idx].id)
                        task_taken = True
                    other_idx = other_idx +1
                    break
                
                while not task_taken and chosen_idx < len(chosen_parent_tasks):
                    if not bm.test(chosen_parent_tasks[chosen_idx].id):
                        print("==============this is other parent and chosen ================")
                        print(f"full queue before assignment {gene.tasksqueue}")
                        print(f"this is the task before {gene.tasksqueue[child_task_idx]}")
                        print(f"what is being assigned {chosen_parent_tasks[chosen_idx]}")
                        gene.tasksqueue[child_task_idx] = chosen_parent_tasks[chosen_idx]
                        print(f"full queue afer assignment {gene.tasksqueue}")
                        bm.set(chosen_parent_tasks[chosen_idx].id)
                        task_taken = True
                    chosen_idx = chosen_idx +1
                    break
                
            for task_idx in range(len(leftover_tasks)-1):
                print("using leftover tasks")
                if task_taken:
                    break
                if not bm.test(leftover_tasks[task_idx].id):
                    task = leftover_tasks[task_idx]
                    leftover_tasks.pop(task_idx)
                    task_taken = True
                    break
            print("increase the child index")
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

parent1 = Genotype([Gene("node1", [Task(2,"pending"),Task(3,"pending"), Task(4,"pending")]), Gene("node2", [Task(1,"pending")]), Gene("node3", [Task(8,"pending"),Task(5,"pending"), Task(7,"pending"),Task(6,"pending")])])
parent2 = Genotype([Gene("node1", [Task(8,"pending"),Task(4,"pending")]), Gene("node2", [Task(6,"pending"), Task(1,"pending"), Task(2,"pending")]), Gene("node3", [Task(5,"pending"),Task(3,"pending"), Task(7,"pending")])])
# bm = BitMap(32)
# bm.set(1)
# print(bm.tostring())
# bm.set(1)
# if bm.test(1):
#     print("if you see this 1 means true")

# elif not bm.test(1):
#     print("if you see this 0 means true")
# else:
#     print("if you see this it doesnt work")
child = partially_mapped_crossover(parent1,parent2)
print(f"this is the child {child}")

                

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

