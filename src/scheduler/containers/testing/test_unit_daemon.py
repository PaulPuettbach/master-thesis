import pytest
import containers.daemon.src.EA_worker as src
import threading
import multiprocessing
import copy
from time import sleep
from queue import Empty

#https://stackoverflow.com/questions/19225279/pytest-init-setup-for-few-modules
def setup_update_batch():
        dic1 = {
            'id': 0,
            'status': 'Pending',
            'tenant': 'Paul'
        }
        dic2 = {
            'id': 2,
            'status': 'Succeeded',
            'tenant': 'Peter'
        }
        dic3 = {
            'id': 1,
            'status': 'Pending',
            'tenant': 'Max'
        }
        with src.thread_lock_update_q:
            src.update_q.append(dic1)
            src.update_q.append(dic2)
            src.update_q.append(dic3)
            src.tasks_arrived.set()
        batch_process = threading.Thread(target=src.update_batch)
        batch_process.start()
        # batch_process.join()

def setup_update_current_resources():
    testNode1 = src.Node(3)
    testNode2 = src.Node(6)
    src.resources_queue.put_nowait((testNode1, "add"))
    src.resources_queue.put_nowait((testNode2, "add"))
    with src.n_node.get_lock():
        src.n_node.value = 2
        print("get here", flush=True)
    src.node_update.set()

def setup_update_current_resources_2():
    testNode1 = src.Node(3)
    src.resources_queue.put_nowait((testNode1, "delete"))
    with src.n_node.get_lock():
        src.n_node.value = 1
    src.node_update.set()

def setup_add_tasks1():
    tenant1 = src.Tenant("paul")
    tenant2 = src.Tenant("peter")
    tenant3 = src.Tenant("max")
    
    toadd = [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1), src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)]
    for task in toadd:
        src.new_tasks.put_nowait(task)
    with src.n_new_tasks.get_lock():
        src.n_new_tasks.value = 5
    with src.no_task.get_lock():
        src.no_task.value = False

def setup_add_tasks2():
    tenant1 = src.Tenant("paul")
    tenant2 = src.Tenant("peter")
    tenant3 = src.Tenant("max")
    toadd2 = [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)]
    for task in toadd2:
        src.new_tasks.put_nowait(task)
    with src.n_new_tasks.get_lock():
        src.n_new_tasks.value = 3
    with src.no_task.get_lock():
        src.no_task.value = False

def setup_del_tasks1():
    tenant1 = src.Tenant("paul")
    tenant3 = src.Tenant("max")
    todel = [src.Task(0, "Pending",tenant1), src.Task(4, "Pending",tenant3), src.Task(2, "Succeeded",tenant1)]
    for task in todel:
        src.old_tasks.put_nowait(task)
        with src.n_old_tasks.get_lock():
            src.n_old_tasks.value = 3

def setup_del_tasks2():
    tenant1 = src.Tenant("paul")
    tenant2 = src.Tenant("peter")
    todel = [src.Task(1, "Succeeded",tenant1), src.Task(6, "Succeeded",tenant2), src.Task(7, "Pending",tenant2)]
       
    for task in todel:
        src.old_tasks.put_nowait(task)
    with src.n_old_tasks.get_lock():
        src.n_old_tasks.value = 3

def setup_del_tasks3():
    tenant1 = src.Tenant("paul")
    tenant3 = src.Tenant("max")
    kept = [src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)]
    for task in kept:
        src.old_tasks.put_nowait(task)
    with src.n_old_tasks.get_lock():
        src.n_old_tasks.value = 2

class TestInstantiation:
    def test_tenant(self):
        with pytest.raises(ValueError):
            #positive id
            invalid = src.Tenant(-2)
        valid1 = src.Tenant("paul")
        assert valid1.id == 0
        valid2 = src.Tenant("peter")
        assert valid2.id == 1
        valid2 = src.Tenant("frank")
        assert valid2.id == 2
        valid3 = src.Tenant("paul")
        assert valid3.id == 0

    def test_task(self):
        with pytest.raises(ValueError):
            # id should be positive integer
            invalid = src.Task(-2, "Pending",src.Tenant("paul"))
        with pytest.raises(ValueError):
            # status has to be one of the allowed strings
            invalid2 = src.Task(0, "something",src.Tenant("paul"))
        with pytest.raises(ValueError):
            invalid3 = src.Task(0, "Pending",0)
        with pytest.raises(ValueError):
            invalid4 = src.Task(0, "Pending",src.Tenant("paul"),"something")
        with pytest.raises(ValueError):
            invalid5 = src.Task(0, "Pending",src.Tenant("paul"),True)
        with pytest.raises(ValueError):
            invalid6 = src.Task(0, "Pending",src.Tenant("paul"),False, src.Node(1))
        valid = src.Task(0, "Pending",src.Tenant("paul"), True, src.Node(1))
        valid2 = src.Task(0, "Running",src.Tenant("paul"))
        valid3 = src.Task(0, "Succeeded",src.Tenant("paul"), False, None)

    def test_gene(self):
        valid = src.Gene(src.Node(1), [])
        valid2 = src.Gene(src.Node(1), [src.Task(0, "Pending",src.Tenant("paul"), True, src.Node(1)), src.Task(0, "Succeeded",src.Tenant("paul"))])
        with pytest.raises(ValueError):
            invalid = src.Gene(src.Task(0, "Finished",src.Tenant("paul"), False, None), [])
        with pytest.raises(ValueError):
            invalid2 = src.Gene(src.Node(0), [src.Task(0, "Succeeded",src.Tenant("paul")),src.Task(1, "Succeeded",src.Tenant("peter")), src.Node(9)])
    
    def test_geneotype(self):
        gene1 = src.Gene(src.Node(1), [])
        gene2 = src.Gene(src.Node(1), [src.Task(0, "Pending",src.Tenant("paul"), True, src.Node(1)), src.Task(1, "Succeeded",src.Tenant("paul"))])
        valid = src.Genotype([])
        valid2 = src.Genotype([gene1,gene2])
        valid3 = src.Genotype([], 1.0)
        valid4 = src.Genotype([gene1,gene2], 0.8)
        with pytest.raises(ValueError):
            invalid = src.Genotype([src.Node(9)])
        with pytest.raises(ValueError):
            invalid2 = src.Genotype([gene1,gene2], -2.0)
        with pytest.raises(ValueError):
            invalid3 = src.Genotype([gene1,gene2], 1)
        with pytest.raises(ValueError):
            invalid3 = src.Genotype([gene1,gene2], 15.0)
        with pytest.raises(ValueError):
            #cant append task with the same id
            valid2.append_task(src.Task(1, "Succeeded",src.Tenant("paul")), src.Node(1))

        valid2.append_task(src.Task(2, "Succeeded",src.Tenant("paul")), src.Node(1))
    
    def test_population(self):
        gene1 = src.Gene(src.Node(1), [])
        gene2 = src.Gene(src.Node(1), [src.Task(0, "Pending",src.Tenant("paul"), True, src.Node(1)), src.Task(1, "Succeeded",src.Tenant("paul"))])
        individual1 = src.Genotype([])
        individual2 = src.Genotype([gene1,gene2])
        individual3 = src.Genotype([], 1.0)
        valid = src.Population([])
        valid2 = src.Population([individual1,individual2,individual3])
        with pytest.raises(ValueError):
            valid2 = src.Population([individual1,individual2,individual3, src.Node(9)])

    def test_node(self):
        valid1 = src.Node(0)
        valid2 = src.Node(15)
        with pytest.raises(ValueError):
            invalid1 = src.Node(0.0)
        with pytest.raises(ValueError):
            invalid2 = src.Node(-3)

class TestEq:
    def test_tenant_eq(self):
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("paul")
        tenant3 = src.Tenant("peter")
        nottenant = src.Node(1)

        assert(tenant1 == tenant2)
        assert(not(tenant2 == tenant3))
        with pytest.raises(ValueError):
            tenant1 == nottenant

    def test_task_eq(self):
        task1 = src.Task(0, "Pending",src.Tenant("paul"), True, src.Node(1))
        task2 = src.Task(0, "Succeeded",src.Tenant("paul"))
        task3 = src.Task(1, "Running",src.Tenant("paul"), False, None)
        nottask = src.Node(1)

        assert(task1 == task2)
        assert(not (task1 == task3))
        with pytest.raises(ValueError):
            task1 == nottask
    
    def test_gene_eq(self):
        tenant1 = src.Tenant("paul")
        tenant3 = src.Tenant("max")
        gene1 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(1), [src.Task(1, "Succeeded",tenant1), src.Task(0, "Pending",tenant1)])
        gene3 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])

        assert(not(gene1 == gene2))
        assert(gene1 == gene3)
        with pytest.raises(ValueError):
            gene1 ==tenant3

    def test_geneotype_eq(self):
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene1 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene3 = src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)])
        gene4 = src.Gene(src.Node(1), [src.Task(1, "Succeeded",tenant1), src.Task(0, "Pending",tenant1)])
        
        test1 = src.Genotype([gene1, gene2, gene3])
        test2 = src.Genotype([gene4, gene2, gene3])
        test3 = src.Genotype([gene1, gene2, gene3])
        test4 = src.Genotype([gene1, gene3, gene2])

        #test basic functionality
        assert(test1 == test3)
        assert(not(test1 == test4))

        #test recursive equality test
        assert(not(test1 == test2))

        with pytest.raises(ValueError):
            test1 == tenant3

class Testfitness:
    def test_fairness(self):
        # the idea for this test is simple do the calculation by hand for what it should be and see if we get the same
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene1 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene3 = src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)])

        test = src.Genotype([gene1, gene2, gene3])
        #tenant1 = [sum of taskposition = 8, number of tasks = 4]
        #tenant2 = [sum of taskposition = 3, number of tasks = 2]
        #tenant3 = [sum of taskposition = 4, number of tasks = 2]
        
        # the number of repeating places after the comma is taken from commandline python
        #eg (python3 -c "print(2/3)")
        #normalized src.fairness = (0.5, 0.6666666666666666,0.5)
        #mean normalized src.fairness = (0.5555555555555555)
        #individual error = (0.05555555555555547, 0.11111111111111116, 0.05555555555555547)
        #sum = 0.2222222222222221
        #inverse_error = 0.07407407407407403
        #error 0.9259259259259259
        assert src.fairness(test) == 0.9259259259259259

        fairgene1 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant3)])
        fairgene2 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant3), src.Task(1, "Succeeded",tenant1), src.Task(2, "Succeeded",tenant2)])
        fairgene3 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant2), src.Task(1, "Succeeded",tenant3), src.Task(2, "Succeeded",tenant1)])

        fairtest = src.Genotype([fairgene1, fairgene2, fairgene3])
        assert src.fairness(fairtest) == 1.0
    
    def test_locality(self):
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene1 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene3 = src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)])
        test = src.Genotype([gene1, gene2, gene3])

        #noramlized_resources_per_tenant is number nodes divided tasks by tenant
        # = (3/4, 2/2, 2/2)
        # = (0.75, 1, 1)
        # mean normalized resources per tenant 0.9166666666666666
        # 1 - mean normalized resources per tenant 1 - 0.9166666666666666 = 0.08333333333333337
        assert src.locality(test) == 0.08333333333333337

class TestInit:
    def test_init(self):
        src.current_resources.extend([src.Node(0), src.Node(1), src.Node(2)])
        poolsize = 10
        src.init(poolsize)
        assert(all(isinstance(element, src.Genotype) for element in src.population.population_array))
        assert(len(src.population.population_array)==poolsize)

class TestUpdateBatch:    
    def test_update_batch(self):
        setup = multiprocessing.Process(target=setup_update_batch)
        setup.start()
        sleep(10)
        assert(src.n_new_tasks.value == 2)
        assert(src.new_tasks.qsize() == 2)
        assert(src.n_old_tasks.value == 1)
        assert(src.old_tasks.qsize() == 1)
        #should be false there are new tasks in the new_tasks
        assert(not src.no_task.value)

        #empty out the new_task and old_task
        rec_task_counter = 0
        while True:
            with src.n_new_tasks.get_lock():
                #done
                if rec_task_counter == src.n_new_tasks.value:
                    break
            try:
                task = src.new_tasks.get_nowait()
            except Empty:
                continue
            rec_task_counter += 1
        with src.n_new_tasks.get_lock():
            src.n_new_tasks.value = 0
        
        rec_task_counter = 0
        while True:
            with src.n_old_tasks.get_lock():
                #done
                if rec_task_counter == src.n_old_tasks.value:
                    break
            try:
                task = src.old_tasks.get_nowait()
            except Empty:
                continue
            rec_task_counter += 1  
        
        with src.n_old_tasks.get_lock():
            src.n_old_tasks.value = 0

        sleep(5)
        assert(src.n_new_tasks.value == 0)
        assert(src.new_tasks.qsize() == 0)
        assert(src.n_old_tasks.value == 0)
        assert(src.old_tasks.qsize() == 0)
        with src.thread_lock_update_q:
            assert(len(src.update_q) == 0)
        #should be false there are new tasks in the new_tasks
        assert(not src.no_task.value)
        with src.no_task.get_lock():
            src.no_task.value = True
        

class TestAddAndDelTaskNode:
    def test_add_node(self):
        testNode1 = src.Node(3)
        testNode2 = src.Node(6)
        src.current_resources = []
        setup1 = multiprocessing.Process(target=setup_update_current_resources)
        setup1.start()

        src.node_update.wait()
        assert(not src.current_resources)

        src.update_current_resources()

        assert(testNode1 in src.current_resources)
        assert(testNode2 in src.current_resources)

        setup2 = multiprocessing.Process(target=setup_update_current_resources_2)
        setup2.start()

        src.node_update.wait()

        assert(testNode1 in src.current_resources)
        assert(testNode2 in src.current_resources)

        src.update_current_resources()

        assert(testNode1 not in src.current_resources)
        assert(testNode2 in src.current_resources)

    def test_add_tasks_to_genetype(self):
        #test every thing is added and if everything is added only once
        poolsize = 10
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        genes = [gene for genotypes in src.population.population_array for gene in genotypes._gene_array]
        toadd = [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1), src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)]
        
        #assert they are empty before
        for gene in genes:
            assert(all(not tasksqueue for tasksqueue in gene.tasksqueue))
        #should be true if there are no tasks yet in the taskqueues
        with src.no_task.get_lock():
            assert(src.no_task.value)
        
        setup1 = multiprocessing.Process(target=setup_add_tasks1)
        setup1.start()

        while src.no_task.value:
            continue
        src.add_tasks_to_genotype()
        setup1.join()
        setup1.close()
        with src.no_task.get_lock():
            src.no_task.value = True

        toadd = toadd *poolsize
        for gene in genes:
            for task in gene.tasksqueue:
                assert(task in toadd)
                toadd.remove(task)
        assert(len(toadd) == 0)

        toadd = [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1), src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)]
        toadd2 = [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)]
        
        setup2 = multiprocessing.Process(target=setup_add_tasks2)
        setup2.start()

        while src.no_task.value:
            continue
        with src.n_new_tasks.get_lock():
            assert(src.n_new_tasks.value == 3)
        src.add_tasks_to_genotype()
        setup2.join()
        setup2.close()
        with src.no_task.get_lock():
            src.no_task.value == True

        toadd = toadd *poolsize
        toadd2 = toadd2 *poolsize
        for gene in genes:
            for task in gene.tasksqueue:
                assert(task in (toadd+toadd2))
                if task in toadd:
                    toadd.remove(task)
                elif task in toadd2:
                    toadd2.remove(task)
        assert(len(toadd) == 0) 
        assert(len(toadd2) == 0)

        assert(len([tasks for gene in genes for tasks in gene.tasksqueue])==8*poolsize)


    def test_del_tasks_from_genotype(self):
        #test the task is deleted and nothing else is delted
        poolsize=10
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        genes = [gene for genotypes in src.population.population_array for gene in genotypes._gene_array]
        
        assert(len([tasks for gene in genes for tasks in gene.tasksqueue])==8*poolsize)
        #manually update the value cause it only gets updated in the update batch naturally
        with src.no_task.get_lock():
            src.no_task.value = False
            assert(not src.no_task.value)

        #[src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1), src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)]
        todel = [src.Task(0, "Pending",tenant1), src.Task(4, "Pending",tenant3), src.Task(2, "Succeeded",tenant1)]
        kept = [src.Task(1, "Succeeded",tenant1), src.Task(6, "Succeeded",tenant2), src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)]
        
        setup1 = multiprocessing.Process(target=setup_del_tasks1)
        setup1.start()
        setup1.join()
        src.del_tasks_from_genotype()
        setup1.close()        
        assert(not src.no_task.value)

        assert(len([tasks for gene in genes for tasks in gene.tasksqueue])==5*poolsize)
        for gene in genes:
            for task in gene.tasksqueue:
                assert(task in kept)
                assert(task not in todel)

        todel = [src.Task(1, "Succeeded",tenant1), src.Task(6, "Succeeded",tenant2), src.Task(7, "Pending",tenant2)]
        kept = [src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)]
        
        
        setup2 = multiprocessing.Process(target=setup_del_tasks2)
        setup2.start()
        setup2.join()
        src.del_tasks_from_genotype()
        setup2.close()
        assert(not src.no_task.value)
        
        assert(len([tasks for gene in genes for tasks in gene.tasksqueue])==2*poolsize)
        for gene in genes:
            for task in [task for task in gene.tasksqueue]:
                assert(task in kept)
                assert(task not in todel)
        
        
        setup3 = multiprocessing.Process(target=setup_del_tasks3)
        setup3.start()
        setup3.join()
        src.del_tasks_from_genotype()
        setup3.join()

        for gene in genes:
            assert(all(not tasksqueue for tasksqueue in gene.tasksqueue))

        #should be true since we delted all tasks and the taskquesues should be empty
        assert(src.no_task.value)

class TestMutation:
    def test_coefficient1(self):

        src.mutation_coefficient1 = 1
        src.mutation_coefficient2 = 0
        src.mutation_coefficient3 = 0
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene0 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene1 = src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)])
        test = src.Genotype([gene0, gene1, gene2])
        output = [copy.deepcopy(test)]

        
        src.mutation(output)

        if output[0]._gene_array[0] == gene0:
            #the other must be switched
            assert(output[0]._gene_array[1].tasksqueue == gene2.tasksqueue )
            assert(output[0]._gene_array[2].tasksqueue == gene1.tasksqueue )
        elif output[0]._gene_array[1] == gene1:
            #the other must be switched
            assert(output[0]._gene_array[0].tasksqueue == gene2.tasksqueue )
            assert(output[0]._gene_array[2].tasksqueue == gene0.tasksqueue )
        else:
            assert(output[0]._gene_array[2] == gene2 )
            assert(output[0]._gene_array[0].tasksqueue == gene1.tasksqueue )
            assert(output[0]._gene_array[1].tasksqueue == gene0.tasksqueue )
    
    def test_coefficient2(self):
        src.mutation_coefficient1 = 1
        src.mutation_coefficient2 = 0
        src.mutation_coefficient3 = 0
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene0 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene1 = src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)])
        test = src.Genotype([gene0, gene1, gene2])
        output = [copy.deepcopy(test)]

        src.mutation_coefficient1 = 0
        src.mutation_coefficient2 = 1 
        src.mutation_coefficient3 = 0
        src.mutation(output)

        #could be a task is taken from the first gene and then later taken from another gene and then added 
        #back to the first changing nothing
        missing = []
        added = []
        output[0]._gene_array[0].tasksqueue
        idx = 0

        for task in output[0]._gene_array[0].tasksqueue:
            if task not in gene0.tasksqueue and task in gene1.tasksqueue + gene2.tasksqueue:
                added.append(task)
            elif task not in gene0.tasksqueue and task not in gene1.tasksqueue + gene2.tasksqueue:
                assert(not task)
            else:
                continue
        for task in gene0.tasksqueue:
            if task not in output[0]._gene_array[0].tasksqueue:
                missing.append(task)
    # -----------------------------------------------------------

        for task in output[0]._gene_array[1].tasksqueue:
            if task not in gene1.tasksqueue and task in gene0.tasksqueue + gene2.tasksqueue:
                added.append(task)
            elif task not in gene1.tasksqueue and task not in gene0.tasksqueue + gene2.tasksqueue:
                assert(not task)
            else:
                continue

        for task in gene1.tasksqueue:
            if task not in output[0]._gene_array[1].tasksqueue:
                missing.append(task)
    
    # -----------------------------------------------------------

        for task in output[0]._gene_array[2].tasksqueue:
            if task not in gene2.tasksqueue and task in gene0.tasksqueue + gene1.tasksqueue:
                added.append(task)
            elif task not in gene2.tasksqueue and task not in gene0.tasksqueue + gene1.tasksqueue:
                assert(not task)
            else:
                continue

        for task in gene2.tasksqueue:
            if task not in output[0]._gene_array[2].tasksqueue:
                missing.append(task)


        assert(len(missing) == len(added))
        assert(len(missing) <= len(output[0]._gene_array))

        for task in missing:
            assert(task in added)

    def test_coefficient3(self):
        src.mutation_coefficient1 = 1
        src.mutation_coefficient2 = 0
        src.mutation_coefficient3 = 0
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene0 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene1 = src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(8, "Succeeded",tenant3)])
        test = src.Genotype([gene0, gene1, gene2])
        output = [copy.deepcopy(test)]
        print(f"this is the original : {test}")

        src.mutation_coefficient1 = 0
        src.mutation_coefficient2 = 0
        src.mutation_coefficient3 = 1
        src.mutation(output)
        print(f"this is the third mutation {output[0]}")

        all_tasks = [task for gene in test._gene_array for task in gene.tasksqueue]
        print(f"this is all tasks {all_tasks}")

        for gene in output[0]._gene_array:
            for task in gene.tasksqueue:
                assert(task in all_tasks)
                all_tasks.remove(task)
        assert(not all_tasks)


            

class TestSelection:
    #cant test internal functions so only test the src.selection
    def test_selection(self):
        #we do not need genes only the fitnessvalue
        arr1 = [src.Genotype([],0.7),src.Genotype([],0.5),src.Genotype([],0.5),src.Genotype([],0.3),src.Genotype([],0.6),src.Genotype([],0.1),src.Genotype([],0.2),src.Genotype([],0.4)]
        arr2 = [src.Genotype([],0.6), src.Genotype([],0.6), src.Genotype([],0.766), src.Genotype([],0.53333), src.Genotype([],0.83342), src.Genotype([],0.23479), src.Genotype([],0.5467)]
        arr3 = [src.Genotype([],0.0), src.Genotype([],1.0), src.Genotype([],0.5)]
        arr4 = [src.Genotype([],0.123123)]

        #there are two gentypes with the fitness of 5 only one should be in the set
        test1 = src.selection(3,arr1)
        assert(len(test1) == 3)
        assert(all(element.fitnessvalue in [0.7,0.6,0.5] for element in test1 ))

        #test with float
        test2 = src.selection(5,arr2)
        assert(len(test2) == 5)
        assert(all(element.fitnessvalue in [0.83342,0.766,0.6,0.6,0.5467] for element in test2))

        #test with 0 and 1 the extreme values
        test3 = src.selection(3,arr3)
        assert(len(test3) == 3)
        assert(all(element.fitnessvalue in [0.0,1.0,0.5] for element in test3))
        
        #test with single element
        test4 = src.selection(1,arr4)
        assert(len(test4) == 1)
        assert(all(element.fitnessvalue in [0.123123] for element in test4))

class TestParentSelection:
    def test_parent_selection(self):
        #we do not need genes only the fitnessvalue
        arr1 = [src.Genotype([],0.7),src.Genotype([],0.5),src.Genotype([],0.5),src.Genotype([],0.3),src.Genotype([],0.6),src.Genotype([],0.1),src.Genotype([],0.2),src.Genotype([],0.4)]
        arr2 = [src.Genotype([],0.6), src.Genotype([],0.6), src.Genotype([],0.766), src.Genotype([],0.53333), src.Genotype([],0.83342), src.Genotype([],0.23479), src.Genotype([],0.5467)]
        arr3 = [src.Genotype([],0.0), src.Genotype([],1.0), src.Genotype([],0.5)]
        arr4 = [src.Genotype([],0.123123)]

        # general test the src.k means the lowest src.k-1 fitnessvalues cannot br chosen
        src.n_parents = 4
        src.k = 4
        assert(src.n_parents == 4)
        test1 = src.parent_selection(arr1)
        assert(len(test1)==4)
        assert(not any(element.fitnessvalue in [0.1,0.2,0.3] for element in test1))
        assert(all(element.fitnessvalue in [0.7,0.6,0.5,0.4] for element in test1))
        
        src.n_parents = 3
        src.k = 5
        test2 = src.parent_selection(arr2)
        assert(len(test2)==3)
        #0.6 is double so it is the lowest 3 even though src.k ==5
        assert(not any(element.fitnessvalue in [0.23479, 0.5467, 0.53333] for element in test2))
        assert(all(element.fitnessvalue in [0.6, 0.766, 0.83342] for element in test2))

        with pytest.raises(ValueError):
            src.n_parents = 1
            src.k = 8
            src.parent_selection(arr1)

        with pytest.raises(ValueError):
            src.n_parents = 1
            src.k = 0
            src.parent_selection(arr1)
        src.n_parents = 1
        src.k = 2
        test3 = src.parent_selection(arr3)
        assert(len(test3)==1)
        assert(all(element.fitnessvalue in [1.0, 0.5] for element in test3))

class TestParticallyMappedCrossover:
    def test_partially_mapped_crossover_basic(self):
        #number of tasks per node is from one parent
        #decide crossover point before that take from one parent after that
        #take from the other parent
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene1 = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(1, "Succeeded",tenant1)])
        gene2 = src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene3 = src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(5, "Succeeded",tenant3)])

        gene4 = src.Gene(src.Node(1), [src.Task(2, "Pending",tenant1), src.Task(4, "Succeeded",tenant3), src.Task(7, "Pending",tenant2)])
        gene5 = src.Gene(src.Node(2), [src.Task(3, "Pending",tenant1)])
        gene6 = src.Gene(src.Node(3), [src.Task(1, "Pending",tenant1), src.Task(0, "Succeeded",tenant1), src.Task(5, "Succeeded",tenant3),src.Task(6, "Pending",tenant2)])
        
        genotype1 = src.Genotype([gene1, gene2, gene3])
        genotype2 = src.Genotype([gene4, gene5, gene6])
        test = src.partially_mapped_crossover(genotype1,genotype2)
        #test is 
        #node1 2 n_of_chunks 1
        #node2 3 n_of_chunks 1
        #node3 3 n_of_chunks 1

        
        #there is two versions one where the chosen parent is parent 1
        #leftover: src.Task(1, "Succeeded",tenant1), src.Task(4, "Succeeded",tenant3) src.Task(7, "Pending",tenant2) src.Task(2, "Succeeded",tenant1)
        # test = src.Gene(src.Node(1), [src.Task(0, "Pending",tenant1), src.Task(2, "Pending",tenant1)])
        #        src.Gene(src.Node(2), [src.Task(4, "Pending",tenant3), src.Task(3, "Pending",tenant1), src.Task(6, "Succeeded",tenant2)])
        #        src.Gene(src.Node(3), [src.Task(7, "Pending",tenant2), src.Task(1, "Pending",tenant1), src.Task(5, "Succeeded",tenant3)])

        #and one version where the chosen parent is parent 2
        #leftover: src.Task(1, "Succeeded",tenant1), src.Task(4, "Succeeded",tenant3) src.Task(7, "Pending",tenant2) src.Task(2, "Succeeded",tenant1)
        # test = src.Gene(src.Node(1), [src.Task(2, "Pending",tenant1), src.Task(0, "Pending",tenant1)])
        #        src.Gene(src.Node(2), [src.Task(3, "Pending",tenant1), src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2)])
        #        src.Gene(src.Node(3), [src.Task(1, "Pending",tenant1), src.Task(7, "Pending",tenant2), src.Task(5, "Succeeded",tenant3)])
        assert(test._gene_array[0].resource.id == 1)
        assert(len(test._gene_array[0].tasksqueue) == 2)
        assert(test._gene_array[0].tasksqueue[0].id in [0,2])
        assert(test._gene_array[0].tasksqueue[1].id in [2,0])

        assert(test._gene_array[1].resource.id == 2)
        assert(len(test._gene_array[1].tasksqueue) == 3)
        assert(test._gene_array[1].tasksqueue[0].id in [4,3])
        assert(test._gene_array[1].tasksqueue[1].id in [3,4])
        assert(test._gene_array[1].tasksqueue[2].id == 6)

        assert(test._gene_array[2].resource.id == 3)
        assert(len(test._gene_array[2].tasksqueue) == 3)
        assert(test._gene_array[2].tasksqueue[0].id in [1,7])
        assert(test._gene_array[2].tasksqueue[1].id in [7,1])
        assert(test._gene_array[2].tasksqueue[2].id == 5)

    def test_partially_mapped_crossover_edge_case_1(self):
        tenant1 = src.Tenant("paul")
        tenant2 = src.Tenant("peter")
        tenant3 = src.Tenant("max")
        gene1 = src.Gene(src.Node(1), [])
        gene2 = src.Gene(src.Node(2), [src.Task(0, "Pending",tenant1), src.Task(4, "Pending",tenant3), src.Task(6, "Succeeded",tenant2), src.Task(2, "Succeeded",tenant1)])
        gene3 = src.Gene(src.Node(3), [src.Task(1, "Succeeded",tenant1), src.Task(7, "Pending",tenant2), src.Task(3, "Succeeded",tenant1), src.Task(5, "Succeeded",tenant3)])

        gene4 = src.Gene(src.Node(1), [src.Task(2, "Pending",tenant1), src.Task(4, "Succeeded",tenant3), src.Task(7, "Pending",tenant2)])
        gene5 = src.Gene(src.Node(2), [src.Task(3, "Pending",tenant1)])
        gene6 = src.Gene(src.Node(3), [src.Task(1, "Pending",tenant1), src.Task(0, "Succeeded",tenant1), src.Task(5, "Succeeded",tenant3), src.Task(6, "Pending",tenant2)])
        
        genotype1 = src.Genotype([gene1, gene2, gene3])
        genotype2 = src.Genotype([gene4, gene5, gene6])
        test = src.partially_mapped_crossover(genotype1,genotype2)
        assert(test._gene_array[0].resource.id == 1)
        assert(len(test._gene_array[0].tasksqueue) == 0)
        assert(test._gene_array[1].resource.id == 2)
        assert(len(test._gene_array[1].tasksqueue) == 4)
        assert(test._gene_array[2].resource.id == 3)
        assert(len(test._gene_array[2].tasksqueue) == 4)

        taskarray = []

        for gene in test._gene_array:
            for task in gene.tasksqueue:
                assert(task.id not in taskarray)
                taskarray.append(task.id)
        assert(len(taskarray) == 8)
        