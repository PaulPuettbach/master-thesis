import pytest
from ..daemon.src.EA_worker import *
import copy

class TestInstantiation:
    def test_tenant(self):
        with pytest.raises(ValueError):
            #positive id
            invalid = Tenant(-2)
        valid1 = Tenant("paul")
        assert valid1.id == 0
        valid2 = Tenant("peter")
        assert valid2.id == 1
        valid2 = Tenant("frank")
        assert valid2.id == 2
        valid3 = Tenant("paul")
        assert valid3.id == 0

    def test_task(self):
        with pytest.raises(ValueError):
            # id should be positive integer
            invalid = Task(-2, "Pending",Tenant("paul"))
        with pytest.raises(ValueError):
            # status has to be one of the allowed strings
            invalid2 = Task(0, "something",Tenant("paul"))
        with pytest.raises(ValueError):
            invalid3 = Task(0, "Pending",0)
        with pytest.raises(ValueError):
            invalid4 = Task(0, "Pending",Tenant("paul"),"something")
        with pytest.raises(ValueError):
            invalid5 = Task(0, "Pending",Tenant("paul"),True)
        with pytest.raises(ValueError):
            invalid6 = Task(0, "Pending",Tenant("paul"),False, Node(1))
        valid = Task(0, "Pending",Tenant("paul"), True, Node(1))
        valid2 = Task(0, "Schedule",Tenant("paul"))
        valid3 = Task(0, "Finished",Tenant("paul"), False, None)

    def test_gene(self):
        valid = Gene(Node(1), [])
        valid2 = Gene(Node(1), [Task(0, "Pending",Tenant("paul"), True, Node(1)), Task(0, "Schedule",Tenant("paul"))])
        with pytest.raises(ValueError):
            invalid = Gene(Task(0, "Finished",Tenant("paul"), False, None), [])
        with pytest.raises(ValueError):
            invalid2 = Gene(Node(0), [Task(0, "Schedule",Tenant("paul")),Task(1, "Schedule",Tenant("peter")), Node(9)])
    
    def test_geneotype(self):
        gene1 = Gene(Node(1), [])
        gene2 = Gene(Node(1), [Task(0, "Pending",Tenant("paul"), True, Node(1)), Task(1, "Schedule",Tenant("paul"))])
        valid = Genotype([])
        valid2 = Genotype([gene1,gene2])
        valid3 = Genotype([], 1.0)
        valid4 = Genotype([gene1,gene2], 0.8)
        with pytest.raises(ValueError):
            invalid = Genotype([Node(9)])
        with pytest.raises(ValueError):
            invalid2 = Genotype([gene1,gene2], -2.0)
        with pytest.raises(ValueError):
            invalid3 = Genotype([gene1,gene2], 1)
        with pytest.raises(ValueError):
            invalid3 = Genotype([gene1,gene2], 15.0)
        with pytest.raises(ValueError):
            #cant append task with the same id
            valid2.append_task(Task(1, "Schedule",Tenant("paul")), Node(1))

        valid2.append_task(Task(2, "Schedule",Tenant("paul")), Node(1))
    
    def test_population(self):
        gene1 = Gene(Node(1), [])
        gene2 = Gene(Node(1), [Task(0, "Pending",Tenant("paul"), True, Node(1)), Task(1, "Schedule",Tenant("paul"))])
        individual1 = Genotype([])
        individual2 = Genotype([gene1,gene2])
        individual3 = Genotype([], 1.0)
        valid = Population([])
        valid2 = Population([individual1,individual2,individual3])
        with pytest.raises(ValueError):
            valid2 = Population([individual1,individual2,individual3, Node(9)])
    
    def test_currentresources(self):
        valid1 = CurrentResources([Node(0), Node(1)])
        valid2 = CurrentResources([])
        with pytest.raises(ValueError):
            invalid1 = CurrentResources([Gene(Node(1), []), Gene(Node(1), [Task(0, "Pending",Tenant("paul"), True, Node(1)), Task(1, "Schedule",Tenant("paul"))])])

    def test_currenttaskqueue(self):
        valid1 = CurrentTaskqueue([Task(0, "Schedule",Tenant("paul")), Task(1, "Pending",Tenant("paul"))])
        valid2 = CurrentTaskqueue([])
        with pytest.raises(ValueError):
            invalid1 = CurrentResources([Gene(Node(1), []), Gene(Node(1), [Task(0, "Pending",Tenant("paul"), True, Node(1)), Task(1, "Schedule",Tenant("paul"))])])

    def test_node(self):
        valid1 = Node(0)
        valid2 = Node(15)
        with pytest.raises(ValueError):
            invalid1 = Node(0.0)
        with pytest.raises(ValueError):
            invalid2 = Node(-3)

class TestEq:
    def test_tenant_eq(self):
        tenant1 = Tenant("paul")
        tenant2 = Tenant("paul")
        tenant3 = Tenant("peter")
        nottenant = Node(1)

        assert(tenant1 == tenant2)
        assert(not(tenant2 == tenant3))
        with pytest.raises(ValueError):
            tenant1 == nottenant

    def test_task_eq(self):
        task1 = Task(0, "Pending",Tenant("paul"), True, Node(1))
        task2 = Task(0, "Schedule",Tenant("paul"))
        task3 = Task(1, "Finished",Tenant("paul"), False, None)
        nottask = Node(1)

        assert(task1 == task2)
        assert(not (task1 == task3))
        with pytest.raises(ValueError):
            task1 == nottask
    
    def test_gene_eq(self):
        tenant1 = Tenant("paul")
        tenant3 = Tenant("max")
        gene1 = Gene(Node(1), [Task(0, "Pending",tenant1), Task(1, "Schedule",tenant1)])
        gene2 = Gene(Node(1), [Task(1, "Schedule",tenant1), Task(0, "Pending",tenant1)])
        gene3 = Gene(Node(1), [Task(0, "Pending",tenant1), Task(1, "Schedule",tenant1)])

        assert(not(gene1 == gene2))
        assert(gene1 == gene3)
        with pytest.raises(ValueError):
            gene1 ==tenant3

    def test_geneotype_eq(self):
        tenant1 = Tenant("paul")
        tenant2 = Tenant("peter")
        tenant3 = Tenant("max")
        gene1 = Gene(Node(1), [Task(0, "Pending",tenant1), Task(1, "Schedule",tenant1)])
        gene2 = Gene(Node(2), [Task(4, "Pending",tenant3), Task(6, "Schedule",tenant2), Task(2, "Schedule",tenant1)])
        gene3 = Gene(Node(3), [Task(7, "Pending",tenant2), Task(3, "Schedule",tenant1), Task(8, "Schedule",tenant3)])
        gene4 = Gene(Node(1), [Task(1, "Schedule",tenant1), Task(0, "Pending",tenant1)])
        
        test1 = Genotype([gene1, gene2, gene3])
        test2 = Genotype([gene4, gene2, gene3])
        test3 = Genotype([gene1, gene2, gene3])
        test4 = Genotype([gene1, gene3, gene2])

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
        tenant1 = Tenant("paul")
        tenant2 = Tenant("peter")
        tenant3 = Tenant("max")
        gene1 = Gene(Node(1), [Task(0, "Pending",tenant1), Task(1, "Schedule",tenant1)])
        gene2 = Gene(Node(2), [Task(4, "Pending",tenant3), Task(6, "Schedule",tenant2), Task(2, "Schedule",tenant1)])
        gene3 = Gene(Node(3), [Task(7, "Pending",tenant2), Task(3, "Schedule",tenant1), Task(8, "Schedule",tenant3)])

        test = Genotype([gene1, gene2, gene3])
        #tenant1 = [sum of taskposition = 8, number of tasks = 4]
        #tenant2 = [sum of taskposition = 3, number of tasks = 2]
        #tenant3 = [sum of taskposition = 4, number of tasks = 2]
        
        # the number of repeating places after the comma is taken from commandline python
        #eg (python3 -c "print(2/3)")
        #normalized fairness = (0.5, 0.6666666666666666,0.5)
        #mean normalized fairness = (0.5555555555555555)
        #individual error = (0.05555555555555547, 0.11111111111111116, 0.05555555555555547)
        #sum = 0.2222222222222221
        #inverse_error = 0.07407407407407403
        #error 0.9259259259259259
        assert fairness(test) == 0.9259259259259259

        fairgene1 = Gene(Node(1), [Task(0, "Pending",tenant1), Task(1, "Schedule",tenant2), Task(2, "Schedule",tenant3)])
        fairgene2 = Gene(Node(1), [Task(0, "Pending",tenant3), Task(1, "Schedule",tenant1), Task(2, "Schedule",tenant2)])
        fairgene3 = Gene(Node(1), [Task(0, "Pending",tenant2), Task(1, "Schedule",tenant3), Task(2, "Schedule",tenant1)])

        fairtest = Genotype([fairgene1, fairgene2, fairgene3])
        assert fairness(fairtest) == 1.0
    
    def test_locality(self):
        tenant1 = Tenant("paul")
        tenant2 = Tenant("peter")
        tenant3 = Tenant("max")
        gene1 = Gene(Node(1), [Task(0, "Pending",tenant1), Task(1, "Schedule",tenant1)])
        gene2 = Gene(Node(2), [Task(4, "Pending",tenant3), Task(6, "Schedule",tenant2), Task(2, "Schedule",tenant1)])
        gene3 = Gene(Node(3), [Task(7, "Pending",tenant2), Task(3, "Schedule",tenant1), Task(8, "Schedule",tenant3)])
        test = Genotype([gene1, gene2, gene3])

        #noramlized_resources_per_tenant is number nodes divided tasks by tenant
        # = (3/4, 2/2, 2/2)
        # = (0.75, 1, 1)
        # mean normalized resources per tenant 0.9166666666666666
        # 1 - mean normalized resources per tenant 1 - 0.9166666666666666 = 0.08333333333333337
        assert locality(test) == 0.08333333333333337

class TestInit:
    def test_init(self):
        current_resources.resource_array.extend([Node(0), Node(1), Node(2)])
        tenant1 = Tenant("paul")
        tenant2 = Tenant("peter")
        tenant3 = Tenant("max")
        poolsize = 10
        test = init(poolsize, [Task(0, "Pending",tenant1),Task(1, "Schedule",tenant1), Task(2, "Schedule",tenant1),\
                         Task(3, "Schedule",tenant1),Task(4, "Pending",tenant3),Task(5, "Schedule",tenant2),\
                         Task(6, "Pending",tenant2),Task(7, "Schedule",tenant3)])
        assert(all(isinstance(element, Genotype) for element in test))
        assert(len(test)==poolsize)

class TestSelection:
    #cant test internal functions so only test the selection
    def test_selection(self):
        #we do not need genes only the fitnessvalue
        arr1 = [Genotype([],0.7),Genotype([],0.5),Genotype([],0.5),Genotype([],0.3),Genotype([],0.6),Genotype([],0.1),Genotype([],0.2),Genotype([],0.4)]
        arr2 = [Genotype([],0.6), Genotype([],0.6), Genotype([],0.766), Genotype([],0.53333), Genotype([],0.83342), Genotype([],0.23479), Genotype([],0.5467)]
        arr3 = [Genotype([],0.0), Genotype([],1.0), Genotype([],0.5)]
        arr4 = [Genotype([],0.123123)]

        #there are two gentypes with the fitness of 5 only one should be in the set
        test1 = selection(3,arr1)
        assert(len(test1) == 3)
        assert(all(element.fitnessvalue in [0.7,0.6,0.5] for element in test1 ))

        #test with float
        test2 = selection(5,arr2)
        assert(len(test2) == 5)
        assert(all(element.fitnessvalue in [0.83342,0.766,0.6,0.6,0.5467] for element in test2))

        #test with 0 and 1 the extreme values
        test3 = selection(3,arr3)
        assert(len(test3) == 3)
        assert(all(element.fitnessvalue in [0.0,1.0,0.5] for element in test3))
        
        #test with single element
        test4 = selection(1,arr4)
        assert(len(test4) == 1)
        assert(all(element.fitnessvalue in [0.123123] for element in test4))

class TestParentSelection:
    def test_parent_selection(self):
        #we do not need genes only the fitnessvalue
        arr1 = [Genotype([],0.7),Genotype([],0.5),Genotype([],0.5),Genotype([],0.3),Genotype([],0.6),Genotype([],0.1),Genotype([],0.2),Genotype([],0.4)]
        arr2 = [Genotype([],0.6), Genotype([],0.6), Genotype([],0.766), Genotype([],0.53333), Genotype([],0.83342), Genotype([],0.23479), Genotype([],0.5467)]
        arr3 = [Genotype([],0.0), Genotype([],1.0), Genotype([],0.5)]
        arr4 = [Genotype([],0.123123)]

        # general test the k means the lowest k-1 fitnessvalues cannot br chosen
        test1 = parent_selection(4,4,arr1)
        assert(len(test1)==4)
        assert(not any(element.fitnessvalue in [0.1,0.2,0.3] for element in test1))
        assert(all(element.fitnessvalue in [0.7,0.6,0.5,0.4] for element in test1))

        test2 = parent_selection(3,5,arr2)
        assert(len(test2)==3)
        #0.6 is double so it is the lowest 3 even though k ==5
        assert(not any(element.fitnessvalue in [0.23479, 0.5467, 0.53333] for element in test2))
        assert(all(element.fitnessvalue in [0.6, 0.766, 0.83342] for element in test2))

        with pytest.raises(ValueError):
            parent_selection(1,8,arr1)

        with pytest.raises(ValueError):
            parent_selection(1,0,arr1)
        
        test3 = parent_selection(1,2,arr3)
        assert(len(test3)==1)
        assert(all(element.fitnessvalue in [1.0, 0.5] for element in test3))

class TestParticallyMappedCrossover:
    def test_partially_mapped_crossover_basic(self):
        #number of tasks per node is from one parent
        #decide crossover point before that take from one parent after that
        #take from the other parent
        tenant1 = Tenant("paul")
        tenant2 = Tenant("peter")
        tenant3 = Tenant("max")
        gene1 = Gene(Node(1), [Task(0, "Pending",tenant1), Task(1, "Schedule",tenant1)])
        gene2 = Gene(Node(2), [Task(4, "Pending",tenant3), Task(6, "Schedule",tenant2), Task(2, "Schedule",tenant1)])
        gene3 = Gene(Node(3), [Task(7, "Pending",tenant2), Task(3, "Schedule",tenant1), Task(5, "Schedule",tenant3)])

        gene4 = Gene(Node(1), [Task(2, "Pending",tenant1), Task(4, "Schedule",tenant3), Task(7, "Pending",tenant2)])
        gene5 = Gene(Node(2), [Task(3, "Pending",tenant1)])
        gene6 = Gene(Node(3), [Task(1, "Pending",tenant1), Task(0, "Schedule",tenant1), Task(5, "Schedule",tenant3),Task(6, "Pending",tenant2)])
        
        genotype1 = Genotype([gene1, gene2, gene3])
        genotype2 = Genotype([gene4, gene5, gene6])
        test = partially_mapped_crossover(genotype1,genotype2)
        #test is 
        #node1 2 n_of_chunks 1
        #node2 3 n_of_chunks 1
        #node3 3 n_of_chunks 1

        
        #there is two versions one where the chosen parent is parent 1
        #leftover: Task(1, "Schedule",tenant1), Task(4, "Schedule",tenant3) Task(7, "Pending",tenant2) Task(2, "Schedule",tenant1)
        # test = Gene(Node(1), [Task(0, "Pending",tenant1), Task(2, "Pending",tenant1)])
        #        Gene(Node(2), [Task(4, "Pending",tenant3), Task(3, "Pending",tenant1), Task(6, "Schedule",tenant2)])
        #        Gene(Node(3), [Task(7, "Pending",tenant2), Task(1, "Pending",tenant1), Task(5, "Schedule",tenant3)])

        #and one version where the chosen parent is parent 2
        #leftover: Task(1, "Schedule",tenant1), Task(4, "Schedule",tenant3) Task(7, "Pending",tenant2) Task(2, "Schedule",tenant1)
        # test = Gene(Node(1), [Task(2, "Pending",tenant1), Task(0, "Pending",tenant1)])
        #        Gene(Node(2), [Task(3, "Pending",tenant1), Task(4, "Pending",tenant3), Task(6, "Schedule",tenant2)])
        #        Gene(Node(3), [Task(1, "Pending",tenant1), Task(7, "Pending",tenant2), Task(5, "Schedule",tenant3)])
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
        tenant1 = Tenant("paul")
        tenant2 = Tenant("peter")
        tenant3 = Tenant("max")
        gene1 = Gene(Node(1), [])
        gene2 = Gene(Node(2), [Task(0, "Pending",tenant1), Task(4, "Pending",tenant3), Task(6, "Schedule",tenant2), Task(2, "Schedule",tenant1)])
        gene3 = Gene(Node(3), [Task(1, "Schedule",tenant1), Task(7, "Pending",tenant2), Task(3, "Schedule",tenant1), Task(5, "Schedule",tenant3)])

        gene4 = Gene(Node(1), [Task(2, "Pending",tenant1), Task(4, "Schedule",tenant3), Task(7, "Pending",tenant2)])
        gene5 = Gene(Node(2), [Task(3, "Pending",tenant1)])
        gene6 = Gene(Node(3), [Task(1, "Pending",tenant1), Task(0, "Schedule",tenant1), Task(5, "Schedule",tenant3), Task(6, "Pending",tenant2)])
        
        genotype1 = Genotype([gene1, gene2, gene3])
        genotype2 = Genotype([gene4, gene5, gene6])
        test = partially_mapped_crossover(genotype1,genotype2)
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
        