from ..daemon.src.EA_worker import *

current_resources.resource_array.extend([Node(0), Node(1), Node(2)])
tenant1 = Tenant(0)
tenant2 = Tenant(1)
tenant3 = Tenant(2)
test = init(10, [Task(0, "Pending",tenant1),Task(1, "Schedule",tenant1), Task(2, "Schedule",tenant1),\
                    Task(3, "Schedule",tenant1),Task(4, "Pending",tenant3),Task(5, "Schedule",tenant2),\
                    Task(6, "Pending",tenant2),Task(7, "Schedule",tenant3)])
print(f"this is the size of test {len(test)}")
for genotype in test:
    print("-------------------------------\n")
    print(genotype)