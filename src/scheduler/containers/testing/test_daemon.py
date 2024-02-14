import pytest
from ..daemon.src.EA_worker import *

class TestInstantiation:
    def test_tenant(self):
        with pytest.raises(ValueError):
            invalid = Tenant(-2)
        valid1 = Tenant(0)
        assert valid1.id == 0
        valid2 = Tenant(1)
        assert valid1.id == 1

    def test_task(self):
        with pytest.raises(ValueError):
            invalid = Task(-2, "Pending",Tenant(0))
        with pytest.raises(ValueError):
            invalid2 = Task(0, "something",Tenant(0))
        with pytest.raises(ValueError):
            invalid3 = Task(0, "Pending",0)
        with pytest.raises(ValueError):
            invalid4 = Task(0, "Pending",Tenant(0),"something")
        with pytest.raises(ValueError):
            invalid5 = Task(0, "Pending",Tenant(0),True)
        with pytest.raises(ValueError):
            invalid6 = Task(0, "Pending",Tenant(0),False, Node(1))
        valid = Task(0, "Pending",Tenant(0), True, Node(1))
        valid2 = Task(0, "Schedule",Tenant(0))
        valid3 = Task(0, "Finished",Tenant(0), False, None)

    def test_gene(self):
        valid = Gene(Node(1), [])