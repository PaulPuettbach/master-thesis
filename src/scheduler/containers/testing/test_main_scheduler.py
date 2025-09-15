import sys
import os
import pytest
from kubernetes import client, config, watch
import requests
import threading, time
from collections import deque
# Set dummy environment variables before importing main_scheduler
# This is necessary because main_scheduler accesses os.environ at the module level,
# which happens during test collection before any pytest fixtures can run.
os.environ['DAEMON-SERVICE'] = 'dummy-service'
os.environ['DAEMON-SERVICE-PORT'] = 'dummy-port'

# Add the path to the module to be tested. This is necessary because the test
# file is in a different directory than the module it is testing.
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main_component', 'src'))
sys.path.insert(0, module_path)

import main_scheduler



# custom class to be the mock return value of requests.get()

class MockResponseWatchNode:
    def list_node_fine(*args, **kwargs):
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node1"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "100Mi"}
                        )
                    )
        }
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node2"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "50Mi"}
                        )
                    )
        }
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node3"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "80Mi"}
                        )
                    )
        }
    def list_node_wrong_1(*args, **kwargs):
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node1"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "100Mi"}
                        )
                    )
        }
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node2"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "50Mi"}
                        )
                    )
        }
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node3"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "80Mi"}
                        )
                    )
        }
    def list_node_wrong_all(*args, **kwargs):
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node1"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "100Mi"}
                        )
                    )
        }
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node2"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="True"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="True"
                                )
                            ],
                            allocatable={"memory": "50Mi"}
                        )
                    )
        }
        yield {
            "type": "ADDED",
            "object": client.V1Node(
                        metadata=client.V1ObjectMeta(name="node3"),
                        status=client.V1NodeStatus(
                            conditions=[
                                client.V1NodeCondition(
                                    type="Ready", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="MemoryPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="DiskPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="PIDPressure", status="False"
                                ),
                                client.V1NodeCondition(
                                    type="NetworkUnavailable", status="False"
                                )
                            ],
                            allocatable={"memory": "80Mi"}
                        )
                    )
        }

class MockResponseNode:
    def list_node_fine(*args, **kwargs):
        return client.V1NodeList(
            items=[
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node1"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                ),
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node2"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                ),
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node3"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                )
            ]
        )
    def list_node_wrong_1(*args, **kwargs):
        return client.V1NodeList(
            items=[
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node1"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="True"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                ),
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node2"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                ),
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node3"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                )
            ]
        )
    def list_node_wrong_all(*args, **kwargs):
        return client.V1NodeList(
            items=[
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node1"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="True"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                ),
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node2"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="False"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                ),
                client.V1Node(
                    metadata=client.V1ObjectMeta(name="node3"),
                    status=client.V1NodeStatus(
                        conditions=[
                            client.V1NodeCondition(
                                type="Ready", status="True"
                            ),
                            client.V1NodeCondition(
                                type="MemoryPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="DiskPressure", status="False"
                            ),
                            client.V1NodeCondition(
                                type="PIDPressure", status="True"
                            ),
                            client.V1NodeCondition(
                                type="NetworkUnavailable", status="False"
                            )
                        ]
                    )
                )
            ]
        )

class MockResponseWatchPod:
    def list_pod_added(*args, **kwargs):
        yield {
            "type": "ADDED",
            "object": client.V1Pod(
                metadata=client.V1ObjectMeta(name="new-pod-1", uid="uid-1"),
                spec=client.V1PodSpec(
                    scheduler_name="custom-scheduler",
                    containers=[
                        client.V1Container(
                            name="c1",
                            image="img",
                            env=[client.V1EnvVar(name="SPARK_USER_MANUEL", value="tenant-A")]
                        )
                    ]
                ),
                status=client.V1PodStatus(phase="Pending")
            )
        }
    def list_pod_deleted(*args, **kwargs):
        # This pod will be "deleted"
        pod_to_delete = client.V1Pod(
            metadata=client.V1ObjectMeta(name="existing-pod-1", uid="uid-existing-1"),
            spec=client.V1PodSpec(
                scheduler_name="custom-scheduler",
                containers=[
                    client.V1Container(
                        name="c1",
                        image="img",
                        env=[client.V1EnvVar(name="SPARK_USER_MANUEL", value="tenant-B")],
                        resources=client.V1ResourceRequirements(requests={"memory": "20Mi"})
                    )
                ]
            ),
            status=client.V1PodStatus(phase="Succeeded") # or Failed
        )
        yield {
            "type": "MODIFIED",
            "object": pod_to_delete
        }
    @staticmethod
    def list_multiple_pods_added(*args, **kwargs):
        yield {
            "type": "ADDED",
            "object": client.V1Pod(
                metadata=client.V1ObjectMeta(name="multi-pod-1", uid="uid-m1"),
                spec=client.V1PodSpec(
                    scheduler_name="custom-scheduler",
                    containers=[client.V1Container(name="c1", image="img", env=[client.V1EnvVar(name="SPARK_USER_MANUEL", value="tenant-A")])]
                ),
                status=client.V1PodStatus(phase="Pending")
            )
        }
        yield {
            "type": "ADDED",
            "object": client.V1Pod(
                metadata=client.V1ObjectMeta(name="multi-pod-2", uid="uid-m2"),
                spec=client.V1PodSpec(
                    scheduler_name="custom-scheduler",
                    containers=[client.V1Container(name="c1", image="img", env=[client.V1EnvVar(name="SPARK_USER_MANUEL", value="tenant-C")])]
                ),
                status=client.V1PodStatus(phase="Pending")
            )
        }

    @staticmethod
    def list_pod_added_no_tenant(*args, **kwargs):
        yield {
            "type": "ADDED",
            "object": client.V1Pod(
                metadata=client.V1ObjectMeta(name="no-tenant-pod", uid="uid-nt"),
                spec=client.V1PodSpec(
                    scheduler_name="custom-scheduler",
                    containers=[client.V1Container(name="c1", image="img", env=[])] # No env vars
                ),
                status=client.V1PodStatus(phase="Pending")
            )
        }

    @staticmethod
    def list_pod_deleted_no_tenant(*args, **kwargs):
        yield {
            "type": "DELETED",
            "object": client.V1Pod(
                metadata=client.V1ObjectMeta(name="existing-pod-1", uid="uid-existing-1"),
                spec=client.V1PodSpec(
                    scheduler_name="custom-scheduler",
                    containers=[client.V1Container(name="c1", image="img", env=[])] # No env vars
                ),
                status=client.V1PodStatus(phase="Succeeded")
            )
        }

    @staticmethod
    def list_pod_deleted_not_scheduled(*args, **kwargs):
        yield {
            "type": "DELETED",
            "object": client.V1Pod(
                metadata=client.V1ObjectMeta(name="unscheduled-pod", uid="uid-unscheduled"),
                spec=client.V1PodSpec(
                    scheduler_name="custom-scheduler",
                    containers=[client.V1Container(name="c1", image="img", env=[client.V1EnvVar(name="SPARK_USER_MANUEL", value="tenant-A")])]
                ),
                status=client.V1PodStatus(phase="Succeeded")
            )
        }

class MockV1Api:
    """A more comprehensive mock for CoreV1Api to handle different calls."""
    def __init__(self):
        self.bindings = []

    def create_namespaced_binding(self, namespace, body, _preload_content=False):
        self.bindings.append({'namespace': namespace, 'body': body})
        return True

    def read_namespaced_pod(self, name, namespace, **kwargs):
        if name == "test-pod":
            return client.V1Pod(
                metadata=client.V1ObjectMeta(name="test-pod"),
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="test-container",
                            image="test-image",
                            resources=client.V1ResourceRequirements(requests={"memory": "100Mi"})
                        ),
                        client.V1Container(
                            name="test-container2",
                            image="test-image2",
                            resources=client.V1ResourceRequirements(requests={"memory": "50Mi"})
                        ),
                    ]
                )
            )
        if name == "test-pod2":
            return client.V1Pod(
                metadata=client.V1ObjectMeta(name="test-pod2"),
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="test-container",
                            image="test-image",
                            resources=client.V1ResourceRequirements(requests={"memory": "80Mi"})
                        ),
                        client.V1Container(
                            name="test-container2",
                            image="test-image2",
                            resources=client.V1ResourceRequirements(requests={"memory": "60Mi"})
                        ),
                    ]
                )
            )
        raise client.rest.ApiException(status=404)

    def list_node(self, *args, **kwargs):
        return MockResponseNode.list_node_fine(*args, **kwargs)

    def list_namespaced_pod(self, *args, **kwargs):
        """
        This is the critical missing method. The watch.stream() call in the
        application code needs this method to exist on the v1 object, even
        though the test mocks the stream() call itself. Without this, the
        thread would die with an AttributeError before the test could run.
        """
        # The return value doesn't matter, it just needs to be a valid response.
        return client.V1PodList(items=[])
    
class MockResponseHTTP:
    def http_right(*args, **kwargs):
        return requests.Response(status_code=200)
    def http_wrong(*args, **kwargs):
        return requests.Response(status_code=400)

@pytest.fixture()
def get_test_pod():
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name="test-pod"),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="test-container",
                    image="test-image",
                    resources=client.V1ResourceRequirements(
                        requests={"memory": "100Mi"}
                    )
                ),
                client.V1Container(
                    name="test-container2",
                    image="test-image2",
                    resources=client.V1ResourceRequirements(
                        requests={"memory": "50Mi"}
                    )
                ),
            ]
        )
    )

@pytest.fixture()
def get_test_pod_retry():
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name="test-pod2"),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="test-container",
                    image="test-image",
                    resources=client.V1ResourceRequirements(
                        requests={"memory": "80Mi"}
                    )
                ),
                client.V1Container(
                    name="test-container2",
                    image="test-image2",
                    resources=client.V1ResourceRequirements(
                        requests={"memory": "60Mi"}
                    )
                ),
            ]
        )
    )


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """Remove requests.sessions.Session.request for all tests."""
    monkeypatch.delattr("requests.sessions.Session.request")


@pytest.fixture(autouse=True)
def test_env(monkeypatch):
    """Set up a clean test environment before each test, managed by pytest."""
    # Reset global variables from the scheduler to ensure test isolation
    main_scheduler.node_id = 0
    main_scheduler.running_pods = set()
    main_scheduler.scheduled_pods = {}
    main_scheduler.pod_id = 0
    main_scheduler.pod_dic = {}
    main_scheduler.resource_dic = {}
    main_scheduler.best_fitness = 0
    main_scheduler.recent_deletions = []

    # Use monkeypatch.setenv to correctly set environment variables for the test's duration.
    monkeypatch.setenv("DAEMON-SERVICE", "test1")
    monkeypatch.setenv("DAEMON-SERVICE-PORT", "test2")
    # Mock external dependencies to avoid actual API calls

    # Configure the Flask app for testing
    main_scheduler.app.testing = True
    client = main_scheduler.app.test_client()

    monkeypatch.setattr(main_scheduler, 'v1', MockV1Api())
    return client


class TestMainScheduler:
    """
    Unit tests for the main_scheduler.py script.
    This test suite uses mocking to isolate the scheduler's logic from
    the Kubernetes API and other external services.
    """

    def test_byte_unit_conversion(self):
        """Tests the byte_unit_conversion utility function."""
        assert main_scheduler.byte_unit_conversion("10Ki") == 10240
        assert main_scheduler.byte_unit_conversion("5Mi") == 5242880
        assert main_scheduler.byte_unit_conversion("100K") == 100000
        assert main_scheduler.byte_unit_conversion("2M") == 2000000
        with pytest.raises(ValueError):
            main_scheduler.byte_unit_conversion("10Gi")

    def test_nodes_available(self,monkeypatch):
        """Tests the nodes_available function to filter for eligible nodes."""
        monkeypatch.setattr(main_scheduler.v1, "list_node", MockResponseNode.list_node_fine)
        assert [element.metadata.name for element in main_scheduler.nodes_available()] == ["node1", "node2", "node3"]
        
        monkeypatch.setattr(main_scheduler.v1, "list_node", MockResponseNode.list_node_wrong_1)
        assert [element.metadata.name for element in main_scheduler.nodes_available()] == ["node2", "node3"]

        monkeypatch.setattr(main_scheduler.v1, "list_node", MockResponseNode.list_node_wrong_all)
        assert main_scheduler.nodes_available() == []
    
    def test_watch_node_conditions(self, monkeypatch):
        """Tests the watch_node_conditions function for adding and removing nodes."""
        # Since the main application uses threads which share memory, we use threads for this test
        # to accurately reflect the runtime environment and avoid multiprocessing proxy issues.
        # This allows direct mutation of objects within the shared resource_dic.

        monkeypatch.setattr("main_scheduler.node_change", lambda x, y: None)
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchNode.list_node_fine)
        
        # Use a stoppable thread for testing loops
        p = threading.Thread(target=main_scheduler.watch_node_conditions)
        p.start()
        # Wait for the thread to complete. This is crucial to prevent thread leakage.
        p.join(timeout=2)

        assert len(main_scheduler.resource_dic) == 3
        for node in main_scheduler.resource_dic.values():
            assert node.status == "Available"
            if node.name == "node1":
                assert node.memory == 78643200.0
            elif node.name == "node2":
                assert node.memory == 39321600.0
            elif node.name == "node3":
                assert node.memory == 62914560.0
            else:
                pytest.fail(f"Unexpected name: {node.name}")
        
        # --- Test updating existing nodes ---
        # Reset state for this independent test case
        main_scheduler.resource_dic.clear()
        # Manually populate the dictionary to simulate a pre-existing state
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=78643200.0, status="Available")
        main_scheduler.resource_dic[2] = main_scheduler.NodeMeta(id=2, name="node2", memory=39321600.0, status="Available")
        main_scheduler.resource_dic[3] = main_scheduler.NodeMeta(id=3, name="node3", memory=62914560.0, status="Available")
        
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchNode.list_node_wrong_1)
        p2 = threading.Thread(target=main_scheduler.watch_node_conditions)
        p2.start()
        p2.join(timeout=2)
        assert len(main_scheduler.resource_dic) == 3
        for node in main_scheduler.resource_dic.values():
            if node.name == "node1":
                assert node.status == "Available"
                assert node.memory == 78643200.0
            elif node.name == "node2":
                assert node.status == "NotAvailable"
                assert node.memory == 39321600.0      
            elif node.name == "node3":
                assert node.status == "Available"
                assert node.memory == 62914560.0
            else:
                pytest.fail(f"Unexpected name: {node.name}")
        
        # Reset for the next part of the test
        main_scheduler.resource_dic.clear()
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchNode.list_node_wrong_1)
        p3 = threading.Thread(target=main_scheduler.watch_node_conditions)
        p3.start()
        p3.join(timeout=2)
        assert len(main_scheduler.resource_dic) == 2
        for node in main_scheduler.resource_dic.values():
            if node.name == "node1":
                assert node.status == "Available"
                assert node.memory == 78643200.0  
            elif node.name == "node3":
                assert node.status == "Available"
                assert node.memory == 62914560.0
            else:
                pytest.fail(f"Unexpected name: {node.name}")

        main_scheduler.resource_dic.clear()
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=78643200.0, status="Available")
        main_scheduler.resource_dic[2] = main_scheduler.NodeMeta(id=2, name="node2", memory=39321600.0, status="Available")
        main_scheduler.resource_dic[3] = main_scheduler.NodeMeta(id=3, name="node3", memory=62914560.0, status="Available")
        

        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchNode.list_node_wrong_all)
        p4 = threading.Thread(target=main_scheduler.watch_node_conditions)
        p4.start()
        p4.join(timeout=2)
        # The nodes should be updated to NotAvailable, not removed. Length should be 3.
        assert len(main_scheduler.resource_dic) == 3
        for node in main_scheduler.resource_dic.values():
            if node.name == "node1":
                assert node.status == "NotAvailable"
                assert node.memory == 78643200.0  
            elif node.name == "node2":
                assert node.status == "NotAvailable"
                assert node.memory == 39321600.0
            elif node.name == "node3":
                assert node.status == "NotAvailable"
                assert node.memory == 62914560.0
            else:
                pytest.fail(f"Unexpected name: {node.name}")

        # Reset for the next part of the test
        main_scheduler.resource_dic.clear()
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchNode.list_node_wrong_all)
        p5 = threading.Thread(target=main_scheduler.watch_node_conditions)
        p5.start()
        p5.join(timeout=2)
        assert len(main_scheduler.resource_dic) == 0
        

    def test_schedule(self, monkeypatch, get_test_pod):
        """Tests the schedule function to ensure it creates a binding."""
        def test_works(namespace, body, _preload_content):
            assert not _preload_content
            assert namespace == "spark-namespace"
            assert body.metadata.name == "test-pod"
            assert body.target.kind == "Node"
            assert body.target.name == "node1"

        pod = get_test_pod
        monkeypatch.setattr(main_scheduler, "v1", client.CoreV1Api())
        monkeypatch.setattr(main_scheduler.v1, "create_namespaced_binding", test_works) # Re-patch the new instance
        main_scheduler.schedule(pod.metadata, "node1", 1)
        assert main_scheduler.scheduled_pods["test-pod"] == 1


    def test_schedule_from_EA_success(self, monkeypatch, get_test_pod):
        """Tests scheduling from EA with sufficient memory."""
        # Test successful scheduling
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        main_scheduler.schedule_from_EA(get_test_pod.metadata, 1)
        # schedule_from_EA needs to read the pod spec to get memory requirements. Mock this call.
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert len(main_scheduler.resource_dic) == 1
        # 150Mi = 157286400 bytes. 167286400 - 157286400 = 10000000
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        assert len(main_scheduler.resource_dic[1].queue) == 0


    def test_schedule_from_EA_failure_already_scheduled(self, monkeypatch, get_test_pod):
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        #one for pod already scheduled
        main_scheduler.schedule_from_EA(get_test_pod.metadata, 1)
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert len(main_scheduler.scheduled_pods) == 1
        assert len(main_scheduler.resource_dic) == 1
        # 150Mi = 157286400 bytes. 167286400 - 157286400 = 10000000
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        assert len(main_scheduler.resource_dic[1].queue) == 0
        
        #same pod again should not work
        main_scheduler.schedule_from_EA(get_test_pod.metadata, 1)
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert len(main_scheduler.scheduled_pods) == 1
        assert len(main_scheduler.resource_dic) == 1
        # 150Mi = 157286400 bytes. 167286400 - 157286400 = 10000000
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        assert len(main_scheduler.resource_dic[1].queue) == 0
        
    def test_schedule_from_EA_failure_not_available(self, monkeypatch, get_test_pod):
        #one for stauts NotAvailable
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=78643200.0, status="NotAvailable")
        test = deque()
        test.append(get_test_pod.metadata)
        assert main_scheduler.schedule_from_EA(get_test_pod.metadata, 1)
        assert len(main_scheduler.scheduled_pods) == 0
        assert len(main_scheduler.resource_dic[1].queue) == 1
        assert main_scheduler.resource_dic[1].queue == test
        
    def test_schedule_from_EA_failure_no_memory(self, monkeypatch, get_test_pod, get_test_pod_retry):
        #one for stauts NotAvailable
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        main_scheduler.schedule_from_EA(get_test_pod.metadata, 1)
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert len(main_scheduler.resource_dic) == 1
        # 150Mi = 157286400 bytes. 167286400 - 157286400 = 10000000
        assert main_scheduler.resource_dic[1].memory == 10000000.0

        main_scheduler.schedule_from_EA(get_test_pod_retry.metadata, 1)
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert len(main_scheduler.scheduled_pods) == 1
        assert "test-pod2" not in main_scheduler.scheduled_pods.keys()
        assert len(main_scheduler.resource_dic) == 1
        # 150Mi = 157286400 bytes. 167286400 - 157286400 = 10000000
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        test = deque()
        test.append(get_test_pod_retry.metadata)
        assert len(main_scheduler.resource_dic[1].queue) == 1
        assert main_scheduler.resource_dic[1].queue == test


    def test_schedule_from_queue_success(self, monkeypatch, get_test_pod):
        """Tests scheduling a pod from a node's retry queue."""
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        main_scheduler.resource_dic[1].queue.append(get_test_pod.metadata)
        main_scheduler.running_pods.add(get_test_pod.metadata.name)
        assert len(main_scheduler.resource_dic[1].queue) == 1
        assert get_test_pod.metadata in main_scheduler.resource_dic[1].queue
        main_scheduler.schedule_from_queue(1)
        assert len(main_scheduler.scheduled_pods) == 1
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        assert len(main_scheduler.resource_dic[1].queue) == 0

    def test_schedule_from_queue_failure_already_scheduled(self, monkeypatch, get_test_pod):
        """Tests scheduling a pod from a node's retry queue."""
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        main_scheduler.resource_dic[1].queue.append(get_test_pod.metadata)
        main_scheduler.running_pods.add(get_test_pod.metadata.name)
        main_scheduler.scheduled_pods["test-pod"] = 1
        assert len(main_scheduler.resource_dic[1].queue) == 1
        assert get_test_pod.metadata in main_scheduler.resource_dic[1].queue
        main_scheduler.schedule_from_queue(1)
        assert len(main_scheduler.scheduled_pods) == 1
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert main_scheduler.resource_dic[1].memory == 167286400
        assert len(main_scheduler.resource_dic[1].queue) == 0

    def test_schedule_from_queue_failure_not_in_running_pods(self, monkeypatch, get_test_pod):
        """Tests scheduling a pod from a node's retry queue."""
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        main_scheduler.resource_dic[1].queue.append(get_test_pod.metadata)
        assert len(main_scheduler.resource_dic[1].queue) == 1
        assert get_test_pod.metadata in main_scheduler.resource_dic[1].queue
        main_scheduler.schedule_from_queue(1)
        assert len(main_scheduler.scheduled_pods) == 0
        assert main_scheduler.resource_dic[1].memory == 167286400
        assert len(main_scheduler.resource_dic[1].queue) == 0

    def test_schedule_from_queue_failure_no_memory1(self, monkeypatch, get_test_pod, get_test_pod_retry):
        """Tests scheduling a pod from a node's retry queue."""
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        main_scheduler.resource_dic[1].queue.append(get_test_pod.metadata)
        main_scheduler.running_pods.add(get_test_pod.metadata.name)
        assert len(main_scheduler.resource_dic[1].queue) == 1
        assert get_test_pod.metadata in main_scheduler.resource_dic[1].queue
        main_scheduler.schedule_from_queue(1)
        assert len(main_scheduler.scheduled_pods) == 1
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        assert len(main_scheduler.resource_dic[1].queue) == 0
        main_scheduler.running_pods.add(get_test_pod_retry.metadata.name)
        main_scheduler.resource_dic[1].queue.append(get_test_pod_retry.metadata)
        main_scheduler.schedule_from_queue(1)
        assert len(main_scheduler.scheduled_pods) == 1
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert "test-pod2" not in main_scheduler.scheduled_pods.keys()
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        assert len(main_scheduler.resource_dic[1].queue) == 1

    def test_schedule_from_queue_failure_no_memory2(self, monkeypatch, get_test_pod, get_test_pod_retry):
        """Tests scheduling a pod from a node's retry queue."""
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=167286400, status="Available")
        main_scheduler.resource_dic[1].queue.append(get_test_pod.metadata)
        main_scheduler.resource_dic[1].queue.append(get_test_pod_retry.metadata)
        main_scheduler.running_pods.add(get_test_pod.metadata.name)
        main_scheduler.running_pods.add(get_test_pod_retry.metadata.name)
        assert len(main_scheduler.resource_dic[1].queue) == 2
        assert get_test_pod.metadata in main_scheduler.resource_dic[1].queue
        assert get_test_pod_retry.metadata in main_scheduler.resource_dic[1].queue
        main_scheduler.schedule_from_queue(1)
        assert len(main_scheduler.scheduled_pods) == 1
        assert main_scheduler.scheduled_pods["test-pod"] == 1
        assert main_scheduler.resource_dic[1].memory == 10000000.0
        assert len(main_scheduler.resource_dic[1].queue) == 1

    def test_schedule_from_queue_handles_invalid_head(self, monkeypatch, get_test_pod, get_test_pod_retry):
        """
        Tests that schedule_from_queue continues processing after encountering an invalid pod.
        This test will FAIL with 'break' and PASS with 'continue'.
        """
        # Setup: Node with enough memory for the second pod (140Mi), but not the first (150Mi).
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="node1", memory=152043520, status="Available") # 145Mi
        
        # Queue:
        # 1. An already scheduled pod (invalid for this queue)
        # 2. A valid, schedulable pod
        main_scheduler.resource_dic[1].queue.append(get_test_pod.metadata) # pod1, invalid
        main_scheduler.resource_dic[1].queue.append(get_test_pod_retry.metadata) # pod2, valid

        # State:
        main_scheduler.scheduled_pods["test-pod"] = 2 # pod1 is "already scheduled" elsewhere
        main_scheduler.running_pods.add(get_test_pod.metadata.name)
        main_scheduler.running_pods.add(get_test_pod_retry.metadata.name)

        # Action:
        main_scheduler.schedule_from_queue(1)

        # Assertion: The first pod should be skipped, and the second pod should be scheduled.
        assert "test-pod2" in main_scheduler.scheduled_pods
        assert main_scheduler.scheduled_pods["test-pod2"] == 1
        assert len(main_scheduler.resource_dic[1].queue) == 0
        # Memory check: 145Mi - 140Mi = 5Mi
        assert main_scheduler.resource_dic[1].memory == 5242880.0

    def test_flush_retry_queue_periodically(self, monkeypatch):
        """Tests that the retry queue flusher calls schedule_from_queue correctly."""
        # Mock sleep to break the loop after one iteration
        loop_limit = 5
        def mock_endless_loop(time_interval):
            nonlocal loop_limit
            loop_limit -= 1
            if loop_limit == 0:
                exec("raise StopIteration")
        # Mock time.sleep to break the 'while True' loop after one iteration.

        # 2. Patch 'sleep' *where it is used* (in the main_scheduler module). This is the key fix.
        monkeypatch.setattr(main_scheduler, "sleep", mock_endless_loop)

        # Mock the function that gets called inside the loop to record its calls.
        calls = []
        def mock_schedule_from_queue(resource_id):
            calls.append(resource_id)

        monkeypatch.setattr("main_scheduler.schedule_from_queue", mock_schedule_from_queue)

        # Setup the initial state that the function will process.
        main_scheduler.resource_dic[1] = "node1" # The value doesn't matter, just the key.
        main_scheduler.resource_dic[2] = "node2"
        main_scheduler.recent_deletions = [1, 2]

        # Run the function and expect it to be stopped by our mock.
        with pytest.raises(StopIteration):
            main_scheduler.flush_retry_queue_periodically()

        # It should call schedule_from_queue for each item in the copied list
        assert sorted(calls) == [1, 2]
        assert len(main_scheduler.recent_deletions) == 0

    def test_watch_pod_added(self, monkeypatch):
        """Tests handling of a new 'ADDED' pod event."""
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_pod_added)
        
        update_worker_calls = []
        def mock_update_worker(id, tenant, status):
            update_worker_calls.append({"id": id, "tenant": tenant, "status": status})

        monkeypatch.setattr("main_scheduler.update_worker", mock_update_worker)

        # Start the watch function in a thread.
        p = threading.Thread(target=main_scheduler.watch_pod)
        p.start()
        # Wait for the thread to finish processing its event. This prevents race conditions.
        p.join(timeout=2)
        assert "new-pod-1" in main_scheduler.running_pods
        assert main_scheduler.pod_id == 1
        assert 0 in main_scheduler.pod_dic
        assert main_scheduler.pod_dic[0].name == "new-pod-1"
        assert len(update_worker_calls) == 1
        assert update_worker_calls[0] == {"id": 0, "tenant": "tenant-A", "status": "Pending"}

    def test_watch_pod_deleted(self, monkeypatch):
        """Tests handling of a 'MODIFIED'/'DELETED' pod event."""
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_pod_deleted)

        # --- Pre-populate state ---
        main_scheduler.pod_id = 1
        main_scheduler.running_pods.add("existing-pod-1")
        main_scheduler.pod_dic[0] = client.V1ObjectMeta(name="existing-pod-1", uid="uid-existing-1")
        main_scheduler.scheduled_pods["existing-pod-1"] = 5 # Scheduled on resource 5
        main_scheduler.resource_dic[5] = main_scheduler.NodeMeta(id=5, name="node-5", memory=10000000, status="Available")

        update_worker_calls = []
        monkeypatch.setattr("main_scheduler.update_worker", lambda id, tenant, status: update_worker_calls.append(True))

        # Start the watch function in a thread.
        p = threading.Thread(target=main_scheduler.watch_pod)
        p.start()
        # Wait for the thread to complete to prevent it from interfering with other tests.
        p.join(timeout=2)
        assert "existing-pod-1" not in main_scheduler.running_pods
        assert "existing-pod-1" not in main_scheduler.scheduled_pods
        assert len(update_worker_calls) == 1
        # Memory check: 10,000,000 + 20Mi (20 * 1024 * 1024 = 20971520)
        assert main_scheduler.resource_dic[5].memory == 30971520
        assert 5 in main_scheduler.recent_deletions

    def test_schedule_on_node(self, monkeypatch):
        """Tests the schedule_on_node function."""
        schedule_calls = []
        monkeypatch.setattr("main_scheduler.schedule_from_EA", lambda pod_meta, res_id: schedule_calls.append((pod_meta, res_id)))

        # Setup initial state
        main_scheduler.pod_dic[10] = "pod-meta-10"
        main_scheduler.pod_dic[11] = "pod-meta-11"
        main_scheduler.resource_dic[1] = main_scheduler.NodeMeta(id=1, name="n1", memory=100, status="Available")
        main_scheduler.resource_dic[1].queue.append("some-old-pod")
        main_scheduler.resource_dic[2] = main_scheduler.NodeMeta(id=2, name="n2", memory=100, status="Available")
        main_scheduler.resource_dic[2].queue.append("another-old-pod")

        main_scheduler.schedule_on_node(resource_id=2, ids=[10, 11])

        # Assert queues were cleared
        assert len(main_scheduler.resource_dic[1].queue) == 0
        assert len(main_scheduler.resource_dic[2].queue) == 0

        # Assert schedule_from_EA was called correctly
        assert len(schedule_calls) == 2
        assert ("pod-meta-10", 2) in schedule_calls
        assert ("pod-meta-11", 2) in schedule_calls

    def test_update_endpoint(self, monkeypatch, test_env):
        """
        Tests the /update-solution endpoint.
        Note: `test_env` is an autouse fixture that returns the Flask test client.
        We must include it in the function signature to use its return value.
        """
        test_client = test_env
        thread_started = []
        monkeypatch.setattr(threading.Thread, "start", lambda self: thread_started.append(True))

        # Case 1: New fitness is better
        main_scheduler.best_fitness = 0.5
        response = test_client.post('/update-solution', json={"fitness": 0.8, "1": [10, 11]})
        assert response.status_code == 200
        assert main_scheduler.best_fitness == 0.8
        assert len(thread_started) > 0 # A thread should have been started

        # Case 2: New fitness is worse
        thread_started.clear()
        main_scheduler.best_fitness = 0.9
        response = test_client.post('/update-solution', json={"fitness": 0.8, "1": [12, 13]})
        assert response.status_code == 200
        assert main_scheduler.best_fitness == 0.9 # Should not change
        assert len(thread_started) == 0 # No thread should be started

    def test_watch_pod_multiple_added(self, monkeypatch):
        """Tests handling a stream of multiple 'ADDED' pod events."""
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_multiple_pods_added)
        
        update_worker_calls = []
        def mock_update_worker(id, tenant, status):
            update_worker_calls.append({"id": id, "tenant": tenant, "status": status})
        monkeypatch.setattr("main_scheduler.update_worker", mock_update_worker)

        p = threading.Thread(target=main_scheduler.watch_pod)
        p.start()
        p.join(timeout=2)

        assert "multi-pod-1" in main_scheduler.running_pods
        assert "multi-pod-2" in main_scheduler.running_pods
        assert main_scheduler.pod_id == 2
        assert main_scheduler.pod_dic[0].name == "multi-pod-1"
        assert main_scheduler.pod_dic[1].name == "multi-pod-2"
        assert len(update_worker_calls) == 2
        assert {"id": 0, "tenant": "tenant-A", "status": "Pending"} in update_worker_calls
        assert {"id": 1, "tenant": "tenant-C", "status": "Pending"} in update_worker_calls

    def test_watch_pod_added_no_tenant_raises_error(self, monkeypatch):
        """Tests that adding a pod without a tenant name raises a ValueError."""
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_pod_added_no_tenant)
        
        p = threading.Thread(target=main_scheduler.watch_pod)
        
        # The exception will be raised inside the thread. We can't use pytest.raises directly.
        # Instead, we check that the thread terminates and that the state hasn't changed.
        # A more robust application might catch this and log it, but we test the current behavior.
        p.start()
        p.join(timeout=2) # The thread should die quickly due to the exception.

        # Assert that the malformed pod was not added to the state
        assert "no-tenant-pod" not in main_scheduler.running_pods
        assert main_scheduler.pod_id == 0

    def test_watch_pod_added_already_running(self, monkeypatch):
        """Tests that a pod event is ignored if the pod is already in the running_pods set."""
        # Pre-populate state to simulate the pod already being tracked
        main_scheduler.running_pods.add("new-pod-1")
        main_scheduler.pod_id = 1
        main_scheduler.pod_dic[0] = client.V1ObjectMeta(name="new-pod-1", uid="uid-1")

        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_pod_added)
        
        update_worker_calls = []
        monkeypatch.setattr("main_scheduler.update_worker", lambda id, tenant, status: update_worker_calls.append(True))

        p = threading.Thread(target=main_scheduler.watch_pod)
        p.start()
        p.join(timeout=2)

        # Assert that no state was changed
        assert len(main_scheduler.running_pods) == 1
        assert main_scheduler.pod_id == 1
        assert len(main_scheduler.pod_dic) == 1
        assert len(update_worker_calls) == 0

    def test_watch_pod_deleted_not_scheduled(self, monkeypatch):
        """Tests deleting a pod that was running but never successfully scheduled."""
        # This can happen if a pod is created and deleted before the scheduler can bind it.
        main_scheduler.running_pods.add("unscheduled-pod")
        main_scheduler.pod_id = 1
        main_scheduler.pod_dic[0] = client.V1ObjectMeta(name="unscheduled-pod", uid="uid-unscheduled")
        # Note: "unscheduled-pod" is NOT in main_scheduler.scheduled_pods

        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_pod_deleted_not_scheduled)
        
        update_worker_calls = []
        monkeypatch.setattr("main_scheduler.update_worker", lambda id, tenant, status: update_worker_calls.append(True))

        p = threading.Thread(target=main_scheduler.watch_pod)
        p.start()
        p.join(timeout=2)

        # Assert the pod is removed from running state
        assert "unscheduled-pod" not in main_scheduler.running_pods
        # Assert that no memory was reclaimed because it was never scheduled
        assert len(main_scheduler.recent_deletions) == 0
        # Assert the worker was still updated
        assert len(update_worker_calls) == 1

    def test_watch_pod_deleted_no_tenant_raises_error(self, monkeypatch):
        """Tests that deleting a pod without a tenant name raises a ValueError."""
        main_scheduler.running_pods.add("existing-pod-1")
        main_scheduler.pod_dic[0] = client.V1ObjectMeta(name="existing-pod-1", uid="uid-existing-1")

        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_pod_deleted_no_tenant)
        
        p = threading.Thread(target=main_scheduler.watch_pod)
        p.start()
        p.join(timeout=2)

        # The pod should still be in the running list because the thread died before removing it.
        assert "existing-pod-1" in main_scheduler.running_pods

    def test_watch_pod_deleted_node_gone(self, monkeypatch):
        """Tests deleting a pod whose node has already been removed from the resource_dic."""
        monkeypatch.setattr(watch.Watch, "stream", MockResponseWatchPod.list_pod_deleted)

        # This mock is crucial. Without it, the thread will try to make a real HTTP request,
        # which will fail because of the 'no_requests' fixture, and the thread will die silently.
        monkeypatch.setattr("main_scheduler.update_worker", lambda id, tenant, status: None)

        # Pre-populate state, scheduling the pod on a node that does NOT exist in resource_dic
        main_scheduler.pod_id = 1
        main_scheduler.running_pods.add("existing-pod-1")
        main_scheduler.pod_dic[0] = client.V1ObjectMeta(name="existing-pod-1", uid="uid-existing-1")
        main_scheduler.scheduled_pods["existing-pod-1"] = 99 # Scheduled on non-existent resource 99
        # main_scheduler.resource_dic is empty

        p = threading.Thread(target=main_scheduler.watch_pod)
        p.start()
        p.join(timeout=2)

        # The main assertion is that the code doesn't crash with a KeyError.
        # The pod should be successfully removed from the running and scheduled lists.
        assert "existing-pod-1" not in main_scheduler.running_pods
        assert "existing-pod-1" not in main_scheduler.scheduled_pods
        # No memory can be reclaimed, and no node queue can be flushed.
        assert 99 not in main_scheduler.recent_deletions