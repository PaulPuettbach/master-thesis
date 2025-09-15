# import unittest
# from unittest.mock import patch, MagicMock

# # Assuming tests are run from a directory where 'containers' is a package.
# # Based on your other test file, this seems to be the case.
# import containers.src.random_scheduler as scheduler

# class TestRandomScheduler(unittest.TestCase):

#     def setUp(self):
#         """
#         This method is called before each test. It resets the global state
#         to ensure tests are isolated from each other.
#         """
#         scheduler.ready_nodes = {}
#         scheduler.node_id = 0

#     @patch('containers.src.random_scheduler.v1')
#     def test_nodes_available_calculates_correctly(self, mock_v1):
#         """
#         Tests the nodes_available function to ensure it correctly identifies
#         ready nodes and calculates their available memory.
#         """
#         # --- 1. Setup Mock Kubernetes Objects ---

#         # Create a mock for a ready node
#         mock_node_ready = MagicMock()
#         mock_node_ready.metadata.name = "node-1"
#         mock_node_ready.status.allocatable = {"memory": "1000Ki"} # 1000 * 1024 bytes
#         mock_node_ready.status.conditions = [
#             MagicMock(type='Ready', status='True'),
#             MagicMock(type='MemoryPressure', status='False')
#         ]

#         # Create a mock for a node that is not ready
#         mock_node_not_ready = MagicMock()
#         mock_node_not_ready.metadata.name = "node-2"
#         mock_node_not_ready.status.allocatable = {"memory": "2000Ki"}
#         mock_node_not_ready.status.conditions = [
#             MagicMock(type='Ready', status='False') # This makes it ineligible
#         ]

#         # Create a mock for a pod running on the ready node
#         mock_pod_on_node1 = MagicMock()
#         mock_pod_on_node1.spec.node_name = "node-1"
#         mock_pod_on_node1.spec.containers = [
#             MagicMock(resources=MagicMock(requests={"memory": "100Ki"})) # 100 * 1024 bytes
#         ]

#         # --- 2. Configure Mock API Calls ---

#         # When v1.list_node() is called, return our list of mock nodes.
#         mock_v1.list_node.return_value.items = [mock_node_ready, mock_node_not_ready]

#         # When v1.list_pod_for_all_namespaces() is called, return our mock pod.
#         mock_v1.list_pod_for_all_namespaces.return_value.items = [mock_pod_on_node1]

#         # --- 3. Execute the function under test ---
#         scheduler.nodes_available()

#         # --- 4. Assert the results ---

#         # Check that only the ready node was added to the global dictionary
#         self.assertEqual(len(scheduler.ready_nodes), 1)
#         self.assertIn(0, scheduler.ready_nodes) # The first node should get id 0

#         # Check the properties of the added node
#         added_node = scheduler.ready_nodes[0]
#         self.assertEqual(added_node.name, "node-1")
#         self.assertEqual(added_node.status, "Available")

#         # Check the memory calculation:
#         # Total allocatable: 1000 * 1024 = 1,024,000 bytes
#         # Function takes 75% of that: 1,024,000 * 0.75 = 768,000 bytes
#         # Pod requests: 100 * 1024 = 102,400 bytes
#         # Expected available memory: 768,000 - 102,400 = 665,600 bytes
#         expected_memory = (1000 * 1024 * 0.75) - (100 * 1024)
#         self.assertEqual(added_node.memory, expected_memory)

#         # Verify that the Kubernetes API calls were made
#         mock_v1.list_node.assert_called_once()
#         mock_v1.list_pod_for_all_namespaces.assert_called_once()

