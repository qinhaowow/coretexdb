import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.cortex_client import CortexClient
from core.embedding_service import EmbeddingService
from core.memory_manager import MemoryManager


class TestMemoryManager(unittest.TestCase):
    def setUp(self):
        self.client = CortexClient()
        self.client.connect()
        self.embedding_service = EmbeddingService()
        self.embedding_service.load_model()
        self.memory_manager = MemoryManager(
            cortex_client=self.client,
            embedding_service=self.embedding_service
        )

    def test_add_memory(self):
        memory_id = self.memory_manager.add_memory(
            content="Test memory content",
            memory_type="episodic",
            importance=0.7
        )
        self.assertIsNotNone(memory_id)
        self.assertTrue(memory_id.startswith("mem_"))

    def test_add_multiple_memories(self):
        for i in range(10):
            self.memory_manager.add_memory(
                content=f"Memory {i}",
                memory_type="episodic",
                importance=0.5
            )
        stats = self.memory_manager.get_stats()
        self.assertEqual(stats["short_term_count"], 10)

    def test_retrieve(self):
        self.memory_manager.add_memory(
            content="Robot encountered obstacle",
            memory_type="episodic",
            importance=0.8
        )
        self.memory_manager.add_memory(
            content="Robot charged battery",
            memory_type="episodic",
            importance=0.6
        )

        results = self.memory_manager.retrieve(
            query="obstacle",
            limit=5
        )
        self.assertIsInstance(results, list)

    def test_retrieve_with_memory_type_filter(self):
        self.memory_manager.add_memory(
            content="Episodic memory",
            memory_type="episodic",
            importance=0.5
        )
        self.memory_manager.add_memory(
            content="Semantic memory",
            memory_type="semantic",
            importance=0.5
        )

        results = self.memory_manager.retrieve(
            query="memory",
            limit=5,
            memory_types=["episodic"]
        )
        self.assertIsInstance(results, list)

    def test_working_memory(self):
        self.memory_manager.set_working_memory("current_task", "Pick up object")
        memory = self.memory_manager.get_working_memory("current_task")
        self.assertIsNotNone(memory)
        self.assertEqual(memory.content, "Pick up object")

    def test_clear_working_memory(self):
        self.memory_manager.set_working_memory("task1", "Task 1")
        self.memory_manager.set_working_memory("task2", "Task 2")

        self.memory_manager.clear_working_memory("task1")
        memory1 = self.memory_manager.get_working_memory("task1")
        memory2 = self.memory_manager.get_working_memory("task2")

        self.assertIsNone(memory1)
        self.assertIsNotNone(memory2)

    def test_clear_all_working_memory(self):
        self.memory_manager.set_working_memory("task1", "Task 1")
        self.memory_manager.set_working_memory("task2", "Task 2")

        self.memory_manager.clear_working_memory()

        memory1 = self.memory_manager.get_working_memory("task1")
        memory2 = self.memory_manager.get_working_memory("task2")

        self.assertIsNone(memory1)
        self.assertIsNone(memory2)

    def test_get_stats(self):
        stats = self.memory_manager.get_stats()
        self.assertIn("short_term_count", stats)
        self.assertIn("long_term_count", stats)
        self.assertIn("working_memory_keys", stats)


class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.client = CortexClient()
        self.client.connect()
        self.embedding_service = EmbeddingService()
        self.embedding_service.load_model()

    def test_data_loader_init(self):
        from core.data_loader import DataLoader
        loader = DataLoader(
            cortex_client=self.client,
            embedding_service=self.embedding_service
        )
        self.assertIsNotNone(loader)


class TestTaskPlanner(unittest.TestCase):
    def setUp(self):
        from llm.task_planner import TaskPlanner, OllamaClient

        self.ollama_client = OllamaClient()
        self.planner = TaskPlanner(llm_client=self.ollama_client)

    def test_plan_creation(self):
        result = self.planner.plan(
            goal="Navigate to the charging station",
            context={"current_battery": 20}
        )
        self.assertIsInstance(result, dict)
        self.assertIn("task_name", result)

    def test_validate_plan(self):
        valid_plan = {
            "task_name": "test_task",
            "steps": [{"step": 1, "action": "do something"}]
        }
        validation = self.planner.validate_plan(valid_plan)
        self.assertTrue(validation["is_valid"])

    def test_validate_invalid_plan(self):
        invalid_plan = {
            "steps": [{"step": 1, "action": "do something"}]
        }
        validation = self.planner.validate_plan(invalid_plan)
        self.assertFalse(validation["is_valid"])

    def test_estimate_duration(self):
        plan = {
            "steps": [
                {"step": 1, "action": "action 1"},
                {"step": 2, "action": "action 2"},
                {"step": 3, "action": "action 3"}
            ]
        }
        duration = self.planner.estimate_duration(plan)
        self.assertGreater(duration, 0)


class TestDecisionEngine(unittest.TestCase):
    def setUp(self):
        from llm.decision_engine import DecisionEngine

        self.engine = DecisionEngine(
            confidence_threshold=0.7,
            risk_threshold=0.5
        )

    def test_decide_with_options(self):
        options = [
            {"id": "option1", "name": "Go left", "priority": 0.8, "success_rate": 0.9},
            {"id": "option2", "name": "Go right", "priority": 0.6, "success_rate": 0.7}
        ]
        result = self.engine.decide(
            situation="Robot at intersection",
            options=options
        )
        self.assertIsInstance(result, dict)
        self.assertIn("chosen_option", result)
        self.assertIn("confidence", result)

    def test_get_statistics(self):
        stats = self.engine.get_statistics()
        self.assertIn("total_decisions", stats)
        self.assertIn("average_confidence", stats)

    def test_clear_history(self):
        options = [{"id": "opt1", "name": "Option 1"}]
        self.engine.decide("Test situation", options)
        self.assertEqual(len(self.engine.decision_history), 1)

        self.engine.clear_history()
        self.assertEqual(len(self.engine.decision_history), 0)


if __name__ == "__main__":
    unittest.main()
