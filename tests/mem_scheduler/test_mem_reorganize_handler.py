"""Unit tests for MemReorganizeHandler."""


import json
import unittest

from unittest.mock import Mock

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import (
    ScheduleLogForWebItem,
    ScheduleMessageItem,
)
from memos.mem_scheduler.schemas.task_schemas import (
    LONG_TERM_MEMORY_TYPE,
    MEM_ORGANIZE_TASK_LABEL,
)
from memos.mem_scheduler.task_schedule_modules.handlers.mem_reorganize_handler import (
    MemReorganizeHandler,
)
from memos.memories.textual.tree import TreeTextMemory


class TestMemReorganizeHandler(unittest.TestCase):
    """Test cases for MemReorganizeHandler."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock scheduler
        self.mock_scheduler = Mock()
        
        # Create mock dependencies
        self.mock_mem_cube = Mock()
        self.mock_text_mem = Mock(spec=TreeTextMemory)
        # Add graph_store with empty edges by default
        self.mock_graph_store = Mock()
        self.mock_graph_store.get_edges = Mock(return_value=[])
        self.mock_text_mem.graph_store = self.mock_graph_store
        self.mock_mem_cube.text_mem = self.mock_text_mem
        
        self.mock_monitor = Mock()
        self.mock_retriever = Mock()
        self.mock_config = Mock()
        self.mock_dispatcher = Mock()
        
        # Set up mock scheduler properties
        self.mock_scheduler.mem_cube = self.mock_mem_cube
        self.mock_scheduler.monitor = self.mock_monitor
        self.mock_scheduler.retriever = self.mock_retriever
        self.mock_scheduler.config = self.mock_config
        self.mock_scheduler.dispatcher = self.mock_dispatcher
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Set up mock methods
        self.mock_scheduler.create_event_log = Mock(return_value=ScheduleLogForWebItem(
            label="mergeMemory",
            from_memory_type=LONG_TERM_MEMORY_TYPE,
            to_memory_type=LONG_TERM_MEMORY_TYPE,
            user_id="test_user",
            mem_cube_id="test_cube",
            log_content="Test log content",
            timestamp=123456789,
        ))
        self.mock_scheduler._submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
        # Set up text_mem.get to return mock memory items
        def create_mock_item(mem_id, user_name=None):
            item = Mock()
            item.id = mem_id
            item.memory = f"Memory content for {mem_id}"
            item.metadata = Mock()
            item.metadata.key = f"key_{mem_id}"
            item.metadata.memory_type = "text"
            item.metadata.status = "active"
            item.metadata.confidence = 0.9
            item.metadata.tags = ["tag1"]
            item.metadata.updated_at = 123456789
            return item
        
        self.mock_text_mem.get = Mock(side_effect=create_mock_item)
        
        # Create context
        self.context = SchedulerContext(self.mock_scheduler)
        
        # Create handler
        self.handler = MemReorganizeHandler(self.context)
        self.handler.process_memories_with_reorganize = Mock()
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, MEM_ORGANIZE_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    def test_main_path_single_message(self):
        """Test main path with a single reorganize message."""
        # Create test message
        mem_ids = ["mem1", "mem2", "mem3"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
            user_name="Test User",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify process_memories_with_reorganize was called
        self.handler.process_memories_with_reorganize.assert_called_once()
        call_args = self.handler.process_memories_with_reorganize.call_args[1]
        self.assertEqual(call_args["mem_ids"], mem_ids)
        self.assertEqual(call_args["user_id"], "user1")
        self.assertEqual(call_args["mem_cube_id"], "cube1")
        self.assertEqual(call_args["text_mem"], self.mock_text_mem)
        self.assertEqual(call_args["user_name"], "Test User")
        
        # Verify text_mem.get was called for each mem_id
        self.assertEqual(self.mock_text_mem.get.call_count, 3)
        
        # Verify create_event_log was called
        self.mock_scheduler.create_event_log.assert_called_once()
        
        # Verify _submit_web_logs was called
        self.mock_scheduler._submit_web_logs.assert_called_once()
        
    def test_mem_cube_none(self):
        """Test handler when mem_cube is None."""
        # Set mem_cube to None
        self.mock_scheduler.mem_cube = None
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should log warning and skip
        with self.assertLogs(level='WARNING') as log:
            self.handler([test_message])
        
        # Verify warning was logged
        self.assertTrue(any('mem_cube is None' in msg for msg in log.output))
        
        # Verify process_memories_with_reorganize was not called
        self.handler.process_memories_with_reorganize.assert_not_called()
        
    def test_invalid_text_mem_type(self):
        """Test handler when text_mem is wrong type."""
        # Set text_mem to plain Mock (not TreeTextMemory)
        self.mock_mem_cube.text_mem = Mock()
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should log error and skip
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Expected TreeTextMemory' in msg for msg in log.output))
        
    def test_process_memories_with_reorganize_not_available(self):
        """Test handler when process_memories_with_reorganize is not available."""
        # Remove process_memories_with_reorganize method
        self.handler.process_memories_with_reorganize = None
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - handler should continue without calling process_memories_with_reorganize
        # Error is logged but execution continues (with contextlib.suppress wrapping)
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should not raise exception: {e}")
        
        # Since process_memories_with_reorganize is None, it cannot be called
        # This test verifies the handler doesn't crash
        
    def test_invalid_json_content(self):
        """Test handler with invalid JSON content."""
        # Create test message with invalid JSON
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content="invalid json {",
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing mem_reorganize message' in msg for msg in log.output))
        
    def test_empty_mem_ids(self):
        """Test handler with empty mem_ids list."""
        # Create test message with empty list
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps([]),
            timestamp=123456789,
        )
        
        # Process message - should skip processing
        self.handler([test_message])
        
        # Verify process_memories_with_reorganize was not called
        self.handler.process_memories_with_reorganize.assert_not_called()
        
    def test_single_memory_no_merge_event(self):
        """Test that single memory doesn't trigger merge event."""
        # Create test message with single mem_id
        mem_ids = ["mem1"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify process_memories_with_reorganize was called
        self.handler.process_memories_with_reorganize.assert_called_once()
        
        # Verify create_event_log was NOT called (need >1 items for merge)
        self.mock_scheduler.create_event_log.assert_not_called()
        
    def test_multiple_memories_with_graph_store(self):
        """Test reorganize with graph store edges."""
        # Add graph_store mock with MERGED_TO edges
        self.mock_text_mem.graph_store = Mock()
        self.mock_text_mem.graph_store.get_edges = Mock(return_value=[
            {"to": "merged_mem1"},
            {"dst": "merged_mem2"}
        ])
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
            user_name="Test User",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify graph_store.get_edges was called
        self.assertEqual(self.mock_text_mem.graph_store.get_edges.call_count, 2)
        
        # Verify create_event_log was called
        self.mock_scheduler.create_event_log.assert_called_once()
        
    def test_exception_in_process_memories_with_reorganize(self):
        """Test handler when process_memories_with_reorganize raises exception."""
        # Make process_memories_with_reorganize raise an exception
        self.handler.process_memories_with_reorganize.side_effect = Exception("Processing failed")
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing mem_reorganize message' in msg for msg in log.output))
        
    def test_exception_in_text_mem_get(self):
        """Test handler continues when text_mem.get raises exception."""
        # Make text_mem.get raise an exception
        self.mock_text_mem.get.side_effect = Exception("Get failed")
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should continue despite exception
        self.handler([test_message])
        
        # Verify process_memories_with_reorganize was still called
        self.handler.process_memories_with_reorganize.assert_called_once()
        
        # No merge event should be created (couldn't get items)
        self.mock_scheduler.create_event_log.assert_not_called()
        
    def test_multiple_messages(self):
        """Test processing multiple reorganize messages."""
        # Create multiple test messages
        test_messages = []
        for i in range(3):
            mem_ids = [f"mem{i}_1", f"mem{i}_2"]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_ORGANIZE_TASK_LABEL,
                content=json.dumps(mem_ids),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify process_memories_with_reorganize was called 3 times
        self.assertEqual(self.handler.process_memories_with_reorganize.call_count, 3)
        
    def test_empty_message_list(self):
        """Test handler with empty message list."""
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify process_memories_with_reorganize was not called
        self.handler.process_memories_with_reorganize.assert_not_called()
        
    def test_merge_event_log_structure(self):
        """Test that merge event log has correct structure."""
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_ORGANIZE_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
            user_name="Test User",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify create_event_log was called with correct parameters
        self.mock_scheduler.create_event_log.assert_called_once()
        call_args = self.mock_scheduler.create_event_log.call_args[1]
        
        self.assertEqual(call_args["label"], "mergeMemory")
        self.assertEqual(call_args["from_memory_type"], LONG_TERM_MEMORY_TYPE)
        self.assertEqual(call_args["to_memory_type"], LONG_TERM_MEMORY_TYPE)
        self.assertEqual(call_args["user_id"], "user1")
        self.assertEqual(call_args["mem_cube_id"], "cube1")
        
        # Verify memcube_log_content structure
        memcube_log_content = call_args["memcube_log_content"]
        self.assertIsInstance(memcube_log_content, list)
        # Should have 2 merged items + 1 postMerge item
        self.assertEqual(len(memcube_log_content), 3)
        
        # Verify metadata structure
        metadata = call_args["metadata"]
        self.assertIsInstance(metadata, list)
        self.assertEqual(len(metadata), 3)
        
    def test_multiple_users_and_cubes(self):
        """Test processing messages from different users and cubes."""
        # Create messages from different users and cubes
        test_messages = []
        for i, (user, cube) in enumerate([("user1", "cube1"), ("user1", "cube2"), ("user2", "cube1")]):
            mem_ids = [f"mem{i}_1", f"mem{i}_2"]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id=user,
                mem_cube_id=cube,
                label=MEM_ORGANIZE_TASK_LABEL,
                content=json.dumps(mem_ids),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify process_memories_with_reorganize was called for each message
        self.assertEqual(self.handler.process_memories_with_reorganize.call_count, 3)
        

class TestMemReorganizeHandlerIntegration(unittest.TestCase):
    """Integration tests for MemReorganizeHandler with more realistic scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up realistic mock behavior
        self.mock_mem_cube = Mock()
        self.mock_text_mem = Mock(spec=TreeTextMemory)
        self.mock_mem_cube.text_mem = self.mock_text_mem
        
        self.mock_scheduler.mem_cube = self.mock_mem_cube
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Track all reorganize calls
        self.reorganize_calls = []
        
        def process_memories_side_effect(**kwargs):
            self.reorganize_calls.append({
                "mem_ids": kwargs.get("mem_ids"),
                "user_id": kwargs.get("user_id"),
                "mem_cube_id": kwargs.get("mem_cube_id"),
            })
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = MemReorganizeHandler(self.context)
        self.handler.process_memories_with_reorganize = Mock(
            side_effect=process_memories_side_effect
        )
        
        # Set up text_mem.get to return mock memory items
        def create_mock_item(mem_id, user_name=None):
            item = Mock()
            item.id = mem_id
            item.memory = f"Memory for {mem_id}"
            item.metadata = Mock()
            item.metadata.key = f"key_{mem_id}"
            item.metadata.memory_type = "text"
            item.metadata.status = "active"
            item.metadata.confidence = 0.9
            item.metadata.tags = []
            item.metadata.updated_at = 123456789
            return item
        
        self.mock_text_mem.get = Mock(side_effect=create_mock_item)
        
        self.mock_scheduler.create_event_log = Mock(return_value=ScheduleLogForWebItem(
            label="mergeMemory",
            from_memory_type=LONG_TERM_MEMORY_TYPE,
            to_memory_type=LONG_TERM_MEMORY_TYPE,
            user_id="test_user",
            mem_cube_id="test_cube",
            log_content="Test log content",
            timestamp=123456789,
        ))
        self.mock_scheduler._submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
    def test_batch_processing_multiple_reorganizations(self):
        """Test batch processing with multiple reorganization operations."""
        # Create messages with different memory sets
        test_messages = []
        for i in range(3):
            mem_ids = [f"mem{i}_1", f"mem{i}_2", f"mem{i}_3"]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_ORGANIZE_TASK_LABEL,
                content=json.dumps(mem_ids),
                timestamp=123456789 + i,
                user_name="Test User",
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify all reorganizations were processed
        self.assertEqual(len(self.reorganize_calls), 3)
        
        # Verify each call has the correct memory IDs
        for i, call in enumerate(self.reorganize_calls):
            expected_ids = [f"mem{i}_1", f"mem{i}_2", f"mem{i}_3"]
            self.assertEqual(call["mem_ids"], expected_ids)
        

if __name__ == "__main__":
    unittest.main()
