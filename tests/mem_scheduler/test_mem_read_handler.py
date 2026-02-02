"""Unit tests for MemReadHandler."""


import json
import unittest

from unittest.mock import Mock, patch

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import MEM_READ_TASK_LABEL
from memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler import MemReadHandler


class TestMemReadHandler(unittest.TestCase):
    """Test cases for MemReadHandler."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock scheduler
        self.mock_scheduler = Mock()
        
        # Create mock dependencies
        self.mock_mem_cube = Mock()
        self.mock_text_mem = Mock()
        self.mock_text_mem.__class__.__name__ = "TreeTextMemory"
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
        
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
        # Create context
        self.context = SchedulerContext(self.mock_scheduler)
        
        # Create handler
        self.handler = MemReadHandler(self.context)
        self.handler.process_memories_with_reader = Mock()
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, MEM_READ_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler.isinstance')
    def test_main_path_single_message(self, mock_isinstance):
        """Test main path with a single memory read message."""
        # Mock isinstance to return True for TreeTextMemory check
        mock_isinstance.return_value = True
        
        # Create test message
        mem_ids = ["mem1", "mem2", "mem3"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
            user_name="Test User",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify process_memories_with_reader was called
        self.handler.process_memories_with_reader.assert_called_once()
        call_args = self.handler.process_memories_with_reader.call_args[1]
        self.assertEqual(call_args["mem_ids"], mem_ids)
        self.assertEqual(call_args["user_id"], "user1")
        self.assertEqual(call_args["mem_cube_id"], "cube1")
        self.assertEqual(call_args["text_mem"], self.mock_text_mem)
        self.assertEqual(call_args["user_name"], "Test User")
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler.isinstance')
    def test_main_path_multiple_messages(self, mock_isinstance):
        """Test main path with multiple memory read messages."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create test messages
        test_messages = []
        for i in range(3):
            mem_ids = [f"mem{i}_1", f"mem{i}_2"]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_READ_TASK_LABEL,
                content=json.dumps(mem_ids),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify process_memories_with_reader was called 3 times
        self.assertEqual(self.handler.process_memories_with_reader.call_count, 3)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler.isinstance')
    def test_main_path_with_custom_tags(self, mock_isinstance):
        """Test main path with custom tags in info."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create test message with custom tags
        mem_ids = ["mem1", "mem2"]
        test_info = {"custom_tags": ["tag1", "tag2"]}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
            info=test_info,
            task_id="task123",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify custom_tags were passed
        call_args = self.handler.process_memories_with_reader.call_args[1]
        self.assertEqual(call_args["custom_tags"], ["tag1", "tag2"])
        self.assertEqual(call_args["task_id"], "task123")
        self.assertEqual(call_args["info"], test_info)
        
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
            label=MEM_READ_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should log error and skip
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('mem_cube is None' in msg for msg in log.output))
        
        # Verify process_memories_with_reader was not called
        self.handler.process_memories_with_reader.assert_not_called()
        
    def test_invalid_text_mem_type(self):
        """Test handler when text_mem is wrong type."""
        # Set text_mem to wrong type
        self.mock_text_mem.__class__.__name__ = "WrongType"
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should log error and skip
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Expected TreeTextMemory' in msg for msg in log.output))
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler.isinstance')
    def test_process_memories_with_reader_not_available(self, mock_isinstance):
        """Test handler when process_memories_with_reader is not available."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Remove process_memories_with_reader method
        self.handler.process_memories_with_reader = None
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        # Verify error was logged
        self.assertTrue(any('Error processing mem_read message' in msg for msg in log.output))
        
    def test_invalid_json_content(self):
        """Test handler with invalid JSON content."""
        # Create test message with invalid JSON
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content="invalid json {",
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing mem_read message' in msg for msg in log.output))
        
    def test_empty_mem_ids(self):
        """Test handler with empty mem_ids list."""
        # Create test message with empty list
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content=json.dumps([]),
            timestamp=123456789,
        )
        
        # Process message - should skip processing
        self.handler([test_message])
        
        # Verify process_memories_with_reader was not called
        self.handler.process_memories_with_reader.assert_not_called()
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler.isinstance')
    def test_exception_in_process_memories_with_reader(self, mock_isinstance):
        """Test handler when process_memories_with_reader raises exception."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Make process_memories_with_reader raise an exception
        self.handler.process_memories_with_reader.side_effect = Exception("Processing failed")
        
        # Create test message
        mem_ids = ["mem1", "mem2"]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content=json.dumps(mem_ids),
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing mem_read message' in msg for msg in log.output))
        
    def test_empty_message_list(self):
        """Test handler with empty message list."""
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify process_memories_with_reader was not called
        self.handler.process_memories_with_reader.assert_not_called()
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler.isinstance')
    def test_multiple_users_and_cubes(self, mock_isinstance):
        """Test processing messages from different users and cubes."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create messages from different users and cubes
        test_messages = []
        for i, (user, cube) in enumerate([("user1", "cube1"), ("user1", "cube2"), ("user2", "cube1")]):
            mem_ids = [f"mem{i}_1", f"mem{i}_2"]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id=user,
                mem_cube_id=cube,
                label=MEM_READ_TASK_LABEL,
                content=json.dumps(mem_ids),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify process_memories_with_reader was called for each message
        self.assertEqual(self.handler.process_memories_with_reader.call_count, 3)
        
    def test_non_json_string_content(self):
        """Test handler with non-JSON string content (should fail parsing)."""
        # Create test message with non-JSON content
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_READ_TASK_LABEL,
            content="not a json list",
            timestamp=123456789,
        )
        
        # Process message - should log error due to JSON parsing failure
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing mem_read message' in msg for msg in log.output))
        

class TestMemReadHandlerIntegration(unittest.TestCase):
    """Integration tests for MemReadHandler with more realistic scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up realistic mock behavior
        self.mock_mem_cube = Mock()
        self.mock_text_mem = Mock()
        self.mock_text_mem.__class__.__name__ = "TreeTextMemory"
        self.mock_mem_cube.text_mem = self.mock_text_mem
        
        self.mock_scheduler.mem_cube = self.mock_mem_cube
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Track all process calls
        self.process_calls = []
        
        def process_memories_side_effect(**kwargs):
            self.process_calls.append({
                "mem_ids": kwargs.get("mem_ids"),
                "user_id": kwargs.get("user_id"),
                "mem_cube_id": kwargs.get("mem_cube_id"),
            })
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = MemReadHandler(self.context)
        self.handler.process_memories_with_reader = Mock(side_effect=process_memories_side_effect)
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler.isinstance')
    def test_batch_processing_multiple_memory_sets(self, mock_isinstance):
        """Test batch processing with multiple sets of memories."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create messages with different memory sets
        test_messages = []
        for i in range(3):
            mem_ids = [f"mem{i}_1", f"mem{i}_2", f"mem{i}_3"]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_READ_TASK_LABEL,
                content=json.dumps(mem_ids),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify all memory sets were processed
        self.assertEqual(len(self.process_calls), 3)
        
        # Verify each call has the correct memory IDs
        for i, call in enumerate(self.process_calls):
            expected_ids = [f"mem{i}_1", f"mem{i}_2", f"mem{i}_3"]
            self.assertEqual(call["mem_ids"], expected_ids)
        

if __name__ == "__main__":
    unittest.main()
