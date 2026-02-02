"""Unit tests for MemoryUpdateHandler."""


import unittest

from unittest.mock import Mock

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import MEM_UPDATE_TASK_LABEL
from memos.mem_scheduler.task_schedule_modules.handlers.memory_update_handler import (
    MemoryUpdateHandler,
)


class TestMemoryUpdateHandler(unittest.TestCase):
    """Test cases for MemoryUpdateHandler."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock scheduler
        self.mock_scheduler = Mock()
        
        # Create mock dependencies
        self.mock_mem_cube = Mock()
        self.mock_monitor = Mock()
        # Ensure monitor methods return lists (which have len()) not Mocks
        self.mock_monitor.extract_query_keywords = Mock(return_value=[])
        
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
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
        # Create context
        self.context = SchedulerContext(self.mock_scheduler)
        
        # Create handler
        self.handler = MemoryUpdateHandler(self.context)
        self.handler.long_memory_update_process = Mock()
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, MEM_UPDATE_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    def test_main_path_single_message(self):
        """Test main path with a single memory update message."""
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_UPDATE_TASK_LABEL,
            content="Memory update content",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify long_memory_update_process was called
        self.handler.long_memory_update_process.assert_called_once_with(
            user_id="user1",
            mem_cube_id="cube1",
            messages=[test_message]
        )
        
    def test_main_path_multiple_messages(self):
        """Test main path with multiple memory update messages."""
        # Create test messages
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_UPDATE_TASK_LABEL,
                content=f"Memory update content {i}",
                timestamp=123456789 + i,
            )
            for i in range(3)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify long_memory_update_process was called with all messages
        self.handler.long_memory_update_process.assert_called_once_with(
            user_id="user1",
            mem_cube_id="cube1",
            messages=test_messages
        )
        
    def test_main_path_multiple_users(self):
        """Test main path with messages from different users."""
        # Create test messages from different users
        test_messages = [
            ScheduleMessageItem(
                item_id="msg1",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_UPDATE_TASK_LABEL,
                content="User 1 memory update",
                timestamp=123456789,
            ),
            ScheduleMessageItem(
                item_id="msg2",
                user_id="user2",
                mem_cube_id="cube2",
                label=MEM_UPDATE_TASK_LABEL,
                content="User 2 memory update",
                timestamp=123456790,
            ),
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify long_memory_update_process was called twice (once per user)
        self.assertEqual(self.handler.long_memory_update_process.call_count, 2)
        
        # Verify correct parameters for each call
        calls = self.handler.long_memory_update_process.call_args_list
        self.assertEqual(calls[0][1]["user_id"], "user1")
        self.assertEqual(calls[0][1]["mem_cube_id"], "cube1")
        self.assertEqual(calls[1][1]["mem_cube_id"], "cube2")
        
    def test_long_memory_update_process_not_available(self):
        """Test handler when long_memory_update_process raises error."""
        # Note: In the real handler, long_memory_update_process is a method of the class,
        # so it's always "available" unless we mock it out to raise error or be None.
        
        # Mock the method on the handler instance
        self.handler.long_memory_update_process = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_UPDATE_TASK_LABEL,
            content="Memory update content",
            timestamp=123456789,
        )
        
        # Process message - will raise TypeError because method is None
        with self.assertRaises(TypeError):
            self.handler([test_message])
        
    def test_empty_message_list(self):
        """Test handler with empty message list."""
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify long_memory_update_process was not called
        self.handler.long_memory_update_process.assert_not_called()
        
    def test_batch_processing_same_user_cube(self):
        """Test that messages from same user/cube are batched together."""
        # Create multiple messages from same user/cube
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_UPDATE_TASK_LABEL,
                content=f"Update {i}",
                timestamp=123456789 + i,
            )
            for i in range(5)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify called once with all messages in batch
        self.handler.long_memory_update_process.assert_called_once()
        call_args = self.handler.long_memory_update_process.call_args[1]
        self.assertEqual(len(call_args["messages"]), 5)
        
    def test_exception_in_long_memory_update_process(self):
        """Test handler when long_memory_update_process raises exception."""
        # Make long_memory_update_process raise an exception
        self.handler.long_memory_update_process.side_effect = Exception("Update failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_UPDATE_TASK_LABEL,
            content="Memory update content",
            timestamp=123456789,
        )
        
        # Process message - exception should propagate
        with self.assertRaises(Exception) as context:
            self.handler([test_message])
        
        self.assertIn("Update failed", str(context.exception))
        
    def test_validate_schedule_messages_called(self):
        """Test that validate_schedule_messages is called if available."""
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_UPDATE_TASK_LABEL,
            content="Memory update content",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify validate_schedule_messages was called
        self.mock_scheduler.validate_schedule_messages.assert_called_once()
        call_args = self.mock_scheduler.validate_schedule_messages.call_args
        self.assertEqual(call_args[0][1], MEM_UPDATE_TASK_LABEL)
        

class TestMemoryUpdateHandlerIntegration(unittest.TestCase):
    """Integration tests for MemoryUpdateHandler with more realistic scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up realistic mock behavior
        self.mock_scheduler.mem_cube = Mock()
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Track all update process calls
        self.update_calls = []
        
        def long_memory_update_process_side_effect(user_id, mem_cube_id, messages):
            self.update_calls.append({
                "user_id": user_id,
                "mem_cube_id": mem_cube_id,
                "message_count": len(messages)
            })
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = MemoryUpdateHandler(self.context)
        self.handler.long_memory_update_process = Mock(
            side_effect=long_memory_update_process_side_effect
        )
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
    def test_mixed_user_and_cube_batching(self):
        """Test that messages are properly batched by user and cube."""
        # Create messages with different user/cube combinations
        test_messages = [
            ScheduleMessageItem(
                item_id="msg1",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_UPDATE_TASK_LABEL,
                content="Update 1",
                timestamp=123456789,
            ),
            ScheduleMessageItem(
                item_id="msg2",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_UPDATE_TASK_LABEL,
                content="Update 2",
                timestamp=123456790,
            ),
            ScheduleMessageItem(
                item_id="msg3",
                user_id="user1",
                mem_cube_id="cube2",
                label=MEM_UPDATE_TASK_LABEL,
                content="Update 3",
                timestamp=123456791,
            ),
            ScheduleMessageItem(
                item_id="msg4",
                user_id="user2",
                mem_cube_id="cube1",
                label=MEM_UPDATE_TASK_LABEL,
                content="Update 4",
                timestamp=123456792,
            ),
        ]
        
        # Process messages
        self.handler(test_messages)
        
        self.assertEqual(len(self.update_calls), 3)
        
        # Find the batch with 2 messages (user1, cube1)
        batch_with_2 = [call for call in self.update_calls if call["message_count"] == 2]
        self.assertEqual(len(batch_with_2), 1)
        self.assertEqual(batch_with_2[0]["user_id"], "user1")
        self.assertEqual(batch_with_2[0]["mem_cube_id"], "cube1")
        

if __name__ == "__main__":
    unittest.main()
