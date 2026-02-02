"""Unit tests for AddHandler."""

import unittest

from unittest.mock import Mock, patch

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import ADD_TASK_LABEL
from memos.mem_scheduler.task_schedule_modules.handlers.add_handler import AddHandler


class TestAddHandler(unittest.TestCase):
    """Test cases for AddHandler."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock scheduler
        self.mock_scheduler = Mock()
        
        # Create mock dependencies
        self.mock_mem_cube = Mock()
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
        self.prepared_add_items = [{"item_id": "add1", "content": "Added item"}]
        self.prepared_update_items = [{"item_id": "update1", "content": "Updated item"}]
        self.mock_scheduler.log_add_messages = Mock(
            return_value=(self.prepared_add_items, self.prepared_update_items)
        )
        self.mock_scheduler.send_add_log_messages_to_cloud_env = Mock()
        self.mock_scheduler.send_add_log_messages_to_local_env = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
        # Create context
        self.context = SchedulerContext(self.mock_scheduler)
        
        # Create handler
        self.handler = AddHandler(self.context)
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, ADD_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_main_path_local_env(self, mock_is_cloud_env):
        """Test main path in local environment."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify log_add_messages was called
        self.mock_scheduler.log_add_messages.assert_called_once_with(msg=test_message)
        
        # Verify local env handler was called
        self.mock_scheduler.send_add_log_messages_to_local_env.assert_called_once_with(
            test_message, self.prepared_add_items, self.prepared_update_items
        )
        
        # Verify cloud env handler was not called
        self.mock_scheduler.send_add_log_messages_to_cloud_env.assert_not_called()
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_main_path_cloud_env(self, mock_is_cloud_env):
        """Test main path in cloud environment."""
        # Set up cloud environment
        mock_is_cloud_env.return_value = True
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify log_add_messages was called
        self.mock_scheduler.log_add_messages.assert_called_once_with(msg=test_message)
        
        # Verify cloud env handler was called
        self.mock_scheduler.send_add_log_messages_to_cloud_env.assert_called_once_with(
            test_message, self.prepared_add_items, self.prepared_update_items
        )
        
        # Verify local env handler was not called
        self.mock_scheduler.send_add_log_messages_to_local_env.assert_not_called()
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_main_path_multiple_messages(self, mock_is_cloud_env):
        """Test main path with multiple messages."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Create test messages
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=ADD_TASK_LABEL,
                content=f"Test add content {i}",
                timestamp=123456789 + i,
            )
            for i in range(3)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify log_add_messages was called for each message
        self.assertEqual(self.mock_scheduler.log_add_messages.call_count, 3)
        
        # Verify local env handler was called for each message
        self.assertEqual(self.mock_scheduler.send_add_log_messages_to_local_env.call_count, 3)
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_log_add_messages_not_available(self, mock_is_cloud_env):
        """Test handler when log_add_messages is not available."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Remove log_add_messages method
        self.mock_scheduler.log_add_messages = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message - should raise TypeError because log_add_messages is None
        with self.assertRaises(TypeError):
            self.handler([test_message])
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_exception_in_log_add_messages(self, mock_is_cloud_env):
        """Test handler when log_add_messages raises exception."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Make log_add_messages raise an exception
        self.mock_scheduler.log_add_messages.side_effect = Exception("Log failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message - should raise exception (not caught in handler)
        with self.assertRaises(Exception) as context:
            self.handler([test_message])
        
        self.assertIn("Log failed", str(context.exception))
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_exception_in_send_to_cloud(self, mock_is_cloud_env):
        """Test handler when send_add_log_messages_to_cloud_env raises exception."""
        # Set up cloud environment
        mock_is_cloud_env.return_value = True
        
        # Make send method raise an exception
        self.mock_scheduler.send_add_log_messages_to_cloud_env.side_effect = Exception("Send to cloud failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(Exception) as context:
            self.handler([test_message])
        
        self.assertIn("Send to cloud failed", str(context.exception))
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_exception_in_send_to_local(self, mock_is_cloud_env):
        """Test handler when send_add_log_messages_to_local_env raises exception."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Make send method raise an exception
        self.mock_scheduler.send_add_log_messages_to_local_env.side_effect = Exception("Send to local failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(Exception) as context:
            self.handler([test_message])
        
        self.assertIn("Send to local failed", str(context.exception))
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_send_method_not_available_cloud(self, mock_is_cloud_env):
        """Test handler when send_add_log_messages_to_cloud_env is not available."""
        # Set up cloud environment
        mock_is_cloud_env.return_value = True
        
        # Remove send method
        self.mock_scheduler.send_add_log_messages_to_cloud_env = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message - should raise TypeError because send method is None
        with self.assertRaises(TypeError):
            self.handler([test_message])
            
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_send_method_not_available_local(self, mock_is_cloud_env):
        """Test handler when send_add_log_messages_to_local_env is not available."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Remove send method
        self.mock_scheduler.send_add_log_messages_to_local_env = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message - should raise TypeError because send method is None
        with self.assertRaises(TypeError):
            self.handler([test_message])
            
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_multiple_users_and_cubes(self, mock_is_cloud_env):
        """Test processing messages from different users and cubes."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Create messages from different users and cubes
        test_messages = [
            ScheduleMessageItem(
                item_id="msg1",
                user_id="user1",
                mem_cube_id="cube1",
                label=ADD_TASK_LABEL,
                content="User1 Cube1",
                timestamp=123456789,
            ),
            ScheduleMessageItem(
                item_id="msg2",
                user_id="user1",
                mem_cube_id="cube2",
                label=ADD_TASK_LABEL,
                content="User1 Cube2",
                timestamp=123456790,
            ),
            ScheduleMessageItem(
                item_id="msg3",
                user_id="user2",
                mem_cube_id="cube1",
                label=ADD_TASK_LABEL,
                content="User2 Cube1",
                timestamp=123456791,
            ),
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify all messages were processed
        self.assertEqual(self.mock_scheduler.log_add_messages.call_count, 3)
        self.assertEqual(self.mock_scheduler.send_add_log_messages_to_local_env.call_count, 3)
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_empty_message_list(self, mock_is_cloud_env):
        """Test handler with empty message list."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify no processing occurred
        self.mock_scheduler.log_add_messages.assert_not_called()
        self.mock_scheduler.send_add_log_messages_to_local_env.assert_not_called()
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.logger")
    def test_logging_info_on_success(self, mock_logger, mock_is_cloud_env):
        """Test that info is logged on successful processing."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add content",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify info was logged
        mock_logger.info.assert_called()
        

class TestAddHandlerIntegration(unittest.TestCase):
    """Integration tests for AddHandler with more realistic scenarios."""
    
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
        
        # Track all log calls
        self.log_calls = []
        
        def log_add_messages_side_effect(msg):
            self.log_calls.append(msg)
            return (
                [{"item_id": f"add_{msg.item_id}", "content": msg.content}],
                [{"item_id": f"update_{msg.item_id}", "content": msg.content}],
            )
        
        self.mock_scheduler.log_add_messages = Mock(side_effect=log_add_messages_side_effect)
        self.mock_scheduler.send_add_log_messages_to_cloud_env = Mock()
        self.mock_scheduler.send_add_log_messages_to_local_env = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = AddHandler(self.context)
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_batch_processing_preserves_order(self, mock_is_cloud_env):
        """Test that batch processing preserves message order."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Create ordered messages
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=ADD_TASK_LABEL,
                content=f"Message {i}",
                timestamp=123456789 + i,
            )
            for i in range(5)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify order is preserved
        self.assertEqual(len(self.log_calls), 5)
        for i, msg in enumerate(self.log_calls):
            self.assertEqual(msg.item_id, f"msg{i}")
            self.assertEqual(msg.content, f"Message {i}")
            
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_environment_switching(self, mock_is_cloud_env):
        """Test handler behavior when environment changes (edge case)."""
        # Start in local environment
        mock_is_cloud_env.return_value = False
        
        # Create and process first message
        msg1 = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Local message",
            timestamp=123456789,
        )
        self.handler([msg1])
        
        # Verify local handler was called
        self.assertEqual(self.mock_scheduler.send_add_log_messages_to_local_env.call_count, 1)
        self.assertEqual(self.mock_scheduler.send_add_log_messages_to_cloud_env.call_count, 0)
        
        # Switch to cloud environment
        mock_is_cloud_env.return_value = True
        
        # Create and process second message
        msg2 = ScheduleMessageItem(
            item_id="msg2",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Cloud message",
            timestamp=123456790,
        )
        self.handler([msg2])
        
        # Verify cloud handler was called
        self.assertEqual(self.mock_scheduler.send_add_log_messages_to_local_env.call_count, 1)
        self.assertEqual(self.mock_scheduler.send_add_log_messages_to_cloud_env.call_count, 1)
        

if __name__ == "__main__":
    unittest.main()
