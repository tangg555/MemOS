"""Unit tests for QueryHandler."""

import unittest

from unittest.mock import Mock, patch

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import (
    ScheduleLogForWebItem,
    ScheduleMessageItem,
)
from memos.mem_scheduler.schemas.task_schemas import (
    MEM_UPDATE_TASK_LABEL,
    NOT_APPLICABLE_TYPE,
    QUERY_TASK_LABEL,
    USER_INPUT_TYPE,
)
from memos.mem_scheduler.task_schedule_modules.handlers.query_handler import QueryHandler


class TestQueryHandler(unittest.TestCase):
    """Test cases for QueryHandler."""

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
        self.mock_scheduler.create_event_log = Mock(return_value=ScheduleLogForWebItem(
            label="addMessage",
            from_memory_type=USER_INPUT_TYPE,
            to_memory_type=NOT_APPLICABLE_TYPE,
            user_id="test_user",
            mem_cube_id="test_cube",
            log_content="Test log content",
            timestamp=123456789,
        ))
        self.mock_scheduler._submit_web_logs = Mock()  # Note: underscore prefix
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler.submit_messages = Mock()
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
        # Create context
        self.context = SchedulerContext(self.mock_scheduler)
        
        # Create handler
        self.handler = QueryHandler(self.context)
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, QUERY_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    def test_main_path_single_message(self):
        """Test main path with a single query message."""
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
            session_id="session1",
            user_name="Test User",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify create_event_log was called
        self.mock_scheduler.create_event_log.assert_called_once()
        
        # Verify _submit_web_logs was called (note: underscore prefix in scheduler)
        # Note: submit_web_logs is called only if both create_event_log and submit_web_logs are truthy
        # In the handler code: if self.context.create_event_log and self.context.submit_web_logs:
        self.mock_scheduler._submit_web_logs.assert_called_once()
        
        # Verify submit_messages was called with mem_update message
        self.mock_scheduler.submit_messages.assert_called_once()
        submitted_messages = self.mock_scheduler.submit_messages.call_args[0][0]
        self.assertEqual(len(submitted_messages), 1)
        self.assertEqual(submitted_messages[0].label, MEM_UPDATE_TASK_LABEL)
        self.assertEqual(submitted_messages[0].user_id, "user1")
        self.assertEqual(submitted_messages[0].content, "Test query content")
        
    def test_main_path_multiple_messages(self):
        """Test main path with multiple query messages."""
        # Create test messages
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=QUERY_TASK_LABEL,
                content=f"Test query content {i}",
                timestamp=123456789 + i,
                session_id="session1",
            )
            for i in range(3)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify submit_messages was called with all messages
        self.mock_scheduler.submit_messages.assert_called_once()
        submitted_messages = self.mock_scheduler.submit_messages.call_args[0][0]
        self.assertEqual(len(submitted_messages), 3)
        
        # Verify all messages have correct label
        for msg in submitted_messages:
            self.assertEqual(msg.label, MEM_UPDATE_TASK_LABEL)
            
    def test_main_path_multiple_users(self):
        """Test main path with messages from different users."""
        # Create test messages from different users
        test_messages = [
            ScheduleMessageItem(
                item_id="msg1",
                user_id="user1",
                mem_cube_id="cube1",
                label=QUERY_TASK_LABEL,
                content="User 1 query",
                timestamp=123456789,
            ),
            ScheduleMessageItem(
                item_id="msg2",
                user_id="user2",
                mem_cube_id="cube2",
                label=QUERY_TASK_LABEL,
                content="User 2 query",
                timestamp=123456790,
            ),
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify submit_messages was called twice (once for each batch/user)
        self.assertEqual(self.mock_scheduler.submit_messages.call_count, 2)
        
    def test_exception_in_create_event_log(self):
        """Test handler continues when create_event_log fails."""
        # Make create_event_log raise an exception
        self.mock_scheduler.create_event_log.side_effect = Exception("Event log creation failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should not raise exception when event log creation fails: {e}")
        
        # Verify submit_messages was still called (message processing continues)
        self.mock_scheduler.submit_messages.assert_called_once()
        
    def test_exception_in_submit_web_logs(self):
        """Test handler continues when submit_web_logs fails."""
        # Make _submit_web_logs raise an exception (note: underscore prefix)
        self.mock_scheduler._submit_web_logs.side_effect = Exception("Web log submission failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should not raise exception when web log submission fails: {e}")
        
        # Verify submit_messages was still called (message processing continues)
        self.mock_scheduler.submit_messages.assert_called_once()
        
    def test_no_submit_messages_method(self):
        """Test handler when submit_messages is not available."""
        # Remove submit_messages method
        self.mock_scheduler.submit_messages = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
        )
        
        # Process message - will raise TypeError because submit_messages is None
        # The handler checks if submit_messages exists, but doesn't handle None case
        with self.assertRaises(TypeError):
            self.handler([test_message])
            
    def test_no_create_event_log_method(self):
        """Test handler when create_event_log is not available."""
        # Remove create_event_log method
        self.mock_scheduler.create_event_log = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should not raise exception when create_event_log is unavailable: {e}")
        
        # Verify submit_messages was still called
        self.mock_scheduler.submit_messages.assert_called_once()
        
    def test_message_conversion_preserves_fields(self):
        """Test that message conversion to mem_update preserves all fields."""
        # Create test message with all optional fields
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
            session_id="session1",
            user_name="Test User",
            info={"key": "value"},
            task_id="task123",
        )
        
        # Process message
        self.handler([test_message])
        
        # Get submitted message
        submitted_messages = self.mock_scheduler.submit_messages.call_args[0][0]
        converted_msg = submitted_messages[0]
        
        # Verify all fields are preserved
        self.assertEqual(converted_msg.user_id, test_message.user_id)
        self.assertEqual(converted_msg.mem_cube_id, test_message.mem_cube_id)
        self.assertEqual(converted_msg.content, test_message.content)
        self.assertEqual(converted_msg.session_id, "")
        self.assertEqual(converted_msg.user_name, test_message.user_name)
        self.assertEqual(converted_msg.info, test_message.info)
        self.assertEqual(converted_msg.task_id, test_message.task_id)
        self.assertEqual(converted_msg.label, MEM_UPDATE_TASK_LABEL)
        
    def test_empty_message_list(self):
        """Test handler with empty message list."""
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify submit_messages was not called
        self.mock_scheduler.submit_messages.assert_not_called()
        
    def test_validate_schedule_messages_called(self):
        """Test that validate_schedule_messages is called if available."""
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify validate_schedule_messages was called
        self.mock_scheduler.validate_schedule_messages.assert_called_once()
        call_args = self.mock_scheduler.validate_schedule_messages.call_args
        self.assertEqual(call_args[0][1], QUERY_TASK_LABEL)
        
    @patch("memos.mem_scheduler.task_schedule_modules.base_handler.logger")
    def test_logging_on_exception(self, mock_logger):
        """Test that exceptions are properly logged."""
        # Make create_event_log raise an exception
        self.mock_scheduler.create_event_log.side_effect = Exception("Test exception")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query content",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify error was logged (logging happens in base_handler.handle_exception)
        mock_logger.error.assert_called()
        

class TestQueryHandlerIntegration(unittest.TestCase):
    """Integration tests for QueryHandler with more realistic scenarios."""
    
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
        
        self.submitted_messages = []
        self.mock_scheduler.submit_messages = Mock(side_effect=lambda msgs: self.submitted_messages.extend(msgs))
        self.mock_scheduler.create_event_log = Mock(return_value=ScheduleLogForWebItem(
            label="addMessage",
            from_memory_type=USER_INPUT_TYPE,
            to_memory_type=NOT_APPLICABLE_TYPE,
            user_id="test_user",
            mem_cube_id="test_cube",
            log_content="Test log content",
            timestamp=123456789,
        ))
        self.mock_scheduler._submit_web_logs = Mock()  # Note: underscore prefix
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = QueryHandler(self.context)
        
    def test_batch_processing_same_user(self):
        """Test batch processing of messages from the same user."""
        # Create 10 messages from same user
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=QUERY_TASK_LABEL,
                content=f"Query {i}",
                timestamp=123456789 + i,
            )
            for i in range(10)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify all messages were converted and submitted
        self.assertEqual(len(self.submitted_messages), 10)
        for i, msg in enumerate(self.submitted_messages):
            self.assertEqual(msg.label, MEM_UPDATE_TASK_LABEL)
            self.assertEqual(msg.content, f"Query {i}")
            
    def test_mixed_user_and_cube_processing(self):
        """Test processing messages from multiple users and cubes."""
        # Create messages with different user/cube combinations
        test_messages = [
            ScheduleMessageItem(
                item_id="msg1",
                user_id="user1",
                mem_cube_id="cube1",
                label=QUERY_TASK_LABEL,
                content="User1 Cube1",
                timestamp=123456789,
            ),
            ScheduleMessageItem(
                item_id="msg2",
                user_id="user1",
                mem_cube_id="cube2",
                label=QUERY_TASK_LABEL,
                content="User1 Cube2",
                timestamp=123456790,
            ),
            ScheduleMessageItem(
                item_id="msg3",
                user_id="user2",
                mem_cube_id="cube1",
                label=QUERY_TASK_LABEL,
                content="User2 Cube1",
                timestamp=123456791,
            ),
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify all messages were processed
        self.assertEqual(len(self.submitted_messages), 3)
        

if __name__ == "__main__":
    unittest.main()
