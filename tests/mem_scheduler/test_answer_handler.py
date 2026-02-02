"""Unit tests for AnswerHandler."""

import unittest

from unittest.mock import Mock, patch

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import (
    ScheduleLogForWebItem,
    ScheduleMessageItem,
)
from memos.mem_scheduler.schemas.task_schemas import (
    ANSWER_TASK_LABEL,
    NOT_APPLICABLE_TYPE,
    USER_INPUT_TYPE,
)
from memos.mem_scheduler.task_schedule_modules.handlers.answer_handler import AnswerHandler


class TestAnswerHandler(unittest.TestCase):
    """Test cases for AnswerHandler."""

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
        self.mock_scheduler._submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
        # Create context
        self.context = SchedulerContext(self.mock_scheduler)
        
        # Create handler
        self.handler = AnswerHandler(self.context)
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, ANSWER_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    def test_main_path_single_message(self):
        """Test main path with a single answer message."""
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ANSWER_TASK_LABEL,
            content="This is the assistant's answer",
            timestamp=123456789,
            session_id="session1",
            user_name="Test User",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify create_event_log was called
        self.mock_scheduler.create_event_log.assert_called_once()
        
        # Verify the content is marked as assistant message
        call_args = self.mock_scheduler.create_event_log.call_args[1]
        memcube_log_content = call_args.get("memcube_log_content", [])
        self.assertEqual(len(memcube_log_content), 1)
        self.assertIn("[Assistant]", memcube_log_content[0]["content"])
        self.assertEqual(memcube_log_content[0]["role"], "assistant")
        
        # Verify _submit_web_logs was called
        self.mock_scheduler._submit_web_logs.assert_called_once()
        
    def test_main_path_multiple_messages(self):
        """Test main path with multiple answer messages."""
        # Create test messages
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=ANSWER_TASK_LABEL,
                content=f"Answer {i}",
                timestamp=123456789 + i,
                session_id="session1",
            )
            for i in range(3)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify create_event_log was called 3 times
        self.assertEqual(self.mock_scheduler.create_event_log.call_count, 3)
        
        # Verify _submit_web_logs was called 3 times
        self.assertEqual(self.mock_scheduler._submit_web_logs.call_count, 3)
        
    def test_main_path_multiple_users(self):
        """Test main path with messages from different users."""
        # Create test messages from different users
        test_messages = [
            ScheduleMessageItem(
                item_id="msg1",
                user_id="user1",
                mem_cube_id="cube1",
                label=ANSWER_TASK_LABEL,
                content="User 1 answer",
                timestamp=123456789,
            ),
            ScheduleMessageItem(
                item_id="msg2",
                user_id="user2",
                mem_cube_id="cube2",
                label=ANSWER_TASK_LABEL,
                content="User 2 answer",
                timestamp=123456790,
            ),
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify create_event_log was called twice
        self.assertEqual(self.mock_scheduler.create_event_log.call_count, 2)
        
    def test_exception_in_create_event_log(self):
        """Test handler continues when create_event_log fails."""
        # Make create_event_log raise an exception
        self.mock_scheduler.create_event_log.side_effect = Exception("Event log creation failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ANSWER_TASK_LABEL,
            content="Test answer",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception (caught and logged)
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should catch exceptions: {e}")
        
    def test_exception_in_submit_web_logs(self):
        """Test handler continues when _submit_web_logs fails."""
        # Make _submit_web_logs raise an exception
        self.mock_scheduler._submit_web_logs.side_effect = Exception("Web log submission failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ANSWER_TASK_LABEL,
            content="Test answer",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception (caught and logged)
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should catch exceptions: {e}")
        
    def test_no_create_event_log_method(self):
        """Test handler when create_event_log is not available."""
        # Remove create_event_log method
        self.mock_scheduler.create_event_log = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ANSWER_TASK_LABEL,
            content="Test answer",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should handle missing create_event_log: {e}")
        
        # Verify _submit_web_logs was not called
        self.mock_scheduler._submit_web_logs.assert_not_called()
        
    def test_no_submit_web_logs_method(self):
        """Test handler when _submit_web_logs is not available."""
        # Remove _submit_web_logs method
        self.mock_scheduler._submit_web_logs = None
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ANSWER_TASK_LABEL,
            content="Test answer",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should handle missing _submit_web_logs: {e}")
        
    def test_empty_message_list(self):
        """Test handler with empty message list."""
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify create_event_log was not called
        self.mock_scheduler.create_event_log.assert_not_called()
        
    def test_task_id_preserved(self):
        """Test that task_id is preserved in event log."""
        # Create test message with task_id
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ANSWER_TASK_LABEL,
            content="Test answer",
            timestamp=123456789,
            task_id="task123",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify task_id was set on the event
        # The event is returned by create_event_log and task_id is set on it
        self.mock_scheduler.create_event_log.assert_called_once()
        event = self.mock_scheduler.create_event_log.return_value
        self.assertEqual(event.task_id, "task123")
        
    def test_answer_content_formatting(self):
        """Test that answer content is properly formatted with [Assistant] prefix."""
        # Create test message
        test_content = "This is a detailed assistant response."
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ANSWER_TASK_LABEL,
            content=test_content,
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify content formatting
        call_args = self.mock_scheduler.create_event_log.call_args[1]
        memcube_log_content = call_args.get("memcube_log_content", [])
        expected_content = f"[Assistant] {test_content}"
        self.assertEqual(memcube_log_content[0]["content"], expected_content)
        
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
            label=ANSWER_TASK_LABEL,
            content="Test answer",
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify error was logged (logging happens in base_handler.handle_exception)
        mock_logger.error.assert_called()
        

class TestAnswerHandlerIntegration(unittest.TestCase):
    """Integration tests for AnswerHandler with more realistic scenarios."""
    
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
        
        self.event_logs = []
        
        def create_event_log_side_effect(**kwargs):
            event = ScheduleLogForWebItem(
                label=kwargs.get("label", "addMessage"),
                from_memory_type=kwargs.get("from_memory_type", USER_INPUT_TYPE),
                to_memory_type=kwargs.get("to_memory_type", NOT_APPLICABLE_TYPE),
                user_id=kwargs.get("user_id", "test_user"),
                mem_cube_id=kwargs.get("mem_cube_id", "test_cube"),
                log_content="Test log content",
                timestamp=123456789,
            )
            self.event_logs.append(event)
            return event
        
        self.mock_scheduler.create_event_log = Mock(side_effect=create_event_log_side_effect)
        self.mock_scheduler._submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = AnswerHandler(self.context)
        
    def test_conversation_flow(self):
        """Test handling a sequence of answer messages in a conversation."""
        # Create a sequence of answer messages
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=ANSWER_TASK_LABEL,
                content=f"Assistant response {i}",
                timestamp=123456789 + i,
                session_id="session1",
            )
            for i in range(5)
        ]
        
        # Process messages
        self.handler(test_messages)
        
        # Verify all events were created
        self.assertEqual(len(self.event_logs), 5)
        
        # Verify all events were submitted
        self.assertEqual(self.mock_scheduler._submit_web_logs.call_count, 5)
        

if __name__ == "__main__":
    unittest.main()
