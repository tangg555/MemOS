"""Unit tests for MemFeedbackHandler."""

import json
import unittest

from unittest.mock import Mock, patch

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import (
    ScheduleLogForWebItem,
    ScheduleMessageItem,
)
from memos.mem_scheduler.schemas.task_schemas import (
    LONG_TERM_MEMORY_TYPE,
    MEM_FEEDBACK_TASK_LABEL,
    USER_INPUT_TYPE,
)
from memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler import (
    MemFeedbackHandler,
)


class TestMemFeedbackHandler(unittest.TestCase):
    """Test cases for MemFeedbackHandler."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock scheduler
        self.mock_scheduler = Mock()
        
        # Create mock dependencies
        self.mock_mem_cube = Mock()
        self.mock_feedback_server = Mock()
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
        self.mock_scheduler.feedback_server = self.mock_feedback_server
        self.mock_scheduler.mem_reader = None
        
        # Set up mock methods
        self.mock_scheduler.create_event_log = Mock(return_value=ScheduleLogForWebItem(
            label="knowledgeBaseUpdate",
            from_memory_type=USER_INPUT_TYPE,
            to_memory_type=LONG_TERM_MEMORY_TYPE,
            user_id="test_user",
            mem_cube_id="test_cube",
            log_content="Test log content",
            timestamp=123456789,
        ))
        self.mock_scheduler._submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
        # Set up feedback_server mock response
        self.mock_feedback_server.process_feedback = Mock(return_value={
            "record": {
                "add": [{"id": "mem1", "memory": "Added memory"}],
                "update": [{"id": "mem2", "memory": "Updated memory", "origin_memory": "Old memory"}]
            }
        })
        
        # Create context
        self.context = SchedulerContext(self.mock_scheduler)
        
        # Create handler
        self.handler = MemFeedbackHandler(self.context)
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, MEM_FEEDBACK_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_main_path_single_message_cloud(self, mock_is_cloud_env):
        """Test main path with a single feedback message in cloud environment."""
        # Set cloud environment
        mock_is_cloud_env.return_value = True
        
        # Create test message
        feedback_data = {
            "task_id": "task123",
            "session_id": "session1",
            "history": [{"role": "user", "content": "Test"}],
            "retrieved_memory_ids": ["mem1", "mem2"],
            "feedback_content": "Good feedback",
            "feedback_time": 123456789,
            "info": {"key": "value"}
        }
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify feedback_server.process_feedback was called
        self.mock_feedback_server.process_feedback.assert_called_once()
        call_args = self.mock_feedback_server.process_feedback.call_args[1]
        self.assertEqual(call_args["user_id"], "user1")
        self.assertEqual(call_args["user_name"], "cube1")
        self.assertEqual(call_args["session_id"], "session1")
        self.assertEqual(call_args["task_id"], "task123")
        
        # Verify create_event_log was called (cloud environment)
        self.mock_scheduler.create_event_log.assert_called_once()
        
        # Verify _submit_web_logs was called
        self.mock_scheduler._submit_web_logs.assert_called_once()
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_main_path_local_environment(self, mock_is_cloud_env):
        """Test main path in local environment (no web logs)."""
        # Set local environment
        mock_is_cloud_env.return_value = False
        
        # Create test message
        feedback_data = {
            "feedback_content": "Test feedback"
        }
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify feedback_server.process_feedback was called
        self.mock_feedback_server.process_feedback.assert_called_once()
        
        # Verify create_event_log was NOT called (local environment)
        self.mock_scheduler.create_event_log.assert_not_called()
        
    def test_invalid_json_content(self):
        """Test handler with invalid JSON content."""
        # Create test message with invalid JSON
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content="invalid json {",
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Invalid JSON content' in msg for msg in log.output))
        
        # Verify feedback_server was not called
        self.mock_feedback_server.process_feedback.assert_not_called()
        
    def test_non_dict_feedback_data(self):
        """Test handler when feedback data is not a dict."""
        # Create test message with non-dict content
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(["not", "a", "dict"]),
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('not a dict' in msg for msg in log.output))
        
    def test_feedback_server_not_available(self):
        """Test handler when feedback_server is not available."""
        # Remove feedback_server
        self.mock_scheduler.feedback_server = None
        
        # Create test message
        feedback_data = {"feedback_content": "Test"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('feedback_server not available' in msg for msg in log.output))
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_extract_fields_from_dict(self, mock_is_cloud_env):
        """Test field extraction from dict-style memory items."""
        # Set cloud environment
        mock_is_cloud_env.return_value = True
        
        # Set up feedback result with dict-style items
        self.mock_feedback_server.process_feedback.return_value = {
            "record": {
                "add": [
                    {
                        "id": "mem1",
                        "text": "Memory text",
                        "source_doc_id": "doc1"
                    }
                ],
                "update": [
                    {
                        "id": "mem2",
                        "memory": "Updated text",
                        "old_memory": "Original text",
                        "source_doc_id": "doc2"
                    }
                ]
            }
        }
        
        # Create test message
        feedback_data = {"feedback_content": "Test"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify create_event_log was called with proper structure
        self.mock_scheduler.create_event_log.assert_called_once()
        call_args = self.mock_scheduler.create_event_log.call_args[1]
        memcube_log_content = call_args["memcube_log_content"]
        
        # Verify ADD operation
        add_log = next(log for log in memcube_log_content if log["operation"] == "ADD")
        self.assertEqual(add_log["memory_id"], "mem1")
        self.assertEqual(add_log["content"], "Memory text")
        self.assertEqual(add_log["source_doc_id"], "doc1")
        
        # Verify UPDATE operation
        update_log = next(log for log in memcube_log_content if log["operation"] == "UPDATE")
        self.assertEqual(update_log["memory_id"], "mem2")
        self.assertEqual(update_log["content"], "Updated text")
        self.assertEqual(update_log["original_content"], "Original text")
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_malformed_add_record_skipped(self, mock_is_cloud_env):
        """Test that malformed add records are skipped with warning."""
        # Set cloud environment
        mock_is_cloud_env.return_value = True
        
        # Set up feedback result with malformed item (missing id or memory)
        self.mock_feedback_server.process_feedback.return_value = {
            "record": {
                "add": [
                    {"id": "mem1"},  # Missing memory field
                    {"memory": "Memory without id"}  # Missing id field
                ]
            }
        }
        
        # Create test message
        feedback_data = {"feedback_content": "Test"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
        )
        
        # Process message - should log warnings
        with self.assertLogs(level='WARNING') as log:
            self.handler([test_message])
        
        # Verify warnings were logged
        self.assertTrue(any('Skipping malformed feedback add item' in msg for msg in log.output))
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_empty_feedback_result(self, mock_is_cloud_env):
        """Test handling of empty feedback result."""
        # Set cloud environment
        mock_is_cloud_env.return_value = True
        
        # Set up empty feedback result
        self.mock_feedback_server.process_feedback.return_value = {
            "record": {
                "add": [],
                "update": []
            }
        }
        
        # Create test message
        feedback_data = {"feedback_content": "Test"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
        )
        
        # Process message
        with self.assertLogs(level='WARNING') as log:
            self.handler([test_message])
        
        # Verify warning about no valid feedback content
        self.assertTrue(any('No valid feedback content generated' in msg for msg in log.output))
        
        # Verify create_event_log was NOT called
        self.mock_scheduler.create_event_log.assert_not_called()
        
    def test_exception_in_process_feedback(self):
        """Test handler when process_feedback raises exception."""
        # Make process_feedback raise an exception
        self.mock_feedback_server.process_feedback.side_effect = Exception("Processing failed")
        
        # Create test message
        feedback_data = {"feedback_content": "Test"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
        )
        
        # Process message - should not raise (caught and logged)
        try:
            self.handler([test_message])
        except Exception as e:
            self.fail(f"Handler should catch exceptions: {e}")
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_multiple_messages(self, mock_is_cloud_env):
        """Test processing multiple feedback messages."""
        # Set cloud environment
        mock_is_cloud_env.return_value = True
        
        # Create multiple test messages
        test_messages = []
        for i in range(3):
            feedback_data = {"feedback_content": f"Feedback {i}"}
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_FEEDBACK_TASK_LABEL,
                content=json.dumps(feedback_data),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify process_feedback was called 3 times
        self.assertEqual(self.mock_feedback_server.process_feedback.call_count, 3)
        
    def test_task_id_from_message(self):
        """Test that task_id from message is used when not in feedback_data."""
        # Create test message with task_id but feedback_data without it
        feedback_data = {"feedback_content": "Test"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
            task_id="msg_task_id",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify task_id from message was used
        call_args = self.mock_feedback_server.process_feedback.call_args[1]
        self.assertEqual(call_args["task_id"], "msg_task_id")
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_task_id_from_feedback_data_takes_precedence(self, mock_is_cloud_env):
        """Test that task_id from feedback_data takes precedence."""
        # Set cloud environment
        mock_is_cloud_env.return_value = True
        
        # Create test message with both task_ids
        feedback_data = {"feedback_content": "Test", "task_id": "feedback_task_id"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=MEM_FEEDBACK_TASK_LABEL,
            content=json.dumps(feedback_data),
            timestamp=123456789,
            task_id="msg_task_id",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify task_id from feedback_data was used
        call_args = self.mock_feedback_server.process_feedback.call_args[1]
        self.assertEqual(call_args["task_id"], "feedback_task_id")
        
    def test_empty_message_list(self):
        """Test handler with empty message list."""
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify process_feedback was not called
        self.mock_feedback_server.process_feedback.assert_not_called()
        

class TestMemFeedbackHandlerIntegration(unittest.TestCase):
    """Integration tests for MemFeedbackHandler with more realistic scenarios."""
    
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def setUp(self, mock_is_cloud_env):
        """Set up test fixtures."""
        # Default to cloud environment for integration tests
        mock_is_cloud_env.return_value = True
        
        self.mock_scheduler = Mock()
        
        # Set up realistic mock behavior
        self.mock_mem_cube = Mock()
        self.mock_feedback_server = Mock()
        
        # Track all feedback processing calls
        self.feedback_calls = []
        
        def process_feedback_side_effect(**kwargs):
            self.feedback_calls.append(kwargs)
            return {
                "record": {
                    "add": [{"id": f"add_{len(self.feedback_calls)}", "memory": "Added"}],
                    "update": []
                }
            }
        
        self.mock_feedback_server.process_feedback = Mock(
            side_effect=process_feedback_side_effect
        )
        
        self.mock_scheduler.mem_cube = self.mock_mem_cube
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = self.mock_feedback_server
        self.mock_scheduler.mem_reader = None
        
        self.mock_scheduler.create_event_log = Mock(return_value=ScheduleLogForWebItem(
            label="knowledgeBaseUpdate",
            from_memory_type=USER_INPUT_TYPE,
            to_memory_type=LONG_TERM_MEMORY_TYPE,
            user_id="test_user",
            mem_cube_id="test_cube",
            log_content="Test log content",
            timestamp=123456789,
        ))
        self.mock_scheduler._submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube_name")
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = MemFeedbackHandler(self.context)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler.is_cloud_env')
    def test_batch_processing_preserves_order(self, mock_is_cloud_env):
        """Test that batch processing preserves message order."""
        # Set cloud environment
        mock_is_cloud_env.return_value = True
        
        # Create ordered messages
        test_messages = []
        for i in range(5):
            feedback_data = {"feedback_content": f"Feedback {i}", "order": i}
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=MEM_FEEDBACK_TASK_LABEL,
                content=json.dumps(feedback_data),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify order is preserved
        self.assertEqual(len(self.feedback_calls), 5)
        for i, call in enumerate(self.feedback_calls):
            self.assertEqual(call["feedback_content"], f"Feedback {i}")
        

if __name__ == "__main__":
    unittest.main()
