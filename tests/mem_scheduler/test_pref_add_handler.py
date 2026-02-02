"""Unit tests for PrefAddHandler."""


import json
import unittest

from unittest.mock import Mock, patch

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import PREF_ADD_TASK_LABEL
from memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler import PrefAddHandler


class TestPrefAddHandler(unittest.TestCase):
    """Test cases for PrefAddHandler."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock scheduler
        self.mock_scheduler = Mock()
        
        # Create mock dependencies
        self.mock_mem_cube = Mock()
        self.mock_pref_mem = Mock()
        self.mock_monitor = Mock()
        self.mock_retriever = Mock()
        self.mock_config = Mock()
        self.mock_dispatcher = Mock()
        
        # Set up preference memory with proper type
        self.mock_pref_mem.get_memory = Mock(return_value=["pref1", "pref2"])
        self.mock_pref_mem.add = Mock(return_value=["pref_id_1", "pref_id_2"])
        self.mock_pref_mem.__class__.__name__ = "PreferenceTextMemory"
        self.mock_mem_cube.pref_mem = self.mock_pref_mem
        
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
        self.handler = PrefAddHandler(self.context)
        
    def test_initialization(self):
        """Test handler initialization."""
        self.assertEqual(self.handler.expected_task_label, PREF_ADD_TASK_LABEL)
        self.assertIsNotNone(self.handler.context)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler.isinstance')
    def test_main_path_single_message(self, mock_isinstance):
        """Test main path with a single preference add message."""
        # Mock isinstance to return True for PreferenceTextMemory check
        mock_isinstance.return_value = True
        
        # Create test message
        messages_list = [{"role": "user", "content": "I prefer dark mode"}]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content=json.dumps(messages_list),
            timestamp=123456789,
            session_id="session1",
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify get_memory was called
        self.mock_pref_mem.get_memory.assert_called_once()
        call_args = self.mock_pref_mem.get_memory.call_args
        self.assertEqual(call_args[0][0], messages_list)
        self.assertEqual(call_args[1]["type"], "chat")
        
        # Verify add was called
        self.mock_pref_mem.add.assert_called_once()
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler.isinstance')
    def test_main_path_multiple_messages(self, mock_isinstance):
        """Test main path with multiple preference add messages."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create test messages
        test_messages = []
        for i in range(3):
            messages_list = [{"role": "user", "content": f"Preference {i}"}]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=PREF_ADD_TASK_LABEL,
                content=json.dumps(messages_list),
                timestamp=123456789 + i,
                session_id="session1",
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify get_memory and add were called 3 times
        self.assertEqual(self.mock_pref_mem.get_memory.call_count, 3)
        self.assertEqual(self.mock_pref_mem.add.call_count, 3)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler.isinstance')
    def test_main_path_with_info(self, mock_isinstance):
        """Test main path with additional info in message."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create test message with info
        messages_list = [{"role": "user", "content": "Test preference"}]
        test_info = {"custom_key": "custom_value"}
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content=json.dumps(messages_list),
            timestamp=123456789,
            session_id="session1",
            info=test_info,
        )
        
        # Process message
        self.handler([test_message])
        
        # Verify info was passed to get_memory
        call_args = self.mock_pref_mem.get_memory.call_args
        info_arg = call_args[1]["info"]
        self.assertIn("custom_key", info_arg)
        self.assertEqual(info_arg["custom_key"], "custom_value")
        self.assertEqual(info_arg["user_id"], "user1")
        self.assertEqual(info_arg["session_id"], "session1")
        self.assertEqual(info_arg["mem_cube_id"], "cube1")
        
    def test_mem_cube_none(self):
        """Test handler when mem_cube is None."""
        # Set mem_cube to None
        self.mock_scheduler.mem_cube = None
        
        # Create test message
        messages_list = [{"role": "user", "content": "Test"}]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content=json.dumps(messages_list),
            timestamp=123456789,
        )
        
        # Process message - should log warning and skip
        with self.assertLogs(level='WARNING') as log:
            self.handler([test_message])
        
        # Verify warning was logged
        self.assertTrue(any('mem_cube is None' in msg for msg in log.output))
        
        # Verify pref_mem operations were not called
        self.mock_pref_mem.get_memory.assert_not_called()
        self.mock_pref_mem.add.assert_not_called()
        
    def test_pref_mem_none(self):
        """Test handler when pref_mem is None."""
        # Set pref_mem to None
        self.mock_mem_cube.pref_mem = None
        
        # Create test message
        messages_list = [{"role": "user", "content": "Test"}]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content=json.dumps(messages_list),
            timestamp=123456789,
        )
        
        # Process message - should log warning and skip
        with self.assertLogs(level='WARNING') as log:
            self.handler([test_message])
        
        # Verify warning was logged
        self.assertTrue(any('Preference memory not initialized' in msg for msg in log.output))
        
    def test_invalid_pref_mem_type(self):
        """Test handler when pref_mem is wrong type."""
        # Set pref_mem to wrong type
        self.mock_mem_cube.pref_mem = Mock()
        self.mock_mem_cube.pref_mem.__class__.__name__ = "WrongType"
        
        # Create test message
        messages_list = [{"role": "user", "content": "Test"}]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content=json.dumps(messages_list),
            timestamp=123456789,
        )
        
        # Process message - should log error and skip
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Expected PreferenceTextMemory' in msg for msg in log.output))
        
    def test_invalid_json_content(self):
        """Test handler with invalid JSON content."""
        # Create test message with invalid JSON
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content="invalid json {",
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing pref_add message' in msg for msg in log.output))
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler.isinstance')
    def test_exception_in_get_memory(self, mock_isinstance):
        """Test handler when get_memory raises exception."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Make get_memory raise an exception
        self.mock_pref_mem.get_memory.side_effect = Exception("Get memory failed")
        
        # Create test message
        messages_list = [{"role": "user", "content": "Test"}]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content=json.dumps(messages_list),
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing pref_add message' in msg for msg in log.output))
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler.isinstance')
    def test_exception_in_add(self, mock_isinstance):
        """Test handler when add raises exception."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Make add raise an exception
        self.mock_pref_mem.add.side_effect = Exception("Add failed")
        
        # Create test message
        messages_list = [{"role": "user", "content": "Test"}]
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=PREF_ADD_TASK_LABEL,
            content=json.dumps(messages_list),
            timestamp=123456789,
        )
        
        # Process message - should log error
        with self.assertLogs(level='ERROR') as log:
            self.handler([test_message])
        
        # Verify error was logged
        self.assertTrue(any('Error processing pref_add message' in msg for msg in log.output))
        
    def test_empty_message_list(self):
        """Test handler with empty message list."""
        # Process empty list
        try:
            self.handler([])
        except Exception as e:
            self.fail(f"Handler should handle empty message list: {e}")
        
        # Verify pref_mem operations were not called
        self.mock_pref_mem.get_memory.assert_not_called()
        self.mock_pref_mem.add.assert_not_called()
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler.isinstance')
    def test_multiple_users_and_cubes(self, mock_isinstance):
        """Test processing messages from different users and cubes."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create messages from different users and cubes
        test_messages = []
        for i, (user, cube) in enumerate([("user1", "cube1"), ("user1", "cube2"), ("user2", "cube1")]):
            messages_list = [{"role": "user", "content": f"Pref {i}"}]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id=user,
                mem_cube_id=cube,
                label=PREF_ADD_TASK_LABEL,
                content=json.dumps(messages_list),
                timestamp=123456789 + i,
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify get_memory and add were called for each message
        self.assertEqual(self.mock_pref_mem.get_memory.call_count, 3)
        self.assertEqual(self.mock_pref_mem.add.call_count, 3)
        

class TestPrefAddHandlerIntegration(unittest.TestCase):
    """Integration tests for PrefAddHandler with more realistic scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up realistic mock behavior
        self.mock_mem_cube = Mock()
        self.mock_pref_mem = Mock()
        
        # Track all add calls
        self.added_prefs = []
        
        def add_side_effect(prefs):
            pref_ids = [f"pref_id_{i}" for i in range(len(prefs))]
            self.added_prefs.extend(prefs)
            return pref_ids
        
        self.mock_pref_mem.get_memory = Mock(return_value=["pref1", "pref2"])
        self.mock_pref_mem.add = Mock(side_effect=add_side_effect)
        self.mock_pref_mem.__class__.__name__ = "PreferenceTextMemory"
        self.mock_mem_cube.pref_mem = self.mock_pref_mem
        
        self.mock_scheduler.mem_cube = self.mock_mem_cube
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
        self.context = SchedulerContext(self.mock_scheduler)
        self.handler = PrefAddHandler(self.context)
        
    @patch('memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler.isinstance')
    def test_batch_processing_preserves_order(self, mock_isinstance):
        """Test that batch processing preserves message order."""
        # Mock isinstance to return True
        mock_isinstance.return_value = True
        
        # Create ordered messages
        test_messages = []
        for i in range(5):
            messages_list = [{"role": "user", "content": f"Preference {i}"}]
            test_messages.append(ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=PREF_ADD_TASK_LABEL,
                content=json.dumps(messages_list),
                timestamp=123456789 + i,
                session_id="session1",
            ))
        
        # Process messages
        self.handler(test_messages)
        
        # Verify order is preserved (5 batches of 2 prefs each)
        self.assertEqual(len(self.added_prefs), 10)  # 5 messages * 2 prefs
        

if __name__ == "__main__":
    unittest.main()
