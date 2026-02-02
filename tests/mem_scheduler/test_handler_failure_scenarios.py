"""Tests for typical failure scenarios in handlers.

This test file covers common failure scenarios that can affect handlers:
- Redis connection failures
- MemCube operation failures
- Monitor failures
- Network timeouts
- Resource exhaustion
"""


import unittest

from unittest.mock import Mock, patch

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import ADD_TASK_LABEL, QUERY_TASK_LABEL
from memos.mem_scheduler.task_schedule_modules.handlers.add_handler import AddHandler
from memos.mem_scheduler.task_schedule_modules.handlers.query_handler import QueryHandler


class TestRedisFailureScenarios(unittest.TestCase):
    """Test handler behavior when Redis fails."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up mock dependencies
        self.mock_scheduler.mem_cube = Mock()
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Set up context methods
        self.mock_scheduler.create_event_log = Mock()
        self.mock_scheduler.submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler.submit_messages = Mock()
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube")
        
        self.context = SchedulerContext(self.mock_scheduler)
        
    def test_query_handler_redis_connection_failure_on_submit(self):
        """Test QueryHandler when Redis connection fails during submit_messages."""
        # Set up handler
        handler = QueryHandler(self.context)
        
        # Make submit_messages raise Redis connection error
        self.mock_scheduler.submit_messages.side_effect = RedisConnectionError("Connection refused")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(RedisConnectionError):
            handler([test_message])
            
    def test_query_handler_redis_timeout_on_submit(self):
        """Test QueryHandler when Redis times out during submit_messages."""
        # Set up handler
        handler = QueryHandler(self.context)
        
        # Make submit_messages raise Redis timeout error
        self.mock_scheduler.submit_messages.side_effect = RedisTimeoutError("Operation timed out")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(RedisTimeoutError):
            handler([test_message])
            
    def test_query_handler_redis_intermittent_failure(self):
        """Test QueryHandler with intermittent Redis failures."""
        # Set up handler
        handler = QueryHandler(self.context)
        
        # Set up intermittent failures: fail, succeed, fail
        call_count = [0]
        
        def intermittent_submit(messages):
            call_count[0] += 1
            if call_count[0] % 2 == 1:  # Odd calls fail
                raise RedisConnectionError("Connection refused")
            # Even calls succeed
            return None
        
        self.mock_scheduler.submit_messages.side_effect = intermittent_submit
        
        # Create test messages
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query",
            timestamp=123456789,
        )
        
        # First call should fail
        with self.assertRaises(RedisConnectionError):
            handler([test_message])
        
        # Second call should succeed
        try:
            handler([test_message])
        except RedisConnectionError:
            self.fail("Second call should have succeeded")
            

class TestMemCubeFailureScenarios(unittest.TestCase):
    """Test handler behavior when MemCube operations fail."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up mock dependencies
        self.mock_mem_cube = Mock()
        self.mock_scheduler.mem_cube = self.mock_mem_cube
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Set up context methods
        self.mock_scheduler.log_add_messages = Mock()
        self.mock_scheduler.send_add_log_messages_to_local_env = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
        self.context = SchedulerContext(self.mock_scheduler)
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_add_handler_memcube_access_failure(self, mock_is_cloud_env):
        """Test AddHandler when MemCube access fails."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Set up handler
        handler = AddHandler(self.context)
        
        # Make log_add_messages raise exception (simulating MemCube access failure)
        self.mock_scheduler.log_add_messages.side_effect = Exception("MemCube access failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(Exception) as context:
            handler([test_message])
        
        self.assertIn("MemCube access failed", str(context.exception))
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_add_handler_memcube_timeout(self, mock_is_cloud_env):
        """Test AddHandler when MemCube operation times out."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Set up handler
        handler = AddHandler(self.context)
        
        # Make log_add_messages raise timeout exception
        self.mock_scheduler.log_add_messages.side_effect = TimeoutError("MemCube operation timed out")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(TimeoutError):
            handler([test_message])
            

class TestMonitorFailureScenarios(unittest.TestCase):
    """Test handler behavior when Monitor operations fail."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up mock dependencies
        self.mock_scheduler.mem_cube = Mock()
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Set up context methods
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler.submit_messages = Mock()
        
        self.context = SchedulerContext(self.mock_scheduler)
        
    def test_query_handler_monitor_failure(self):
        """Test QueryHandler continues when Monitor fails."""
        # Set up handler
        handler = QueryHandler(self.context)
        
        # Make monitor operation fail
        self.mock_scheduler.monitor.record_event = Mock(side_effect=Exception("Monitor failed"))
        
        # Make create_event_log fail to simulate monitor-related failure
        self.mock_scheduler.create_event_log = Mock(side_effect=Exception("Monitor event creation failed"))
        self.mock_scheduler.submit_web_logs = Mock()
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query",
            timestamp=123456789,
        )
        
        # Process message - should not raise exception (handler catches it)
        try:
            handler([test_message])
        except Exception as e:
            self.fail(f"Handler should catch monitor failures: {e}")
        
        # Verify submit_messages was still called
        self.mock_scheduler.submit_messages.assert_called_once()
        

class TestNetworkFailureScenarios(unittest.TestCase):
    """Test handler behavior under network failure conditions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up mock dependencies
        self.mock_scheduler.mem_cube = Mock()
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Set up context methods
        self.mock_scheduler.log_add_messages = Mock()
        self.mock_scheduler.send_add_log_messages_to_cloud_env = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        
        self.context = SchedulerContext(self.mock_scheduler)
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_add_handler_network_timeout_to_cloud(self, mock_is_cloud_env):
        """Test AddHandler when network times out sending to cloud."""
        # Set up cloud environment
        mock_is_cloud_env.return_value = True
        
        # Set up handler
        handler = AddHandler(self.context)
        
        # Set up successful log preparation
        self.mock_scheduler.log_add_messages.return_value = (
            [{"item_id": "add1"}],
            [{"item_id": "update1"}],
        )
        
        # Make send_to_cloud raise network timeout
        self.mock_scheduler.send_add_log_messages_to_cloud_env.side_effect = TimeoutError("Network timeout")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(TimeoutError):
            handler([test_message])
            
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_add_handler_connection_error_to_cloud(self, mock_is_cloud_env):
        """Test AddHandler when connection fails to cloud service."""
        # Set up cloud environment
        mock_is_cloud_env.return_value = True
        
        # Set up handler
        handler = AddHandler(self.context)
        
        # Set up successful log preparation
        self.mock_scheduler.log_add_messages.return_value = (
            [{"item_id": "add1"}],
            [{"item_id": "update1"}],
        )
        
        # Make send_to_cloud raise connection error
        self.mock_scheduler.send_add_log_messages_to_cloud_env.side_effect = ConnectionError("Connection failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(ConnectionError):
            handler([test_message])
            

class TestResourceExhaustionScenarios(unittest.TestCase):
    """Test handler behavior under resource exhaustion."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up mock dependencies
        self.mock_scheduler.mem_cube = Mock()
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Set up context methods
        self.mock_scheduler.log_add_messages = Mock()
        self.mock_scheduler.send_add_log_messages_to_local_env = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler.submit_messages = Mock()
        self.mock_scheduler.create_event_log = Mock()
        self.mock_scheduler.submit_web_logs = Mock()
        
        self.context = SchedulerContext(self.mock_scheduler)
        
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_add_handler_memory_error(self, mock_is_cloud_env):
        """Test AddHandler when memory is exhausted."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Set up handler
        handler = AddHandler(self.context)
        
        # Make log_add_messages raise MemoryError
        self.mock_scheduler.log_add_messages.side_effect = MemoryError("Out of memory")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=ADD_TASK_LABEL,
            content="Test add",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(MemoryError):
            handler([test_message])
            
    def test_query_handler_memory_error(self):
        """Test QueryHandler when memory is exhausted."""
        # Set up handler
        handler = QueryHandler(self.context)
        
        # Make submit_messages raise MemoryError
        self.mock_scheduler.submit_messages.side_effect = MemoryError("Out of memory")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query",
            timestamp=123456789,
        )
        
        # Process message - should raise exception
        with self.assertRaises(MemoryError):
            handler([test_message])
            

class TestCascadingFailureScenarios(unittest.TestCase):
    """Test handler behavior under cascading failure scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_scheduler = Mock()
        
        # Set up mock dependencies
        self.mock_scheduler.mem_cube = Mock()
        self.mock_scheduler.monitor = Mock()
        self.mock_scheduler.retriever = Mock()
        self.mock_scheduler.config = Mock()
        self.mock_scheduler.dispatcher = Mock()
        self.mock_scheduler.db_engine = None
        self.mock_scheduler.feedback_server = None
        self.mock_scheduler.mem_reader = None
        
        # Set up context methods
        self.mock_scheduler.create_event_log = Mock()
        self.mock_scheduler.submit_web_logs = Mock()
        self.mock_scheduler.validate_schedule_messages = Mock(return_value=True)
        self.mock_scheduler.submit_messages = Mock()
        self.mock_scheduler._map_memcube_name = Mock(return_value="test_cube")
        
        self.context = SchedulerContext(self.mock_scheduler)
        
    def test_query_handler_multiple_failures(self):
        """Test QueryHandler when multiple operations fail."""
        # Set up handler
        handler = QueryHandler(self.context)
        
        # Make multiple operations fail
        self.mock_scheduler.create_event_log.side_effect = Exception("Event log failed")
        self.mock_scheduler.submit_web_logs.side_effect = Exception("Web log failed")
        self.mock_scheduler.submit_messages.side_effect = RedisConnectionError("Redis failed")
        
        # Create test message
        test_message = ScheduleMessageItem(
            item_id="msg1",
            user_id="user1",
            mem_cube_id="cube1",
            label=QUERY_TASK_LABEL,
            content="Test query",
            timestamp=123456789,
        )
        
        # Process message - should raise the final exception (Redis)
        with self.assertRaises(RedisConnectionError):
            handler([test_message])
            
    @patch("memos.mem_scheduler.task_schedule_modules.handlers.add_handler.is_cloud_env")
    def test_add_handler_partial_batch_failure(self, mock_is_cloud_env):
        """Test AddHandler when some messages in batch fail."""
        # Set up local environment
        mock_is_cloud_env.return_value = False
        
        # Set up handler
        handler = AddHandler(self.context)
        
        # Set up log_add_messages to fail on second call
        call_count = [0]
        
        def log_with_failure(msg):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Failed on second message")
            return ([{"item_id": f"add_{msg.item_id}"}], [{"item_id": f"update_{msg.item_id}"}])
        
        self.mock_scheduler.log_add_messages.side_effect = log_with_failure
        
        # Create test messages
        test_messages = [
            ScheduleMessageItem(
                item_id=f"msg{i}",
                user_id="user1",
                mem_cube_id="cube1",
                label=ADD_TASK_LABEL,
                content=f"Test add {i}",
                timestamp=123456789 + i,
            )
            for i in range(3)
        ]
        
        # Process messages - should raise exception on second message
        with self.assertRaises(Exception) as context:
            handler(test_messages)
        
        self.assertIn("Failed on second message", str(context.exception))
        
        # Verify first message was processed
        self.assertEqual(self.mock_scheduler.send_add_log_messages_to_local_env.call_count, 1)
        

if __name__ == "__main__":
    unittest.main()
