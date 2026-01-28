from collections.abc import Callable

from memos.configs.mem_scheduler import GeneralSchedulerConfig
from memos.log import get_logger
from memos.mem_scheduler.base_scheduler import BaseScheduler
from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import (
    ADD_TASK_LABEL,
    ANSWER_TASK_LABEL,
    MEM_FEEDBACK_TASK_LABEL,
    MEM_ORGANIZE_TASK_LABEL,
    MEM_READ_TASK_LABEL,
    MEM_UPDATE_TASK_LABEL,
    PREF_ADD_TASK_LABEL,
    QUERY_TASK_LABEL,
)
from memos.mem_scheduler.task_schedule_modules.handlers.add_handler import AddHandler
from memos.mem_scheduler.task_schedule_modules.handlers.answer_handler import AnswerHandler
from memos.mem_scheduler.task_schedule_modules.handlers.mem_feedback_handler import (
    MemFeedbackHandler,
)
from memos.mem_scheduler.task_schedule_modules.handlers.mem_read_handler import MemReadHandler
from memos.mem_scheduler.task_schedule_modules.handlers.mem_reorganize_handler import (
    MemReorganizeHandler,
)
from memos.mem_scheduler.task_schedule_modules.handlers.memory_update_handler import (
    MemoryUpdateHandler,
)
from memos.mem_scheduler.task_schedule_modules.handlers.pref_add_handler import PrefAddHandler
from memos.mem_scheduler.task_schedule_modules.handlers.query_handler import QueryHandler


logger = get_logger(__name__)


class GeneralScheduler(BaseScheduler):
    def __init__(self, config: GeneralSchedulerConfig):
        """Initialize the scheduler with the given configuration."""
        super().__init__(config)

        self.query_key_words_limit = self.config.get("query_key_words_limit", 20)

        # Initialize context
        context = SchedulerContext(self)

        # register handlers
        memory_update_handler = MemoryUpdateHandler(context)
        handlers = {
            QUERY_TASK_LABEL: QueryHandler(context, memory_update_handler),
            ANSWER_TASK_LABEL: AnswerHandler(context),
            MEM_UPDATE_TASK_LABEL: memory_update_handler,
            ADD_TASK_LABEL: AddHandler(context),
            MEM_READ_TASK_LABEL: MemReadHandler(context),
            MEM_ORGANIZE_TASK_LABEL: MemReorganizeHandler(context),
            PREF_ADD_TASK_LABEL: PrefAddHandler(context),
            MEM_FEEDBACK_TASK_LABEL: MemFeedbackHandler(context),
        }
        self.dispatcher.register_handlers(handlers)

    def register_handlers(
        self, handlers: dict[str, Callable[[list[ScheduleMessageItem]], None]]
    ) -> None:
        """
        Bulk register multiple handlers from a dictionary.

        Args:
            handlers: Dictionary mapping labels to handler functions
                      Format: {label: handler_callable}
        """
        if not self.dispatcher:
            logger.warning("Dispatcher is not initialized, cannot register handlers")
            return

        self.dispatcher.register_handlers(handlers)

    def unregister_handlers(self, labels: list[str]) -> dict[str, bool]:
        """
        Unregister handlers from the dispatcher by their labels.

        Args:
            labels: List of labels to unregister handlers for

        Returns:
            dict[str, bool]: Dictionary mapping each label to whether it was successfully unregistered
        """
        if not self.dispatcher:
            logger.warning("Dispatcher is not initialized, cannot unregister handlers")
            return dict.fromkeys(labels, False)

        return self.dispatcher.unregister_handlers(labels)

    def get_running_tasks(self, filter_func: Callable | None = None) -> dict[str, dict]:
        if not self.dispatcher:
            logger.warning("Dispatcher is not initialized, returning empty tasks dict")
            return {}

        running_tasks = self.dispatcher.get_running_tasks(filter_func=filter_func)

        # Convert RunningTaskItem objects to dictionaries for easier consumption
        result = {}
        for task_id, task_item in running_tasks.items():
            result[task_id] = {
                "item_id": task_item.item_id,
                "user_id": task_item.user_id,
                "mem_cube_id": task_item.mem_cube_id,
                "task_info": task_item.task_info,
                "task_name": task_item.task_name,
                "start_time": task_item.start_time,
                "end_time": task_item.end_time,
                "status": task_item.status,
                "result": task_item.result,
                "error_message": task_item.error_message,
                "messages": task_item.messages,
            }

        return result

    def get_tasks_status(self):
        """Delegate status collection to TaskScheduleMonitor."""
        return self.task_schedule_monitor.get_tasks_status()

    def print_tasks_status(self, tasks_status: dict | None = None) -> None:
        """Delegate pretty printing to TaskScheduleMonitor."""
        self.task_schedule_monitor.print_tasks_status(tasks_status=tasks_status)

    def _gather_queue_stats(self) -> dict:
        """Collect queue/dispatcher stats for reporting."""
        memos_message_queue = self.memos_message_queue.memos_message_queue
        stats: dict[str, int | float | str] = {}
        stats["use_redis_queue"] = bool(self.use_redis_queue)
        # local queue metrics
        if not self.use_redis_queue:
            try:
                stats["qsize"] = int(memos_message_queue.qsize())
            except Exception:
                stats["qsize"] = -1
            # unfinished_tasks if available
            try:
                stats["unfinished_tasks"] = int(
                    getattr(memos_message_queue, "unfinished_tasks", 0) or 0
                )
            except Exception:
                stats["unfinished_tasks"] = -1
            stats["maxsize"] = int(self.max_internal_message_queue_size)
            try:
                maxsize = int(self.max_internal_message_queue_size) or 1
                qsize = int(stats.get("qsize", 0))
                stats["utilization"] = min(1.0, max(0.0, qsize / maxsize))
            except Exception:
                stats["utilization"] = 0.0
        # dispatcher stats
        try:
            d_stats = self.dispatcher.stats()
            stats.update(
                {
                    "running": int(d_stats.get("running", 0)),
                    "inflight": int(d_stats.get("inflight", 0)),
                    "handlers": int(d_stats.get("handlers", 0)),
                }
            )
        except Exception:
            stats.update({"running": 0, "inflight": 0, "handlers": 0})
        return stats
