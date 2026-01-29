from collections.abc import Callable

from memos.configs.mem_scheduler import GeneralSchedulerConfig
from memos.log import get_logger
from memos.mem_cube.general import GeneralMemCube
from memos.mem_scheduler.base_scheduler import BaseScheduler
from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.monitor_schemas import MemoryMonitorItem
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
from memos.mem_scheduler.utils.filter_utils import (
    transform_name_to_key,
)
from memos.memories.textual.item import TextualMemoryItem
from memos.memories.textual.naive import NaiveTextMemory
from memos.memories.textual.tree import TreeTextMemory
from memos.types import (
    MemCubeID,
    UserID,
)


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

    def transform_working_memories_to_monitors(
        self, query_keywords, memories: list[TextualMemoryItem]
    ) -> list[MemoryMonitorItem]:
        """
        Convert a list of TextualMemoryItem objects into MemoryMonitorItem objects
        with importance scores based on keyword matching.

        Args:
            memories: List of TextualMemoryItem objects to be transformed.

        Returns:
            List of MemoryMonitorItem objects with computed importance scores.
        """

        result = []
        mem_length = len(memories)
        for idx, mem in enumerate(memories):
            text_mem = mem.memory
            mem_key = transform_name_to_key(name=text_mem)

            # Calculate importance score based on keyword matches
            keywords_score = 0
            if query_keywords and text_mem:
                for keyword, count in query_keywords.items():
                    keyword_count = text_mem.count(keyword)
                    if keyword_count > 0:
                        keywords_score += keyword_count * count
                        logger.debug(
                            f"Matched keyword '{keyword}' {keyword_count} times, added {keywords_score} to keywords_score"
                        )

            # rank score
            sorting_score = mem_length - idx

            mem_monitor = MemoryMonitorItem(
                memory_text=text_mem,
                tree_memory_item=mem,
                tree_memory_item_mapping_key=mem_key,
                sorting_score=sorting_score,
                keywords_score=keywords_score,
                recording_count=1,
            )
            result.append(mem_monitor)

        logger.info(f"Transformed {len(result)} memories to monitors")
        return result

    def _filter_fast_memories(
        self, original_memory: list[TextualMemoryItem]
    ) -> list[TextualMemoryItem]:
        """Filter out memories tagged with 'mode:fast'."""
        filtered_original_memory = []
        original_count = len(original_memory)
        for origin_mem in original_memory:
            if "mode:fast" not in origin_mem.metadata.tags:
                filtered_original_memory.append(origin_mem)
            else:
                logger.debug(
                    f"Filtered out memory - ID: {getattr(origin_mem, 'id', 'unknown')}, Tags: {origin_mem.metadata.tags}"
                )

        filtered_count = original_count - len(filtered_original_memory)
        remaining_count = len(filtered_original_memory)
        logger.info(
            f"Filtering complete. Removed {filtered_count} memories with tag 'mode:fast'. Remaining memories: {remaining_count}"
        )
        return filtered_original_memory

    def _apply_rerank_and_filter(
        self,
        query_history: list[str],
        original_memory: list[TextualMemoryItem],
        new_memory: list[TextualMemoryItem],
    ) -> tuple[list[TextualMemoryItem], bool]:
        """Apply reranking and filtering to memories."""
        memories_with_new_order, rerank_success_flag = (
            self.post_processor.process_and_rerank_memories(
                queries=query_history,
                original_memory=original_memory,
                new_memory=new_memory,
                top_k=self.top_k,
            )
        )

        logger.info(f"Filtering memories based on query history: {len(query_history)} queries")
        filtered_memories, filter_success_flag = self.post_processor.filter_unrelated_memories(
            query_history=query_history,
            memories=memories_with_new_order,
        )

        if filter_success_flag:
            logger.info(
                f"Memory filtering completed successfully. "
                f"Filtered from {len(memories_with_new_order)} to {len(filtered_memories)} memories"
            )
            memories_with_new_order = filtered_memories
        else:
            logger.warning(
                "Memory filtering failed - keeping all memories as fallback. "
                f"Original count: {len(memories_with_new_order)}"
            )

        return memories_with_new_order, rerank_success_flag

    def _update_working_memory_monitors(
        self,
        memories_with_new_order: list[TextualMemoryItem],
        rerank_success_flag: bool,
        query_db_manager,
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
    ) -> list[TextualMemoryItem]:
        """Update working memory monitors and return new working memories."""
        query_keywords = query_db_manager.obj.get_keywords_collections()
        logger.info(
            f"Processing {len(memories_with_new_order)} memories with {len(query_keywords)} query keywords"
        )

        new_working_memory_monitors = self.transform_working_memories_to_monitors(
            query_keywords=query_keywords,
            memories=memories_with_new_order,
        )

        if not rerank_success_flag:
            for one in new_working_memory_monitors:
                one.sorting_score = 0

        logger.info(f"update {len(new_working_memory_monitors)} working_memory_monitors")
        self.monitor.update_working_memory_monitors(
            new_working_memory_monitors=new_working_memory_monitors,
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
        )

        mem_monitors: list[MemoryMonitorItem] = self.monitor.working_memory_monitors[user_id][
            mem_cube_id
        ].obj.get_sorted_mem_monitors(reverse=True)
        return [mem_monitor.tree_memory_item for mem_monitor in mem_monitors]

    def replace_working_memory(
        self,
        user_id: UserID | str,
        mem_cube_id: MemCubeID | str,
        mem_cube: GeneralMemCube,
        original_memory: list[TextualMemoryItem],
        new_memory: list[TextualMemoryItem],
    ) -> None | list[TextualMemoryItem]:
        """Replace working memory with new memories after reranking."""
        text_mem_base = mem_cube.text_mem

        if isinstance(text_mem_base, TreeTextMemory):
            text_mem_base: TreeTextMemory = text_mem_base

            # process rerank memories with llm
            query_db_manager = self.monitor.query_monitors[user_id][mem_cube_id]
            query_db_manager.sync_with_orm()
            query_history = query_db_manager.obj.get_queries_with_timesort()

            # 1. Filter fast memories
            filtered_original_memory = self._filter_fast_memories(original_memory)

            # 2. Rerank and Filter
            memories_with_new_order, rerank_success_flag = self._apply_rerank_and_filter(
                query_history, filtered_original_memory, new_memory
            )

            # 3. Update Monitors
            new_working_memories = self._update_working_memory_monitors(
                memories_with_new_order,
                rerank_success_flag,
                query_db_manager,
                user_id,
                mem_cube_id,
                mem_cube,
            )

            text_mem_base.replace_working_memory(memories=new_working_memories)

            logger.info(
                f"The working memory has been replaced with {len(memories_with_new_order)} new memories."
            )
            self.log_working_memory_replacement(
                original_memory=original_memory,
                new_memory=new_working_memories,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=mem_cube,
                log_func_callback=self._submit_web_logs,
            )
            return memories_with_new_order

        elif isinstance(text_mem_base, NaiveTextMemory):
            # For NaiveTextMemory, we populate the monitors with the new candidates so activation memory can pick them up
            logger.info(
                f"NaiveTextMemory: Updating working memory monitors with {len(new_memory)} candidates."
            )

            # Use query keywords if available, otherwise just basic monitoring
            query_db_manager = self.monitor.query_monitors[user_id][mem_cube_id]
            query_db_manager.sync_with_orm()
            query_keywords = query_db_manager.obj.get_keywords_collections()

            new_working_memory_monitors = self.transform_working_memories_to_monitors(
                query_keywords=query_keywords,
                memories=new_memory,
            )

            self.monitor.update_working_memory_monitors(
                new_working_memory_monitors=new_working_memory_monitors,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=mem_cube,
            )
            return new_memory
        else:
            logger.error("memory_base is not supported")
            return new_memory

    def update_activation_memory(
        self,
        new_memories: list[str | TextualMemoryItem],
        label: str,
        user_id: UserID | str,
        mem_cube_id: MemCubeID | str,
        mem_cube: GeneralMemCube,
    ) -> None:
        """
        Update activation memory by extracting KVCacheItems from new_memory (list of str),
        add them to a KVCacheMemory instance, and dump to disk.
        """
        if self.activation_memory_manager:
            self.activation_memory_manager.update_activation_memory(
                new_memories=new_memories,
                label=label,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=mem_cube,
            )
        else:
            logger.warning("Activation memory manager not initialized")

    def update_activation_memory_periodically(
        self,
        interval_seconds: int,
        label: str,
        user_id: UserID | str,
        mem_cube_id: MemCubeID | str,
        mem_cube: GeneralMemCube,
    ):
        if self.activation_memory_manager:
            self.activation_memory_manager.update_activation_memory_periodically(
                interval_seconds=interval_seconds,
                label=label,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=mem_cube,
            )
        else:
            logger.warning("Activation memory manager not initialized")
