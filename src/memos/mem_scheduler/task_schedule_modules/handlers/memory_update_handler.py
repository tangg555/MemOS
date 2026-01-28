from memos.log import get_logger
from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.monitor_schemas import QueryMonitorItem
from memos.mem_scheduler.schemas.task_schemas import (
    DEFAULT_MAX_QUERY_KEY_WORDS,
    QUERY_TASK_LABEL,
)
from memos.mem_scheduler.task_schedule_modules.base_handler import BaseHandler
from memos.mem_scheduler.utils.filter_utils import is_all_chinese, is_all_english


logger = get_logger(__name__)


class MemoryUpdateHandler(BaseHandler):
    def __init__(self, context: SchedulerContext):
        super().__init__(context)
        self.expected_task_label = "mem_update"

    def batch_handler(self, user_id: str, mem_cube_id: str, batch: list[ScheduleMessageItem]):
        # Process the whole batch once; no need to iterate per message
        self.long_memory_update_process(user_id=user_id, mem_cube_id=mem_cube_id, messages=batch)

    def long_memory_update_process(
        self, user_id: str, mem_cube_id: str, messages: list[ScheduleMessageItem]
    ):
        mem_cube = self.context.mem_cube

        # update query monitors
        for msg in messages:
            self.context.monitor.register_query_monitor_if_not_exists(
                user_id=user_id, mem_cube_id=mem_cube_id
            )

            query = msg.content
            query_keywords = self.context.monitor.extract_query_keywords(query=query)
            logger.info(
                f'Extracted keywords "{query_keywords}" from query "{query}" for user_id={user_id}'
            )

            if len(query_keywords) == 0:
                stripped_query = query.strip()
                # Determine measurement method based on language
                if is_all_english(stripped_query):
                    words = stripped_query.split()  # Word count for English
                elif is_all_chinese(stripped_query):
                    words = stripped_query  # Character count for Chinese
                else:
                    logger.debug(
                        f"Mixed-language memory, using character count: {stripped_query[:50]}..."
                    )
                    words = stripped_query  # Default to character count

                query_keywords = list(set(words[: self.context.query_key_words_limit]))
                logger.error(
                    f"Keyword extraction failed for query '{query}' (user_id={user_id}). Using fallback keywords: {query_keywords[:10]}... (truncated)",
                    exc_info=True,
                )

            item = QueryMonitorItem(
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                query_text=query,
                keywords=query_keywords,
                max_keywords=DEFAULT_MAX_QUERY_KEY_WORDS,
            )

            query_db_manager = self.context.monitor.query_monitors[user_id][mem_cube_id]
            query_db_manager.obj.put(item=item)
        # Sync with database after adding new item
        query_db_manager.sync_with_orm()
        logger.debug(
            f"Queries in monitor for user_id={user_id}, mem_cube_id={mem_cube_id}: {query_db_manager.obj.get_queries_with_timesort()}"
        )

        queries = [msg.content for msg in messages]

        # recall
        cur_working_memory, new_candidates = self.context.process_session_turn(
            queries=queries,
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
            top_k=self.context.top_k,
        )
        logger.info(
            # Build the candidate preview string outside the f-string to avoid backslashes in expression
            f"[long_memory_update_process] Processed {len(queries)} queries {queries} and retrieved {len(new_candidates)} "
            f"new candidate memories for user_id={user_id}: "
            + ("\n- " + "\n- ".join([f"{one.id}: {one.memory}" for one in new_candidates]))
        )

        # rerank
        new_order_working_memory = self.context.replace_working_memory(
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
            original_memory=cur_working_memory,
            new_memory=new_candidates,
        )
        logger.debug(
            f"[long_memory_update_process] Final working memory size: {len(new_order_working_memory)} memories for user_id={user_id}"
        )

        old_memory_texts = "\n- " + "\n- ".join(
            [f"{one.id}: {one.memory}" for one in cur_working_memory]
        )
        new_memory_texts = "\n- " + "\n- ".join(
            [f"{one.id}: {one.memory}" for one in new_order_working_memory]
        )

        logger.info(
            f"[long_memory_update_process] For user_id='{user_id}', mem_cube_id='{mem_cube_id}': "
            f"Scheduler replaced working memory based on query history {queries}. "
            f"Old working memory ({len(cur_working_memory)} items): {old_memory_texts}. "
            f"New working memory ({len(new_order_working_memory)} items): {new_memory_texts}."
        )

        # update activation memories
        logger.debug(
            f"Activation memory update {'enabled' if self.context.enable_activation_memory else 'disabled'} "
            f"(interval: {self.context.monitor.act_mem_update_interval}s)"
        )
        if self.context.enable_activation_memory:
            self.context.update_activation_memory_periodically(
                interval_seconds=self.context.monitor.act_mem_update_interval,
                label=QUERY_TASK_LABEL,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=self.context.mem_cube,
            )
