from memos.log import get_logger
from memos.mem_cube.general import GeneralMemCube
from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.general_schemas import (
    TreeTextMemory_FINE_SEARCH_METHOD,
    TreeTextMemory_SEARCH_METHOD,
)
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.monitor_schemas import QueryMonitorItem
from memos.mem_scheduler.schemas.task_schemas import (
    DEFAULT_MAX_QUERY_KEY_WORDS,
    QUERY_TASK_LABEL,
)
from memos.mem_scheduler.task_schedule_modules.base_handler import BaseHandler
from memos.mem_scheduler.utils.filter_utils import is_all_chinese, is_all_english
from memos.memories.textual.item import TextualMemoryItem
from memos.memories.textual.naive import NaiveTextMemory
from memos.memories.textual.tree import TreeTextMemory
from memos.types import (
    MemCubeID,
    UserID,
)
from memos.types.general_types import SearchMode


logger = get_logger(__name__)


class MemoryUpdateHandler(BaseHandler):
    def __init__(self, context: SchedulerContext):
        super().__init__(context)
        self.expected_task_label = "mem_update"

    def batch_handler(self, user_id: str, mem_cube_id: str, batch: list[ScheduleMessageItem]):
        # Process the whole batch once; no need to iterate per message
        self.long_memory_update_process(user_id=user_id, mem_cube_id=mem_cube_id, messages=batch)

    def process_session_turn(
        self,
        queries: str | list[str],
        user_id: UserID | str,
        mem_cube_id: MemCubeID | str,
        mem_cube: GeneralMemCube,
        top_k: int = 10,
    ) -> tuple[list[TextualMemoryItem], list[TextualMemoryItem]] | None:
        """
        Process a dialog turn:
        - If q_list reaches window size, trigger retrieval;
        - Immediately switch to the new memory if retrieval is triggered.
        """

        text_mem_base = mem_cube.text_mem
        if not isinstance(text_mem_base, TreeTextMemory):
            if isinstance(text_mem_base, NaiveTextMemory):
                logger.debug(
                    f"NaiveTextMemory used for mem_cube_id={mem_cube_id}, processing session turn with simple search."
                )
                # Treat NaiveTextMemory similar to TreeTextMemory but with simpler logic
                # We will perform retrieval to get "working memory" candidates for activation memory
                # But we won't have a distinct "current working memory"
                cur_working_memory = []
            else:
                logger.warning(
                    f"Not implemented! Expected TreeTextMemory but got {type(text_mem_base).__name__} "
                    f"for mem_cube_id={mem_cube_id}, user_id={user_id}. "
                    f"text_mem_base value: {text_mem_base}"
                )
                return [], []
        else:
            cur_working_memory: list[TextualMemoryItem] = text_mem_base.get_working_memory(
                user_name=mem_cube_id
            )
            cur_working_memory = cur_working_memory[:top_k]

        logger.info(
            f"[process_session_turn] Processing {len(queries)} queries for user_id={user_id}, mem_cube_id={mem_cube_id}"
        )

        text_working_memory: list[str] = [w_m.memory for w_m in cur_working_memory]
        intent_result = self.context.monitor.detect_intent(
            q_list=queries, text_working_memory=text_working_memory
        )

        time_trigger_flag = False
        if self.context.monitor.timed_trigger(
            last_time=self.context.monitor.last_query_consume_time,
            interval_seconds=self.context.monitor.query_trigger_interval,
        ):
            time_trigger_flag = True

        if (not intent_result["trigger_retrieval"]) and (not time_trigger_flag):
            logger.info(
                f"[process_session_turn] Query schedule not triggered for user_id={user_id}, mem_cube_id={mem_cube_id}. Intent_result: {intent_result}"
            )
            return [], []
        elif (not intent_result["trigger_retrieval"]) and time_trigger_flag:
            logger.info(
                f"[process_session_turn] Query schedule forced to trigger due to time ticker for user_id={user_id}, mem_cube_id={mem_cube_id}"
            )
            intent_result["trigger_retrieval"] = True
            intent_result["missing_evidences"] = queries
        else:
            logger.info(
                f"[process_session_turn] Query schedule triggered for user_id={user_id}, mem_cube_id={mem_cube_id}. "
                f"Missing evidences: {intent_result['missing_evidences']}"
            )

        missing_evidences = intent_result["missing_evidences"]
        num_evidence = len(missing_evidences)
        k_per_evidence = max(1, top_k // max(1, num_evidence))
        new_candidates = []
        for item in missing_evidences:
            logger.info(
                f"[process_session_turn] Searching for missing evidence: '{item}' with top_k={k_per_evidence} for user_id={user_id}"
            )

            # Convert search_method to SearchMode
            if self.context.config.get("search_method") == TreeTextMemory_FINE_SEARCH_METHOD:
                mode = SearchMode.FINE
            elif self.context.config.get("search_method") == TreeTextMemory_SEARCH_METHOD:
                mode = SearchMode.FAST
            else:
                # Fallback to FAST mode for unknown methods
                logger.warning(
                    f"Unknown search_method '{self.context.config.get('search_method')}', falling back to SearchMode.FAST"
                )
                mode = SearchMode.FAST

            if isinstance(text_mem_base, NaiveTextMemory):
                # NaiveTextMemory: Use direct search as fallback
                try:
                    results = text_mem_base.search(query=item, top_k=k_per_evidence)
                except Exception as e:
                    logger.warning(f"NaiveTextMemory search failed: {e}")
                    results = []
            else:
                # Use unified search service from context (if available, otherwise fallback needed)
                # Assuming context has search_service via scheduler property if exposed, or we need to access it differently.
                # Since SchedulerContext usually proxies to scheduler properties:
                if hasattr(
                    self.context, "search_service"
                ):  # This might need to be added to SchedulerContext if not present
                    pass

                # Using private access as temporary measure if property is missing, or assume context update.
                # The user didn't ask to update SchedulerContext, but I might need to if I want this to work.
                # Actually, I'll use `self.context._scheduler.search_service` to be safe given current instructions.
                results: list[TextualMemoryItem] = self.context._scheduler.search_service.search(
                    query=item,
                    user_id=user_id,
                    mem_cube=mem_cube,
                    top_k=k_per_evidence,
                    mode=mode,
                    mem_cube_id=mem_cube_id,
                )

            logger.info(
                f"[process_session_turn] Search results for missing evidence '{item}': "
                + ("\n- " + "\n- ".join([f"{one.id}: {one.memory}" for one in results]))
            )
            new_candidates.extend(results)
        return cur_working_memory, new_candidates

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
        cur_working_memory, new_candidates = self.process_session_turn(
            queries=queries,
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
            top_k=self.context.config.get("top_k", 10),
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
            f"[long_memory_update_process] Final working memory size: {len(new_order_working_memory) if new_order_working_memory else 0} memories for user_id={user_id}"
        )

        old_memory_texts = "\n- " + "\n- ".join(
            [f"{one.id}: {one.memory}" for one in cur_working_memory]
        )
        new_memory_texts = (
            "\n- " + "\n- ".join([f"{one.id}: {one.memory}" for one in new_order_working_memory])
            if new_order_working_memory
            else ""
        )

        logger.info(
            f"[long_memory_update_process] For user_id='{user_id}', mem_cube_id='{mem_cube_id}': "
            f"Scheduler replaced working memory based on query history {queries}. "
            f"Old working memory ({len(cur_working_memory)} items): {old_memory_texts}. "
            f"New working memory ({len(new_order_working_memory) if new_order_working_memory else 0} items): {new_memory_texts}."
        )

        # update activation memories
        logger.debug(
            f"Activation memory update {'enabled' if self.context.config.get('enable_activation_memory', False) else 'disabled'} "
            f"(interval: {self.context.monitor.act_mem_update_interval}s)"
        )
        if self.context.config.get("enable_activation_memory", False):
            self.context.update_activation_memory_periodically(
                interval_seconds=self.context.monitor.act_mem_update_interval,
                label=QUERY_TASK_LABEL,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=self.context.mem_cube,
            )
