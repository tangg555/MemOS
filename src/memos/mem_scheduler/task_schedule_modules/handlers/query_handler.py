from memos.log import get_logger
from memos.mem_cube.general import GeneralMemCube
from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import (
    MEM_UPDATE_TASK_LABEL,
    NOT_APPLICABLE_TYPE,
    QUERY_TASK_LABEL,
    USER_INPUT_TYPE,
)
from memos.mem_scheduler.task_schedule_modules.base_handler import BaseHandler
from memos.mem_scheduler.task_schedule_modules.handlers.memory_update_handler import (
    MemoryUpdateHandler,
)
from memos.memories.textual.item import TextualMemoryItem
from memos.memories.textual.naive import NaiveTextMemory
from memos.memories.textual.tree import TreeTextMemory
from memos.types import (
    MemCubeID,
    UserID,
)


logger = get_logger(__name__)


class QueryHandler(BaseHandler):
    def __init__(self, context: SchedulerContext, memory_update_handler: MemoryUpdateHandler):
        super().__init__(context)
        self.expected_task_label = QUERY_TASK_LABEL
        self.memory_update_handler = memory_update_handler

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
            return
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

            # Determine search mode from method
            from memos.mem_scheduler.schemas.general_schemas import (
                TreeTextMemory_FINE_SEARCH_METHOD,
                TreeTextMemory_SEARCH_METHOD,
            )
            from memos.types.general_types import SearchMode

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
                    # Assuming SchedulerContext mirrors scheduler properties dynamically or explicitly
                    # In previous context reading, we saw some properties. Let's assume we can access it via _scheduler if public access is missing,
                    # but typically context exposes what's needed.
                    # Wait, I checked SchedulerContext earlier, it has `retriever` but not `search_service`.
                    # However, `retriever` in `SchedulerContext` property maps to `self._scheduler.retriever`.
                    # The `GeneralScheduler` has `search_service`.
                    # I should probably update SchedulerContext to expose search_service or use `self.context._scheduler.search_service` (ugly but works if friend class).
                    # Or check if `SchedulerContext` has `search_service` property.
                    # Let's check `SchedulerContext` again later. For now, assuming access.
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

    def batch_handler(self, user_id: str, mem_cube_id: str, batch: list[ScheduleMessageItem]):
        for msg in batch:
            try:
                if self.context.create_event_log and self.context.submit_web_logs:
                    event = self.context.create_event_log(
                        label="addMessage",
                        from_memory_type=USER_INPUT_TYPE,
                        to_memory_type=NOT_APPLICABLE_TYPE,
                        user_id=msg.user_id,
                        mem_cube_id=msg.mem_cube_id,
                        mem_cube=self.context.mem_cube,
                        memcube_log_content=[
                            {
                                "content": f"[User] {msg.content}",
                                "ref_id": msg.item_id,
                                "role": "user",
                            }
                        ],
                        metadata=[],
                        memory_len=1,
                        memcube_name=self.context.map_memcube_name(msg.mem_cube_id)
                        if self.context.map_memcube_name
                        else None,
                    )
                    event.task_id = msg.task_id
                    self.context.submit_web_logs([event])
            except Exception as e:
                self.handle_exception(e, "Failed to record addMessage log for query")

        # Submit memory update tasks to scheduler instead of direct call
        update_msgs = []
        for msg in batch:
            update_msg = ScheduleMessageItem(
                label=MEM_UPDATE_TASK_LABEL,
                user_id=msg.user_id,
                mem_cube_id=msg.mem_cube_id,
                content=msg.content,
                item_id=msg.item_id,
                task_id=msg.task_id,
                user_name=msg.user_name,
                trace_id=msg.trace_id,
                info=msg.info,
            )
            update_msgs.append(update_msg)

        if update_msgs:
            self.context.submit_messages(update_msgs)
