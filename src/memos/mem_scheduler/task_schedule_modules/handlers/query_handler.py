from memos.log import get_logger
from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import (
    NOT_APPLICABLE_TYPE,
    QUERY_TASK_LABEL,
    USER_INPUT_TYPE,
)
from memos.mem_scheduler.task_schedule_modules.base_handler import BaseHandler
from memos.mem_scheduler.task_schedule_modules.handlers.memory_update_handler import (
    MemoryUpdateHandler,
)


logger = get_logger(__name__)


class QueryHandler(BaseHandler):
    def __init__(self, context: SchedulerContext, memory_update_handler: MemoryUpdateHandler):
        super().__init__(context)
        self.expected_task_label = QUERY_TASK_LABEL
        self.memory_update_handler = memory_update_handler

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

        # Directly call the MemoryUpdateHandler
        self.memory_update_handler.batch_handler(user_id, mem_cube_id, batch)
