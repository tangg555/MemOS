import hashlib
import json

from collections.abc import Callable

from memos.log import get_logger
from memos.mem_cube.general import GeneralMemCube
from memos.mem_scheduler.general_modules.base import BaseSchedulerModule
from memos.mem_scheduler.schemas.general_schemas import (
    ACTIVATION_MEMORY_TYPE,
    LONG_TERM_MEMORY_TYPE,
    NOT_INITIALIZED,
    PARAMETER_MEMORY_TYPE,
    TEXT_MEMORY_TYPE,
    WORKING_MEMORY_TYPE,
)
from memos.mem_scheduler.schemas.message_schemas import (
    ScheduleLogForWebItem,
    ScheduleMessageItem,
)
from memos.mem_scheduler.schemas.task_schemas import (
    ADD_TASK_LABEL,
    MEM_ARCHIVE_TASK_LABEL,
    MEM_UPDATE_TASK_LABEL,
    USER_INPUT_TYPE,
)
from memos.mem_scheduler.utils.filter_utils import (
    transform_name_to_key,
)
from memos.mem_scheduler.utils.misc_utils import log_exceptions
from memos.memories.textual.tree import TextualMemoryItem, TreeTextMemory


logger = get_logger(__name__)


class SchedulerLoggerModule(BaseSchedulerModule):
    def __init__(self):
        """
        Initialize RabbitMQ connection settings.
        """
        super().__init__()

    @log_exceptions(logger=logger)
    def create_autofilled_log_item(
        self,
        log_content: str,
        label: str,
        from_memory_type: str,
        to_memory_type: str,
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
    ) -> ScheduleLogForWebItem:
        if mem_cube is None:
            logger.error(
                "mem_cube is None — this should not happen in production!", stack_info=True
            )
        text_mem_base: TreeTextMemory = mem_cube.text_mem

        current_memory_sizes = {}
        if hasattr(text_mem_base, "get_current_memory_size"):
            current_memory_sizes = text_mem_base.get_current_memory_size(user_name=mem_cube_id)

        current_memory_sizes = {
            "long_term_memory_size": current_memory_sizes.get("LongTermMemory", 0),
            "user_memory_size": current_memory_sizes.get("UserMemory", 0),
            "working_memory_size": current_memory_sizes.get("WorkingMemory", 0),
            "transformed_act_memory_size": NOT_INITIALIZED,
            "parameter_memory_size": NOT_INITIALIZED,
        }

        memory_capacities = {
            "long_term_memory_capacity": 0,
            "user_memory_capacity": 0,
            "working_memory_capacity": 0,
            "transformed_act_memory_capacity": NOT_INITIALIZED,
            "parameter_memory_capacity": NOT_INITIALIZED,
        }

        if hasattr(text_mem_base, "memory_manager") and hasattr(
            text_mem_base.memory_manager, "memory_size"
        ):
            memory_capacities.update(
                {
                    "long_term_memory_capacity": text_mem_base.memory_manager.memory_size.get(
                        "LongTermMemory", 0
                    ),
                    "user_memory_capacity": text_mem_base.memory_manager.memory_size.get(
                        "UserMemory", 0
                    ),
                    "working_memory_capacity": text_mem_base.memory_manager.memory_size.get(
                        "WorkingMemory", 0
                    ),
                }
            )

        if hasattr(self, "monitor"):
            if (
                user_id in self.monitor.activation_memory_monitors
                and mem_cube_id in self.monitor.activation_memory_monitors[user_id]
            ):
                activation_monitor = self.monitor.activation_memory_monitors[user_id][mem_cube_id]
                transformed_act_memory_size = len(activation_monitor.obj.memories)
                logger.info(
                    f'activation_memory_monitors currently has "{transformed_act_memory_size}" transformed memory size'
                )
            else:
                transformed_act_memory_size = 0
                logger.info(
                    f'activation_memory_monitors is not initialized for user "{user_id}" and mem_cube "{mem_cube_id}'
                )
            current_memory_sizes["transformed_act_memory_size"] = transformed_act_memory_size
            current_memory_sizes["parameter_memory_size"] = 1

            memory_capacities["transformed_act_memory_capacity"] = (
                self.monitor.activation_mem_monitor_capacity
            )
            memory_capacities["parameter_memory_capacity"] = 1

        log_message = ScheduleLogForWebItem(
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            label=label,
            from_memory_type=from_memory_type,
            to_memory_type=to_memory_type,
            log_content=log_content,
            current_memory_sizes=current_memory_sizes,
            memory_capacities=memory_capacities,
        )
        return log_message

    @log_exceptions(logger=logger)
    def create_event_log(
        self,
        label: str,
        from_memory_type: str,
        to_memory_type: str,
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
        memcube_log_content: list[dict],
        metadata: list[dict],
        memory_len: int,
        memcube_name: str | None = None,
        log_content: str | None = None,
    ) -> ScheduleLogForWebItem:
        item = self.create_autofilled_log_item(
            log_content=log_content or "",
            label=label,
            from_memory_type=from_memory_type,
            to_memory_type=to_memory_type,
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
        )
        item.memcube_log_content = memcube_log_content
        item.metadata = metadata
        item.memory_len = memory_len
        item.memcube_name = memcube_name or self._map_memcube_name(mem_cube_id)
        return item

    def _map_memcube_name(self, mem_cube_id: str) -> str:
        x = mem_cube_id or ""
        if "public" in x.lower():
            return "PublicMemCube"
        return "UserMemCube"

    # TODO: Log output count is incorrect
    @log_exceptions(logger=logger)
    def log_working_memory_replacement(
        self,
        original_memory: list[TextualMemoryItem],
        new_memory: list[TextualMemoryItem],
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
        log_func_callback: Callable[[list[ScheduleLogForWebItem]], None],
    ):
        """Log changes when working memory is replaced."""
        original_text_memories = [m.memory for m in original_memory]
        new_text_memories = [m.memory for m in new_memory]
        original_set = set(original_text_memories)
        new_set = set(new_text_memories)
        added_texts = []
        for new_mem in new_set:
            if new_mem not in original_set:
                added_texts.append(new_mem)
        memcube_content = []
        meta = []
        by_text = {m.memory: m for m in new_memory}
        for t in added_texts:
            itm = by_text.get(t)
            if not itm:
                continue
            key_name = getattr(itm.metadata, "key", None) or itm.memory
            k = transform_name_to_key(name=key_name)
            memcube_content.append(
                {
                    "content": f"[{itm.metadata.memory_type}→{WORKING_MEMORY_TYPE}] {k}: {itm.memory}",
                    "ref_id": itm.id,
                }
            )
            meta.append(
                {
                    "ref_id": itm.id,
                    "id": itm.id,
                    "key": itm.metadata.key,
                    "memory": itm.memory,
                    "memory_type": itm.metadata.memory_type,
                    "status": itm.metadata.status,
                    "confidence": itm.metadata.confidence,
                    "tags": itm.metadata.tags,
                    "updated_at": getattr(itm.metadata, "updated_at", None)
                    or getattr(itm.metadata, "update_at", None),
                }
            )
        # Only create log if there are actual memory changes
        if memcube_content:
            ev = self.create_event_log(
                label="scheduleMemory",
                from_memory_type=TEXT_MEMORY_TYPE,
                to_memory_type=WORKING_MEMORY_TYPE,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=mem_cube,
                memcube_log_content=memcube_content,
                metadata=meta,
                memory_len=len(memcube_content),
                memcube_name=self._map_memcube_name(mem_cube_id),
            )
            log_func_callback([ev])

    @log_exceptions(logger=logger)
    def log_activation_memory_update(
        self,
        original_text_memories: list[str],
        new_text_memories: list[str],
        label: str,
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
        log_func_callback: Callable[[list[ScheduleLogForWebItem]], None],
    ):
        """Log changes when activation memory is updated."""
        original_set = set(original_text_memories)
        new_set = set(new_text_memories)

        added_memories = list(new_set - original_set)
        memcube_content = []
        meta = []
        for mem in added_memories:
            key = transform_name_to_key(mem)
            ref_id = f"actparam-{hashlib.md5(mem.encode()).hexdigest()}"
            memcube_content.append(
                {
                    "content": f"[{ACTIVATION_MEMORY_TYPE}→{PARAMETER_MEMORY_TYPE}] {key}: {mem}",
                    "ref_id": ref_id,
                }
            )
            meta.append(
                {
                    "ref_id": ref_id,
                    "id": ref_id,
                    "key": key,
                    "memory": mem,
                    "memory_type": ACTIVATION_MEMORY_TYPE,
                    "status": None,
                    "confidence": None,
                    "tags": None,
                    "updated_at": None,
                }
            )
        # Only create log if there are actual memory changes
        if memcube_content:
            ev = self.create_event_log(
                label="scheduleMemory",
                from_memory_type=ACTIVATION_MEMORY_TYPE,
                to_memory_type=PARAMETER_MEMORY_TYPE,
                user_id=user_id,
                mem_cube_id=mem_cube_id,
                mem_cube=mem_cube,
                memcube_log_content=memcube_content,
                metadata=meta,
                memory_len=len(added_memories),
                memcube_name=self._map_memcube_name(mem_cube_id),
            )
            log_func_callback([ev])

    @log_exceptions(logger=logger)
    def log_adding_memory(
        self,
        memory: str,
        memory_type: str,
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
        log_func_callback: Callable[[list[ScheduleLogForWebItem]], None],
    ):
        """Deprecated: legacy text log. Use create_event_log with structured fields instead."""
        log_message = self.create_autofilled_log_item(
            log_content=memory,
            label=ADD_TASK_LABEL,
            from_memory_type=USER_INPUT_TYPE,
            to_memory_type=memory_type,
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
        )
        log_func_callback([log_message])
        logger.info(
            f"{USER_INPUT_TYPE} memory for user {user_id} "
            f"converted to {memory_type} memory in mem_cube {mem_cube_id}: {memory}"
        )

    @log_exceptions(logger=logger)
    def log_updating_memory(
        self,
        memory: str,
        memory_type: str,
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
        log_func_callback: Callable[[list[ScheduleLogForWebItem]], None],
    ):
        """Deprecated: legacy text log. Use create_event_log with structured fields instead."""
        log_message = self.create_autofilled_log_item(
            log_content=memory,
            label=MEM_UPDATE_TASK_LABEL,
            from_memory_type=memory_type,
            to_memory_type=memory_type,
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
        )
        log_func_callback([log_message])

    @log_exceptions(logger=logger)
    def log_archiving_memory(
        self,
        memory: str,
        memory_type: str,
        user_id: str,
        mem_cube_id: str,
        mem_cube: GeneralMemCube,
        log_func_callback: Callable[[list[ScheduleLogForWebItem]], None],
    ):
        """Deprecated: legacy text log. Use create_event_log with structured fields instead."""
        log_message = self.create_autofilled_log_item(
            log_content=memory,
            label=MEM_ARCHIVE_TASK_LABEL,
            from_memory_type=memory_type,
            to_memory_type=memory_type,
            user_id=user_id,
            mem_cube_id=mem_cube_id,
            mem_cube=mem_cube,
        )
        log_func_callback([log_message])

    @log_exceptions(logger=logger)
    def validate_schedule_message(self, message: ScheduleMessageItem, label: str):
        """Validate if the message matches the expected label.

        Args:
            message: Incoming message item to validate.
            label: Expected message label (e.g., QUERY_LABEL/ANSWER_LABEL).

        Returns:
            bool: True if validation passed, False otherwise.
        """
        if message.label != label:
            logger.error(f"Handler validation failed: expected={label}, actual={message.label}")
            return False
        return True

    @log_exceptions(logger=logger)
    def validate_schedule_messages(self, messages: list[ScheduleMessageItem], label: str):
        """Validate if all messages match the expected label.

        Args:
            messages: List of message items to validate.
            label: Expected message label (e.g., QUERY_LABEL/ANSWER_LABEL).

        Returns:
            bool: True if all messages passed validation, False if any failed.
        """
        for message in messages:
            if not self.validate_schedule_message(message, label):
                logger.error("Message batch contains invalid labels, aborting processing")
                return False
        return True

    @log_exceptions(logger=logger)
    def log_add_messages(self, msg: ScheduleMessageItem):
        try:
            userinput_memory_ids = json.loads(msg.content)
        except Exception as e:
            logger.error(f"Error: {e}. Content: {msg.content}", exc_info=True)
            userinput_memory_ids = []

        # Prepare data for both logging paths, fetching original content for updates
        prepared_add_items = []
        prepared_update_items_with_original = []
        missing_ids: list[str] = []

        for memory_id in userinput_memory_ids:
            try:
                # This mem_item represents the NEW content that was just added/processed
                mem_item: TextualMemoryItem | None = None
                mem_item = self.mem_cube.text_mem.get(
                    memory_id=memory_id, user_name=msg.mem_cube_id
                )
                if mem_item is None:
                    raise ValueError(f"Memory {memory_id} not found after retries")
                # Check if a memory with the same key already exists (determining if it's an update)
                key = getattr(mem_item.metadata, "key", None) or transform_name_to_key(
                    name=mem_item.memory
                )
                exists = False
                original_content = None
                original_item_id = None

                # Only check graph_store if a key exists and the text_mem has a graph_store
                if key and hasattr(self.mem_cube.text_mem, "graph_store"):
                    candidates = self.mem_cube.text_mem.graph_store.get_by_metadata(
                        [
                            {"field": "key", "op": "=", "value": key},
                            {
                                "field": "memory_type",
                                "op": "=",
                                "value": mem_item.metadata.memory_type,
                            },
                        ]
                    )
                    if candidates:
                        exists = True
                        original_item_id = candidates[0]
                        # Crucial step: Fetch the original content for updates
                        # This `get` is for the *existing* memory that will be updated
                        original_mem_item = self.mem_cube.text_mem.get(
                            memory_id=original_item_id, user_name=msg.mem_cube_id
                        )
                        original_content = original_mem_item.memory

                if exists:
                    prepared_update_items_with_original.append(
                        {
                            "new_item": mem_item,
                            "original_content": original_content,
                            "original_item_id": original_item_id,
                        }
                    )
                else:
                    prepared_add_items.append(mem_item)

            except Exception:
                missing_ids.append(memory_id)
                logger.debug(
                    f"This MemoryItem {memory_id} has already been deleted or an error occurred during preparation."
                )

        if missing_ids:
            content_preview = (
                msg.content[:200] + "..."
                if isinstance(msg.content, str) and len(msg.content) > 200
                else msg.content
            )
            logger.warning(
                "Missing TextualMemoryItem(s) during add log preparation. "
                "memory_ids=%s user_id=%s mem_cube_id=%s task_id=%s item_id=%s redis_msg_id=%s label=%s stream_key=%s content_preview=%s",
                missing_ids,
                msg.user_id,
                msg.mem_cube_id,
                msg.task_id,
                msg.item_id,
                getattr(msg, "redis_message_id", ""),
                msg.label,
                getattr(msg, "stream_key", ""),
                content_preview,
            )

        if not prepared_add_items and not prepared_update_items_with_original:
            logger.warning(
                "No add/update items prepared; skipping addMemory/knowledgeBaseUpdate logs. "
                "user_id=%s mem_cube_id=%s task_id=%s item_id=%s redis_msg_id=%s label=%s stream_key=%s missing_ids=%s",
                msg.user_id,
                msg.mem_cube_id,
                msg.task_id,
                msg.item_id,
                getattr(msg, "redis_message_id", ""),
                msg.label,
                getattr(msg, "stream_key", ""),
                missing_ids,
            )
        return prepared_add_items, prepared_update_items_with_original

    @log_exceptions(logger=logger)
    def send_add_log_messages_to_local_env(
        self, msg: ScheduleMessageItem, prepared_add_items, prepared_update_items_with_original
    ):
        # Existing: Playground/Default Logging
        # Reconstruct add_content/add_meta/update_content/update_meta from prepared_items
        # This ensures existing logging path continues to work with pre-existing data structures
        add_content_legacy: list[dict] = []
        add_meta_legacy: list[dict] = []
        update_content_legacy: list[dict] = []
        update_meta_legacy: list[dict] = []

        for item in prepared_add_items:
            key = getattr(item.metadata, "key", None) or transform_name_to_key(name=item.memory)
            add_content_legacy.append({"content": f"{key}: {item.memory}", "ref_id": item.id})
            add_meta_legacy.append(
                {
                    "ref_id": item.id,
                    "id": item.id,
                    "key": item.metadata.key,
                    "memory": item.memory,
                    "memory_type": item.metadata.memory_type,
                    "status": item.metadata.status,
                    "confidence": item.metadata.confidence,
                    "tags": item.metadata.tags,
                    "updated_at": getattr(item.metadata, "updated_at", None)
                    or getattr(item.metadata, "update_at", None),
                }
            )

        for item_data in prepared_update_items_with_original:
            item = item_data["new_item"]
            key = getattr(item.metadata, "key", None) or transform_name_to_key(name=item.memory)
            update_content_legacy.append({"content": f"{key}: {item.memory}", "ref_id": item.id})
            update_meta_legacy.append(
                {
                    "ref_id": item.id,
                    "id": item.id,
                    "key": item.metadata.key,
                    "memory": item.memory,
                    "memory_type": item.metadata.memory_type,
                    "status": item.metadata.status,
                    "confidence": item.metadata.confidence,
                    "tags": item.metadata.tags,
                    "updated_at": getattr(item.metadata, "updated_at", None)
                    or getattr(item.metadata, "update_at", None),
                }
            )

        events = []
        if add_content_legacy:
            event = self.create_event_log(
                label="addMemory",
                from_memory_type=USER_INPUT_TYPE,
                to_memory_type=LONG_TERM_MEMORY_TYPE,
                user_id=msg.user_id,
                mem_cube_id=msg.mem_cube_id,
                mem_cube=self.mem_cube,
                memcube_log_content=add_content_legacy,
                metadata=add_meta_legacy,
                memory_len=len(add_content_legacy),
                memcube_name=self._map_memcube_name(msg.mem_cube_id),
            )
            event.task_id = msg.task_id
            events.append(event)
        if update_content_legacy:
            event = self.create_event_log(
                label="updateMemory",
                from_memory_type=LONG_TERM_MEMORY_TYPE,
                to_memory_type=LONG_TERM_MEMORY_TYPE,
                user_id=msg.user_id,
                mem_cube_id=msg.mem_cube_id,
                mem_cube=self.mem_cube,
                memcube_log_content=update_content_legacy,
                metadata=update_meta_legacy,
                memory_len=len(update_content_legacy),
                memcube_name=self._map_memcube_name(msg.mem_cube_id),
            )
            event.task_id = msg.task_id
            events.append(event)
        logger.info(f"send_add_log_messages_to_local_env: {len(events)}")
        if events:
            self._submit_web_logs(events, additional_log_info="send_add_log_messages_to_cloud_env")

    @log_exceptions(logger=logger)
    def send_add_log_messages_to_cloud_env(
        self, msg: ScheduleMessageItem, prepared_add_items, prepared_update_items_with_original
    ):
        """
        Cloud logging path for add/update events.
        """
        kb_log_content: list[dict] = []
        info = msg.info or {}

        # Process added items
        for item in prepared_add_items:
            metadata = getattr(item, "metadata", None)
            file_ids = getattr(metadata, "file_ids", None) if metadata else None
            source_doc_id = file_ids[0] if isinstance(file_ids, list) and file_ids else None
            kb_log_content.append(
                {
                    "log_source": "KNOWLEDGE_BASE_LOG",
                    "trigger_source": info.get("trigger_source", "Messages"),
                    "operation": "ADD",
                    "memory_id": item.id,
                    "content": item.memory,
                    "original_content": None,
                    "source_doc_id": source_doc_id,
                }
            )

        # Process updated items
        for item_data in prepared_update_items_with_original:
            item = item_data["new_item"]
            metadata = getattr(item, "metadata", None)
            file_ids = getattr(metadata, "file_ids", None) if metadata else None
            source_doc_id = file_ids[0] if isinstance(file_ids, list) and file_ids else None
            kb_log_content.append(
                {
                    "log_source": "KNOWLEDGE_BASE_LOG",
                    "trigger_source": info.get("trigger_source", "Messages"),
                    "operation": "UPDATE",
                    "memory_id": item.id,
                    "content": item.memory,
                    "original_content": item_data.get("original_content"),
                    "source_doc_id": source_doc_id,
                }
            )

        if kb_log_content:
            logger.info(
                f"[DIAGNOSTIC] general_scheduler.send_add_log_messages_to_cloud_env: Creating event log for KB update. Label: knowledgeBaseUpdate, user_id: {msg.user_id}, mem_cube_id: {msg.mem_cube_id}, task_id: {msg.task_id}. KB content: {json.dumps(kb_log_content, indent=2)}"
            )
            event = self.create_event_log(
                label="knowledgeBaseUpdate",
                from_memory_type=USER_INPUT_TYPE,
                to_memory_type=LONG_TERM_MEMORY_TYPE,
                user_id=msg.user_id,
                mem_cube_id=msg.mem_cube_id,
                mem_cube=self.mem_cube,
                memcube_log_content=kb_log_content,
                metadata=None,
                memory_len=len(kb_log_content),
                memcube_name=self._map_memcube_name(msg.mem_cube_id),
            )
            event.log_content = f"Knowledge Base Memory Update: {len(kb_log_content)} changes."
            event.task_id = msg.task_id
            self._submit_web_logs([event])
