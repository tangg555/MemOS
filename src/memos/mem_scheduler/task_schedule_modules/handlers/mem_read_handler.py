import contextlib
import json
import traceback

from memos.log import get_logger
from memos.mem_scheduler.general_modules.scheduler_context import SchedulerContext
from memos.mem_scheduler.schemas.message_schemas import ScheduleMessageItem
from memos.mem_scheduler.schemas.task_schemas import (
    LONG_TERM_MEMORY_TYPE,
    MEM_READ_TASK_LABEL,
    USER_INPUT_TYPE,
)
from memos.mem_scheduler.task_schedule_modules.base_handler import BaseHandler
from memos.mem_scheduler.utils.filter_utils import (
    transform_name_to_key,
)
from memos.mem_scheduler.utils.misc_utils import (
    is_cloud_env,
)
from memos.memories.textual.tree import TreeTextMemory
from memos.memories.textual.tree_text_memory.organize.manager import (
    extract_working_binding_ids,
)


logger = get_logger(__name__)


class MemReadHandler(BaseHandler):
    def __init__(self, context: SchedulerContext):
        super().__init__(context)
        self.expected_task_label = MEM_READ_TASK_LABEL

    def process_memories_with_reader(
        self,
        mem_ids: list[str],
        user_id: str,
        mem_cube_id: str,
        text_mem: TreeTextMemory,
        user_name: str,
        custom_tags: list[str] | None = None,
        task_id: str | None = None,
        info: dict | None = None,
    ) -> None:
        logger.info(
            f"[DIAGNOSTIC] MemReadHandler.process_memories_with_reader called. mem_ids: {mem_ids}, user_id: {user_id}, mem_cube_id: {mem_cube_id}, task_id: {task_id}"
        )
        """
        Process memories using mem_reader for enhanced memory processing.

        Args:
            mem_ids: List of memory IDs to process
            user_id: User ID
            mem_cube_id: Memory cube ID
            text_mem: Text memory instance
            custom_tags: Optional list of custom tags for memory processing
        """
        kb_log_content: list[dict] = []
        try:
            # Get the mem_reader from the parent MOSCore
            if not self.context.mem_reader:
                logger.warning(
                    "mem_reader not available in scheduler context, skipping enhanced processing"
                )
                return

            # Get the original memory items
            memory_items = []
            for mem_id in mem_ids:
                try:
                    memory_item = text_mem.get(mem_id, user_name=user_name)
                    memory_items.append(memory_item)
                except Exception as e:
                    logger.warning(
                        f"[process_memories_with_reader] Failed to get memory {mem_id}: {e}"
                    )
                    continue

            if not memory_items:
                logger.warning("No valid memory items found for processing")
                return

            # parse working_binding ids from the *original* memory_items (the raw items created in /add)
            # these still carry metadata.background with "[working_binding:...]" so we can know
            # which WorkingMemory clones should be cleaned up later.

            bindings_to_delete = extract_working_binding_ids(memory_items)
            logger.info(
                f"Extracted {len(bindings_to_delete)} working_binding ids to cleanup: {list(bindings_to_delete)}"
            )

            # Use mem_reader to process the memories
            logger.info(f"Processing {len(memory_items)} memories with mem_reader")

            # Extract memories using mem_reader
            try:
                processed_memories = self.context.mem_reader.fine_transfer_simple_mem(
                    memory_items,
                    type="chat",
                    custom_tags=custom_tags,
                    user_name=user_name,
                )
            except Exception as e:
                logger.warning(f"{e}: Fail to transfer mem: {memory_items}")
                processed_memories = []

            if processed_memories and len(processed_memories) > 0:
                # Flatten the results (mem_reader returns list of lists)
                flattened_memories = []
                for memory_list in processed_memories:
                    flattened_memories.extend(memory_list)

                logger.info(f"mem_reader processed {len(flattened_memories)} enhanced memories")

                # Add the enhanced memories back to the memory system
                if flattened_memories:
                    enhanced_mem_ids = text_mem.add(flattened_memories, user_name=user_name)
                    logger.info(
                        f"Added {len(enhanced_mem_ids)} enhanced memories: {enhanced_mem_ids}"
                    )

                    # Mark merged_from memories as archived when provided in memory metadata
                    if self.context.mem_reader.graph_db:
                        for memory in flattened_memories:
                            merged_from = (memory.metadata.info or {}).get("merged_from")
                            if merged_from:
                                old_ids = (
                                    merged_from
                                    if isinstance(merged_from, (list | tuple | set))
                                    else [merged_from]
                                )
                                for old_id in old_ids:
                                    try:
                                        self.context.mem_reader.graph_db.update_node(
                                            str(old_id), {"status": "archived"}, user_name=user_name
                                        )
                                        logger.info(
                                            f"[Scheduler] Archived merged_from memory: {old_id}"
                                        )
                                    except Exception as e:
                                        logger.warning(
                                            f"[Scheduler] Failed to archive merged_from memory {old_id}: {e}"
                                        )
                    else:
                        # Check if any memory has merged_from but graph_db is unavailable
                        has_merged_from = any(
                            (m.metadata.info or {}).get("merged_from") for m in flattened_memories
                        )
                        if has_merged_from:
                            logger.warning(
                                "[Scheduler] merged_from provided but graph_db is unavailable; skip archiving."
                            )

                    # LOGGING BLOCK START
                    # This block is replicated from _add_message_consumer to ensure consistent logging
                    cloud_env = is_cloud_env()
                    if cloud_env:
                        # New: Knowledge Base Logging (Cloud Service)
                        kb_log_content = []
                        for item in flattened_memories:
                            metadata = getattr(item, "metadata", None)
                            file_ids = getattr(metadata, "file_ids", None) if metadata else None
                            source_doc_id = (
                                file_ids[0] if isinstance(file_ids, list) and file_ids else None
                            )
                            kb_log_content.append(
                                {
                                    "log_source": "KNOWLEDGE_BASE_LOG",
                                    "trigger_source": info.get("trigger_source", "Messages")
                                    if info
                                    else "Messages",
                                    "operation": "ADD",
                                    "memory_id": item.id,
                                    "content": item.memory,
                                    "original_content": None,
                                    "source_doc_id": source_doc_id,
                                }
                            )
                        if kb_log_content:
                            logger.info(
                                f"[DIAGNOSTIC] MemReadHandler.process_memories_with_reader: Creating event log for KB update. Label: knowledgeBaseUpdate, user_id: {user_id}, mem_cube_id: {mem_cube_id}, task_id: {task_id}. KB content: {json.dumps(kb_log_content, indent=2)}"
                            )
                            event = self.context.create_event_log(
                                label="knowledgeBaseUpdate",
                                from_memory_type=USER_INPUT_TYPE,
                                to_memory_type=LONG_TERM_MEMORY_TYPE,
                                user_id=user_id,
                                mem_cube_id=mem_cube_id,
                                mem_cube=self.context.mem_cube,
                                memcube_log_content=kb_log_content,
                                metadata=None,
                                memory_len=len(kb_log_content),
                                memcube_name=self.context.map_memcube_name(mem_cube_id),
                            )
                            event.log_content = (
                                f"Knowledge Base Memory Update: {len(kb_log_content)} changes."
                            )
                            event.task_id = task_id
                            self.context.submit_web_logs([event])
                    else:
                        # Existing: Playground/Default Logging
                        add_content_legacy: list[dict] = []
                        add_meta_legacy: list[dict] = []
                        for item_id, item in zip(
                            enhanced_mem_ids, flattened_memories, strict=False
                        ):
                            key = getattr(item.metadata, "key", None) or transform_name_to_key(
                                name=item.memory
                            )
                            add_content_legacy.append(
                                {"content": f"{key}: {item.memory}", "ref_id": item_id}
                            )
                            add_meta_legacy.append(
                                {
                                    "ref_id": item_id,
                                    "id": item_id,
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
                        if add_content_legacy:
                            event = self.context.create_event_log(
                                label="addMemory",
                                from_memory_type=USER_INPUT_TYPE,
                                to_memory_type=LONG_TERM_MEMORY_TYPE,
                                user_id=user_id,
                                mem_cube_id=mem_cube_id,
                                mem_cube=self.context.mem_cube,
                                memcube_log_content=add_content_legacy,
                                metadata=add_meta_legacy,
                                memory_len=len(add_content_legacy),
                                memcube_name=self.context.map_memcube_name(mem_cube_id),
                            )
                            event.task_id = task_id
                            self.context.submit_web_logs([event])
                    # LOGGING BLOCK END
                else:
                    logger.info("No enhanced memories generated by mem_reader")
            else:
                logger.info("mem_reader returned no processed memories")

            # build full delete list:
            # - original raw mem_ids (temporary fast memories)
            # - any bound working memories referenced by the enhanced memories
            delete_ids = list(mem_ids)
            if bindings_to_delete:
                delete_ids.extend(list(bindings_to_delete))
            # deduplicate
            delete_ids = list(dict.fromkeys(delete_ids))
            if delete_ids:
                try:
                    text_mem.delete(delete_ids, user_name=user_name)
                    logger.info(
                        f"Delete raw/working mem_ids: {delete_ids} for user_name: {user_name}"
                    )
                except Exception as e:
                    logger.warning(f"Failed to delete some mem_ids {delete_ids}: {e}")
            else:
                logger.info("No mem_ids to delete (nothing to cleanup)")

            text_mem.memory_manager.remove_and_refresh_memory(user_name=user_name)
            logger.info("Remove and Refresh Memories")
            logger.debug(f"Finished add {user_id} memory: {mem_ids}")

        except Exception as exc:
            logger.error(
                f"Error in process_memories_with_reader: {traceback.format_exc()}", exc_info=True
            )
            with contextlib.suppress(Exception):
                cloud_env = is_cloud_env()
                if cloud_env:
                    if not kb_log_content:
                        trigger_source = (
                            info.get("trigger_source", "Messages") if info else "Messages"
                        )
                        kb_log_content = [
                            {
                                "log_source": "KNOWLEDGE_BASE_LOG",
                                "trigger_source": trigger_source,
                                "operation": "ADD",
                                "memory_id": mem_id,
                                "content": None,
                                "original_content": None,
                                "source_doc_id": None,
                            }
                            for mem_id in mem_ids
                        ]
                    event = self.context.create_event_log(
                        label="knowledgeBaseUpdate",
                        from_memory_type=USER_INPUT_TYPE,
                        to_memory_type=LONG_TERM_MEMORY_TYPE,
                        user_id=user_id,
                        mem_cube_id=mem_cube_id,
                        mem_cube=self.context.mem_cube,
                        memcube_log_content=kb_log_content,
                        metadata=None,
                        memory_len=len(kb_log_content),
                        memcube_name=self.context.map_memcube_name(mem_cube_id),
                    )
                    event.log_content = f"Knowledge Base Memory Update failed: {exc!s}"
                    event.task_id = task_id
                    event.status = "failed"
                    self.context.submit_web_logs([event])

    def batch_handler(self, user_id: str, mem_cube_id: str, batch: list[ScheduleMessageItem]):
        for message in batch:
            try:
                user_id = message.user_id
                mem_cube_id = message.mem_cube_id
                mem_cube = self.context.mem_cube
                if mem_cube is None:
                    logger.error(
                        f"mem_cube is None for user_id={user_id}, mem_cube_id={mem_cube_id}, skipping processing",
                        stack_info=True,
                    )
                    continue

                content = message.content
                user_name = message.user_name
                info = message.info or {}

                # Parse the memory IDs from content
                mem_ids = json.loads(content) if isinstance(content, str) else content
                if not mem_ids:
                    continue

                logger.info(
                    f"Processing mem_read for user_id={user_id}, mem_cube_id={mem_cube_id}, mem_ids={mem_ids}"
                )

                # Get the text memory from the mem_cube
                text_mem = mem_cube.text_mem
                if not isinstance(text_mem, TreeTextMemory):
                    logger.error(f"Expected TreeTextMemory but got {type(text_mem).__name__}")
                    continue

                # Use mem_reader to process the memories
                self.process_memories_with_reader(
                    mem_ids=mem_ids,
                    user_id=user_id,
                    mem_cube_id=mem_cube_id,
                    text_mem=text_mem,
                    user_name=user_name,
                    custom_tags=info.get("custom_tags", None),
                    task_id=message.task_id,
                    info=info,
                )

                logger.info(
                    f"Successfully processed mem_read for user_id={user_id}, mem_cube_id={mem_cube_id}"
                )

            except Exception as e:
                logger.error(f"Error processing mem_read message: {e}", stack_info=True)
