"""
LightRAG graph memory updater.
Drop-in replacement for zep_graph_memory_updater.py.
Inserts agent activity text into the LightRAG knowledge graph.
Exports identical class names so callers need only swap the import.
"""

import os
import json
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
from queue import Queue, Empty

from ..utils.logger import get_logger
from .lightrag_client import get_rag, run_async

logger = get_logger('mirofish.lightrag_graph_memory_updater')


@dataclass
class AgentActivity:
    """Agent activity record (matches zep_graph_memory_updater.AgentActivity)."""
    platform: str
    agent_id: int
    agent_name: str
    action_type: str
    action_args: Dict[str, Any]
    round_num: int
    timestamp: str

    def to_episode_text(self) -> str:
        """Convert activity to natural language text for LightRAG ingestion."""
        action_descriptions = {
            "CREATE_POST": self._describe_create_post,
            "LIKE_POST": self._describe_like_post,
            "DISLIKE_POST": self._describe_dislike_post,
            "REPOST": self._describe_repost,
            "QUOTE_POST": self._describe_quote_post,
            "FOLLOW": self._describe_follow,
            "CREATE_COMMENT": self._describe_create_comment,
            "LIKE_COMMENT": self._describe_like_comment,
            "DISLIKE_COMMENT": self._describe_dislike_comment,
            "MUTE": self._describe_mute,
            "SEARCH_POSTS": self._describe_search,
            "SEARCH_USER": self._describe_search,
        }
        handler = action_descriptions.get(self.action_type)
        if handler:
            return handler()
        return (f"{self.agent_name} performed {self.action_type} on {self.platform} "
                f"(round {self.round_num}).")

    def _describe_create_post(self) -> str:
        content = self.action_args.get('content', '')
        return (f"{self.agent_name} posted on {self.platform}: \"{content}\" "
                f"(round {self.round_num}).")

    def _describe_like_post(self) -> str:
        post_content = self.action_args.get('post_content', '')
        author = self.action_args.get('post_author_name', 'another user')
        return (f"{self.agent_name} liked a post by {author} on {self.platform}: "
                f"\"{post_content}\" (round {self.round_num}).")

    def _describe_dislike_post(self) -> str:
        post_content = self.action_args.get('post_content', '')
        author = self.action_args.get('post_author_name', 'another user')
        return (f"{self.agent_name} disliked a post by {author} on {self.platform}: "
                f"\"{post_content}\" (round {self.round_num}).")

    def _describe_repost(self) -> str:
        original = self.action_args.get('original_content', '')
        author = self.action_args.get('original_author_name', 'another user')
        return (f"{self.agent_name} reposted {author}'s post on {self.platform}: "
                f"\"{original}\" (round {self.round_num}).")

    def _describe_quote_post(self) -> str:
        original = self.action_args.get('original_content', '')
        quote = self.action_args.get('quote_content', '')
        author = self.action_args.get('original_author_name', 'another user')
        return (f"{self.agent_name} quote-posted {author}'s post on {self.platform} "
                f"saying \"{quote}\" in response to \"{original}\" (round {self.round_num}).")

    def _describe_follow(self) -> str:
        target = self.action_args.get('target_user_name', 'another user')
        return (f"{self.agent_name} followed {target} on {self.platform} "
                f"(round {self.round_num}).")

    def _describe_create_comment(self) -> str:
        content = self.action_args.get('content', '')
        post_content = self.action_args.get('post_content', '')
        return (f"{self.agent_name} commented \"{content}\" on a post \"{post_content}\" "
                f"on {self.platform} (round {self.round_num}).")

    def _describe_like_comment(self) -> str:
        comment = self.action_args.get('comment_content', '')
        author = self.action_args.get('comment_author_name', 'another user')
        return (f"{self.agent_name} liked {author}'s comment \"{comment}\" "
                f"on {self.platform} (round {self.round_num}).")

    def _describe_dislike_comment(self) -> str:
        comment = self.action_args.get('comment_content', '')
        author = self.action_args.get('comment_author_name', 'another user')
        return (f"{self.agent_name} disliked {author}'s comment \"{comment}\" "
                f"on {self.platform} (round {self.round_num}).")

    def _describe_mute(self) -> str:
        target = self.action_args.get('target_user_name', 'another user')
        return (f"{self.agent_name} muted {target} on {self.platform} "
                f"(round {self.round_num}).")

    def _describe_search(self) -> str:
        query = self.action_args.get('query', '')
        return (f"{self.agent_name} searched for \"{query}\" on {self.platform} "
                f"(round {self.round_num}).")


class ZepGraphMemoryUpdater:
    """
    Drop-in replacement for ZepGraphMemoryUpdater.
    Inserts activity text into LightRAG via ainsert().
    """

    def __init__(self, graph_id: str, api_key: Optional[str] = None):
        self.graph_id = graph_id

    def update_memory(self, activity_text: str) -> bool:
        """Insert a text episode into the LightRAG knowledge graph."""
        if not activity_text or not activity_text.strip():
            return False
        try:
            rag = get_rag(self.graph_id, create_if_missing=True)
            run_async(rag.ainsert(activity_text))
            logger.debug(f"Memory updated for graph_id={self.graph_id}: {activity_text[:80]}...")
            return True
        except Exception as e:
            logger.error(f"Memory update failed for graph_id={self.graph_id}: {e}")
            return False

    def update_from_activity(self, activity: AgentActivity) -> bool:
        """Convert an AgentActivity to text and insert into the graph."""
        return self.update_memory(activity.to_episode_text())


class ZepGraphMemoryManager:
    """
    Drop-in replacement for ZepGraphMemoryManager (background batching manager).
    Collects agent activities and flushes them to LightRAG in the background.
    """

    # Class-level registry: simulation_id -> ZepGraphMemoryManager instance
    _instances: Dict[str, 'ZepGraphMemoryManager'] = {}
    _lock = threading.Lock()

    def __init__(
        self,
        graph_id: str,
        api_key: Optional[str] = None,
        batch_size: int = 10,
        flush_interval: float = 30.0,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        self.graph_id = graph_id
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.on_error = on_error

        self._queue: Queue = Queue()
        self._updater = ZepGraphMemoryUpdater(graph_id=graph_id)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._total_sent = 0
        self._failed_count = 0

    # ------------------------------------------------------------------
    # Class-level factory / registry methods (called by simulation_runner)
    # ------------------------------------------------------------------

    @classmethod
    def create_updater(cls, simulation_id: str, graph_id: str) -> 'ZepGraphMemoryManager':
        """Create, start, and register a manager for the given simulation."""
        with cls._lock:
            if simulation_id in cls._instances:
                # Stop the old one first
                cls._instances[simulation_id].stop()
            manager = cls(graph_id=graph_id)
            manager.start()
            cls._instances[simulation_id] = manager
            logger.info(f"Created ZepGraphMemoryManager: simulation_id={simulation_id}, graph_id={graph_id}")
            return manager

    @classmethod
    def get_updater(cls, simulation_id: str) -> Optional['ZepGraphMemoryManager']:
        """Retrieve the registered manager for a simulation, or None."""
        return cls._instances.get(simulation_id)

    @classmethod
    def stop_updater(cls, simulation_id: str, timeout: float = 10.0):
        """Stop and deregister the manager for a simulation."""
        with cls._lock:
            manager = cls._instances.pop(simulation_id, None)
        if manager:
            manager.stop(timeout=timeout)
            logger.info(f"Stopped ZepGraphMemoryManager: simulation_id={simulation_id}")

    @classmethod
    def stop_all(cls, timeout: float = 10.0):
        """Stop and deregister all managers."""
        with cls._lock:
            instances = dict(cls._instances)
            cls._instances.clear()
        for sim_id, manager in instances.items():
            try:
                manager.stop(timeout=timeout)
                logger.info(f"Stopped ZepGraphMemoryManager (stop_all): simulation_id={sim_id}")
            except Exception as e:
                logger.error(f"Error stopping manager for {sim_id}: {e}")

    # ------------------------------------------------------------------
    # Instance methods
    # ------------------------------------------------------------------

    def start(self):
        """Start the background flush thread."""
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"ZepGraphMemoryManager started: graph_id={self.graph_id}")

    def stop(self, timeout: float = 10.0):
        """Stop the background thread and flush remaining activities."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=timeout)
        self._flush_remaining()
        logger.info(f"ZepGraphMemoryManager stopped: graph_id={self.graph_id}, "
                    f"total_sent={self._total_sent}, failed={self._failed_count}")

    def add_activity(self, activity: AgentActivity):
        """Add an AgentActivity object to the queue."""
        self._queue.put(activity)

    def add_activity_from_dict(self, action_data: Dict[str, Any], platform: str):
        """
        Convert a raw action-log dict (as written by the simulation script)
        into an AgentActivity and add it to the queue.
        Called by simulation_runner when it reads action-log lines.
        """
        try:
            activity = AgentActivity(
                platform=platform,
                agent_id=action_data.get("agent_id", 0),
                agent_name=action_data.get("agent_name", "Unknown"),
                action_type=action_data.get("action_type", ""),
                action_args=action_data.get("action_args", {}),
                round_num=action_data.get("round", 0),
                timestamp=action_data.get("timestamp", datetime.now().isoformat()),
            )
            self._queue.put(activity)
        except Exception as e:
            logger.warning(f"add_activity_from_dict failed: {e}")

    def _run(self):
        """Background thread: flush activities periodically."""
        import time
        while self._running:
            self._flush_batch()
            time.sleep(self.flush_interval)

    def _flush_batch(self):
        """Flush up to batch_size activities from the queue."""
        activities: List[AgentActivity] = []
        try:
            while len(activities) < self.batch_size:
                activities.append(self._queue.get_nowait())
        except Empty:
            pass

        for activity in activities:
            try:
                self._updater.update_from_activity(activity)
                self._total_sent += 1
            except Exception as e:
                self._failed_count += 1
                logger.error(f"Failed to update memory for activity: {e}")
                if self.on_error:
                    self.on_error(e)

    def _flush_remaining(self):
        """Flush all remaining activities in the queue."""
        while not self._queue.empty():
            self._flush_batch()

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_sent": self._total_sent,
            "failed_count": self._failed_count,
            "queue_size": self._queue.qsize(),
        }
