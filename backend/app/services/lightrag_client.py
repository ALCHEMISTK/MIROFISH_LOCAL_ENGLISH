"""
LightRAG instance manager.
Maintains a per-graph_id cache of initialized LightRAG instances.
All LightRAG services import get_rag() and run_async() from here.

Key design: a single, long-lived background event loop thread handles ALL
async operations so that LightRAG's internal asyncio locks never cross
event-loop boundaries (which would raise 'bound to a different event loop').
"""

import os
import asyncio
import threading
import concurrent.futures
from typing import Dict, Optional
from urllib.parse import urlparse

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.lightrag_client')

_rag_instances: Dict[str, object] = {}
_rag_lock = threading.Lock()


def _is_local_ollama(base_url: str) -> bool:
    """Return True when the configured LLM base URL points to a local Ollama host."""
    try:
        host = (urlparse(base_url).hostname or "").lower()
    except Exception:
        host = ""
    return host in {"localhost", "127.0.0.1", "0.0.0.0"}


def build_lightrag_llm_binding():
    """
    Select the correct LightRAG LLM binding from the app config.

    - Local Ollama URL -> LightRAG Ollama client
    - Remote/cloud URL -> LightRAG OpenAI-compatible client
    """
    if _is_local_ollama(Config.LLM_BASE_URL):
        from lightrag.llm.ollama import ollama_model_complete

        logger.info(f"LightRAG LLM binding: local Ollama via {Config.OLLAMA_BASE_URL}")
        return ollama_model_complete, {
            "host": Config.OLLAMA_BASE_URL,
            "options": {"num_ctx": 32768},
        }

    try:
        from lightrag.llm.openai import openai_complete_if_cache as openai_llm_complete
        use_if_cache = True
    except ImportError:
        from lightrag.llm.openai import openai_complete as openai_llm_complete
        use_if_cache = False

    logger.info(f"LightRAG LLM binding: OpenAI-compatible API via {Config.LLM_BASE_URL}")

    async def cloud_llm_func(
        prompt,
        model=None,
        system_prompt=None,
        history_messages=None,
        **kwargs,
    ):
        history_messages = history_messages or []
        if use_if_cache:
            return await openai_llm_complete(
                model=model or Config.LLM_MODEL_NAME,
                prompt=prompt,
                system_prompt=system_prompt,
                history_messages=history_messages,
                api_key=Config.LLM_API_KEY,
                base_url=Config.LLM_BASE_URL,
                **kwargs,
            )

        return await openai_llm_complete(
            prompt=prompt,
            system_prompt=system_prompt,
            history_messages=history_messages,
            api_key=Config.LLM_API_KEY,
            base_url=Config.LLM_BASE_URL,
            **kwargs,
        )

    return cloud_llm_func, {}


# ---------------------------------------------------------------------------
# Persistent event-loop thread
# ---------------------------------------------------------------------------

class _LoopThread:
    """
    A daemon thread that owns a single asyncio event loop.
    All LightRAG coroutines are submitted here via asyncio.run_coroutine_threadsafe()
    so that every LightRAG instance is created *and* used in the same loop.
    """

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True, name='lightrag-loop')
        self._thread.start()

    def _run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(self, coro, timeout: Optional[float] = None):
        """Submit *coro* to the loop and block the calling thread until done."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise TimeoutError(f"LightRAG coroutine timed out after {timeout} seconds")


# Module-level singleton — created once when lightrag_client is first imported.
_loop_thread = _LoopThread()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def run_async(coro, timeout: Optional[float] = None):
    """
    Run an async coroutine synchronously from any thread (including Flask
    request threads and background task threads).

    All calls go through the single persistent _LoopThread so that
    LightRAG's internal asyncio locks are always satisfied.
    """
    return _loop_thread.run(coro, timeout=timeout)


def _detect_embedding_dim(embed_model: str, ollama_host: str) -> int:
    """Probe Ollama to find the actual embedding dimension for the configured model."""
    try:
        import ollama as _ollama
        client = _ollama.Client(host=ollama_host)
        resp = client.embeddings(model=embed_model, prompt="dim probe")
        return len(resp.embedding)
    except Exception as e:
        logger.warning(f"Could not detect embedding dim for {embed_model}: {e}. Defaulting to 768.")
        return 768


def get_working_dir(graph_id: str) -> str:
    """Return the filesystem path for a graph's LightRAG working directory."""
    data_dir = Config.LIGHTRAG_DATA_DIR
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, graph_id)


def get_rag(graph_id: str, create_if_missing: bool = True):
    """
    Get or create a LightRAG instance for the given graph_id.
    Thread-safe singleton per graph_id.
    Returns None if create_if_missing=False and working dir doesn't exist.
    """
    # Fast path — no lock needed for reads
    if graph_id in _rag_instances:
        return _rag_instances[graph_id]

    with _rag_lock:
        # Double-check inside lock
        if graph_id in _rag_instances:
            return _rag_instances[graph_id]

        working_dir = get_working_dir(graph_id)

        if not create_if_missing and not os.path.exists(working_dir):
            return None

        os.makedirs(working_dir, exist_ok=True)

        try:
            from lightrag import LightRAG

            ollama_host = Config.OLLAMA_BASE_URL
            embed_model = Config.OLLAMA_EMBED_MODEL
            llm_model_func, llm_model_kwargs = build_lightrag_llm_binding()

            # Detect actual embedding dimension from the configured model
            # so we can configure LightRAG's vector DB correctly.
            embed_dim = _detect_embedding_dim(embed_model, ollama_host)
            logger.info(f"Embedding model: {embed_model}, dim={embed_dim}")

            # Build a properly decorated embedding function.
            # LightRAG's built-in ollama_embed is hardcoded to 1024 (bge-m3),
            # so we use wrap_embedding_func_with_attrs with the actual dim.
            from lightrag.utils import wrap_embedding_func_with_attrs
            import ollama as _ollama

            @wrap_embedding_func_with_attrs(
                embedding_dim=embed_dim,
                max_token_size=8192,
            )
            async def _embed(texts, **_kwargs):
                import numpy as np
                client = _ollama.AsyncClient(host=ollama_host)
                embeddings = []
                for text in texts:
                    resp = await client.embeddings(model=embed_model, prompt=text)
                    embeddings.append(resp.embedding)
                return np.array(embeddings, dtype=np.float32)

            rag = LightRAG(
                working_dir=working_dir,
                llm_model_func=llm_model_func,
                llm_model_name=Config.LLM_MODEL_NAME,
                llm_model_kwargs=llm_model_kwargs,
                embedding_func=_embed,
            )

            # Store early so other threads won't create a duplicate instance
            _rag_instances[graph_id] = rag

        except ImportError as e:
            logger.error(f"LightRAG not installed: {e}. Run: pip install lightrag-hku")
            raise

    # initialize_storages() must run in the persistent loop (same loop that
    # will be used for all subsequent ainsert/aquery calls).
    try:
        run_async(rag.initialize_storages())
        logger.info(f"Initialized LightRAG: graph_id={graph_id}, dir={working_dir}")
    except Exception as e:
        with _rag_lock:
            _rag_instances.pop(graph_id, None)
        raise

    return rag


def invalidate_rag(graph_id: str) -> None:
    """Remove a cached LightRAG instance (call after graph deletion)."""
    with _rag_lock:
        _rag_instances.pop(graph_id, None)
