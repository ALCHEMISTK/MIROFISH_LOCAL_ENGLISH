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
        call_kwargs = dict(
            prompt=prompt,
            system_prompt=system_prompt,
            history_messages=history_messages,
            api_key=Config.LLM_API_KEY,
            base_url=Config.LLM_BASE_URL,
            **kwargs,
        )
        if use_if_cache:
            call_kwargs["model"] = model or Config.LLM_MODEL_NAME

        # Retry with exponential backoff on rate-limit / retry errors.
        # LightRAG's internal tenacity retries are too short (~0.3-1s);
        # this outer loop waits longer so the quota can recover.
        max_attempts = 6
        for attempt in range(1, max_attempts + 1):
            try:
                return await openai_llm_complete(**call_kwargs)
            except Exception as e:
                from tenacity import RetryError
                from openai import RateLimitError
                is_rate_limit = isinstance(e, (RateLimitError, RetryError))
                if not is_rate_limit:
                    raise
                if attempt == max_attempts:
                    raise
                wait = min(2 ** attempt, 60)  # 2, 4, 8, 16, 32, 60s
                logger.warning(
                    f"Rate limited (attempt {attempt}/{max_attempts}), "
                    f"waiting {wait}s before retry..."
                )
                await asyncio.sleep(wait)

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


def _detect_embedding_dim_ollama(embed_model: str, ollama_host: str) -> int:
    """Probe Ollama to find the actual embedding dimension for the configured model."""
    try:
        import ollama as _ollama
        client = _ollama.Client(host=ollama_host)
        resp = client.embeddings(model=embed_model, prompt="dim probe")
        return len(resp.embedding)
    except Exception as e:
        logger.warning(f"Could not detect embedding dim for {embed_model}: {e}. Defaulting to 768.")
        return 768


def _detect_embedding_dim_openai(embed_model: str, api_key: str, base_url: str) -> int:
    """Probe an OpenAI-compatible API to find the embedding dimension."""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=base_url)
        resp = client.embeddings.create(model=embed_model, input="dim probe")
        return len(resp.data[0].embedding)
    except Exception as e:
        logger.warning(f"Could not detect embedding dim for {embed_model}: {e}. Defaulting to 1536.")
        return 1536


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
            from lightrag.utils import wrap_embedding_func_with_attrs

            llm_model_func, llm_model_kwargs = build_lightrag_llm_binding()
            use_local_ollama = _is_local_ollama(Config.LLM_BASE_URL)

            embed_model = Config.EMBED_MODEL

            if use_local_ollama:
                # --- Ollama embeddings (local) ---
                import ollama as _ollama
                ollama_host = Config.OLLAMA_BASE_URL
                embed_dim = _detect_embedding_dim_ollama(embed_model, ollama_host)
                logger.info(f"Embedding: Ollama local | model={embed_model}, dim={embed_dim}")

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
            else:
                # --- OpenAI-compatible embeddings (cloud) ---
                api_key = Config.LLM_API_KEY
                base_url = Config.LLM_BASE_URL
                embed_dim = _detect_embedding_dim_openai(embed_model, api_key, base_url)
                logger.info(f"Embedding: OpenAI-compatible API | model={embed_model}, dim={embed_dim}")

                @wrap_embedding_func_with_attrs(
                    embedding_dim=embed_dim,
                    max_token_size=8192,
                )
                async def _embed(texts, **_kwargs):
                    import numpy as np
                    from openai import AsyncOpenAI
                    client = AsyncOpenAI(api_key=api_key, base_url=base_url)
                    resp = await client.embeddings.create(model=embed_model, input=texts)
                    return np.array(
                        [d.embedding for d in resp.data], dtype=np.float32
                    )

            # Reduce concurrency for cloud APIs to avoid rate limits.
            # Ollama (local) can handle high parallelism; cloud APIs cannot.
            concurrency_kwargs = {}
            if not use_local_ollama:
                concurrency_kwargs = {
                    "llm_model_max_async": 2,
                    "max_parallel_insert": 1,
                    "embedding_func_max_async": 4,
                }
                logger.info("Cloud API detected — throttling concurrency to avoid rate limits")

            rag = LightRAG(
                working_dir=working_dir,
                llm_model_func=llm_model_func,
                llm_model_name=Config.LLM_MODEL_NAME,
                llm_model_kwargs=llm_model_kwargs,
                embedding_func=_embed,
                **concurrency_kwargs,
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
