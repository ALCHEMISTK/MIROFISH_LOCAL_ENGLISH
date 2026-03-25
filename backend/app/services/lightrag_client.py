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


class AdaptiveRateLimiter:
    """
    AIMD-style adaptive rate limiter (like TCP congestion control).
    Starts at max concurrency and dynamically finds the provider's limit.

    - On success streak: additive increase (+1 concurrency)
    - On 429/rate-limit: multiplicative decrease (halve concurrency + cooldown)
    """

    def __init__(self, max_concurrency: int = 16, min_concurrency: int = 1):
        self._max = max_concurrency
        self._min = min_concurrency
        self._current = max_concurrency
        # Lazily initialized in the event loop thread to avoid
        # "attached to a different loop" errors on Python 3.10+.
        self._semaphore = None
        self._lock = None
        self._consecutive_success = 0
        self._success_threshold = 5  # successes before increasing
        self._cooldown_seconds = 2.0
        self._last_decrease_time = 0.0
        # Track how many permits we've "logically removed" via on_rate_limit
        # so on_success knows not to release extra ones.
        self._pending_drains = 0
        import time as _t
        self._time = _t

    def _ensure_primitives(self):
        """Create asyncio primitives lazily inside the running event loop."""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max)
        if self._lock is None:
            self._lock = asyncio.Lock()

    async def acquire(self):
        self._ensure_primitives()
        await self._semaphore.acquire()

    def release(self):
        if self._semaphore is not None:
            self._semaphore.release()

    async def on_success(self):
        self._ensure_primitives()
        async with self._lock:
            # If there are pending drains, consume one instead of releasing a new permit
            if self._pending_drains > 0:
                self._pending_drains -= 1
                return
            self._consecutive_success += 1
            if self._consecutive_success >= self._success_threshold and self._current < self._max:
                self._current += 1
                self._consecutive_success = 0
                # Add a permit to the semaphore
                self._semaphore.release()
                logger.debug(f"Rate limiter: increased concurrency to {self._current}")

    async def on_rate_limit(self):
        self._ensure_primitives()
        async with self._lock:
            self._consecutive_success = 0
            now = self._time.time()
            # Don't decrease too rapidly
            if now - self._last_decrease_time < 2.0:
                return self._cooldown_seconds
            self._last_decrease_time = now

            old = self._current
            self._current = max(self._min, self._current // 2)
            permits_to_drain = old - self._current

            # Instead of trying to acquire_nowait (which fails for in-flight permits),
            # record the number of permits to drain. These will be consumed as
            # in-flight requests complete and call on_success().
            self._pending_drains += permits_to_drain

            # Increase cooldown when we're already at minimum
            if self._current == self._min:
                self._cooldown_seconds = min(self._cooldown_seconds * 1.5, 30.0)
            else:
                self._cooldown_seconds = max(2.0, self._cooldown_seconds * 0.8)

            logger.info(
                f"Rate limiter: decreased concurrency {old} → {self._current}, "
                f"cooldown={self._cooldown_seconds:.1f}s"
            )
            return self._cooldown_seconds

    @property
    def current_concurrency(self):
        return self._current


# Shared rate limiter for cloud API calls (LLM + embeddings)
_cloud_rate_limiter: Optional[AdaptiveRateLimiter] = None
_rate_limiter_lock = threading.Lock()


def _get_rate_limiter() -> AdaptiveRateLimiter:
    global _cloud_rate_limiter
    if _cloud_rate_limiter is None:
        with _rate_limiter_lock:
            if _cloud_rate_limiter is None:
                _cloud_rate_limiter = AdaptiveRateLimiter(max_concurrency=32, min_concurrency=1)
    return _cloud_rate_limiter


def _is_local_ollama(base_url: str) -> bool:
    """Return True when the configured LLM base URL points to a local Ollama host."""
    try:
        host = (urlparse(base_url).hostname or "").lower()
    except Exception:
        host = ""
    return host in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}


def build_lightrag_llm_binding():
    """
    Select the correct LightRAG LLM binding from the app config.

    Always uses the OpenAI-compatible endpoint so that all fixes
    (thinking model /no_think, rate limiting, retry logic) apply uniformly
    — even for local Ollama.
    """
    try:
        from lightrag.llm.openai import openai_complete_if_cache as openai_llm_complete
        use_if_cache = True
    except ImportError:
        from lightrag.llm.openai import openai_complete as openai_llm_complete
        use_if_cache = False

    is_local = _is_local_ollama(Config.LLM_BASE_URL)
    # For local Ollama, use the /v1 OpenAI-compatible endpoint
    effective_base_url = Config.LLM_BASE_URL
    if is_local:
        base = Config.OLLAMA_BASE_URL.rstrip("/")
        effective_base_url = f"{base}/v1" if not base.endswith("/v1") else base

    logger.info(f"LightRAG LLM binding: {'local Ollama' if is_local else 'OpenAI-compatible API'} via {effective_base_url}")

    # Detect thinking models that need /no_think
    _model_lower = Config.LLM_MODEL_NAME.lower()
    _is_thinking_model = 'qwen3' in _model_lower or 'qwq' in _model_lower

    async def cloud_llm_func(
        prompt,
        model=None,
        system_prompt=None,
        history_messages=None,
        **kwargs,
    ):
        history_messages = history_messages or []
        # Disable thinking mode for qwen3 models via system prompt
        effective_system = system_prompt
        if _is_thinking_model:
            if effective_system:
                if "/no_think" not in effective_system:
                    effective_system = effective_system + "\n/no_think"
            else:
                effective_system = "/no_think"
        call_kwargs = dict(
            prompt=prompt,
            system_prompt=effective_system,
            history_messages=history_messages,
            api_key=Config.LLM_API_KEY or "ollama",
            base_url=effective_base_url,
            **kwargs,
        )
        if use_if_cache:
            call_kwargs["model"] = model or Config.LLM_MODEL_NAME

        limiter = _get_rate_limiter()
        max_attempts = 8
        for attempt in range(1, max_attempts + 1):
            await limiter.acquire()
            try:
                result = await openai_llm_complete(**call_kwargs)
                await limiter.on_success()
                return result
            except Exception as e:
                from tenacity import RetryError
                from openai import RateLimitError
                is_rate_limit = isinstance(e, (RateLimitError, RetryError))
                if not is_rate_limit:
                    raise
                if attempt == max_attempts:
                    raise
                cooldown = await limiter.on_rate_limit()
                logger.warning(
                    f"Rate limited (attempt {attempt}/{max_attempts}), "
                    f"concurrency={limiter.current_concurrency}, "
                    f"waiting {cooldown:.1f}s..."
                )
                await asyncio.sleep(cooldown)
            finally:
                limiter.release()

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
        # Register graceful shutdown to avoid corrupting LightRAG storage
        import atexit
        atexit.register(self._shutdown)

    def _run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _shutdown(self):
        """Gracefully stop the event loop so in-flight writes can complete."""
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
        except Exception:
            pass

    def run(self, coro, timeout: Optional[float] = None):
        """Submit *coro* to the loop and block the calling thread until done."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            logger.warning(
                f"LightRAG coroutine timed out after {timeout}s — "
                f"the underlying task may still be running in the event loop"
            )
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


# Ollama-specific model names that should NOT be sent to cloud APIs.
_OLLAMA_EMBED_MODELS = {
    "nomic-embed-text", "mxbai-embed-large", "all-minilm",
    "snowflake-arctic-embed", "bge-m3", "bge-large",
}

# Well-known cloud provider base URLs → candidate embedding models (tried in order).
_CLOUD_EMBED_CANDIDATES = {
    "api.openai.com": ["text-embedding-3-small", "text-embedding-ada-002"],
    "open.bigmodel.cn": ["embedding-3", "embedding-2", "text_embedding"],
    "api.deepseek.com": ["text-embedding-3-small"],
    "api.moonshot.cn": ["text-embedding-3-small"],
    "api.siliconflow.cn": ["text-embedding-3-small"],
}

# Generic fallbacks for unknown providers
_GENERIC_EMBED_CANDIDATES = [
    "text-embedding-3-small", "text-embedding-ada-002",
    "embedding-3", "embedding-2",
]


def _probe_embed_model(model: str, api_key: str, base_url: str) -> bool:
    """Test if an embedding model works by making a minimal API call."""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=base_url)
        client.embeddings.create(model=model, input="test")
        return True
    except Exception:
        return False


def _probe_ollama_available(ollama_host: str, model: str = "nomic-embed-text") -> bool:
    """Check if a local Ollama instance is reachable and has the embedding model."""
    try:
        import ollama as _ollama
        client = _ollama.Client(host=ollama_host)
        client.embeddings(model=model, prompt="test")
        return True
    except Exception:
        return False


def _resolve_embed_model(configured_model: str, base_url: str, api_key: str = "") -> tuple:
    """
    Pick the right embedding model and provider for the current setup.
    Returns (model_name, provider) where provider is "cloud" or "ollama".

    Strategy:
    1. If user explicitly set a cloud-compatible model, use it with cloud.
    2. If model is Ollama-specific but LLM is cloud, probe cloud candidates.
    3. If no cloud embedding works, fall back to local Ollama for embeddings only.
    """
    if configured_model.lower() not in _OLLAMA_EMBED_MODELS:
        return configured_model, "cloud"  # User set a specific model, respect it.

    # We're on a cloud API but the model is Ollama-specific — find a working one.
    try:
        host = (urlparse(base_url).hostname or "").lower()
    except Exception:
        host = ""

    # Build candidate list: provider-specific first, then generic
    candidates = []
    for domain, models in _CLOUD_EMBED_CANDIDATES.items():
        if domain in host:
            candidates.extend(models)
            break
    for m in _GENERIC_EMBED_CANDIDATES:
        if m not in candidates:
            candidates.append(m)

    # Probe each cloud candidate
    for model in candidates:
        logger.info(f"Probing embedding model '{model}' on {host}...")
        if _probe_embed_model(model, api_key, base_url):
            logger.info(f"Embedding model '{model}' works on {host}")
            return model, "cloud"
        logger.warning(f"Embedding model '{model}' not available on {host}")

    # No cloud embedding available — fall back to local Ollama
    ollama_host = Config.OLLAMA_BASE_URL
    ollama_model = "nomic-embed-text"
    logger.warning(
        f"No cloud embedding model found on {host} (tried: {candidates}). "
        f"Falling back to local Ollama at {ollama_host} for embeddings..."
    )
    if _probe_ollama_available(ollama_host, ollama_model):
        logger.info(f"Ollama fallback OK: using '{ollama_model}' at {ollama_host} for embeddings")
        return ollama_model, "ollama"

    # Nothing works at all
    logger.error(
        f"No embedding provider available. Cloud models failed on {host}, "
        f"and Ollama is not running at {ollama_host}. "
        f"Either set EMBED_MODEL in .env to a valid cloud model, "
        f"or start Ollama with: ollama pull {ollama_model}"
    )
    return candidates[0] if candidates else "text-embedding-3-small", "cloud"


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

            if use_local_ollama:
                embed_model = Config.EMBED_MODEL
                embed_provider = "ollama"
            else:
                embed_model, embed_provider = _resolve_embed_model(
                    Config.EMBED_MODEL, Config.LLM_BASE_URL, Config.LLM_API_KEY
                )

            if embed_provider == "ollama":
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

                from openai import AsyncOpenAI
                _cloud_embed_client = AsyncOpenAI(api_key=api_key, base_url=base_url)

                @wrap_embedding_func_with_attrs(
                    embedding_dim=embed_dim,
                    max_token_size=8192,
                )
                async def _embed(texts, **_kwargs):
                    import numpy as np
                    limiter = _get_rate_limiter()
                    await limiter.acquire()
                    try:
                        resp = await _cloud_embed_client.embeddings.create(model=embed_model, input=texts)
                        await limiter.on_success()
                        return np.array(
                            [d.embedding for d in resp.data], dtype=np.float32
                        )
                    except Exception as e:
                        if "429" in str(e) or "rate" in str(getattr(e, 'status_code', '')):
                            await limiter.on_rate_limit()
                        raise
                    finally:
                        limiter.release()

            # LightRAG concurrency settings.
            # Cloud APIs: high concurrency with adaptive rate limiter.
            # Local Ollama: moderate concurrency (hardware-bound, not rate-limited).
            if use_local_ollama:
                concurrency_kwargs = {
                    "llm_model_max_async": 4,
                    "max_parallel_insert": 2,
                    "embedding_func_max_async": 8,
                }
                logger.info("Local Ollama: concurrency llm=4, embed=8")
            else:
                concurrency_kwargs = {
                    "llm_model_max_async": 32,
                    "max_parallel_insert": 8,
                    "embedding_func_max_async": 32,
                }
                logger.info("Cloud API: starting with adaptive rate limiting (max concurrency=32)")

            rag = LightRAG(
                working_dir=working_dir,
                llm_model_func=llm_model_func,
                llm_model_name=Config.LLM_MODEL_NAME,
                llm_model_kwargs=llm_model_kwargs,
                embedding_func=_embed,
                max_graph_nodes=Config.LIGHTRAG_MAX_GRAPH_NODES,
                **concurrency_kwargs,
            )

            # initialize_storages() must run in the persistent loop (same loop that
            # will be used for all subsequent ainsert/aquery calls).
            # Keep inside the lock so a second thread cannot create a duplicate instance.
            run_async(rag.initialize_storages())
            _rag_instances[graph_id] = rag
            logger.info(f"Initialized LightRAG: graph_id={graph_id}, dir={working_dir}")

        except ImportError as e:
            logger.error(f"LightRAG not installed: {e}. Run: pip install lightrag-hku")
            raise

    return rag


def invalidate_rag(graph_id: str) -> None:
    """Remove a cached LightRAG instance (call after graph deletion)."""
    with _rag_lock:
        _rag_instances.pop(graph_id, None)
