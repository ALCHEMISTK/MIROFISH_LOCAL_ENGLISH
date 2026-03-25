"""
Custom Graph Extractor — drop-in replacement for LightRAG.

Preserves the exact same public API as the original lightrag_client.py:
  - get_rag(graph_id)        → returns a GraphExtractor instance
  - run_async(coro)          → runs a coroutine from any thread
  - invalidate_rag(graph_id) → clears cached instance
  - get_working_dir(graph_id)→ returns graph filesystem path

Key feature: ADAPTIVE CONCURRENCY
  Workers start at INITIAL_WORKERS and climb toward MAX_WORKERS on success.
  On a 429 rate limit, workers are immediately halved and recover gradually.
  The system finds and holds the optimal concurrency for whatever API tier
  is configured — no manual tuning needed when switching accounts or tiers.

  Free tier Zhipu:  settles at 1-2 workers automatically
  Paid tier Zhipu:  climbs to 6-8 workers and holds
  Local Ollama:     climbs to MAX_WORKERS immediately (no rate limits)
"""

import os
import json
import asyncio
import threading
import concurrent.futures
import xml.etree.ElementTree as ET
from typing import Dict, Optional
from openai import AsyncOpenAI

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.lightrag_client')

# ---------------------------------------------------------------------------
# Concurrency tuning
# ---------------------------------------------------------------------------

INITIAL_WORKERS = 4      # workers at boot — drops automatically if rate limited
MAX_WORKERS     = 8      # hard ceiling — never exceeds this
MIN_WORKERS     = 1      # hard floor — always at least 1 worker

# How many successes before adding a worker (higher = more cautious climb)
SCALE_UP_AFTER  = 5

# Delay (seconds) each worker waits between its own calls
# Lower = faster, higher = gentler on API
WORKER_DELAY    = 0.5

# Retry config
MAX_RETRIES     = 3
RETRY_DELAY     = 10.0   # seconds to wait after a 429 before retrying

# Chunking
CHUNK_SIZE      = Config.DEFAULT_CHUNK_SIZE
CHUNK_OVERLAP   = Config.DEFAULT_CHUNK_OVERLAP


# ---------------------------------------------------------------------------
# Adaptive Concurrency Controller (shared across all extraction calls)
# ---------------------------------------------------------------------------

class AdaptiveConcurrencyController:
    """
    Dynamically adjusts the number of parallel LLM workers based on
    API responses. Acts like TCP congestion control:
      - Sustained success  → gradually increase workers (additive)
      - 429 rate limit hit → immediately halve workers (multiplicative decrease)
      - Recovers slowly after backoff
    """

    def __init__(self, initial: int, maximum: int, minimum: int):
        self._workers   = initial
        self._max       = maximum
        self._min       = minimum
        self._successes = 0          # consecutive successes since last scale-up
        self._lock      = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(initial)
        logger.info(
            f"AdaptiveConcurrency: initial={initial}, "
            f"max={maximum}, min={minimum}"
        )

    @property
    def current_workers(self) -> int:
        return self._workers

    async def on_success(self):
        """Call after a successful LLM response."""
        async with self._lock:
            self._successes += 1
            if self._successes >= SCALE_UP_AFTER and self._workers < self._max:
                self._workers   += 1
                self._successes  = 0
                # Release one extra permit to reflect the new worker slot
                self._semaphore.release()
                logger.info(f"AdaptiveConcurrency: scaled UP → {self._workers} workers")

    async def on_rate_limit(self):
        """Call on a 429 response — immediately halve concurrency."""
        async with self._lock:
            new_workers     = max(self._min, self._workers // 2)
            drop            = self._workers - new_workers
            self._workers   = new_workers
            self._successes = 0
            # Drain permits to reflect the reduced worker count
            for _ in range(drop):
                try:
                    self._semaphore.acquire_nowait()
                except:
                    pass
            logger.warning(
                f"AdaptiveConcurrency: rate limited — "
                f"scaled DOWN → {self._workers} workers"
            )

    async def acquire(self):
        await self._semaphore.acquire()

    def release(self):
        self._semaphore.release()


# Module-level controller — shared across all GraphExtractor instances
_concurrency = AdaptiveConcurrencyController(
    initial=INITIAL_WORKERS,
    maximum=MAX_WORKERS,
    minimum=MIN_WORKERS,
)


# ---------------------------------------------------------------------------
# Persistent event-loop thread (identical to original — preserved for
# run_async() compatibility with the rest of the app)
# ---------------------------------------------------------------------------

class _LoopThread:
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name='lightrag-loop'
        )
        self._thread.start()

    def _run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(self, coro, timeout: Optional[float] = None):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise TimeoutError(f"Coroutine timed out after {timeout} seconds")


_loop_thread = _LoopThread()


def run_async(coro, timeout: Optional[float] = None):
    """
    Run an async coroutine synchronously from any thread.
    Identical interface to original — all callers work unchanged.
    """
    return _loop_thread.run(coro, timeout=timeout)


# ---------------------------------------------------------------------------
# OpenAI-compatible client (works for Ollama AND cloud APIs via .env)
# ---------------------------------------------------------------------------

def _get_openai_client() -> AsyncOpenAI:
    return AsyncOpenAI(
        api_key=Config.LLM_API_KEY,
        base_url=Config.LLM_BASE_URL,
    )


# ---------------------------------------------------------------------------
# Text chunking
# ---------------------------------------------------------------------------

def _chunk_text(text: str) -> list[str]:
    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunks.append(text[start:end])
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return [c.strip() for c in chunks if c.strip()]


# ---------------------------------------------------------------------------
# LLM extraction
# ---------------------------------------------------------------------------

_EXTRACTION_PROMPT = """You are a knowledge graph extractor. Given the text below, extract:
1. ENTITIES — important people, places, organizations, concepts, events, products.
2. RELATIONSHIPS — meaningful connections between those entities.

Return ONLY valid JSON in this exact format, nothing else:
{{
  "entities": [
    {{"id": "unique_snake_case_id", "name": "Display Name", "type": "PERSON|ORG|PLACE|CONCEPT|EVENT|PRODUCT|OTHER", "description": "brief description"}}
  ],
  "relationships": [
    {{"source": "entity_id", "target": "entity_id", "type": "RELATIONSHIP_TYPE", "description": "brief description"}}
  ]
}}

Rules:
- Use snake_case for entity IDs (e.g. "elon_musk", "tesla_inc")
- Only include entities and relationships clearly present in the text
- Relationship type should be short uppercase (e.g. WORKS_AT, FOUNDED, LOCATED_IN)
- If nothing meaningful can be extracted, return {{"entities": [], "relationships": []}}

TEXT:
{text}
"""


async def _call_llm_for_chunk(
    chunk: str,
    client: AsyncOpenAI,
    chunk_label: str = "",
) -> dict:
    """
    Call LLM for a single chunk with adaptive concurrency control.
    Acquires a permit before calling, releases after.
    Signals the controller on success or rate limit.
    """
    await _concurrency.acquire()
    try:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.chat.completions.create(
                    model=Config.LLM_MODEL_NAME,
                    messages=[{
                        "role": "user",
                        "content": _EXTRACTION_PROMPT.format(text=chunk)
                    }],
                    temperature=0.0,
                    max_tokens=2048,
                )
                raw = response.choices[0].message.content.strip()

                # Strip markdown code fences if model wraps response in ```json
                if raw.startswith("```"):
                    parts = raw.split("```")
                    raw = parts[1] if len(parts) > 1 else raw
                    if raw.startswith("json"):
                        raw = raw[4:]
                raw = raw.strip()

                result = json.loads(raw)

                # Report success to controller — may trigger scale-up
                await _concurrency.on_success()

                # Small per-worker delay to stay gentle on API
                await asyncio.sleep(WORKER_DELAY)

                return result

            except Exception as e:
                err = str(e)
                if "429" in err or "rate" in err.lower():
                    # Report rate limit — triggers immediate scale-down
                    await _concurrency.on_rate_limit()
                    wait = RETRY_DELAY * (attempt + 1)
                    logger.warning(
                        f"{chunk_label} Rate limited. "
                        f"Waiting {wait}s (retry {attempt + 1}/{MAX_RETRIES}) "
                        f"[workers now: {_concurrency.current_workers}]"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"{chunk_label} LLM error (chunk skipped): {e}")
                    return {"entities": [], "relationships": []}

        logger.error(f"{chunk_label} Max retries exceeded — skipping chunk.")
        return {"entities": [], "relationships": []}

    finally:
        _concurrency.release()


# ---------------------------------------------------------------------------
# GraphML output builder
# ---------------------------------------------------------------------------

def _build_graphml(entities: dict, relationships: list) -> str:
    root = ET.Element("graphml", {
        "xmlns": "http://graphml.graphstruct.org/graphml",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    })

    for attr_id, attr_for, attr_name in [
        ("d0", "node", "type"),
        ("d1", "node", "description"),
        ("d2", "node", "name"),
        ("d3", "edge", "type"),
        ("d4", "edge", "description"),
    ]:
        ET.SubElement(root, "key", {
            "id": attr_id, "for": attr_for,
            "attr.name": attr_name, "attr.type": "string"
        })

    graph_el = ET.SubElement(root, "graph", {"id": "G", "edgedefault": "directed"})

    for eid, entity in entities.items():
        node = ET.SubElement(graph_el, "node", {"id": eid})
        for key_id, val in [
            ("d0", entity.get("type", "OTHER")),
            ("d1", entity.get("description", "")),
            ("d2", entity.get("name", eid)),
        ]:
            d = ET.SubElement(node, "data", {"key": key_id})
            d.text = val

    for i, rel in enumerate(relationships):
        src, tgt = rel.get("source", ""), rel.get("target", "")
        if src not in entities or tgt not in entities:
            continue
        edge = ET.SubElement(graph_el, "edge", {
            "id": f"e{i}", "source": src, "target": tgt
        })
        for key_id, val in [
            ("d3", rel.get("type", "RELATED_TO")),
            ("d4", rel.get("description", "")),
        ]:
            d = ET.SubElement(edge, "data", {"key": key_id})
            d.text = val

    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


# ---------------------------------------------------------------------------
# GraphExtractor — mimics the LightRAG instance interface
# ---------------------------------------------------------------------------

class GraphExtractor:
    """
    Replaces the LightRAG instance returned by get_rag().
    Public method ainsert() is preserved — all existing callers unchanged.
    """

    def __init__(self, graph_id: str, working_dir: str):
        self.graph_id    = graph_id
        self.working_dir = working_dir
        self.graphml_path = os.path.join(
            working_dir, "graph_chunk_entity_relation.graphml"
        )
        os.makedirs(working_dir, exist_ok=True)

    async def ainsert(self, text: str, source_name: str = "unknown_source"):
        """
        Extract entities and relationships from text, save as GraphML.
        Replaces LightRAG's rag.ainsert(). Same call signature.
        Runs all chunks in parallel via the adaptive concurrency controller.
        """
        logger.info(
            f"[{self.graph_id}] Starting extraction: '{source_name}' "
            f"[workers: {_concurrency.current_workers}/{MAX_WORKERS}]"
        )
        client = _get_openai_client()
        chunks = _chunk_text(text)
        logger.info(f"[{self.graph_id}] {len(chunks)} chunk(s) to process")

        # Fire all chunks concurrently — the semaphore inside _call_llm_for_chunk
        # controls how many actually run at once
        tasks = [
            _call_llm_for_chunk(
                chunk=chunk,
                client=client,
                chunk_label=f"[{self.graph_id}][{i+1}/{len(chunks)}]"
            )
            for i, chunk in enumerate(chunks)
        ]
        results = await asyncio.gather(*tasks)

        all_entities: dict = {}
        all_relationships: list = []

        for result in results:
            for entity in result.get("entities", []):
                eid = entity.get("id", "").strip()
                if eid and eid not in all_entities:
                    all_entities[eid] = entity
            all_relationships.extend(result.get("relationships", []))

        graphml = _build_graphml(all_entities, all_relationships)
        with open(self.graphml_path, "w", encoding="utf-8") as f:
            f.write(graphml)

        logger.info(
            f"[{self.graph_id}] Done: "
            f"{len(all_entities)} entities, {len(all_relationships)} relationships "
            f"[workers now: {_concurrency.current_workers}/{MAX_WORKERS}]"
        )
        return {"nodes": len(all_entities), "edges": len(all_relationships)}

    async def initialize_storages(self):
        """No-op — kept so callers of run_async(rag.initialize_storages()) still work."""
        pass

    def get_entity_count(self) -> int:
        if not os.path.exists(self.graphml_path):
            return 0
        try:
            tree = ET.parse(self.graphml_path)
            root = tree.getroot()
            ns = {"g": "http://graphml.graphstruct.org/graphml"}
            return len(root.findall(".//g:node", ns))
        except Exception:
            return 0


# ---------------------------------------------------------------------------
# Instance cache (same pattern as original)
# ---------------------------------------------------------------------------

_rag_instances: Dict[str, GraphExtractor] = {}
_rag_lock = threading.Lock()


def get_working_dir(graph_id: str) -> str:
    data_dir = Config.LIGHTRAG_DATA_DIR
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, graph_id)


def get_rag(graph_id: str, create_if_missing: bool = True) -> Optional[GraphExtractor]:
    """
    Get or create a GraphExtractor for the given graph_id.
    Identical interface to original get_rag() — all callers unchanged.
    """
    if graph_id in _rag_instances:
        return _rag_instances[graph_id]

    with _rag_lock:
        if graph_id in _rag_instances:
            return _rag_instances[graph_id]

        working_dir = get_working_dir(graph_id)

        if not create_if_missing and not os.path.exists(working_dir):
            return None

        extractor = GraphExtractor(graph_id=graph_id, working_dir=working_dir)
        run_async(extractor.initialize_storages())
        logger.info(f"Initialized GraphExtractor: graph_id={graph_id}, dir={working_dir}")

        _rag_instances[graph_id] = extractor
        return extractor


def invalidate_rag(graph_id: str) -> None:
    """Remove a cached instance (call after graph deletion). Same as original."""
    with _rag_lock:
        _rag_instances.pop(graph_id, None)
