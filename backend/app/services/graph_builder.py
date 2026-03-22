"""
Graph builder service.
Uses LightRAG (local GraphRAG) instead of Zep Cloud.
"""

import os
import uuid
import json
import shutil
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass

from ..config import Config
from ..models.task import TaskManager, TaskStatus
from ..utils.logger import get_logger
from .lightrag_client import get_rag, get_working_dir, invalidate_rag, run_async
from .text_processor import TextProcessor

logger = get_logger('mirofish.graph_builder')


def format_graph_build_error(exc: Exception) -> str:
    """Convert low-level provider errors into actionable graph-build messages."""
    message = str(exc).strip()
    lowered = message.lower()

    if any(marker in lowered for marker in (
        "status code: 429",
        "session usage limit",
        "too many requests",
        "rate limit",
    )):
        return (
            "Graph building failed because the configured Ollama/LLM backend rejected extraction "
            "requests with a 429 quota/rate-limit error. The current backend appears to be a hosted "
            "Ollama account without remaining session quota. Point `OLLAMA_BASE_URL` to a local "
            "Ollama server or switch to a backend/account with available quota, then rebuild the graph."
        )

    return message


@dataclass
class GraphInfo:
    """Graph metadata."""
    graph_id: str
    node_count: int
    edge_count: int
    entity_types: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "entity_types": self.entity_types,
        }


class GraphBuilderService:
    """
    Graph builder service.
    Builds knowledge graphs using LightRAG (Ollama + local storage).
    """

    def __init__(self, api_key: Optional[str] = None):
        # api_key ignored — LightRAG needs no API key
        self.task_manager = TaskManager()

    def build_graph_async(
        self,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str = "MiroFish Graph",
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        batch_size: int = 3
    ) -> str:
        """
        Start async graph build in a background thread.

        Returns:
            task_id: poll task status to check progress
        """
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={
                "graph_name": graph_name,
                "chunk_size": chunk_size,
                "text_length": len(text),
            }
        )

        thread = threading.Thread(
            target=self._build_graph_worker,
            args=(task_id, text, ontology, graph_name, chunk_size, chunk_overlap, batch_size)
        )
        thread.daemon = True
        thread.start()

        return task_id

    def _build_graph_worker(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int
    ):
        """Background worker: creates graph and ingests text into LightRAG."""
        try:
            self.task_manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                progress=5,
                message="Starting graph build..."
            )

            # 1. Create graph
            graph_id = self.create_graph(graph_name)
            self.task_manager.update_task(
                task_id,
                progress=10,
                message=f"Graph created: {graph_id}"
            )

            # 2. Save ontology hints
            self.set_ontology(graph_id, ontology)
            self.task_manager.update_task(
                task_id,
                progress=15,
                message="Ontology hints saved"
            )

            # 3. Split text into chunks
            chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
            total_chunks = len(chunks)
            self.task_manager.update_task(
                task_id,
                progress=20,
                message=f"Text split into {total_chunks} chunks"
            )

            # 4. Insert chunks into LightRAG (15%–90%)
            self.add_text_batches(
                graph_id, chunks, batch_size,
                lambda msg, prog: self.task_manager.update_task(
                    task_id,
                    progress=20 + int(prog * 70),  # 20–90%
                    message=msg
                )
            )

            # 5. Get final graph info
            self.task_manager.update_task(
                task_id,
                progress=90,
                message="Retrieving graph info..."
            )
            graph_info = self._get_graph_info(graph_id)

            self.task_manager.complete_task(task_id, {
                "graph_id": graph_id,
                "graph_info": graph_info.to_dict(),
                "chunks_processed": total_chunks,
            })

        except Exception as e:
            import traceback
            self.task_manager.fail_task(task_id, f"{str(e)}\n{traceback.format_exc()}")

    def create_graph(self, name: str) -> str:
        """Create a new LightRAG graph and return its graph_id."""
        graph_id = f"mirofish_{uuid.uuid4().hex[:16]}"
        # Initialise LightRAG (creates working directory)
        get_rag(graph_id, create_if_missing=True)
        logger.info(f"Graph created: {graph_id} (name={name!r})")
        return graph_id

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        """
        Save ontology as extraction hints JSON.
        LightRAG uses these as context hints during entity extraction.
        """
        working_dir = get_working_dir(graph_id)
        hints_path = os.path.join(working_dir, "extraction_hints.json")
        with open(hints_path, 'w', encoding='utf-8') as f:
            json.dump(ontology, f, indent=2, ensure_ascii=False)
        logger.debug(f"Ontology hints saved to {hints_path}")

    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None
    ) -> List[str]:
        """
        Insert text chunks into LightRAG, returning a list of chunk IDs.

        LightRAG's ainsert() is synchronous from the caller's perspective
        (no episode polling required).
        """
        rag = get_rag(graph_id, create_if_missing=True)
        total_chunks = len(chunks)
        chunk_ids = []

        for i, chunk in enumerate(chunks):
            chunk_id = f"chunk_{i}"
            try:
                run_async(rag.ainsert(chunk))
                chunk_ids.append(chunk_id)
            except Exception as e:
                friendly_error = format_graph_build_error(e)
                logger.error(
                    f"Failed to insert chunk {i} into graph {graph_id}: {friendly_error}"
                )
                raise RuntimeError(friendly_error) from e

            if progress_callback:
                progress = (i + 1) / total_chunks
                progress_callback(
                    f"Inserted chunk {i + 1}/{total_chunks}",
                    progress
                )

        return chunk_ids

    def _get_graph_info(self, graph_id: str) -> GraphInfo:
        """Read GraphML and return node/edge counts and entity types."""
        nodes, edges = self._read_graphml(graph_id)

        entity_types: set = set()
        for _, data in nodes:
            label = data.get("entity_type", "") or data.get("type", "")
            if label and label not in ("Entity", "Node", ""):
                entity_types.add(label)

        return GraphInfo(
            graph_id=graph_id,
            node_count=len(nodes),
            edge_count=len(edges),
            entity_types=sorted(entity_types),
        )

    def _read_graphml(self, graph_id: str):
        """
        Read LightRAG's GraphML file via NetworkX.
        Returns (nodes, edges) as lists of (id, attr_dict) tuples.
        """
        import networkx as nx

        working_dir = get_working_dir(graph_id)
        graphml_path = os.path.join(working_dir, "graph_chunk_entity_relation.graphml")

        if not os.path.exists(graphml_path):
            return [], []

        try:
            G = nx.read_graphml(graphml_path)
            nodes = list(G.nodes(data=True))
            edges = list(G.edges(data=True))
            return nodes, edges
        except Exception as e:
            logger.warning(f"Could not read GraphML for {graph_id}: {e}")
            return [], []

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """
        Return full graph data in the same schema as before.

        Returns:
            dict with keys: graph_id, nodes, edges, node_count, edge_count
        """
        nodes_raw, edges_raw = self._read_graphml(graph_id)

        # Build node map: node_id -> name
        node_name_map = {}
        for node_id, data in nodes_raw:
            node_name_map[node_id] = data.get("entity_name", "") or str(node_id)

        nodes_data = []
        for node_id, data in nodes_raw:
            entity_type = data.get("entity_type", "") or data.get("type", "")
            labels = [entity_type] if entity_type else []
            nodes_data.append({
                "uuid": str(node_id),
                "name": node_name_map[node_id],
                "labels": labels,
                "summary": data.get("description", ""),
                "attributes": {k: v for k, v in data.items()
                               if k not in ("entity_name", "entity_type", "description", "type")},
                "created_at": None,
            })

        edges_data = []
        for src_id, tgt_id, data in edges_raw:
            edge_id = data.get("id", f"{src_id}__{tgt_id}")
            edges_data.append({
                "uuid": str(edge_id),
                "name": data.get("keywords", "") or data.get("relation_type", ""),
                "fact": data.get("description", ""),
                "fact_type": data.get("relation_type", "") or data.get("keywords", ""),
                "source_node_uuid": str(src_id),
                "target_node_uuid": str(tgt_id),
                "source_node_name": node_name_map.get(src_id, str(src_id)),
                "target_node_name": node_name_map.get(tgt_id, str(tgt_id)),
                "attributes": {k: v for k, v in data.items()
                               if k not in ("id", "keywords", "description", "relation_type",
                                            "weight", "order", "source_id")},
                "created_at": None,
                "valid_at": None,
                "invalid_at": None,
                "expired_at": None,
                "episodes": [],
            })

        return {
            "graph_id": graph_id,
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }

    def delete_graph(self, graph_id: str):
        """Delete the graph's working directory and invalidate cached instance."""
        working_dir = get_working_dir(graph_id)
        if os.path.exists(working_dir):
            shutil.rmtree(working_dir)
            logger.info(f"Graph deleted: {graph_id}")
        invalidate_rag(graph_id)
