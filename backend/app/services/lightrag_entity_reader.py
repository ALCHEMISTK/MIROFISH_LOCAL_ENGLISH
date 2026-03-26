"""
LightRAG entity reader.
Drop-in replacement for zep_entity_reader.py.
Reads entities from LightRAG's GraphML storage using NetworkX.
Exports identical class/dataclass names so callers need only swap the import.
"""

import os
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field

from ..config import Config
from ..utils.logger import get_logger
from .lightrag_client import get_working_dir

logger = get_logger('mirofish.lightrag_entity_reader')


@dataclass
class EntityNode:
    """Entity node data structure (matches zep_entity_reader.EntityNode)."""
    uuid: str
    name: str
    labels: List[str]
    summary: str
    attributes: Dict[str, Any]
    related_edges: List[Dict[str, Any]] = field(default_factory=list)
    related_nodes: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "labels": self.labels,
            "summary": self.summary,
            "attributes": self.attributes,
            "related_edges": self.related_edges,
            "related_nodes": self.related_nodes,
        }

    def get_entity_type(self) -> Optional[str]:
        """Return the entity type label (excludes generic 'Entity'/'Node' labels)."""
        for label in self.labels:
            if label not in ("Entity", "Node"):
                return label
        return None


@dataclass
class FilteredEntities:
    """Filtered entity collection (matches zep_entity_reader.FilteredEntities)."""
    entities: List[EntityNode]
    entity_types: Set[str]
    total_count: int
    filtered_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entities": [e.to_dict() for e in self.entities],
            "entity_types": list(self.entity_types),
            "total_count": self.total_count,
            "filtered_count": self.filtered_count,
        }


import time as _time
import threading

_graph_cache: Dict[str, Any] = {}  # graph_id -> (graph, timestamp)
_graph_cache_lock = threading.Lock()


def _load_graph(graph_id: str):
    """Read the LightRAG GraphML file for a given graph_id. Returns empty Graph if not found.
    Results are cached with configurable TTL to avoid repeated disk reads."""
    import networkx as nx

    cache_ttl = Config.CACHE_TTL_SECONDS
    cache_max = Config.CACHE_MAX_SIZE
    now = _time.time()
    with _graph_cache_lock:
        if graph_id in _graph_cache:
            graph, ts = _graph_cache[graph_id]
            if now - ts < cache_ttl:
                return graph

    working_dir = get_working_dir(graph_id)
    graphml_path = os.path.join(working_dir, 'graph_chunk_entity_relation.graphml')
    if not os.path.exists(graphml_path):
        logger.warning(f"GraphML not found for graph_id={graph_id}: {graphml_path}")
        return nx.Graph()
    graph = nx.read_graphml(graphml_path)
    with _graph_cache_lock:
        # Evict expired entries
        expired = [k for k, (_, ts) in list(_graph_cache.items()) if now - ts >= cache_ttl]
        for k in expired:
            _graph_cache.pop(k, None)
        # Evict oldest if cache exceeds max size
        while len(_graph_cache) >= cache_max:
            oldest_key = min(_graph_cache, key=lambda ck: _graph_cache[ck][1])
            _graph_cache.pop(oldest_key, None)
        _graph_cache[graph_id] = (graph, now)
    return graph


class ZepEntityReader:
    """
    Drop-in replacement for the original ZepEntityReader.
    Reads entities directly from LightRAG's local GraphML file.
    Constructor accepts optional api_key for signature compatibility (ignored).
    """

    def __init__(self, api_key: Optional[str] = None):
        pass  # No API key needed for local LightRAG storage

    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        """Return all entity nodes from the LightRAG graph."""
        G = _load_graph(graph_id)
        nodes = []
        for node_id, data in G.nodes(data=True):
            entity_type = data.get('entity_type', 'UNKNOWN').upper()
            nodes.append({
                "uuid": node_id,
                "name": data.get('entity_name', node_id),
                "labels": ["Entity", entity_type],
                "summary": data.get('description', ''),
                "attributes": dict(data),
            })
        logger.debug(f"get_all_nodes: graph_id={graph_id}, count={len(nodes)}")
        return nodes

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        """Return all edges from the LightRAG graph."""
        G = _load_graph(graph_id)
        edges = []
        for src, tgt, data in G.edges(data=True):
            edges.append({
                "uuid": f"{src}-{tgt}",
                "name": data.get('keywords', data.get('relation_name', 'RELATED_TO')),
                "fact": data.get('description', ''),
                "source_node_uuid": src,
                "target_node_uuid": tgt,
                "attributes": dict(data),
            })
        return edges

    def get_entity_with_context(
        self,
        graph_id: str,
        entity_uuid: str,
    ) -> Optional[EntityNode]:
        """
        Get a single entity with its full context (edges and related nodes).

        Args:
            graph_id: Graph ID
            entity_uuid: Entity UUID

        Returns:
            EntityNode or None
        """
        try:
            G = _load_graph(graph_id)
            if entity_uuid not in G.nodes:
                return None

            data = dict(G.nodes[entity_uuid])
            entity_type = data.get('entity_type', 'UNKNOWN').upper()

            # Build edges for this node
            all_edges = self.get_all_edges(graph_id)
            all_nodes = self.get_all_nodes(graph_id)
            node_map = {n["uuid"]: n for n in all_nodes}

            related_edges = []
            related_node_uuids: Set[str] = set()

            for edge in all_edges:
                if edge["source_node_uuid"] == entity_uuid:
                    related_edges.append({
                        "direction": "outgoing",
                        "edge_name": edge["name"],
                        "fact": edge["fact"],
                        "target_node_uuid": edge["target_node_uuid"],
                    })
                    related_node_uuids.add(edge["target_node_uuid"])
                elif edge["target_node_uuid"] == entity_uuid:
                    related_edges.append({
                        "direction": "incoming",
                        "edge_name": edge["name"],
                        "fact": edge["fact"],
                        "source_node_uuid": edge["source_node_uuid"],
                    })
                    related_node_uuids.add(edge["source_node_uuid"])

            related_nodes = [
                {
                    "uuid": ru,
                    "name": node_map[ru]["name"],
                    "labels": node_map[ru]["labels"],
                    "summary": node_map[ru]["summary"],
                }
                for ru in related_node_uuids if ru in node_map
            ]

            return EntityNode(
                uuid=entity_uuid,
                name=data.get('entity_name', entity_uuid),
                labels=["Entity", entity_type],
                summary=data.get('description', ''),
                attributes=data,
                related_edges=related_edges,
                related_nodes=related_nodes,
            )

        except Exception as e:
            logger.error(f"Failed to get entity {entity_uuid}: {str(e)}")
            return None

    def filter_defined_entities(
        self,
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True,
    ) -> FilteredEntities:
        """
        Return entities from the LightRAG graph, optionally filtered by type.

        LightRAG auto-extracts entity types (PERSON, ORGANIZATION, LOCATION, etc.)
        rather than using a custom ontology. If defined_entity_types is provided,
        case-insensitive matching is applied. If None, all entities are returned.
        """
        all_nodes = self.get_all_nodes(graph_id)
        total_count = len(all_nodes)

        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []
        node_map = {n["uuid"]: n for n in all_nodes}

        filtered: List[EntityNode] = []
        entity_types_found: Set[str] = set()

        for node in all_nodes:
            labels = node.get("labels", [])
            custom_labels = [l for l in labels if l not in ("Entity", "Node")]
            entity_type = custom_labels[0] if custom_labels else "UNKNOWN"

            # Apply type filter if specified
            if defined_entity_types:
                match = next(
                    (t for t in defined_entity_types
                     if t.upper() == entity_type.upper()),
                    None,
                )
                if not match:
                    continue
                entity_type = match

            entity_types_found.add(entity_type)
            entity = EntityNode(
                uuid=node["uuid"],
                name=node["name"],
                labels=labels,
                summary=node["summary"],
                attributes=node["attributes"],
            )

            if enrich_with_edges:
                related_edges = []
                related_node_uuids: Set[str] = set()
                for edge in all_edges:
                    if edge["source_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "outgoing",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "target_node_uuid": edge["target_node_uuid"],
                        })
                        related_node_uuids.add(edge["target_node_uuid"])
                    elif edge["target_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "incoming",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "source_node_uuid": edge["source_node_uuid"],
                        })
                        related_node_uuids.add(edge["source_node_uuid"])

                entity.related_edges = related_edges
                entity.related_nodes = [
                    {
                        "uuid": ru,
                        "name": node_map[ru]["name"],
                        "labels": node_map[ru]["labels"],
                        "summary": node_map[ru]["summary"],
                    }
                    for ru in related_node_uuids if ru in node_map
                ]

            filtered.append(entity)

        logger.info(f"filter_defined_entities: graph_id={graph_id}, "
                    f"total={total_count}, filtered={len(filtered)}")
        return FilteredEntities(
            entities=filtered,
            entity_types=entity_types_found,
            total_count=total_count,
            filtered_count=len(filtered),
        )

    def get_entities_by_type(
        self, graph_id: str, entity_type: str, enrich_with_edges: bool = True
    ) -> List[EntityNode]:
        """Return entities matching a specific type."""
        result = self.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=[entity_type],
            enrich_with_edges=enrich_with_edges,
        )
        return result.entities
