"""
LightRAG retrieval tools service.
Drop-in replacement for zep_tools.py.
Maps Zep search patterns to LightRAG query modes.
Exports identical class/dataclass names so callers need only swap the import.
"""

import os
import threading
import networkx as nx
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from ..config import Config
from ..utils.logger import get_logger
from ..utils.llm_client import LLMClient
from .lightrag_client import get_working_dir

logger = get_logger('mirofish.lightrag_tools')


@dataclass
class SearchResult:
    """Search result (matches zep_tools.SearchResult)."""
    facts: List[str]
    edges: List[Dict[str, Any]]
    nodes: List[Dict[str, Any]]
    query: str
    total_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "facts": self.facts,
            "edges": self.edges,
            "nodes": self.nodes,
            "query": self.query,
            "total_count": self.total_count,
        }

    def to_text(self) -> str:
        text_parts = [f"Search query: {self.query}", f"Found {self.total_count} relevant results"]
        if self.facts:
            text_parts.append("\n### Relevant facts:")
            for i, fact in enumerate(self.facts, 1):
                text_parts.append(f"{i}. {fact}")
        return "\n".join(text_parts)


@dataclass
class NodeInfo:
    """Node info (matches zep_tools.NodeInfo)."""
    uuid: str
    name: str
    labels: List[str]
    summary: str
    attributes: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "labels": self.labels,
            "summary": self.summary,
            "attributes": self.attributes,
        }

    def to_text(self) -> str:
        entity_type = next((l for l in self.labels if l not in ("Entity", "Node")), "Unknown")
        return f"Entity: {self.name} (type: {entity_type})\nSummary: {self.summary}"


@dataclass
class EdgeInfo:
    """Edge info (matches zep_tools.EdgeInfo)."""
    uuid: str
    name: str
    fact: str
    source_node_uuid: str
    target_node_uuid: str
    source_node_name: str
    target_node_name: str
    attributes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "fact": self.fact,
            "source_node_uuid": self.source_node_uuid,
            "target_node_uuid": self.target_node_uuid,
            "source_node_name": self.source_node_name,
            "target_node_name": self.target_node_name,
            "attributes": self.attributes,
        }

    def to_text(self) -> str:
        return f"{self.source_node_name} → {self.name} → {self.target_node_name}: {self.fact}"


@dataclass
class InsightForgeResult:
    """Deep insight retrieval result (matches zep_tools.InsightForgeResult)."""
    query: str
    sub_questions: List[str]
    facts: List[str]
    nodes: List[NodeInfo]
    edges: List[EdgeInfo]
    total_facts: int
    total_nodes: int
    total_edges: int
    simulation_requirement: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "simulation_requirement": self.simulation_requirement,
            "sub_questions": self.sub_questions,
            "sub_queries": self.sub_questions,
            "facts": self.facts,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "total_facts": self.total_facts,
            "total_nodes": self.total_nodes,
            "total_edges": self.total_edges,
        }

    def to_text(self) -> str:
        """Produce structured text consumed by the frontend's parseInsightForge parser."""
        parts = [
            f"Analysis question: {self.query}",
            f"Prediction scenario: {self.simulation_requirement or '(simulation context)'}",
            f"Related prediction facts: {self.total_facts}",
            f"Entities involved: {self.total_nodes}",
            f"Relation chains: {self.total_edges}",
        ]

        if self.sub_questions:
            parts.append("\n### Analyzed sub-questions")
            for i, q in enumerate(self.sub_questions, 1):
                parts.append(f"{i}. {q}")

        if self.facts:
            parts.append("\n### [Key Facts]")
            for i, f in enumerate(self.facts, 1):
                parts.append(f'{i}. "{f}"')

        if self.nodes:
            parts.append("\n### [Core Entities]")
            for node in self.nodes:
                entity_type = next(
                    (l for l in node.labels if l not in ("Entity", "Node")), "Entity"
                ) if hasattr(node, 'labels') else "Entity"
                parts.append(f"- **{node.name}** ({entity_type})")
                if node.summary:
                    parts.append(f'  Summary: "{node.summary}"')
                related = sum(
                    1 for e in self.edges
                    if hasattr(e, 'source_node_name') and (
                        e.source_node_name == node.name or e.target_node_name == node.name
                    )
                )
                parts.append(f"  Related facts: {related}")

        if self.edges:
            parts.append("\n### [Relation Chains]")
            for edge in self.edges:
                if hasattr(edge, 'source_node_name'):
                    parts.append(
                        f"- {edge.source_node_name} --[{edge.name}]--> {edge.target_node_name}"
                    )

        return "\n".join(parts)


@dataclass
class PanoramaResult:
    """Full graph snapshot result (matches zep_tools.PanoramaResult)."""
    graph_id: str
    nodes: List[NodeInfo]
    edges: List[EdgeInfo]
    total_nodes: int
    total_edges: int
    summary_text: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "total_nodes": self.total_nodes,
            "total_edges": self.total_edges,
            "summary_text": self.summary_text,
        }

    def to_text(self) -> str:
        """Produce structured text consumed by the frontend's parsePanorama parser."""
        # Heuristically split active vs historical facts from the summary text
        active_facts = []
        historical_facts = []
        for edge in self.edges[:30]:
            if edge.fact:
                active_facts.append(edge.fact)

        parts = [
            f"Query: (graph overview)",
            f"Total nodes: {self.total_nodes}",
            f"Total edges: {self.total_edges}",
            f"Active facts: {len(active_facts)}",
            f"Historical/expired facts: 0",
        ]

        if active_facts:
            parts.append("\n### [Active Facts]")
            for i, f in enumerate(active_facts, 1):
                parts.append(f'{i}. "{f}"')

        if self.nodes:
            parts.append("\n### [Involved Entities]")
            for node in self.nodes[:20]:
                entity_type = next(
                    (l for l in node.labels if l not in ("Entity", "Node")), "Entity"
                ) if hasattr(node, 'labels') else "Entity"
                parts.append(f"- **{node.name}** ({entity_type})")

        # Append the LLM community summary at the end
        if self.summary_text:
            parts.append(f"\n{self.summary_text}")

        return "\n".join(parts)


@dataclass
class InterviewResult:
    """Interview result (matches zep_tools.InterviewResult)."""
    agent_id: int
    agent_name: str
    response: str
    platform: str
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "response": self.response,
            "platform": self.platform,
            "timestamp": self.timestamp,
        }


@dataclass
class AgentInterview:
    """Agent interview data (matches zep_tools.AgentInterview)."""
    agent_id: int
    prompt: str
    platform: Optional[str] = None


class ZepToolsService:
    """
    Drop-in replacement for ZepToolsService.
    Reads the knowledge graph directly from GraphML and uses the LLM
    (via LLMClient) to synthesise answers. Works for both local Ollama
    and any cloud API configured in .env - no LightRAG vector engine needed.
    Constructor accepts optional api_key for signature compatibility (ignored).

    TODO: This class has grown into a god-class (650+ lines, 25+ methods).
    Future refactor should split it into:
      - GraphReader: graph loading, caching, node/edge retrieval
      - QuerySynthesizer: LLM-based search, insight_forge, panorama
      - InterviewSimulator: agent interview logic
    """

    _MODE_NODE_LIMIT = {"global": 30, "hybrid": 20, "local": 15, "naive": 10}
    _MODE_EDGE_LIMIT = {"global": 30, "hybrid": 20, "local": 15, "naive": 10}

    def __init__(self, api_key: Optional[str] = None, llm_client: Optional[LLMClient] = None):
        self._llm_client = llm_client
        self._graph_cache: Dict[str, Any] = {}  # graph_id -> (graph, timestamp)
        self._graph_cache_lock = threading.Lock()

    def _get_graph(self, graph_id: str):
        """Load and cache GraphML to avoid repeated disk reads."""
        import time as _time
        cache_ttl = Config.CACHE_TTL_SECONDS
        cache_max = Config.CACHE_MAX_SIZE
        now = _time.time()
        with self._graph_cache_lock:
            if graph_id in self._graph_cache:
                graph, ts = self._graph_cache[graph_id]
                if now - ts < cache_ttl:
                    return graph

        graphml_path = os.path.join(get_working_dir(graph_id), "graph_chunk_entity_relation.graphml")
        if not os.path.exists(graphml_path):
            return None
        try:
            graph = nx.read_graphml(graphml_path)
            with self._graph_cache_lock:
                # Evict expired entries
                expired = [k for k, (_, ts) in list(self._graph_cache.items()) if now - ts >= cache_ttl]
                for k in expired:
                    self._graph_cache.pop(k, None)
                # Evict oldest if cache exceeds max size
                while len(self._graph_cache) >= cache_max:
                    oldest_key = min(self._graph_cache, key=lambda k: self._graph_cache[k][1])
                    self._graph_cache.pop(oldest_key, None)
                self._graph_cache[graph_id] = (graph, now)
            return graph
        except Exception as e:
            logger.warning(f"Could not read GraphML for {graph_id}: {e}")
            return None

    @property
    def llm(self) -> LLMClient:
        if self._llm_client is None:
            self._llm_client = LLMClient()
        return self._llm_client

    def _llm_call_with_retry(self, prompt: str, max_retries: int = 3) -> str:
        """
        Call LLM with exponential backoff on 429 rate-limit errors.
        Uses LLMClient which routes to local Ollama or cloud API based on .env.
        """
        import time

        for attempt in range(max_retries):
            try:
                return self.llm.chat([{"role": "user", "content": prompt}])
            except Exception as e:
                err = str(e)
                if "429" in err or "rate" in err.lower():
                    wait = 15.0 * (attempt + 1)
                    logger.warning(
                        f"LLM rate limited in graph query. "
                        f"Waiting {wait}s (retry {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"LLM call failed: {e}")
                    raise

        logger.warning(f"All {max_retries} LLM retry attempts exhausted for query")
        return ""

    def _query_rag(self, graph_id: str, query: str, mode: str = "hybrid") -> Dict[str, Any]:
        """
        Search the knowledge graph and synthesise an answer using the LLM.

        Replaces LightRAG's rag.aquery():
          - Reads GraphML directly via NetworkX (already used elsewhere in the app)
          - No vector DB, no Ollama embeddings, no timeouts
          - LLM synthesis via LLMClient (auto-detects local Ollama vs cloud API)
          - Rate-limit safe: retries with backoff on 429

        mode semantics mirror LightRAG naming:
          local  -> top matching entities + their direct edges
          hybrid -> broader match across nodes AND relationships
          global -> full graph overview
          naive  -> quick keyword match, minimal context

        Returns a dict with keys: text, nodes, edges
        """
        _empty = {"text": "", "nodes": [], "edges": []}
        G = self._get_graph(graph_id)
        if G is None or G.number_of_nodes() == 0:
            return _empty

        node_limit = self._MODE_NODE_LIMIT.get(mode, 15)
        edge_limit = self._MODE_EDGE_LIMIT.get(mode, 15)

        node_name_map = {
            nid: data.get("name", data.get("entity_name", nid))
            for nid, data in G.nodes(data=True)
        }

        query_words = {
            word.lower()
            for word in query.replace(",", " ").replace(".", " ").split()
            if len(word) > 2
        }

        if mode == "global":
            selected_nodes = list(G.nodes(data=True))[:node_limit]
            selected_edges = list(G.edges(data=True))[:edge_limit]
        else:
            scored_nodes = []
            for nid, data in G.nodes(data=True):
                text = " ".join([
                    data.get("name", ""),
                    data.get("entity_name", ""),
                    data.get("description", ""),
                    data.get("type", ""),
                    data.get("entity_type", ""),
                ]).lower()
                score = sum(1 for word in query_words if word in text)
                scored_nodes.append((score, nid, data))
            scored_nodes.sort(key=lambda item: item[0], reverse=True)
            selected_nodes = (
                [(nid, data) for _, nid, data in scored_nodes[:node_limit]]
                or list(G.nodes(data=True))[:node_limit]
            )

            scored_edges = []
            for src, tgt, data in G.edges(data=True):
                text = " ".join([
                    data.get("description", ""),
                    data.get("type", ""),
                    data.get("keywords", ""),
                    node_name_map.get(src, ""),
                    node_name_map.get(tgt, ""),
                ]).lower()
                score = sum(1 for word in query_words if word in text)
                scored_edges.append((score, src, tgt, data))
            scored_edges.sort(key=lambda item: item[0], reverse=True)
            selected_edges = (
                [(src, tgt, data) for _, src, tgt, data in scored_edges[:edge_limit]]
                or list(G.edges(data=True))[:edge_limit]
            )

        # Build structured node/edge dicts for callers
        result_nodes = []
        for nid, data in selected_nodes:
            name = data.get("name", data.get("entity_name", nid))
            entity_type = data.get("type", data.get("entity_type", ""))
            desc = data.get("description", "")
            result_nodes.append({
                "id": nid,
                "name": name,
                "type": entity_type,
                "description": desc,
            })

        result_edges = []
        for src, tgt, data in selected_edges:
            rel = data.get("type", data.get("keywords", "RELATED_TO"))
            desc = data.get("description", "")
            src_name = node_name_map.get(src, src)
            tgt_name = node_name_map.get(tgt, tgt)
            result_edges.append({
                "source": src_name,
                "target": tgt_name,
                "relation": rel,
                "description": desc,
            })

        node_lines = []
        for n in result_nodes:
            if n["type"]:
                node_lines.append(f"- {n['name']} ({n['type']}): {n['description']}")
            else:
                node_lines.append(f"- {n['name']}: {n['description']}")

        edge_lines = []
        for e in result_edges:
            edge_lines.append(f"- {e['source']} --[{e['relation']}]--> {e['target']}: {e['description']}")

        context_parts = []
        if node_lines:
            context_parts.append("Entities:\n" + "\n".join(node_lines))
        if edge_lines:
            context_parts.append("Relationships:\n" + "\n".join(edge_lines))
        context = "\n\n".join(context_parts)

        if not context:
            return _empty

        synthesis_prompt = (
            "Based on the following knowledge graph data, answer this question concisely:\n"
            f"Question: {query}\n\n"
            f"Knowledge graph context:\n{context}\n\n"
            "Provide a concise factual answer based only on the information above. "
            "If the information is insufficient, briefly summarise what is available."
        )

        llm_failed = False
        try:
            result = self._llm_call_with_retry(synthesis_prompt)
            logger.info(f"_query_rag ({mode}): {query[:50]}... -> {len(result)} chars")
            text = result or context
            if not result:
                llm_failed = True
        except Exception as e:
            logger.warning(f"_query_rag LLM synthesis failed, returning raw context: {e}")
            text = context
            llm_failed = True

        return {"text": text, "nodes": result_nodes, "edges": result_edges, "llm_failed": llm_failed}

    def _parse_facts(self, text: str, limit: int = 20, raw_context: bool = False) -> List[str]:
        """Parse LightRAG text output into a list of fact strings."""
        lines = [l.strip() for l in text.split('\n') if l.strip() and not l.startswith('#')]
        if raw_context:
            # Filter out formatting artifacts from raw context fallback
            lines = [f for f in lines if f and not f.startswith("Entities:") and not f.startswith("Relationships:") and not f.startswith("---")]
        return lines[:limit]

    def search_graph(
        self,
        graph_id: str,
        query: str,
        limit: int = 10,
        scope: str = "edges",
    ) -> SearchResult:
        """Quick local search (maps to LightRAG local mode)."""
        result = self._query_rag(graph_id, query, mode="local")
        facts = self._parse_facts(result["text"], limit=limit, raw_context=result.get("llm_failed", False))
        return SearchResult(
            facts=facts,
            edges=result["edges"],
            nodes=result["nodes"],
            query=query,
            total_count=len(facts),
        )

    def insight_forge(
        self,
        graph_id: str,
        query: str,
        max_facts: int = 20,
        max_nodes: int = 10,
        max_edges: int = 10,
        generate_sub_questions: bool = True,
        simulation_requirement: str = "",
        report_context: str = "",
        max_sub_queries: int = 5,
    ) -> InsightForgeResult:
        """Deep hybrid search (maps to LightRAG hybrid mode)."""
        logger.info(f"InsightForge deep insight retrieval: {query[:80]}...")
        result = self._query_rag(graph_id, query, mode="hybrid")
        facts = self._parse_facts(result["text"], limit=max_facts, raw_context=result.get("llm_failed", False))
        collected_nodes = list(result["nodes"])
        collected_edges = list(result["edges"])

        # Generate sub-questions via LLM if requested
        sub_questions: List[str] = []
        bounded_sub_queries = min(max_sub_queries, 2)
        if generate_sub_questions and query:
            try:
                context_lines = []
                if simulation_requirement:
                    context_lines.append(f"Simulation background: {simulation_requirement}")
                if report_context:
                    context_lines.append(f"Report context: {report_context[:800]}")
                context_block = "\n".join(context_lines)
                prompt = (
                    f"{context_block}\n\n" if context_block else ""
                ) + (
                    f"Generate {bounded_sub_queries} specific sub-questions to deeply investigate this topic: {query}\n"
                    "Output only the questions, one per line."
                )
                resp = self._llm_call_with_retry(prompt)
                sub_questions = [l.strip() for l in resp.split('\n') if l.strip()][:bounded_sub_queries]
                logger.info(f"InsightForge generated {len(sub_questions)} sub-questions")
                # Run additional queries for each sub-question and merge results
                for sq in sub_questions:
                    sq_result = self._query_rag(graph_id, sq, mode="local")
                    sq_facts = self._parse_facts(sq_result["text"], limit=5, raw_context=sq_result.get("llm_failed", False))
                    for f in sq_facts:
                        if f not in facts:
                            facts.append(f)
                    # Merge nodes/edges from sub-queries
                    for n in sq_result["nodes"]:
                        if n not in collected_nodes:
                            collected_nodes.append(n)
                    for e in sq_result["edges"]:
                        if e not in collected_edges:
                            collected_edges.append(e)
            except Exception as e:
                logger.warning(f"Sub-question generation failed: {e}")

        logger.info(
            f"InsightForge complete: query={query[:50]}..., facts={len(facts)}, "
            f"sub_questions={len(sub_questions)}"
        )

        # Convert collected dicts to NodeInfo/EdgeInfo objects
        node_infos = [
            NodeInfo(
                uuid=n.get("id", ""),
                name=n.get("name", ""),
                labels=["Entity", n.get("type", "UNKNOWN")] if n.get("type") else ["Entity"],
                summary=n.get("description", ""),
                attributes={},
            )
            for n in collected_nodes[:max_nodes]
        ]
        edge_infos = [
            EdgeInfo(
                uuid=f"{e.get('source', '')}-{e.get('target', '')}",
                name=e.get("relation", "RELATED_TO"),
                fact=e.get("description", ""),
                source_node_uuid="",
                target_node_uuid="",
                source_node_name=e.get("source", ""),
                target_node_name=e.get("target", ""),
            )
            for e in collected_edges[:max_edges]
        ]

        return InsightForgeResult(
            query=query,
            simulation_requirement=simulation_requirement,
            sub_questions=sub_questions,
            facts=facts[:max_facts],
            nodes=node_infos,
            edges=edge_infos,
            total_facts=len(facts),
            total_nodes=len(node_infos),
            total_edges=len(edge_infos),
        )

    def panorama_search(self, graph_id: str, query: str = "", include_expired: bool = True) -> PanoramaResult:
        """
        Full graph snapshot — reads all nodes and edges from the GraphML file.
        Also runs a global LightRAG query to get a community-level summary.
        """
        nodes_info: List[NodeInfo] = []
        edges_info: List[EdgeInfo] = []

        G = self._get_graph(graph_id)
        if G is not None:
            node_name_map: Dict[str, str] = {}

            for node_id, data in G.nodes(data=True):
                entity_type = data.get('entity_type', 'UNKNOWN').upper()
                name = data.get('entity_name', node_id)
                node_name_map[node_id] = name
                nodes_info.append(NodeInfo(
                    uuid=node_id,
                    name=name,
                    labels=["Entity", entity_type],
                    summary=data.get('description', ''),
                    attributes=dict(data),
                ))

            for src, tgt, data in G.edges(data=True):
                edges_info.append(EdgeInfo(
                    uuid=f"{src}-{tgt}",
                    name=data.get('keywords', data.get('relation_name', 'RELATED_TO')),
                    fact=data.get('description', ''),
                    source_node_uuid=src,
                    target_node_uuid=tgt,
                    source_node_name=node_name_map.get(src, src),
                    target_node_name=node_name_map.get(tgt, tgt),
                ))

        # Get global community summary from LightRAG
        summary_result = self._query_rag(
            graph_id,
            "Provide a comprehensive overview of all entities, their relationships, and the main themes in this knowledge graph.",
            mode="global",
        )
        summary_text = summary_result["text"]
        if not summary_text:
            entity_names = [n.name for n in nodes_info[:20]]
            summary_text = (
                f"Graph contains {len(nodes_info)} entities and {len(edges_info)} relationships.\n"
                f"Key entities: {', '.join(entity_names)}"
            )

        return PanoramaResult(
            graph_id=graph_id,
            nodes=nodes_info,
            edges=edges_info,
            total_nodes=len(nodes_info),
            total_edges=len(edges_info),
            summary_text=summary_text,
        )

    def quick_search(
        self,
        graph_id: str,
        query: str,
        limit: int = 5,
    ) -> SearchResult:
        """Fast naive search."""
        result = self._query_rag(graph_id, query, mode="naive")
        facts = self._parse_facts(result["text"], limit=limit, raw_context=result.get("llm_failed", False))
        return SearchResult(
            facts=facts,
            edges=result["edges"],
            nodes=result["nodes"],
            query=query,
            total_count=len(facts),
        )

    def get_all_nodes(self, graph_id: str) -> List[NodeInfo]:
        """Return all nodes from the graph."""
        G = self._get_graph(graph_id)
        if G is None:
            return []
        return [
            NodeInfo(
                uuid=nid,
                name=data.get('entity_name', nid),
                labels=["Entity", data.get('entity_type', 'UNKNOWN').upper()],
                summary=data.get('description', ''),
                attributes=dict(data),
            )
            for nid, data in G.nodes(data=True)
        ]

    def get_all_edges(self, graph_id: str) -> List[EdgeInfo]:
        """Return all edges from the graph."""
        G = self._get_graph(graph_id)
        if G is None:
            return []
        node_name_map = {nid: d.get('entity_name', nid) for nid, d in G.nodes(data=True)}
        return [
            EdgeInfo(
                uuid=f"{src}-{tgt}",
                name=data.get('keywords', 'RELATED_TO'),
                fact=data.get('description', ''),
                source_node_uuid=src,
                target_node_uuid=tgt,
                source_node_name=node_name_map.get(src, src),
                target_node_name=node_name_map.get(tgt, tgt),
            )
            for src, tgt, data in G.edges(data=True)
        ]

    def get_node_edges(self, graph_id: str, node_uuid: str) -> List[EdgeInfo]:
        """Return edges incident to the given node."""
        G = self._get_graph(graph_id)
        if G is None:
            return []
        node_name_map = {nid: d.get('entity_name', nid) for nid, d in G.nodes(data=True)}
        results = []
        for src, tgt, data in G.edges(data=True):
            if src == node_uuid or tgt == node_uuid:
                results.append(EdgeInfo(
                    uuid=f"{src}-{tgt}",
                    name=data.get('keywords', 'RELATED_TO'),
                    fact=data.get('description', ''),
                    source_node_uuid=src,
                    target_node_uuid=tgt,
                    source_node_name=node_name_map.get(src, src),
                    target_node_name=node_name_map.get(tgt, tgt),
                ))
        return results

    def get_entities_by_type(self, graph_id: str, entity_type: str) -> List[NodeInfo]:
        """Return all nodes whose entity_type matches (case-insensitive)."""
        all_nodes = self.get_all_nodes(graph_id)
        matched = [
            n for n in all_nodes
            if entity_type.upper() in [l.upper() for l in n.labels]
        ]
        logger.info(f"get_entities_by_type({entity_type}): {len(matched)} found")
        return matched

    def get_entity_summary(self, graph_id: str, entity_name: str) -> Dict[str, Any]:
        """Get an entity's node info, related facts, and incident edges."""
        search_result = self.search_graph(graph_id=graph_id, query=entity_name, limit=20)
        all_nodes = self.get_all_nodes(graph_id)
        entity_node = next(
            (n for n in all_nodes if n.name.lower() == entity_name.lower()), None
        )
        related_edges = self.get_node_edges(graph_id, entity_node.uuid) if entity_node else []
        return {
            "entity_name": entity_name,
            "entity_info": entity_node.to_dict() if entity_node else None,
            "related_facts": search_result.facts,
            "related_edges": [e.to_dict() for e in related_edges],
            "total_relations": len(related_edges),
        }

    def get_graph_statistics(self, graph_id: str) -> Dict[str, Any]:
        """Return node/edge counts and type distributions."""
        nodes = self.get_all_nodes(graph_id)
        edges = self.get_all_edges(graph_id)
        entity_types: Dict[str, int] = {}
        for n in nodes:
            for label in n.labels:
                if label not in ("Entity", "Node"):
                    entity_types[label] = entity_types.get(label, 0) + 1
        relation_types: Dict[str, int] = {}
        for e in edges:
            relation_types[e.name] = relation_types.get(e.name, 0) + 1
        return {
            "graph_id": graph_id,
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "entity_types": entity_types,
            "relation_types": relation_types,
        }

    def get_simulation_context(
        self,
        graph_id: str,
        simulation_requirement: str,
        limit: int = 30,
    ) -> Dict[str, Any]:
        """Aggregate search results, graph stats, and entity list for report generation."""
        logger.info(f"get_simulation_context: {simulation_requirement[:60]}...")
        search_result = self.search_graph(
            graph_id=graph_id, query=simulation_requirement, limit=limit
        )
        stats = self.get_graph_statistics(graph_id)
        all_nodes = self.get_all_nodes(graph_id)
        entities = [
            {
                "name": n.name,
                "type": next((l for l in n.labels if l not in ("Entity", "Node")), "UNKNOWN"),
                "summary": n.summary,
            }
            for n in all_nodes
            if any(l not in ("Entity", "Node") for l in n.labels)
        ]
        return {
            "simulation_requirement": simulation_requirement,
            "related_facts": search_result.facts,
            "graph_statistics": stats,
            "entities": entities[:limit],
            "total_entities": len(entities),
        }

    def interview_agents(
        self,
        simulation_id: str,
        interview_requirement: str,
        simulation_requirement: str = "",
        max_agents: int = 5,
        custom_questions: Optional[List[str]] = None,
    ) -> "InterviewResult":
        """
        Simulate agent interviews using LightRAG queries.
        Queries the knowledge graph from each entity's perspective and synthesises responses.
        Returns an InterviewResult-compatible object.
        """
        import json as _json

        logger.info(f"interview_agents (LightRAG): {interview_requirement[:60]}...")

        # Resolve graph_id from simulation state file
        graph_id = self._resolve_graph_id(simulation_id)

        # Pick representative entities to 'interview'
        all_nodes = self.get_all_nodes(graph_id) if graph_id else []
        # Filter to non-generic nodes; prefer named entities
        candidate_nodes = [
            n for n in all_nodes
            if any(l not in ("Entity", "Node", "UNKNOWN") for l in n.labels)
        ][:max_agents]

        agent_responses: List[Dict[str, Any]] = []
        questions = custom_questions or [interview_requirement]

        for node in candidate_nodes:
            entity_perspective = f"From the perspective of {node.name}, {interview_requirement}"
            raw_result = self._query_rag(graph_id, entity_perspective, mode="local")
            raw = raw_result["text"]
            agent_responses.append({
                "agent_name": node.name,
                "agent_type": next((l for l in node.labels if l not in ("Entity", "Node")), "Unknown"),
                "response": raw[:800] if raw else f"No information available about {node.name}'s perspective.",
            })

        # Build a summary-style InterviewResult object using existing dataclass
        # We return a duck-typed object with the fields report_agent.py accesses
        class _InterviewResultCompat:
            def __init__(self, topic, questions, responses):
                self.interview_topic = topic
                self.interview_questions = questions
                self.agent_responses = responses
                self.total_agents = len(responses)
                self.selected_agents = [r["agent_name"] for r in responses]
                self.selection_reasoning = "Selected based on graph entity prominence"
                self.summary = "\n\n".join(
                    f"**{r['agent_name']} ({r['agent_type']})**: {r['response']}"
                    for r in responses
                )

            def to_dict(self):
                return {
                    "interview_topic": self.interview_topic,
                    "interview_questions": self.interview_questions,
                    "agent_responses": self.agent_responses,
                    "total_agents": self.total_agents,
                    "selected_agents": self.selected_agents,
                    "summary": self.summary,
                }

            def to_text(self):
                """Produce structured text consumed by the frontend's parseInterview parser."""
                parts = [
                    f"**Interview topic:** {self.interview_topic}",
                    f"**Interviewees:** {len(self.agent_responses)} / {self.total_agents} simulated agents",
                ]

                if self.selection_reasoning:
                    parts.append(f"\n### Interviewee selection rationale\n{self.selection_reasoning}\n---")

                parts.append(f"\n### Interview Transcripts")
                for i, r in enumerate(self.agent_responses, 1):
                    agent_type = r.get("agent_type", "Unknown")
                    parts.append(f"\n#### Interview #{i}: {r['agent_name']}")
                    parts.append(f"_Type: {agent_type}_")
                    parts.append(f"\n**Q:** {self.interview_topic}")
                    parts.append(f"\n**A:** {r['response']}")

                return "\n".join(parts)

        return _InterviewResultCompat(
            topic=interview_requirement,
            questions=questions,
            responses=agent_responses,
        )

    def _resolve_graph_id(self, simulation_id: str) -> Optional[str]:
        """Try to find graph_id for a simulation by reading its state file."""
        try:
            from ..services.simulation_manager import SimulationManager
            mgr = SimulationManager()
            state = mgr.get_simulation(simulation_id)
            if state:
                return state.graph_id
        except Exception:
            pass
        return None
