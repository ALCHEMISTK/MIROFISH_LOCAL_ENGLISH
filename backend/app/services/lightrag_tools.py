"""
LightRAG retrieval tools service.
Drop-in replacement for zep_tools.py.
Maps Zep search patterns to LightRAG query modes.
Exports identical class/dataclass names so callers need only swap the import.
"""

import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from ..utils.logger import get_logger
from ..utils.llm_client import LLMClient
from .lightrag_client import get_rag, get_working_dir, run_async

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

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "sub_questions": self.sub_questions,
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
            f"Prediction scenario: (simulation context)",
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
    Maps Zep search patterns to LightRAG query modes.
    Constructor accepts optional api_key for signature compatibility (ignored).
    """

    def __init__(self, api_key: Optional[str] = None):
        pass  # No API key needed

    def _query_rag(self, graph_id: str, query: str, mode: str = "hybrid") -> str:
        """Run a LightRAG query and return the text result."""
        rag = get_rag(graph_id, create_if_missing=False)
        if not rag:
            logger.warning(f"No LightRAG instance for graph_id={graph_id}")
            return ""
        try:
            from lightrag import QueryParam
            result = run_async(rag.aquery(query, param=QueryParam(mode=mode)))
            return result or ""
        except Exception as e:
            logger.error(f"LightRAG query failed (mode={mode}): {e}")
            return ""

    def _parse_facts(self, text: str, limit: int = 20) -> List[str]:
        """Parse LightRAG text output into a list of fact strings."""
        lines = [l.strip() for l in text.split('\n') if l.strip() and not l.startswith('#')]
        return lines[:limit]

    def search_graph(
        self,
        graph_id: str,
        query: str,
        limit: int = 10,
        scope: str = "edges",
    ) -> SearchResult:
        """Quick local search (maps to LightRAG local mode)."""
        raw = self._query_rag(graph_id, query, mode="local")
        facts = self._parse_facts(raw, limit=limit)
        return SearchResult(
            facts=facts,
            edges=[],
            nodes=[],
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
    ) -> InsightForgeResult:
        """Deep hybrid search (maps to LightRAG hybrid mode)."""
        raw = self._query_rag(graph_id, query, mode="hybrid")
        facts = self._parse_facts(raw, limit=max_facts)

        # Generate sub-questions via LLM if requested
        sub_questions: List[str] = []
        if generate_sub_questions and query:
            try:
                llm = LLMClient()
                resp = llm.chat([
                    {"role": "user", "content":
                     f"Generate 3 specific sub-questions to deeply investigate this topic: {query}\n"
                     "Output only the questions, one per line."}
                ])
                sub_questions = [l.strip() for l in resp.split('\n') if l.strip()][:3]
                # Run additional queries for each sub-question and merge results
                for sq in sub_questions:
                    sq_raw = self._query_rag(graph_id, sq, mode="local")
                    sq_facts = self._parse_facts(sq_raw, limit=5)
                    for f in sq_facts:
                        if f not in facts:
                            facts.append(f)
            except Exception as e:
                logger.warning(f"Sub-question generation failed: {e}")

        return InsightForgeResult(
            query=query,
            sub_questions=sub_questions,
            facts=facts[:max_facts],
            nodes=[],
            edges=[],
            total_facts=len(facts),
            total_nodes=0,
            total_edges=0,
        )

    def panorama_search(self, graph_id: str, query: str = "", include_expired: bool = True) -> PanoramaResult:
        """
        Full graph snapshot — reads all nodes and edges from the GraphML file.
        Also runs a global LightRAG query to get a community-level summary.
        """
        import networkx as nx
        working_dir = get_working_dir(graph_id)
        graphml_path = os.path.join(working_dir, 'graph_chunk_entity_relation.graphml')

        nodes_info: List[NodeInfo] = []
        edges_info: List[EdgeInfo] = []

        if os.path.exists(graphml_path):
            G = nx.read_graphml(graphml_path)
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
        summary_text = self._query_rag(
            graph_id,
            "Provide a comprehensive overview of all entities, their relationships, and the main themes in this knowledge graph.",
            mode="global",
        )
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
        raw = self._query_rag(graph_id, query, mode="naive")
        facts = self._parse_facts(raw, limit=limit)
        return SearchResult(
            facts=facts,
            edges=[],
            nodes=[],
            query=query,
            total_count=len(facts),
        )

    def get_all_nodes(self, graph_id: str) -> List[NodeInfo]:
        """Return all nodes from the graph."""
        import networkx as nx
        working_dir = get_working_dir(graph_id)
        graphml_path = os.path.join(working_dir, 'graph_chunk_entity_relation.graphml')
        if not os.path.exists(graphml_path):
            return []
        G = nx.read_graphml(graphml_path)
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
        import networkx as nx
        working_dir = get_working_dir(graph_id)
        graphml_path = os.path.join(working_dir, 'graph_chunk_entity_relation.graphml')
        if not os.path.exists(graphml_path):
            return []
        G = nx.read_graphml(graphml_path)
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
        import networkx as nx
        working_dir = get_working_dir(graph_id)
        graphml_path = os.path.join(working_dir, 'graph_chunk_entity_relation.graphml')
        if not os.path.exists(graphml_path):
            return []
        G = nx.read_graphml(graphml_path)
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
            raw = self._query_rag(graph_id, entity_perspective, mode="local")
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
