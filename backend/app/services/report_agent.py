"""
Report Agent service.
Implements ReACT-pattern simulation report generation using LangChain + Zep.

Features:
1. Generate reports based on simulation requirements and Zep graph data
2. Plan the outline structure first, then generate section by section
3. Each section uses a ReACT multi-turn thinking and reflection pattern
4. Supports conversation with users, autonomously calling retrieval tools during chat
"""

import os
import json
import time
import re
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from ..config import Config
from ..utils.llm_client import LLMClient
from ..utils.logger import get_logger
from .lightrag_tools import (
    ZepToolsService,
    SearchResult,
    InsightForgeResult,
    PanoramaResult,
    InterviewResult
)

logger = get_logger('mirofish.report_agent')


class ReportLogger:
    """
    Report Agent detailed logger.

    Generates an agent_log.jsonl file in the report folder, recording every step in detail.
    Each line is a complete JSON object containing timestamp, action type, detailed content, etc.
    """

    def __init__(self, report_id: str):
        """
        Initialize the logger.

        Args:
            report_id: Report ID, used to determine the log file path
        """
        self.report_id = report_id
        self.log_file_path = os.path.join(
            Config.UPLOAD_FOLDER, 'reports', report_id, 'agent_log.jsonl'
        )
        self.start_time = datetime.now()
        self._ensure_log_file()

    def _ensure_log_file(self):
        """Ensure the directory containing the log file exists."""
        log_dir = os.path.dirname(self.log_file_path)
        os.makedirs(log_dir, exist_ok=True)

    def _get_elapsed_time(self) -> float:
        """Get elapsed time in seconds since start."""
        return (datetime.now() - self.start_time).total_seconds()

    def log(
        self,
        action: str,
        stage: str,
        details: Dict[str, Any],
        section_title: str = None,
        section_index: int = None
    ):
        """
        Record a log entry.

        Args:
            action: Action type, e.g. 'start', 'tool_call', 'llm_response', 'section_complete'
            stage: Current stage, e.g. 'planning', 'generating', 'completed'
            details: Dictionary of detailed content, not truncated
            section_title: Current section title (optional)
            section_index: Current section index (optional)
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": round(self._get_elapsed_time(), 2),
            "report_id": self.report_id,
            "action": action,
            "stage": stage,
            "section_title": section_title,
            "section_index": section_index,
            "details": details
        }

        # Append to JSONL file
        with open(self.log_file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')

    def log_start(self, simulation_id: str, graph_id: str, simulation_requirement: str):
        """Record report generation start."""
        self.log(
            action="report_start",
            stage="pending",
            details={
                "simulation_id": simulation_id,
                "graph_id": graph_id,
                "simulation_requirement": simulation_requirement,
                "message": "Report generation task started"
            }
        )

    def log_planning_start(self):
        """Record outline planning start."""
        self.log(
            action="planning_start",
            stage="planning",
            details={"message": "Starting report outline planning"}
        )

    def log_planning_context(self, context: Dict[str, Any]):
        """Record context information retrieved during planning."""
        self.log(
            action="planning_context",
            stage="planning",
            details={
                "message": "Retrieved simulation context information",
                "context": context
            }
        )

    def log_planning_complete(self, outline_dict: Dict[str, Any]):
        """Record outline planning completion."""
        self.log(
            action="planning_complete",
            stage="planning",
            details={
                "message": "Outline planning complete",
                "outline": outline_dict
            }
        )

    def log_section_start(self, section_title: str, section_index: int):
        """Record section generation start."""
        self.log(
            action="section_start",
            stage="generating",
            section_title=section_title,
            section_index=section_index,
            details={"message": f"Starting section generation: {section_title}"}
        )

    def log_react_thought(self, section_title: str, section_index: int, iteration: int, thought: str):
        """Record ReACT thinking process."""
        self.log(
            action="react_thought",
            stage="generating",
            section_title=section_title,
            section_index=section_index,
            details={
                "iteration": iteration,
                "thought": thought,
                "message": f"ReACT iteration {iteration} thinking"
            }
        )

    def log_tool_call(
        self,
        section_title: str,
        section_index: int,
        tool_name: str,
        parameters: Dict[str, Any],
        iteration: int
    ):
        """Record tool call."""
        self.log(
            action="tool_call",
            stage="generating",
            section_title=section_title,
            section_index=section_index,
            details={
                "iteration": iteration,
                "tool_name": tool_name,
                "parameters": parameters,
                "message": f"Calling tool: {tool_name}"
            }
        )

    def log_tool_result(
        self,
        section_title: str,
        section_index: int,
        tool_name: str,
        result: str,
        iteration: int
    ):
        """Record tool call result (full content, not truncated)."""
        self.log(
            action="tool_result",
            stage="generating",
            section_title=section_title,
            section_index=section_index,
            details={
                "iteration": iteration,
                "tool_name": tool_name,
                "result": result,  # Full result, not truncated
                "result_length": len(result),
                "message": f"Tool {tool_name} returned result"
            }
        )

    def log_llm_response(
        self,
        section_title: str,
        section_index: int,
        response: str,
        iteration: int,
        has_tool_calls: bool,
        has_final_answer: bool
    ):
        """Record LLM response (full content, not truncated)."""
        self.log(
            action="llm_response",
            stage="generating",
            section_title=section_title,
            section_index=section_index,
            details={
                "iteration": iteration,
                "response": response,  # Full response, not truncated
                "response_length": len(response),
                "has_tool_calls": has_tool_calls,
                "has_final_answer": has_final_answer,
                "message": f"LLM response (tool_calls: {has_tool_calls}, final_answer: {has_final_answer})"
            }
        )

    def log_section_content(
        self,
        section_title: str,
        section_index: int,
        content: str,
        tool_calls_count: int
    ):
        """Record section content generation complete (content only, not the full section completion)."""
        self.log(
            action="section_content",
            stage="generating",
            section_title=section_title,
            section_index=section_index,
            details={
                "content": content,  # Full content, not truncated
                "content_length": len(content),
                "tool_calls_count": tool_calls_count,
                "message": f"Section {section_title} content generation complete"
            }
        )

    def log_section_full_complete(
        self,
        section_title: str,
        section_index: int,
        full_content: str
    ):
        """
        Record section generation complete.

        The frontend should listen for this log to determine whether a section is truly finished
        and to retrieve the full content.
        """
        self.log(
            action="section_complete",
            stage="generating",
            section_title=section_title,
            section_index=section_index,
            details={
                "content": full_content,
                "content_length": len(full_content),
                "message": f"Section {section_title} generation complete"
            }
        )

    def log_report_complete(self, total_sections: int, total_time_seconds: float):
        """Record report generation complete."""
        self.log(
            action="report_complete",
            stage="completed",
            details={
                "total_sections": total_sections,
                "total_time_seconds": round(total_time_seconds, 2),
                "message": "Report generation complete"
            }
        )

    def log_error(self, error_message: str, stage: str, section_title: str = None):
        """Record error."""
        self.log(
            action="error",
            stage=stage,
            section_title=section_title,
            section_index=None,
            details={
                "error": error_message,
                "message": f"An error occurred: {error_message}"
            }
        )


class ReportConsoleLogger:
    """
    Report Agent console logger.

    Writes console-style logs (INFO, WARNING, etc.) to a console_log.txt file
    in the report folder. These logs differ from agent_log.jsonl in that they
    are plain-text console output.
    """

    def __init__(self, report_id: str):
        """
        Initialize the console logger.

        Args:
            report_id: Report ID, used to determine the log file path
        """
        self.report_id = report_id
        self.log_file_path = os.path.join(
            Config.UPLOAD_FOLDER, 'reports', report_id, 'console_log.txt'
        )
        self._ensure_log_file()
        self._file_handler = None
        self._setup_file_handler()

    def _ensure_log_file(self):
        """Ensure the directory containing the log file exists."""
        log_dir = os.path.dirname(self.log_file_path)
        os.makedirs(log_dir, exist_ok=True)

    def _setup_file_handler(self):
        """Set up file handler to write logs to file simultaneously."""
        import logging

        # Create file handler
        self._file_handler = logging.FileHandler(
            self.log_file_path,
            mode='a',
            encoding='utf-8'
        )
        self._file_handler.setLevel(logging.INFO)

        # Use the same concise format as the console
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%H:%M:%S'
        )
        self._file_handler.setFormatter(formatter)

        # Attach to report_agent-related loggers
        loggers_to_attach = [
            'mirofish.report_agent',
            'mirofish.zep_tools',
            'mirofish.lightrag_tools',
            'mirofish.llm_client',
        ]

        for logger_name in loggers_to_attach:
            target_logger = logging.getLogger(logger_name)
            # Avoid adding duplicates
            if self._file_handler not in target_logger.handlers:
                target_logger.addHandler(self._file_handler)

    def close(self):
        """Close the file handler and remove it from loggers."""
        import logging

        if self._file_handler:
            loggers_to_detach = [
                'mirofish.report_agent',
                'mirofish.zep_tools',
                'mirofish.lightrag_tools',
                'mirofish.llm_client',
            ]

            for logger_name in loggers_to_detach:
                target_logger = logging.getLogger(logger_name)
                if self._file_handler in target_logger.handlers:
                    target_logger.removeHandler(self._file_handler)

            self._file_handler.close()
            self._file_handler = None

    def __del__(self):
        """Ensure file handler is closed on destruction."""
        self.close()


class ReportStatus(str, Enum):
    """Report status."""
    PENDING = "pending"
    PLANNING = "planning"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ReportSection:
    """Report section."""
    title: str
    content: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "content": self.content
        }

    def to_markdown(self, level: int = 2) -> str:
        """Convert to Markdown format."""
        md = f"{'#' * level} {self.title}\n\n"
        if self.content:
            md += f"{self.content}\n\n"
        return md


@dataclass
class ReportOutline:
    """Report outline."""
    title: str
    summary: str
    sections: List[ReportSection]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "summary": self.summary,
            "sections": [s.to_dict() for s in self.sections]
        }

    def to_markdown(self) -> str:
        """Convert to Markdown format."""
        md = f"# {self.title}\n\n"
        md += f"> {self.summary}\n\n"
        for section in self.sections:
            md += section.to_markdown()
        return md


@dataclass
class Report:
    """Complete report."""
    report_id: str
    simulation_id: str
    graph_id: str
    simulation_requirement: str
    status: ReportStatus
    outline: Optional[ReportOutline] = None
    markdown_content: str = ""
    created_at: str = ""
    completed_at: str = ""
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "simulation_id": self.simulation_id,
            "graph_id": self.graph_id,
            "simulation_requirement": self.simulation_requirement,
            "status": self.status.value,
            "outline": self.outline.to_dict() if self.outline else None,
            "markdown_content": self.markdown_content,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "error": self.error
        }


# ═══════════════════════════════════════════════════════════════
# Prompt template constants
# ═══════════════════════════════════════════════════════════════

# ── Tool descriptions ──

TOOL_DESC_INSIGHT_FORGE = """\
[Deep Insight Retrieval - Powerful retrieval tool]
This is our powerful retrieval function, designed for deep analysis. It will:
1. Automatically decompose your question into multiple sub-questions
2. Retrieve information from the simulation graph across multiple dimensions
3. Integrate results from semantic search, entity analysis, and relationship chain tracing
4. Return the most comprehensive and in-depth retrieval content

[Use cases]
- Need to deeply analyze a topic
- Need to understand multiple aspects of an event
- Need rich material to support a report section

[Returns]
- Relevant raw facts (can be quoted directly)
- Core entity insights
- Relationship chain analysis"""

TOOL_DESC_PANORAMA_SEARCH = """\
[Breadth Search - Get a panoramic view]
This tool retrieves a complete panoramic view of simulation results, especially suited for understanding
how events evolved. It will:
1. Retrieve all related nodes and relationships
2. Distinguish between currently valid facts and historical/expired facts
3. Help you understand how public opinion evolved

[Use cases]
- Need to understand the complete development trajectory of an event
- Need to compare public opinion changes at different stages
- Need comprehensive entity and relationship information

[Returns]
- Currently valid facts (latest simulation results)
- Historical/expired facts (evolution records)
- All involved entities"""

TOOL_DESC_QUICK_SEARCH = """\
[Simple Search - Fast retrieval]
A lightweight fast retrieval tool, suitable for simple, direct information queries.

[Use cases]
- Need to quickly find a specific piece of information
- Need to verify a fact
- Simple information retrieval

[Returns]
- A list of facts most relevant to the query"""

TOOL_DESC_INTERVIEW_AGENTS = """\
[Deep Interview - Real Agent Interview (dual platform)]
Calls the OASIS simulation environment's interview API to conduct real interviews with running simulation agents!
This is not LLM simulation — it calls the real interview interface to get raw answers from simulation agents.
By default interviews are conducted on both Twitter and Reddit platforms simultaneously for broader perspectives.

Workflow:
1. Automatically reads persona files to understand all simulation agents
2. Intelligently selects agents most relevant to the interview topic (e.g. students, media, officials)
3. Automatically generates interview questions
4. Calls /api/simulation/interview/batch to conduct real interviews on both platforms
5. Integrates all interview results and provides multi-perspective analysis

[Use cases]
- Need to understand event opinions from different role perspectives (what do students think? what does the media say? what is the official position?)
- Need to collect opinions and positions from multiple parties
- Need to get real answers from simulation agents (from the OASIS simulation environment)
- Want to make the report more vivid with "interview transcripts"

[Returns]
- Identity information of interviewed agents
- Interview answers from each agent on both Twitter and Reddit platforms
- Key quotes (can be quoted directly)
- Interview summary and opinion comparison

[Important] The OASIS simulation environment must be running to use this feature!"""

# ── Outline planning prompt ──

PLAN_SYSTEM_PROMPT = """\
You are an expert author of "Future Prediction Reports," possessing a "god's-eye view" of the simulated world — you can observe the behavior, statements, and interactions of every Agent in the simulation.

[Core concept]
We have built a simulated world and injected a specific "simulation requirement" as a variable. The evolution results of the simulated world are predictions of what may happen in the future. What you are observing is not "experimental data" but a "preview of the future."

[Your task]
Write a "Future Prediction Report" that answers:
1. Under the conditions we have set, what happened in the future?
2. How did the various agents (populations) react and act?
3. What future trends and risks worthy of attention does this simulation reveal?

[Report positioning]
- ✅ This is a future prediction report based on simulation, revealing "if this happens, what will the future look like"
- ✅ Focus on prediction results: event trajectory, group reactions, emergent phenomena, potential risks
- ✅ The speech and behavior of agents in the simulated world are predictions of future population behavior
- ❌ Not an analysis of the current state of the real world
- ❌ Not a vague overview of public opinion

[Section count limits]
- Minimum 2 sections, maximum 5 sections
- No subsections needed; each section should be written directly with complete content
- Content should be concise, focused on core predictive findings
- Section structure should be designed by you based on the prediction results

Please output the report outline in JSON format as follows:
{
    "title": "Report title",
    "summary": "Report summary (one sentence summarizing the core predictive finding)",
    "sections": [
        {
            "title": "Section title",
            "description": "Section content description"
        }
    ]
}

Note: The sections array must have at least 2 and at most 5 elements!"""

PLAN_USER_PROMPT_TEMPLATE = """\
[Prediction scenario setup]
The variable injected into the simulated world (simulation requirement): {simulation_requirement}

[Simulated world scale]
- Number of entities participating in the simulation: {total_nodes}
- Number of relationships generated between entities: {total_edges}
- Entity type distribution: {entity_types}
- Number of active agents: {total_entities}

[Sample future facts predicted by the simulation]
{related_facts_json}

Please examine this future preview from a "god's-eye view":
1. Under the conditions we have set, what state did the future present?
2. How did the various populations (agents) react and act?
3. What future trends worthy of attention does this simulation reveal?

Based on the prediction results, design the most appropriate report section structure.

[Reminder] Number of report sections: minimum 2, maximum 5. Content should be concise and focused on core predictive findings."""

# ── Section generation prompt ──

SECTION_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert author of "Future Prediction Reports," currently writing one section of the report.

Report title: {report_title}
Report summary: {report_summary}
Prediction scenario (simulation requirement): {simulation_requirement}

Current section to write: {section_title}

═══════════════════════════════════════════════════════════════
[Core concept]
═══════════════════════════════════════════════════════════════

The simulated world is a preview of the future. We injected specific conditions (simulation requirements)
into the simulated world. The behavior and interactions of agents in the simulation are predictions of
future population behavior.

Your task is to:
- Reveal what happened in the future under the set conditions
- Predict how various populations (agents) reacted and acted
- Discover future trends, risks, and opportunities worth noting

❌ Do not write as an analysis of the current state of the real world
✅ Focus on "what will happen in the future" — simulation results are the predicted future

═══════════════════════════════════════════════════════════════
[Most important rules - must be followed]
═══════════════════════════════════════════════════════════════

1. [Must call tools to observe the simulated world]
   - You are observing a future preview from a "god's-eye view"
   - All content must come from events and agent speech/actions in the simulated world
   - Prohibited from using your own knowledge to write report content
   - Each section must call tools at least 3 times (maximum 5) to observe the simulated world, which represents the future

2. [Must quote the original speech and actions of agents]
   - Agent speech and behavior are predictions of future population behavior
   - Use quote format in the report to present these predictions, e.g.:
     > "A certain type of population would say: original content..."
   - These quotes are the core evidence of simulation predictions

3. [Language consistency - quoted content must be translated to the report language]
   - Content returned by tools may contain English or mixed English/Chinese expressions
   - If the simulation requirement and source material are in Chinese, the report must be written entirely in Chinese
   - When quoting tool-returned English or mixed content, you must translate it into fluent Chinese before including it in the report
   - Keep the original meaning intact when translating, ensure natural and clear expression
   - This rule applies to both body text and blockquote (> format) content

4. [Faithfully present prediction results]
   - Report content must reflect simulation results representing the future in the simulated world
   - Do not add information that does not exist in the simulation
   - If information on a certain aspect is insufficient, state this honestly

═══════════════════════════════════════════════════════════════
[⚠️ Format specifications - extremely important!]
═══════════════════════════════════════════════════════════════

[One section = minimum content unit]
- Each section is the minimum chunk unit of the report
- ❌ Prohibited from using any Markdown headings (#, ##, ###, ####, etc.) within a section
- ❌ Prohibited from adding the section main heading at the beginning of content
- ✅ Section titles are automatically added by the system; you only need to write the body text
- ✅ Use **bold**, paragraph breaks, quotes, and lists to organize content, but do not use headings

[Correct example]
```
This section analyzes the public opinion propagation trend of the event. Through in-depth analysis of simulation data, we found...

**Initial ignition phase**

Weibo, as the first scene of public opinion, served the core function of first-release information:

> "Weibo contributed 68% of first-release volume..."

**Emotional amplification phase**

The TikTok platform further amplified the event's impact:

- Strong visual impact
- High emotional resonance
```

[Incorrect example]
```
## Executive Summary          ← Wrong! Do not add any headings
### I. Initial phase          ← Wrong! Do not use ### for subsections
#### 1.1 Detailed analysis    ← Wrong! Do not use #### for subdivisions
...
This section analyzed...
```

═══════════════════════════════════════════════════════════════
[Available retrieval tools] (call 3–5 times per section)
═══════════════════════════════════════════════════════════════

{tools_description}

[Tool usage suggestions - mix different tools, do not rely on just one]
- insight_forge: Deep insight analysis, automatically decomposes questions and retrieves facts/relationships from multiple dimensions
- panorama_search: Wide-angle panoramic search, understand the full picture, timeline, and evolution of events
- quick_search: Quickly verify a specific information point
- interview_agents: Interview simulation agents, get first-person perspectives and real reactions from different roles

═══════════════════════════════════════════════════════════════
[Workflow]
═══════════════════════════════════════════════════════════════

Each reply can only do one of the following two things (not both simultaneously):

Option A - Call a tool:
Output only one JSON object in the following format:
{{"name": "tool_name", "parameters": {{"param_name": "param_value"}}}}
The system will execute the tool and return the result to you. Do not wrap the JSON in XML tags, code fences, or extra commentary.

Option B - Output final content:
When you have obtained enough information through tools, output the section content starting with "Final Answer:".

⚠️ Strictly prohibited:
- Prohibited from including both a tool call and a Final Answer in a single reply
- Prohibited from fabricating tool return results (Observation); all tool results are injected by the system
- Maximum one tool call per reply

═══════════════════════════════════════════════════════════════
[Section content requirements]
═══════════════════════════════════════════════════════════════

1. Content must be based on simulation data retrieved through tools
2. Extensively quote original text to demonstrate simulation results
3. Use Markdown format (but headings are prohibited):
   - Use **bold text** to highlight key points (instead of subheadings)
   - Use lists (- or 1. 2. 3.) to organize points
   - Use blank lines to separate different paragraphs
   - ❌ Prohibited from using #, ##, ###, #### or any heading syntax
4. [Quote format specification - must be standalone paragraphs]
   Quotes must be standalone paragraphs with a blank line before and after; they cannot be embedded in paragraphs:

   ✅ Correct format:
   ```
   The school's response was considered to lack substance.

   > "The school's response pattern appeared rigid and slow in the rapidly changing social media environment."

   This assessment reflects widespread public dissatisfaction.
   ```

   ❌ Incorrect format:
   ```
   The school's response was considered to lack substance. > "The school's response pattern..." This assessment reflects...
   ```
5. Maintain logical coherence with other sections
6. [Avoid repetition] Carefully read the completed section content below and do not repeat the same information
7. [Reemphasis] Do not add any headings! Use **bold** instead of subsection headings"""

SECTION_USER_PROMPT_TEMPLATE = """\
Completed section content (please read carefully to avoid repetition):
{previous_content}

═══════════════════════════════════════════════════════════════
[Current task] Write section: {section_title}
═══════════════════════════════════════════════════════════════

[Important reminders]
1. Carefully read the completed sections above to avoid repeating the same content!
2. You must call tools to retrieve simulation data before starting
3. Please mix different tools; do not rely on only one
4. Report content must come from retrieval results; do not use your own knowledge

[⚠️ Format warning - must be followed]
- ❌ Do not write any headings (#, ##, ###, #### are all prohibited)
- ❌ Do not write "{section_title}" as the opening line
- ✅ Section titles are automatically added by the system
- ✅ Write body text directly; use **bold** instead of subsection headings

Please begin:
1. First decide what information this section needs
2. Then output one JSON tool call to retrieve simulation data
3. After collecting enough information, output Final Answer (plain body text, no headings at all)"""

SECTION_SYSTEM_PROMPT_COMPACT_TEMPLATE = """\
You are writing one section of a future prediction report based only on simulation data retrieved through tools.

Report title: {report_title}
Report summary: {report_summary}
Simulation requirement: {simulation_requirement}
Current section: {section_title}

Rules:
- Use only information retrieved from tools in this conversation.
- Do not use Markdown headings (#, ##, ###) inside the section.
- Do not include the section title in the body.
- In one reply, do exactly one of these:
  1. Output only one JSON object:
     {{"name": "tool_name", "parameters": {{"param": "value"}}}}
  2. Output final body text starting with "Final Answer:"
- Never include both a JSON tool call and Final Answer in the same reply.
- Do not wrap the JSON in XML tags, code fences, or extra commentary.

Available tools:
{tools_description}
"""

SECTION_USER_PROMPT_COMPACT_TEMPLATE = """\
Completed section content:
{previous_content}

Write the section "{section_title}".

Requirements:
- First, output one JSON tool call to retrieve simulation data.
- After enough evidence is collected, output Final Answer:
- No headings inside the section body.
"""

# ── ReACT loop message templates ──

REACT_OBSERVATION_TEMPLATE = """\
Observation (retrieval result):

═══ Tool {tool_name} returned ═══
{result}

═══════════════════════════════════════════════════════════════
Tools called: {tool_calls_count}/{max_tool_calls} (used: {used_tools_str}){unused_hint}
- If information is sufficient: output section content starting with "Final Answer:" (must quote the above source text)
- If more information is needed: call one more tool to continue retrieval
═══════════════════════════════════════════════════════════════"""

REACT_INSUFFICIENT_TOOLS_MSG = (
    "[Note] You have only called {tool_calls_count} tool(s); at least {min_tool_calls} are required. "
    "Please call more tools to retrieve additional simulation data, then output Final Answer. {unused_hint}"
)

REACT_INSUFFICIENT_TOOLS_MSG_ALT = (
    "Only {tool_calls_count} tool call(s) have been made so far; at least {min_tool_calls} are required. "
    "Please call a tool to retrieve simulation data. {unused_hint}"
)

REACT_TOOL_LIMIT_MSG = (
    "The tool call limit has been reached ({tool_calls_count}/{max_tool_calls}); no more tool calls are allowed. "
    'Please immediately output the section content starting with "Final Answer:" based on the information already retrieved.'
)

REACT_UNUSED_TOOLS_HINT = "\n💡 You have not yet used: {unused_list}. Consider trying different tools to get multi-angle information."

REACT_FORCE_FINAL_MSG = "The tool call limit has been reached. Please output Final Answer: directly and generate the section content."

# ── Chat prompt ──

CHAT_SYSTEM_PROMPT_TEMPLATE = """\
You are a concise and efficient simulation prediction assistant.

[Background]
Prediction conditions: {simulation_requirement}

[Generated analysis report]
{report_content}

[Rules]
1. Prioritize answering questions based on the above report content
2. Answer questions directly; avoid lengthy reasoning
3. Only call tools to retrieve more data when the report content is insufficient to answer
4. Answers should be concise, clear, and well-organized

[Available tools] (use only when needed; maximum 1–2 calls)
{tools_description}

[Tool call format]
When you need a tool, output only one JSON object:
{{"name": "tool_name", "parameters": {{"param_name": "param_value"}}}}
Do not wrap the JSON in XML tags, code fences, or extra commentary.

[Answer style]
- Concise and direct; avoid lengthy explanations
- Use > format to quote key content
- Lead with conclusions, then explain the reasoning"""

CHAT_OBSERVATION_SUFFIX = "\n\nPlease answer the question concisely."


# ═══════════════════════════════════════════════════════════════
# ReportAgent main class
# ═══════════════════════════════════════════════════════════════


class ReportAgent:
    """
    Report Agent - Simulation report generation agent.

    Uses the ReACT (Reasoning + Acting) pattern:
    1. Planning phase: Analyze simulation requirements, plan the report outline structure
    2. Generation phase: Generate content section by section; each section may call tools multiple times
    3. Reflection phase: Check content completeness and accuracy
    """

    # Maximum tool calls per section
    MAX_TOOL_CALLS_PER_SECTION = 5

    # Maximum reflection rounds
    MAX_REFLECTION_ROUNDS = 3

    # Maximum tool calls per chat turn
    MAX_TOOL_CALLS_PER_CHAT = 2

    def __init__(
        self,
        graph_id: str,
        simulation_id: str,
        simulation_requirement: str,
        llm_client: Optional[LLMClient] = None,
        zep_tools: Optional[ZepToolsService] = None
    ):
        """
        Initialize Report Agent.

        Args:
            graph_id: Graph ID
            simulation_id: Simulation ID
            simulation_requirement: Simulation requirement description
            llm_client: LLM client (optional)
            zep_tools: Zep tools service (optional)
        """
        self.graph_id = graph_id
        self.simulation_id = simulation_id
        self.simulation_requirement = simulation_requirement

        self.llm = llm_client or LLMClient()
        self.zep_tools = zep_tools or ZepToolsService()

        # Tool definitions
        self.tools = self._define_tools()

        # Logger (initialized in generate_report)
        self.report_logger: Optional[ReportLogger] = None
        # Console logger (initialized in generate_report)
        self.console_logger: Optional[ReportConsoleLogger] = None

        logger.info(f"ReportAgent initialized: graph_id={graph_id}, simulation_id={simulation_id}")

    def _define_tools(self) -> Dict[str, Dict[str, Any]]:
        """Define available tools."""
        return {
            "insight_forge": {
                "name": "insight_forge",
                "description": TOOL_DESC_INSIGHT_FORGE,
                "parameters": {
                    "query": "The question or topic you want to deeply analyze",
                    "report_context": "Context of the current report section (optional; helps generate more precise sub-questions)"
                }
            },
            "panorama_search": {
                "name": "panorama_search",
                "description": TOOL_DESC_PANORAMA_SEARCH,
                "parameters": {
                    "query": "Search query for relevance ranking",
                    "include_expired": "Whether to include expired/historical content (default True)"
                }
            },
            "quick_search": {
                "name": "quick_search",
                "description": TOOL_DESC_QUICK_SEARCH,
                "parameters": {
                    "query": "Search query string",
                    "limit": "Number of results to return (optional, default 10)"
                }
            },
            "interview_agents": {
                "name": "interview_agents",
                "description": TOOL_DESC_INTERVIEW_AGENTS,
                "parameters": {
                    "interview_topic": "Interview topic or requirement description (e.g. 'understand student opinions on the dormitory formaldehyde incident')",
                    "max_agents": "Maximum number of agents to interview (optional, default 5, max 10)"
                }
            }
        }

    def _llm_chat_with_retry(
        self,
        messages: list,
        temperature: float = 0.5,
        max_tokens: int = 4096,
        max_retries: int = 3,
    ) -> Optional[str]:
        """
        Call self.llm.chat() with exponential backoff on 429 rate-limit errors.
        Returns None if all retries are exhausted (caller handles None as before).
        Works for both local Ollama and cloud APIs - LLMClient handles routing.
        """
        for attempt in range(max_retries):
            try:
                return self.llm.chat(
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            except Exception as e:
                err = str(e)
                if self.llm.is_quota_exhausted_error(e):
                    raise
                if "429" in err or "rate" in err.lower():
                    wait = 15.0 * (attempt + 1)
                    logger.warning(
                        f"LLM rate limited (attempt {attempt + 1}/{max_retries}). "
                        f"Waiting {wait}s before retry..."
                    )
                    time.sleep(wait)
                else:
                    raise
        logger.error("Max retries exceeded for LLM chat call.")
        return None

    def _execute_tool(self, tool_name: str, parameters: Dict[str, Any], report_context: str = "") -> str:
        """
        Execute a tool call.

        Args:
            tool_name: Tool name
            parameters: Tool parameters
            report_context: Report context (used for InsightForge)

        Returns:
            Tool execution result (text format)
        """
        logger.info(f"Executing tool: {tool_name}, parameters: {parameters}")

        try:
            if tool_name == "insight_forge":
                query = parameters.get("query", "")
                ctx = parameters.get("report_context", "") or report_context
                result = self.zep_tools.insight_forge(
                    graph_id=self.graph_id,
                    query=query,
                    simulation_requirement=self.simulation_requirement,
                    report_context=ctx
                )
                return result.to_text()

            elif tool_name == "panorama_search":
                # Breadth search - get panoramic view
                query = parameters.get("query", "")
                include_expired = parameters.get("include_expired", True)
                if isinstance(include_expired, str):
                    include_expired = include_expired.lower() in ['true', '1', 'yes']
                result = self.zep_tools.panorama_search(
                    graph_id=self.graph_id,
                    query=query,
                    include_expired=include_expired
                )
                return result.to_text()

            elif tool_name == "quick_search":
                # Simple search - fast retrieval
                query = parameters.get("query", "")
                limit = parameters.get("limit", 10)
                if isinstance(limit, str):
                    limit = int(limit)
                result = self.zep_tools.quick_search(
                    graph_id=self.graph_id,
                    query=query,
                    limit=limit
                )
                return result.to_text()

            elif tool_name == "interview_agents":
                # Deep interview - calls the real OASIS interview API to get simulation agent answers (dual platform)
                interview_topic = parameters.get("interview_topic", parameters.get("query", ""))
                max_agents = parameters.get("max_agents", 5)
                if isinstance(max_agents, str):
                    max_agents = int(max_agents)
                max_agents = min(max_agents, 10)
                result = self.zep_tools.interview_agents(
                    simulation_id=self.simulation_id,
                    interview_requirement=interview_topic,
                    simulation_requirement=self.simulation_requirement,
                    max_agents=max_agents
                )
                return result.to_text()

            # ========== Legacy tools for backward compatibility (redirect to new tools) ==========

            elif tool_name == "search_graph":
                # Redirect to quick_search
                logger.info("search_graph redirected to quick_search")
                return self._execute_tool("quick_search", parameters, report_context)

            elif tool_name == "get_graph_statistics":
                result = self.zep_tools.get_graph_statistics(self.graph_id)
                return json.dumps(result, ensure_ascii=False, indent=2)

            elif tool_name == "get_entity_summary":
                entity_name = parameters.get("entity_name", "")
                result = self.zep_tools.get_entity_summary(
                    graph_id=self.graph_id,
                    entity_name=entity_name
                )
                return json.dumps(result, ensure_ascii=False, indent=2)

            elif tool_name == "get_simulation_context":
                # Redirect to insight_forge as it is more powerful
                logger.info("get_simulation_context redirected to insight_forge")
                query = parameters.get("query", self.simulation_requirement)
                return self._execute_tool("insight_forge", {"query": query}, report_context)

            elif tool_name == "get_entities_by_type":
                entity_type = parameters.get("entity_type", "")
                nodes = self.zep_tools.get_entities_by_type(
                    graph_id=self.graph_id,
                    entity_type=entity_type
                )
                result = [n.to_dict() for n in nodes]
                return json.dumps(result, ensure_ascii=False, indent=2)

            else:
                return f"Unknown tool: {tool_name}. Please use one of: insight_forge, panorama_search, quick_search"

        except Exception as e:
            logger.error(f"Tool execution failed: {tool_name}, error: {str(e)}")
            return f"Tool execution failed: {str(e)}"

    # Set of valid tool names used to validate bare JSON fallback parsing
    VALID_TOOL_NAMES = {"insight_forge", "panorama_search", "quick_search", "interview_agents"}

    def _parse_tool_calls(self, response: str) -> List[Dict[str, Any]]:
        """
        Parse tool calls from LLM response.

        Supported formats:
        1. Bare JSON (the entire response or a single line is a tool call JSON)
        2. Legacy XML wrapper: <tool_call>{"name": "tool_name", "parameters": {...}}</tool_call>
        """
        tool_calls = []

        # Legacy format: XML-style wrapper kept for backward compatibility.
        xml_pattern = r'<tool_call>\s*(\{.*?\})\s*</tool_call>'
        for match in re.finditer(xml_pattern, response, re.DOTALL):
            try:
                call_data = json.loads(match.group(1))
                tool_calls.append(call_data)
            except json.JSONDecodeError:
                pass

        if tool_calls:
            return tool_calls

        # Preferred format: bare JSON without XML tags.
        # Only attempted when the legacy XML wrapper matched nothing, to avoid false-matching JSON in body text.
        stripped = response.strip()
        if stripped.startswith('{') and stripped.endswith('}'):
            try:
                call_data = json.loads(stripped)
                if self._is_valid_tool_call(call_data):
                    tool_calls.append(call_data)
                    return tool_calls
            except json.JSONDecodeError:
                pass

        # Response may contain thinking text + bare JSON; try extracting the last JSON object
        json_pattern = r'(\{"(?:name|tool)"\s*:.*?\})\s*$'
        match = re.search(json_pattern, stripped, re.DOTALL)
        if match:
            try:
                call_data = json.loads(match.group(1))
                if self._is_valid_tool_call(call_data):
                    tool_calls.append(call_data)
            except json.JSONDecodeError:
                pass

        return tool_calls

    def _is_valid_tool_call(self, data: dict) -> bool:
        """Validate whether the parsed JSON is a valid tool call."""
        # Supports both {"name": ..., "parameters": ...} and {"tool": ..., "params": ...} key names
        tool_name = data.get("name") or data.get("tool")
        if tool_name and tool_name in self.VALID_TOOL_NAMES:
            # Normalize key names to name / parameters
            if "tool" in data:
                data["name"] = data.pop("tool")
            if "params" in data and "parameters" not in data:
                data["parameters"] = data.pop("params")
            return True
        return False

    def _get_tools_description(self) -> str:
        """Generate tool description text."""
        desc_parts = ["Available tools:"]
        for name, tool in self.tools.items():
            params_desc = ", ".join([f"{k}: {v}" for k, v in tool["parameters"].items()])
            desc_parts.append(f"- {name}: {tool['description']}")
            if params_desc:
                desc_parts.append(f"  Parameters: {params_desc}")
        return "\n".join(desc_parts)

    def plan_outline(
        self,
        progress_callback: Optional[Callable] = None
    ) -> ReportOutline:
        """
        Plan the report outline.

        Uses LLM to analyze simulation requirements and plan the report's table of contents structure.

        Args:
            progress_callback: Progress callback function

        Returns:
            ReportOutline: Report outline
        """
        logger.info("Starting report outline planning...")

        if progress_callback:
            progress_callback("planning", 0, "Analyzing simulation requirements...")

        # First retrieve simulation context
        context = self.zep_tools.get_simulation_context(
            graph_id=self.graph_id,
            simulation_requirement=self.simulation_requirement
        )

        if progress_callback:
            progress_callback("planning", 30, "Generating report outline...")

        system_prompt = PLAN_SYSTEM_PROMPT
        user_prompt = PLAN_USER_PROMPT_TEMPLATE.format(
            simulation_requirement=self.simulation_requirement,
            total_nodes=context.get('graph_statistics', {}).get('total_nodes', 0),
            total_edges=context.get('graph_statistics', {}).get('total_edges', 0),
            entity_types=list(context.get('graph_statistics', {}).get('entity_types', {}).keys()),
            total_entities=context.get('total_entities', 0),
            related_facts_json=json.dumps(context.get('related_facts', [])[:10], ensure_ascii=False, indent=2),
        )

        try:
            response = self.llm.chat_json(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3
            )

            if progress_callback:
                progress_callback("planning", 80, "Parsing outline structure...")

            # Parse outline
            sections = []
            for section_data in response.get("sections", []):
                sections.append(ReportSection(
                    title=section_data.get("title", ""),
                    content=""
                ))

            outline = ReportOutline(
                title=response.get("title", "Simulation Analysis Report"),
                summary=response.get("summary", ""),
                sections=sections
            )

            if progress_callback:
                progress_callback("planning", 100, "Outline planning complete")

            logger.info(f"Outline planning complete: {len(sections)} sections")
            return outline

        except Exception as e:
            if self.llm.is_quota_exhausted_error(e):
                raise RuntimeError(f"Report generation blocked by LLM quota limit: {str(e)}") from e
            logger.error(f"Outline planning failed: {str(e)}")
            # Return default outline (3 sections as fallback)
            return ReportOutline(
                title="Future Prediction Report",
                summary="Analysis of future trends and risks based on simulation predictions",
                sections=[
                    ReportSection(title="Prediction Scenario and Core Findings"),
                    ReportSection(title="Population Behavior Prediction Analysis"),
                    ReportSection(title="Trend Outlook and Risk Indicators")
                ]
            )

    def _generate_section_react(
        self,
        section: ReportSection,
        outline: ReportOutline,
        previous_sections: List[str],
        progress_callback: Optional[Callable] = None,
        section_index: int = 0
    ) -> str:
        """
        Generate a single section's content using the ReACT pattern.

        ReACT loop:
        1. Thought - analyze what information is needed
        2. Action - call a tool to retrieve information
        3. Observation - analyze tool return results
        4. Repeat until information is sufficient or max iterations reached
        5. Final Answer - generate section content

        Args:
            section: Section to generate
            outline: Complete outline
            previous_sections: Content of previous sections (for maintaining coherence)
            progress_callback: Progress callback
            section_index: Section index (for logging)

        Returns:
            Section content (Markdown format)
        """
        logger.info(f"ReACT generating section: {section.title}")

        # Log section start
        if self.report_logger:
            self.report_logger.log_section_start(section.title, section_index)

        system_prompt = SECTION_SYSTEM_PROMPT_TEMPLATE.format(
            report_title=outline.title,
            report_summary=outline.summary,
            simulation_requirement=self.simulation_requirement,
            section_title=section.title,
            tools_description=self._get_tools_description(),
        )
        compact_system_prompt = SECTION_SYSTEM_PROMPT_COMPACT_TEMPLATE.format(
            report_title=outline.title,
            report_summary=outline.summary,
            simulation_requirement=self.simulation_requirement,
            section_title=section.title,
            tools_description=self._get_tools_description(),
        )

        # Build user prompt - summarize previous sections to avoid quadratic growth
        if previous_sections:
            previous_parts = []
            for i, sec in enumerate(previous_sections):
                # Keep first 800 chars as summary — enough for coherence, avoids quadratic growth
                summary = sec[:800].rsplit(' ', 1)[0] + "..." if len(sec) > 800 else sec
                previous_parts.append(f"[Section {i+1} summary]: {summary}")
            previous_content = "\n".join(previous_parts)
        else:
            previous_content = "(This is the first section)"

        user_prompt = SECTION_USER_PROMPT_TEMPLATE.format(
            previous_content=previous_content,
            section_title=section.title,
        )
        compact_user_prompt = SECTION_USER_PROMPT_COMPACT_TEMPLATE.format(
            previous_content=previous_content,
            section_title=section.title,
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        use_compact_prompt = False

        # ReACT loop
        tool_calls_count = 0
        max_iterations = 5  # Maximum iteration rounds
        min_tool_calls = 3  # Minimum tool calls required
        conflict_retries = 0  # Consecutive conflict count when tool call and Final Answer appear together
        used_tools = set()  # Track which tools have been called
        all_tools = {"insight_forge", "panorama_search", "quick_search", "interview_agents"}
        auto_bootstrapped = False  # Whether we already seeded the section with a fallback tool call

        # Report context used for InsightForge sub-question generation
        report_context = f"Section title: {section.title}\nSimulation requirement: {self.simulation_requirement}"

        for iteration in range(max_iterations):
            if progress_callback:
                progress_callback(
                    "generating",
                    int((iteration / max_iterations) * 100),
                    f"Deep retrieval and writing ({tool_calls_count}/{self.MAX_TOOL_CALLS_PER_SECTION})"
                )

            # Call LLM - retries internally on 429 rate-limit errors
            try:
                response = self._llm_chat_with_retry(
                    messages=messages,
                    temperature=0.5,
                    max_tokens=3072 if use_compact_prompt else 4096
                )
            except Exception as e:
                error_text = str(e)
                is_provider_5xx = "500" in error_text or "internal server error" in error_text.lower()
                if self.llm.is_quota_exhausted_error(e):
                    raise RuntimeError(f"Report generation blocked by LLM quota limit: {error_text}") from e
                logger.warning(
                    f"Section {section.title} iteration {iteration + 1}: "
                    f"LLM call failed: {error_text}"
                )

                # If the provider fails before the first tool call, seed the section with a
                # deterministic retrieval so we can continue from evidence instead of retrying
                # the fragile "choose a tool" step over and over.
                if is_provider_5xx and tool_calls_count == 0 and not auto_bootstrapped:
                    use_compact_prompt = True
                    messages[0] = {"role": "system", "content": compact_system_prompt}
                    messages[1] = {"role": "user", "content": compact_user_prompt}
                    logger.info(
                        f"Section {section.title}: provider failed before first tool call; "
                        "auto-bootstrapping insight_forge"
                    )

                    bootstrap_parameters = {
                        "query": f"{section.title} - {self.simulation_requirement}",
                        "report_context": report_context,
                    }
                    if self.report_logger:
                        self.report_logger.log_tool_call(
                            section_title=section.title,
                            section_index=section_index,
                            tool_name="insight_forge",
                            parameters=bootstrap_parameters,
                            iteration=iteration + 1
                        )

                    bootstrap_result = self._execute_tool(
                        "insight_forge",
                        bootstrap_parameters,
                        report_context=report_context
                    )

                    if self.report_logger:
                        self.report_logger.log_tool_result(
                            section_title=section.title,
                            section_index=section_index,
                            tool_name="insight_forge",
                            result=bootstrap_result,
                            iteration=iteration + 1
                        )

                    tool_calls_count += 1
                    used_tools.add("insight_forge")
                    auto_bootstrapped = True

                    unused_tools = all_tools - used_tools
                    unused_hint = ""
                    if unused_tools and tool_calls_count < self.MAX_TOOL_CALLS_PER_SECTION:
                        unused_hint = REACT_UNUSED_TOOLS_HINT.format(unused_list=", ".join(unused_tools))

                    messages.append({
                        "role": "assistant",
                        "content": json.dumps(
                            {"name": "insight_forge", "parameters": bootstrap_parameters},
                            ensure_ascii=False
                        ),
                    })
                    messages.append({
                        "role": "user",
                        "content": REACT_OBSERVATION_TEMPLATE.format(
                            tool_name="insight_forge",
                            result=bootstrap_result[:1500] if len(bootstrap_result) > 1500 else bootstrap_result,
                            tool_calls_count=tool_calls_count,
                            max_tool_calls=self.MAX_TOOL_CALLS_PER_SECTION,
                            used_tools_str=", ".join(sorted(used_tools)),
                            unused_hint=unused_hint,
                        ),
                    })
                    continue

                # If the large ReACT prompt trips provider-side 5xx errors, retry with a shorter prompt.
                if (not use_compact_prompt) and is_provider_5xx:
                    use_compact_prompt = True
                    messages[0] = {"role": "system", "content": compact_system_prompt}
                    messages[1] = {"role": "user", "content": compact_user_prompt}
                    logger.info(
                        f"Section {section.title}: switching to compact section prompt after provider error"
                    )
                    continue

                if iteration < max_iterations - 1:
                    messages.append({
                        "role": "user",
                        "content": "The previous model call failed. Retry and continue with the same task."
                    })
                    continue
                raise

            # Check if LLM returned None (API error or empty content)
            if response is None:
                logger.warning(f"Section {section.title} iteration {iteration + 1}: LLM returned None")
                # If iterations remain, add a message and retry
                if iteration < max_iterations - 1:
                    messages.append({"role": "assistant", "content": "(empty response)"})
                    messages.append({"role": "user", "content": "Please continue generating content."})
                    continue
                # Last iteration also returned None; break out to forced wrap-up
                break

            logger.debug(f"LLM response: {response[:200]}...")

            # Parse once and reuse results
            tool_calls = self._parse_tool_calls(response)
            has_tool_calls = bool(tool_calls)
            has_final_answer = "Final Answer:" in response

            # ── Conflict handling: LLM output both a tool call and Final Answer ──
            if has_tool_calls and has_final_answer:
                conflict_retries += 1
                logger.warning(
                    f"Section {section.title} iteration {iteration+1}: "
                    f"LLM output both tool call and Final Answer (conflict #{conflict_retries})"
                )

                if conflict_retries <= 2:
                    # First two times: discard this response and ask LLM to retry
                    messages.append({"role": "assistant", "content": response})
                    messages.append({
                        "role": "user",
                        "content": (
                            "[Format error] Your reply contains both a tool call and a Final Answer, which is not allowed.\n"
                            "Each reply can only do one of the following two things:\n"
                            "- Call one tool (output only one JSON object with name and parameters, do not write Final Answer)\n"
                            "- Output final content (start with 'Final Answer:', do not include a JSON tool call)\n"
                            "Please reply again, doing only one of these things."
                        ),
                    })
                    continue
                else:
                    # Third time: degraded handling — keep the first parsed tool call and ignore the final answer
                    logger.warning(
                        f"Section {section.title}: {conflict_retries} consecutive conflicts; "
                        "degrading to execute the first parsed tool call"
                    )
                    has_final_answer = False
                    conflict_retries = 0

            # Log LLM response
            if self.report_logger:
                self.report_logger.log_llm_response(
                    section_title=section.title,
                    section_index=section_index,
                    response=response,
                    iteration=iteration + 1,
                    has_tool_calls=has_tool_calls,
                    has_final_answer=has_final_answer
                )

            # ── Case 1: LLM output Final Answer ──
            if has_final_answer:
                # Insufficient tool calls — reject and require more tool calls
                if tool_calls_count < min_tool_calls:
                    messages.append({"role": "assistant", "content": response})
                    unused_tools = all_tools - used_tools
                    unused_hint = f"(These tools have not been used yet; consider trying them: {', '.join(unused_tools)})" if unused_tools else ""
                    messages.append({
                        "role": "user",
                        "content": REACT_INSUFFICIENT_TOOLS_MSG.format(
                            tool_calls_count=tool_calls_count,
                            min_tool_calls=min_tool_calls,
                            unused_hint=unused_hint,
                        ),
                    })
                    continue

                # Normal completion
                final_answer = response.split("Final Answer:")[-1].strip()
                logger.info(f"Section {section.title} generation complete (tool calls: {tool_calls_count})")

                if self.report_logger:
                    self.report_logger.log_section_content(
                        section_title=section.title,
                        section_index=section_index,
                        content=final_answer,
                        tool_calls_count=tool_calls_count
                    )
                return final_answer

            # ── Case 2: LLM attempted to call a tool ──
            if has_tool_calls:
                # Tool quota exhausted → explicitly inform, require Final Answer output
                if tool_calls_count >= self.MAX_TOOL_CALLS_PER_SECTION:
                    messages.append({"role": "assistant", "content": response})
                    messages.append({
                        "role": "user",
                        "content": REACT_TOOL_LIMIT_MSG.format(
                            tool_calls_count=tool_calls_count,
                            max_tool_calls=self.MAX_TOOL_CALLS_PER_SECTION,
                        ),
                    })
                    continue

                # Execute only the first tool call
                call = tool_calls[0]
                if len(tool_calls) > 1:
                    logger.info(f"LLM attempted {len(tool_calls)} tool calls; executing only the first: {call['name']}")

                if self.report_logger:
                    self.report_logger.log_tool_call(
                        section_title=section.title,
                        section_index=section_index,
                        tool_name=call["name"],
                        parameters=call.get("parameters", {}),
                        iteration=iteration + 1
                    )

                result = self._execute_tool(
                    call["name"],
                    call.get("parameters", {}),
                    report_context=report_context
                )

                if self.report_logger:
                    self.report_logger.log_tool_result(
                        section_title=section.title,
                        section_index=section_index,
                        tool_name=call["name"],
                        result=result,
                        iteration=iteration + 1
                    )

                # Truncate tool result to prevent context overflow
                truncated_result = result[:1500] if len(result) > 1500 else result

                tool_calls_count += 1
                used_tools.add(call['name'])

                # Build unused tools hint
                unused_tools = all_tools - used_tools
                unused_hint = ""
                if unused_tools and tool_calls_count < self.MAX_TOOL_CALLS_PER_SECTION:
                    unused_hint = REACT_UNUSED_TOOLS_HINT.format(unused_list=", ".join(unused_tools))

                messages.append({"role": "assistant", "content": response})
                messages.append({
                    "role": "user",
                    "content": REACT_OBSERVATION_TEMPLATE.format(
                        tool_name=call["name"],
                        result=truncated_result,
                        tool_calls_count=tool_calls_count,
                        max_tool_calls=self.MAX_TOOL_CALLS_PER_SECTION,
                        used_tools_str=", ".join(used_tools),
                        unused_hint=unused_hint,
                    ),
                })
                continue

            # ── Case 3: Neither tool call nor Final Answer ──
            messages.append({"role": "assistant", "content": response})

            if tool_calls_count < min_tool_calls:
                # Insufficient tool calls — suggest unused tools
                unused_tools = all_tools - used_tools
                unused_hint = f"(These tools have not been used yet; consider trying them: {', '.join(unused_tools)})" if unused_tools else ""

                messages.append({
                    "role": "user",
                    "content": REACT_INSUFFICIENT_TOOLS_MSG_ALT.format(
                        tool_calls_count=tool_calls_count,
                        min_tool_calls=min_tool_calls,
                        unused_hint=unused_hint,
                    ),
                })
                continue

            # Sufficient tool calls; LLM output content without "Final Answer:" prefix
            # Directly adopt this content as the final answer without idle looping
            logger.info(f"Section {section.title}: 'Final Answer:' prefix not detected; adopting LLM output directly as final content (tool calls: {tool_calls_count})")
            final_answer = response.strip()

            if self.report_logger:
                self.report_logger.log_section_content(
                    section_title=section.title,
                    section_index=section_index,
                    content=final_answer,
                    tool_calls_count=tool_calls_count
                )
            return final_answer

        # Max iterations reached; force content generation
        logger.warning(f"Section {section.title} reached max iterations; forcing generation")
        messages.append({"role": "user", "content": REACT_FORCE_FINAL_MSG})

        try:
            response = self._llm_chat_with_retry(
                messages=messages,
                temperature=0.5,
                max_tokens=3072 if use_compact_prompt else 4096
            )
        except Exception as e:
            logger.error(f"Section {section.title}: forced wrap-up call failed: {e}")
            response = None

        # Check if LLM returned None during forced wrap-up
        if response is None:
            logger.error(f"Section {section.title}: LLM returned None during forced wrap-up; using default error message")
            final_answer = f"(This section failed to generate: LLM returned empty response; please retry later)"
        elif "Final Answer:" in response:
            final_answer = response.split("Final Answer:")[-1].strip()
        else:
            final_answer = response

        # Log section content generation complete
        if self.report_logger:
            self.report_logger.log_section_content(
                section_title=section.title,
                section_index=section_index,
                content=final_answer,
                tool_calls_count=tool_calls_count
            )

        return final_answer

    def generate_report(
        self,
        progress_callback: Optional[Callable[[str, int, str], None]] = None,
        report_id: Optional[str] = None
    ) -> Report:
        """
        Generate a complete report (with real-time section-by-section output).

        Each section is saved to the folder immediately after generation,
        without waiting for the entire report to complete.
        File structure:
        reports/{report_id}/
            meta.json       - Report metadata
            outline.json    - Report outline
            progress.json   - Generation progress
            section_01.md   - Section 1
            section_02.md   - Section 2
            ...
            full_report.md  - Complete report

        Args:
            progress_callback: Progress callback function (stage, progress, message)
            report_id: Report ID (optional; auto-generated if not provided)

        Returns:
            Report: Complete report
        """
        import uuid

        # Auto-generate report_id if not provided
        if not report_id:
            report_id = f"report_{uuid.uuid4().hex[:12]}"
        start_time = datetime.now()

        report = Report(
            report_id=report_id,
            simulation_id=self.simulation_id,
            graph_id=self.graph_id,
            simulation_requirement=self.simulation_requirement,
            status=ReportStatus.PENDING,
            created_at=datetime.now().isoformat()
        )

        # List of completed section titles (for progress tracking)
        completed_section_titles = []

        try:
            # Initialize: create report folder and save initial state
            ReportManager._ensure_report_folder(report_id)

            # Initialize structured logger (agent_log.jsonl)
            self.report_logger = ReportLogger(report_id)
            self.report_logger.log_start(
                simulation_id=self.simulation_id,
                graph_id=self.graph_id,
                simulation_requirement=self.simulation_requirement
            )

            # Initialize console logger (console_log.txt)
            self.console_logger = ReportConsoleLogger(report_id)

            ReportManager.update_progress(
                report_id, "pending", 0, "Initializing report...",
                completed_sections=[]
            )
            ReportManager.save_report(report)

            # Phase 1: Plan outline
            report.status = ReportStatus.PLANNING
            ReportManager.update_progress(
                report_id, "planning", 5, "Starting report outline planning...",
                completed_sections=[]
            )

            # Log planning start
            self.report_logger.log_planning_start()

            if progress_callback:
                progress_callback("planning", 0, "Starting report outline planning...")

            outline = self.plan_outline(
                progress_callback=lambda stage, prog, msg:
                    progress_callback(stage, prog // 5, msg) if progress_callback else None
            )
            report.outline = outline

            # Log planning complete
            self.report_logger.log_planning_complete(outline.to_dict())

            # Save outline to file
            ReportManager.save_outline(report_id, outline)
            ReportManager.update_progress(
                report_id, "planning", 15, f"Outline planning complete, {len(outline.sections)} sections",
                completed_sections=[]
            )
            ReportManager.save_report(report)

            logger.info(f"Outline saved to file: {report_id}/outline.json")

            # Phase 2: Generate sections one by one (save section by section)
            report.status = ReportStatus.GENERATING

            total_sections = len(outline.sections)
            generated_sections = []  # Save content for context

            for i, section in enumerate(outline.sections):
                section_num = i + 1
                base_progress = 20 + int((i / total_sections) * 70)

                # Update progress
                ReportManager.update_progress(
                    report_id, "generating", base_progress,
                    f"Generating section: {section.title} ({section_num}/{total_sections})",
                    current_section=section.title,
                    completed_sections=completed_section_titles
                )

                if progress_callback:
                    progress_callback(
                        "generating",
                        base_progress,
                        f"Generating section: {section.title} ({section_num}/{total_sections})"
                    )

                # Generate main section content
                section_content = self._generate_section_react(
                    section=section,
                    outline=outline,
                    previous_sections=generated_sections,
                    progress_callback=lambda stage, prog, msg:
                        progress_callback(
                            stage,
                            base_progress + int(prog * 0.7 / total_sections),
                            msg
                        ) if progress_callback else None,
                    section_index=section_num
                )

                section.content = section_content
                generated_sections.append(f"## {section.title}\n\n{section_content}")

                # Save section
                ReportManager.save_section(report_id, section_num, section)
                completed_section_titles.append(section.title)

                # Log section complete
                full_section_content = f"## {section.title}\n\n{section_content}"

                if self.report_logger:
                    self.report_logger.log_section_full_complete(
                        section_title=section.title,
                        section_index=section_num,
                        full_content=full_section_content.strip()
                    )

                logger.info(f"Section saved: {report_id}/section_{section_num:02d}.md")

                # Update progress
                ReportManager.update_progress(
                    report_id, "generating",
                    base_progress + int(70 / total_sections),
                    f"Section {section.title} complete",
                    current_section=None,
                    completed_sections=completed_section_titles
                )

            # Phase 3: Assemble complete report
            if progress_callback:
                progress_callback("generating", 95, "Assembling complete report...")

            ReportManager.update_progress(
                report_id, "generating", 95, "Assembling complete report...",
                completed_sections=completed_section_titles
            )

            # Use ReportManager to assemble complete report
            report.markdown_content = ReportManager.assemble_full_report(report_id, outline)
            report.status = ReportStatus.COMPLETED
            report.completed_at = datetime.now().isoformat()

            # Calculate total elapsed time
            total_time_seconds = (datetime.now() - start_time).total_seconds()

            # Log report complete
            if self.report_logger:
                self.report_logger.log_report_complete(
                    total_sections=total_sections,
                    total_time_seconds=total_time_seconds
                )

            # Save final report
            ReportManager.save_report(report)
            ReportManager.update_progress(
                report_id, "completed", 100, "Report generation complete",
                completed_sections=completed_section_titles
            )

            if progress_callback:
                progress_callback("completed", 100, "Report generation complete")

            logger.info(f"Report generation complete: {report_id}")

            # Close console logger
            if self.console_logger:
                self.console_logger.close()
                self.console_logger = None

            return report

        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}", exc_info=True)
            report.status = ReportStatus.FAILED
            report.error = str(e)

            # Log error
            if self.report_logger:
                self.report_logger.log_error(str(e), "failed")

            # Save failed state
            try:
                ReportManager.save_report(report)
                ReportManager.update_progress(
                    report_id, "failed", -1, f"Report generation failed: {str(e)}",
                    completed_sections=completed_section_titles
                )
            except Exception:
                pass  # Ignore errors when saving failed state

            # Close console logger
            if self.console_logger:
                self.console_logger.close()
                self.console_logger = None

            return report

    def chat(
        self,
        message: str,
        chat_history: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Chat with the Report Agent.

        During conversation the agent can autonomously call retrieval tools to answer questions.

        Args:
            message: User message
            chat_history: Conversation history

        Returns:
            {
                "response": "Agent reply",
                "tool_calls": [list of tools called],
                "sources": [information sources]
            }
        """
        logger.info(f"Report Agent chat: {message[:50]}...")

        chat_history = chat_history or []

        # Retrieve already-generated report content
        report_content = ""
        try:
            report = ReportManager.get_report_by_simulation(self.simulation_id)
            if report and report.markdown_content:
                # Limit report length to avoid context overflow
                report_content = report.markdown_content[:15000]
                if len(report.markdown_content) > 15000:
                    report_content += "\n\n... [Report content truncated] ..."
        except Exception as e:
            logger.warning(f"Failed to retrieve report content: {e}")

        # Cap report content to save tokens in chat turns
        capped_report = report_content[:6000] + "\n...(truncated)" if report_content and len(report_content) > 6000 else report_content
        system_prompt = CHAT_SYSTEM_PROMPT_TEMPLATE.format(
            simulation_requirement=self.simulation_requirement,
            report_content=capped_report if capped_report else "(No report available)",
            tools_description=self._get_tools_description(),
        )

        # Build messages
        messages = [{"role": "system", "content": system_prompt}]

        # Add conversation history
        for h in chat_history[-10:]:  # Limit history length
            messages.append(h)

        # Add user message
        messages.append({
            "role": "user",
            "content": message
        })

        # ReACT loop (simplified)
        tool_calls_made = []
        max_iterations = 2  # Reduced iteration rounds

        for iteration in range(max_iterations):
            response = self._llm_chat_with_retry(
                messages=messages,
                temperature=0.5
            )

            if response is None:
                logger.warning(f"Chat iteration {iteration + 1}: LLM returned None")
                if iteration < max_iterations - 1:
                    messages.append({"role": "assistant", "content": "(empty response)"})
                    messages.append({"role": "user", "content": "Please continue."})
                    continue
                break

            # Parse tool calls
            tool_calls = self._parse_tool_calls(response)

            if not tool_calls:
                # No tool calls; return response directly
                clean_response = re.sub(r'<tool_call>.*?</tool_call>', '', response, flags=re.DOTALL)
                clean_response = re.sub(r'\[TOOL_CALL\].*?\)', '', clean_response)

                return {
                    "response": clean_response.strip(),
                    "tool_calls": tool_calls_made,
                    "sources": [tc.get("parameters", {}).get("query", "") for tc in tool_calls_made]
                }

            # Execute tool calls (limited count)
            tool_results = []
            for call in tool_calls[:1]:  # Maximum 1 tool call per round
                if len(tool_calls_made) >= self.MAX_TOOL_CALLS_PER_CHAT:
                    break
                result = self._execute_tool(call["name"], call.get("parameters", {}))
                tool_results.append({
                    "tool": call["name"],
                    "result": result[:1500]  # Limit result length
                })
                tool_calls_made.append(call)

            # Add results to messages
            messages.append({"role": "assistant", "content": response})
            observation = "\n".join([f"[{r['tool']} result]\n{r['result']}" for r in tool_results])
            messages.append({
                "role": "user",
                "content": observation + CHAT_OBSERVATION_SUFFIX
            })

        # Max iterations reached; get final response
        final_response = self._llm_chat_with_retry(
            messages=messages,
            temperature=0.5
        )

        if final_response is None:
            final_response = "(This reply failed to generate because the LLM returned no content.)"

        # Clean response
        clean_response = re.sub(r'<tool_call>.*?</tool_call>', '', final_response, flags=re.DOTALL)
        clean_response = re.sub(r'\[TOOL_CALL\].*?\)', '', clean_response)

        return {
            "response": clean_response.strip(),
            "tool_calls": tool_calls_made,
            "sources": [tc.get("parameters", {}).get("query", "") for tc in tool_calls_made]
        }


class ReportManager:
    """
    Report manager.

    Responsible for persistent storage and retrieval of reports.

    File structure (section-by-section output):
    reports/
      {report_id}/
        meta.json          - Report metadata and status
        outline.json       - Report outline
        progress.json      - Generation progress
        section_01.md      - Section 1
        section_02.md      - Section 2
        ...
        full_report.md     - Complete report
    """

    # Report storage directory
    REPORTS_DIR = os.path.join(Config.UPLOAD_FOLDER, 'reports')

    @classmethod
    def _ensure_reports_dir(cls):
        """Ensure the reports root directory exists."""
        os.makedirs(cls.REPORTS_DIR, exist_ok=True)

    @classmethod
    def _get_report_folder(cls, report_id: str) -> str:
        """Get the report folder path."""
        return os.path.join(cls.REPORTS_DIR, report_id)

    @classmethod
    def _ensure_report_folder(cls, report_id: str) -> str:
        """Ensure the report folder exists and return its path."""
        folder = cls._get_report_folder(report_id)
        os.makedirs(folder, exist_ok=True)
        return folder

    @classmethod
    def _get_report_path(cls, report_id: str) -> str:
        """Get the report metadata file path."""
        return os.path.join(cls._get_report_folder(report_id), "meta.json")

    @classmethod
    def _get_report_markdown_path(cls, report_id: str) -> str:
        """Get the complete report Markdown file path."""
        return os.path.join(cls._get_report_folder(report_id), "full_report.md")

    @classmethod
    def _get_outline_path(cls, report_id: str) -> str:
        """Get the outline file path."""
        return os.path.join(cls._get_report_folder(report_id), "outline.json")

    @classmethod
    def _get_progress_path(cls, report_id: str) -> str:
        """Get the progress file path."""
        return os.path.join(cls._get_report_folder(report_id), "progress.json")

    @classmethod
    def _get_section_path(cls, report_id: str, section_index: int) -> str:
        """Get the section Markdown file path."""
        return os.path.join(cls._get_report_folder(report_id), f"section_{section_index:02d}.md")

    @classmethod
    def _get_agent_log_path(cls, report_id: str) -> str:
        """Get the agent log file path."""
        return os.path.join(cls._get_report_folder(report_id), "agent_log.jsonl")

    @classmethod
    def _get_console_log_path(cls, report_id: str) -> str:
        """Get the console log file path."""
        return os.path.join(cls._get_report_folder(report_id), "console_log.txt")

    @classmethod
    def get_console_log(cls, report_id: str, from_line: int = 0) -> Dict[str, Any]:
        """
        Get console log content.

        This is the console output log (INFO, WARNING, etc.) produced during report generation,
        differing from the structured agent_log.jsonl.

        Args:
            report_id: Report ID
            from_line: Starting line number for reading (for incremental retrieval; 0 = from the beginning)

        Returns:
            {
                "logs": [list of log lines],
                "total_lines": total line count,
                "from_line": starting line number,
                "has_more": whether there are more logs
            }
        """
        log_path = cls._get_console_log_path(report_id)

        if not os.path.exists(log_path):
            return {
                "logs": [],
                "total_lines": 0,
                "from_line": 0,
                "has_more": False
            }

        logs = []
        total_lines = 0

        with open(log_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                total_lines = i + 1
                if i >= from_line:
                    # Keep original log line; strip trailing newline
                    logs.append(line.rstrip('\n\r'))

        return {
            "logs": logs,
            "total_lines": total_lines,
            "from_line": from_line,
            "has_more": False  # Read to end
        }

    @classmethod
    def get_console_log_stream(cls, report_id: str) -> List[str]:
        """
        Get complete console log (retrieve all at once).

        Args:
            report_id: Report ID

        Returns:
            List of log lines
        """
        result = cls.get_console_log(report_id, from_line=0)
        return result["logs"]

    @classmethod
    def get_agent_log(cls, report_id: str, from_line: int = 0) -> Dict[str, Any]:
        """
        Get agent log content.

        Args:
            report_id: Report ID
            from_line: Starting line number for reading (for incremental retrieval; 0 = from the beginning)

        Returns:
            {
                "logs": [list of log entries],
                "total_lines": total line count,
                "from_line": starting line number,
                "has_more": whether there are more logs
            }
        """
        log_path = cls._get_agent_log_path(report_id)

        if not os.path.exists(log_path):
            return {
                "logs": [],
                "total_lines": 0,
                "from_line": 0,
                "has_more": False
            }

        logs = []
        total_lines = 0

        with open(log_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                total_lines = i + 1
                if i >= from_line:
                    try:
                        log_entry = json.loads(line.strip())
                        logs.append(log_entry)
                    except json.JSONDecodeError:
                        # Skip lines that fail to parse
                        continue

        return {
            "logs": logs,
            "total_lines": total_lines,
            "from_line": from_line,
            "has_more": False  # Read to end
        }

    @classmethod
    def get_agent_log_stream(cls, report_id: str) -> List[Dict[str, Any]]:
        """
        Get complete agent log (retrieve all at once).

        Args:
            report_id: Report ID

        Returns:
            List of log entries
        """
        result = cls.get_agent_log(report_id, from_line=0)
        return result["logs"]

    @classmethod
    def save_outline(cls, report_id: str, outline: ReportOutline) -> None:
        """
        Save the report outline.

        Called immediately after the planning phase completes.
        """
        cls._ensure_report_folder(report_id)

        with open(cls._get_outline_path(report_id), 'w', encoding='utf-8') as f:
            json.dump(outline.to_dict(), f, ensure_ascii=False, indent=2)

        logger.info(f"Outline saved: {report_id}")

    @classmethod
    def save_section(
        cls,
        report_id: str,
        section_index: int,
        section: ReportSection
    ) -> str:
        """
        Save a single section.

        Called immediately after each section is generated, enabling section-by-section output.

        Args:
            report_id: Report ID
            section_index: Section index (starting from 1)
            section: Section object

        Returns:
            Saved file path
        """
        cls._ensure_report_folder(report_id)

        # Build section Markdown content - clean up any duplicate headings
        cleaned_content = cls._clean_section_content(section.content, section.title)
        md_content = f"## {section.title}\n\n"
        if cleaned_content:
            md_content += f"{cleaned_content}\n\n"

        # Save file
        file_suffix = f"section_{section_index:02d}.md"
        file_path = os.path.join(cls._get_report_folder(report_id), file_suffix)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(md_content)

        logger.info(f"Section saved: {report_id}/{file_suffix}")
        return file_path

    @classmethod
    def _clean_section_content(cls, content: str, section_title: str) -> str:
        """
        Clean section content.

        1. Remove Markdown heading lines at the start of content that duplicate the section title
        2. Convert all ### and deeper headings to bold text

        Args:
            content: Raw content
            section_title: Section title

        Returns:
            Cleaned content
        """
        import re

        if not content:
            return content

        content = content.strip()
        lines = content.split('\n')
        cleaned_lines = []
        skip_next_empty = False

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Check if this is a Markdown heading line
            heading_match = re.match(r'^(#{1,6})\s+(.+)$', stripped)

            if heading_match:
                level = len(heading_match.group(1))
                title_text = heading_match.group(2).strip()

                # Check if it duplicates the section title (skip within first 5 lines)
                if i < 5:
                    if title_text == section_title or title_text.replace(' ', '') == section_title.replace(' ', ''):
                        skip_next_empty = True
                        continue

                # Convert all heading levels (#, ##, ###, ####, etc.) to bold
                # Section titles are added by the system; content should have no headings
                cleaned_lines.append(f"**{title_text}**")
                cleaned_lines.append("")  # Add blank line
                continue

            # If the previous line was a skipped heading and the current line is blank, also skip it
            if skip_next_empty and stripped == '':
                skip_next_empty = False
                continue

            skip_next_empty = False
            cleaned_lines.append(line)

        # Remove leading blank lines
        while cleaned_lines and cleaned_lines[0].strip() == '':
            cleaned_lines.pop(0)

        # Remove leading horizontal rules
        while cleaned_lines and cleaned_lines[0].strip() in ['---', '***', '___']:
            cleaned_lines.pop(0)
            # Also remove blank lines immediately following the rule
            while cleaned_lines and cleaned_lines[0].strip() == '':
                cleaned_lines.pop(0)

        return '\n'.join(cleaned_lines)

    @classmethod
    def update_progress(
        cls,
        report_id: str,
        status: str,
        progress: int,
        message: str,
        current_section: str = None,
        completed_sections: List[str] = None
    ) -> None:
        """
        Update report generation progress.

        The frontend can read progress.json to get real-time progress.
        """
        cls._ensure_report_folder(report_id)

        progress_data = {
            "status": status,
            "progress": progress,
            "message": message,
            "current_section": current_section,
            "completed_sections": completed_sections or [],
            "updated_at": datetime.now().isoformat()
        }

        with open(cls._get_progress_path(report_id), 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)

    @classmethod
    def get_progress(cls, report_id: str) -> Optional[Dict[str, Any]]:
        """Get report generation progress."""
        path = cls._get_progress_path(report_id)

        if not os.path.exists(path):
            return None

        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @classmethod
    def get_generated_sections(cls, report_id: str) -> List[Dict[str, Any]]:
        """
        Get the list of already-generated sections.

        Returns information about all saved section files.
        """
        folder = cls._get_report_folder(report_id)

        if not os.path.exists(folder):
            return []

        sections = []
        for filename in sorted(os.listdir(folder)):
            if filename.startswith('section_') and filename.endswith('.md'):
                file_path = os.path.join(folder, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Parse section index from filename
                parts = filename.replace('.md', '').split('_')
                section_index = int(parts[1])

                sections.append({
                    "filename": filename,
                    "section_index": section_index,
                    "content": content
                })

        return sections

    @classmethod
    def assemble_full_report(cls, report_id: str, outline: ReportOutline) -> str:
        """
        Assemble the complete report.

        Assembles the complete report from saved section files, with heading cleanup.
        """
        folder = cls._get_report_folder(report_id)

        # Build report header
        md_content = f"# {outline.title}\n\n"
        md_content += f"> {outline.summary}\n\n"
        md_content += f"---\n\n"

        # Read all section files in order
        sections = cls.get_generated_sections(report_id)
        for section_info in sections:
            md_content += section_info["content"]

        # Post-process: clean up heading issues in the full report
        md_content = cls._post_process_report(md_content, outline)

        # Save complete report
        full_path = cls._get_report_markdown_path(report_id)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(md_content)

        logger.info(f"Complete report assembled: {report_id}")
        return md_content

    @classmethod
    def _post_process_report(cls, content: str, outline: ReportOutline) -> str:
        """
        Post-process report content.

        1. Remove duplicate headings
        2. Keep the report main title (#) and section titles (##); convert other levels (###, ####, etc.) to bold
        3. Clean up excessive blank lines and horizontal rules

        Args:
            content: Raw report content
            outline: Report outline

        Returns:
            Processed content
        """
        import re

        lines = content.split('\n')
        processed_lines = []
        prev_was_heading = False

        # Collect all section titles from the outline
        section_titles = set()
        for section in outline.sections:
            section_titles.add(section.title)

        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            # Check if this is a heading line
            heading_match = re.match(r'^(#{1,6})\s+(.+)$', stripped)

            if heading_match:
                level = len(heading_match.group(1))
                title = heading_match.group(2).strip()

                # Check if it is a duplicate heading (same content within the last 5 lines)
                is_duplicate = False
                for j in range(max(0, len(processed_lines) - 5), len(processed_lines)):
                    prev_line = processed_lines[j].strip()
                    prev_match = re.match(r'^(#{1,6})\s+(.+)$', prev_line)
                    if prev_match:
                        prev_title = prev_match.group(2).strip()
                        if prev_title == title:
                            is_duplicate = True
                            break

                if is_duplicate:
                    # Skip duplicate heading and any following blank lines
                    i += 1
                    while i < len(lines) and lines[i].strip() == '':
                        i += 1
                    continue

                # Heading level handling:
                # - # (level=1) retain only the report main title
                # - ## (level=2) retain section titles
                # - ### and below (level>=3) convert to bold text

                if level == 1:
                    if title == outline.title:
                        # Keep report main title
                        processed_lines.append(line)
                        prev_was_heading = True
                    elif title in section_titles:
                        # Section title incorrectly using #; correct to ##
                        processed_lines.append(f"## {title}")
                        prev_was_heading = True
                    else:
                        # Other level-1 headings become bold
                        processed_lines.append(f"**{title}**")
                        processed_lines.append("")
                        prev_was_heading = False
                elif level == 2:
                    if title in section_titles or title == outline.title:
                        # Keep section titles
                        processed_lines.append(line)
                        prev_was_heading = True
                    else:
                        # Non-section level-2 headings become bold
                        processed_lines.append(f"**{title}**")
                        processed_lines.append("")
                        prev_was_heading = False
                else:
                    # ### and below convert to bold text
                    processed_lines.append(f"**{title}**")
                    processed_lines.append("")
                    prev_was_heading = False

                i += 1
                continue

            elif stripped == '---' and prev_was_heading:
                # Skip horizontal rules immediately following headings
                i += 1
                continue

            elif stripped == '' and prev_was_heading:
                # Keep only one blank line after a heading
                if processed_lines and processed_lines[-1].strip() != '':
                    processed_lines.append(line)
                prev_was_heading = False

            else:
                processed_lines.append(line)
                prev_was_heading = False

            i += 1

        # Clean up consecutive multiple blank lines (keep maximum 2)
        result_lines = []
        empty_count = 0
        for line in processed_lines:
            if line.strip() == '':
                empty_count += 1
                if empty_count <= 2:
                    result_lines.append(line)
            else:
                empty_count = 0
                result_lines.append(line)

        return '\n'.join(result_lines)

    @classmethod
    def save_report(cls, report: Report) -> None:
        """Save report metadata and the complete report."""
        cls._ensure_report_folder(report.report_id)

        # Save metadata JSON
        with open(cls._get_report_path(report.report_id), 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)

        # Save outline
        if report.outline:
            cls.save_outline(report.report_id, report.outline)

        # Save complete Markdown report
        if report.markdown_content:
            with open(cls._get_report_markdown_path(report.report_id), 'w', encoding='utf-8') as f:
                f.write(report.markdown_content)

        logger.info(f"Report saved: {report.report_id}")

    @classmethod
    def get_report(cls, report_id: str) -> Optional[Report]:
        """Get a report."""
        path = cls._get_report_path(report_id)

        if not os.path.exists(path):
            # Compatibility with old format: check for files stored directly under the reports directory
            old_path = os.path.join(cls.REPORTS_DIR, f"{report_id}.json")
            if os.path.exists(old_path):
                path = old_path
            else:
                return None

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Reconstruct Report object
        outline = None
        if data.get('outline'):
            outline_data = data['outline']
            sections = []
            for s in outline_data.get('sections', []):
                sections.append(ReportSection(
                    title=s['title'],
                    content=s.get('content', '')
                ))
            outline = ReportOutline(
                title=outline_data['title'],
                summary=outline_data['summary'],
                sections=sections
            )

        # If markdown_content is empty, try reading from full_report.md
        markdown_content = data.get('markdown_content', '')
        if not markdown_content:
            full_report_path = cls._get_report_markdown_path(report_id)
            if os.path.exists(full_report_path):
                with open(full_report_path, 'r', encoding='utf-8') as f:
                    markdown_content = f.read()

        return Report(
            report_id=data['report_id'],
            simulation_id=data['simulation_id'],
            graph_id=data['graph_id'],
            simulation_requirement=data['simulation_requirement'],
            status=ReportStatus(data['status']),
            outline=outline,
            markdown_content=markdown_content,
            created_at=data.get('created_at', ''),
            completed_at=data.get('completed_at', ''),
            error=data.get('error')
        )

    @classmethod
    def get_report_by_simulation(cls, simulation_id: str) -> Optional[Report]:
        """Get a report by simulation ID."""
        cls._ensure_reports_dir()

        for item in os.listdir(cls.REPORTS_DIR):
            item_path = os.path.join(cls.REPORTS_DIR, item)
            # New format: folder
            if os.path.isdir(item_path):
                report = cls.get_report(item)
                if report and report.simulation_id == simulation_id:
                    return report
            # Compatibility with old format: JSON file
            elif item.endswith('.json'):
                report_id = item[:-5]
                report = cls.get_report(report_id)
                if report and report.simulation_id == simulation_id:
                    return report

        return None

    @classmethod
    def list_reports(cls, simulation_id: Optional[str] = None, limit: int = 50) -> List[Report]:
        """List reports."""
        cls._ensure_reports_dir()

        reports = []
        for item in os.listdir(cls.REPORTS_DIR):
            item_path = os.path.join(cls.REPORTS_DIR, item)
            # New format: folder
            if os.path.isdir(item_path):
                report = cls.get_report(item)
                if report:
                    if simulation_id is None or report.simulation_id == simulation_id:
                        reports.append(report)
            # Compatibility with old format: JSON file
            elif item.endswith('.json'):
                report_id = item[:-5]
                report = cls.get_report(report_id)
                if report:
                    if simulation_id is None or report.simulation_id == simulation_id:
                        reports.append(report)

        # Sort by creation time descending
        reports.sort(key=lambda r: r.created_at, reverse=True)

        return reports[:limit]

    @classmethod
    def delete_report(cls, report_id: str) -> bool:
        """Delete a report (entire folder)."""
        import shutil

        folder_path = cls._get_report_folder(report_id)

        # New format: delete entire folder
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            shutil.rmtree(folder_path)
            logger.info(f"Report folder deleted: {report_id}")
            return True

        # Compatibility with old format: delete individual files
        deleted = False
        old_json_path = os.path.join(cls.REPORTS_DIR, f"{report_id}.json")
        old_md_path = os.path.join(cls.REPORTS_DIR, f"{report_id}.md")

        if os.path.exists(old_json_path):
            os.remove(old_json_path)
            deleted = True
        if os.path.exists(old_md_path):
            os.remove(old_md_path)
            deleted = True

        return deleted
