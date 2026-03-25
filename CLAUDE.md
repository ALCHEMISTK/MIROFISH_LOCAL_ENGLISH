# MiroFish — Claude Code Guidelines

## Quick Start

```bash
ollama pull qwen2.5:7b && ollama pull nomic-embed-text
npm start
# → Backend: http://localhost:5001
# → Frontend: http://localhost:3000
# → Setup: http://localhost:3000/setup
```

## What This Project Is

MiroFish is a social simulation engine. Users upload documents, build a knowledge graph, spawn AI agent personas, run multi-agent simulations (Twitter/Reddit), and generate analytical reports. Fully local (Ollama) or cloud (any OpenAI-compatible API).

## Project Structure

```
├── .env                          # Main config file (highest priority)
├── .env.example                  # Config reference
├── package.json                  # Root orchestration (npm start)
├── backend/
│   ├── run.py                    # Entry point
│   ├── pyproject.toml            # Python deps (uv)
│   └── app/
│       ├── __init__.py           # Flask app factory
│       ├── config.py             # Config: .env → settings.json → defaults
│       ├── settings.py           # Web UI settings persistence
│       ├── api/                  # REST endpoints (graph, simulation, report, setup)
│       ├── services/             # Business logic (one class per file)
│       └── utils/                # Logging, LLM client, file parser
├── frontend/
│   └── src/
│       ├── views/                # Page components (PascalCase.vue)
│       ├── components/           # Reusable components
│       ├── api/                  # Axios API clients
│       ├── store/                # Pinia stores
│       └── router/               # Vue Router with setup guard
└── backend/scripts/              # OASIS simulation runners
```

## Tech Stack

- **Backend**: Flask 3, Python 3.12, uv package manager
- **Frontend**: Vue 3 (Composition API), Vite 7, D3, Axios
- **LLM**: OpenAI SDK → Ollama (local) or any OpenAI-compatible API (cloud)
- **GraphRAG**: LightRAG (local graph storage in `backend/data/lightrag_graphs/`)
- **Embeddings**: nomic-embed-text (Ollama) or auto-detected cloud model
- **Simulation**: CAMEL-AI + OASIS (multi-agent social simulation framework)

## Architecture — Model Agnosticism

The system is provider-agnostic. Everything uses the OpenAI-compatible API format:

- `.env` controls ALL provider config (`LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL_NAME`)
- `config.py` priority: `.env` → `settings.json` (web UI) → hardcoded defaults
- Default fallback is local Ollama (`qwen2.5:7b`)
- Embeddings auto-detect: probes cloud provider first, falls back to local Ollama
- Thinking models (`qwen3`, `qwq`) auto-detected — `/no_think` injected in system prompt

## Key Services

| File | Purpose |
|---|---|
| `lightrag_client.py` | LightRAG singleton, async bridge, embed binding, adaptive rate limiter |
| `graph_builder.py` | Knowledge graph construction from documents |
| `simulation_runner.py` | Launches OASIS simulation subprocesses |
| `simulation_manager.py` | Simulation lifecycle (prepare, start, stop) |
| `simulation_config_generator.py` | Generates sim config from graph entities |
| `oasis_profile_generator.py` | Creates agent personas from entities |
| `report_agent.py` | ReACT-based report generation with tool use |
| `lightrag_tools.py` | Graph query tools (drop-in for zep_tools) |
| `lightrag_entity_reader.py` | Entity reader (drop-in for zep_entity_reader) |

## Naming Conventions

- **Python**: PEP 8, snake_case functions, PascalCase classes, UPPER_SNAKE_CASE constants
- **Vue**: PascalCase filenames, `<script setup>`, `ref()`/`computed()`, scoped styles
- **API**: REST, `/api/{domain}/{action}`, JSON responses with `{success, data, error}`
- **Services**: One class per file, file named after the service

## Config Priority

```
.env file (highest) → settings.json (web UI) → Config class defaults (lowest)
```

All simulation tuning is in `.env`:
- `SIMULATION_MODE=fast|slow` — toggles preset simulation parameters
- `SIM_*` — fine-grained overrides (hours, minutes per round, agent count, etc.)
- `LIGHTRAG_*` — graph building settings (max nodes, chunk size)
- `REPORT_AGENT_*` — report generation settings

## Common Patterns

- **LLM calls**: Use `LLMClient` from `utils/llm_client.py` (handles thinking models, retries)
- **Graph queries**: Use `ZepToolsService` from `lightrag_tools.py` (drop-in Zep replacement)
- **Entity reading**: Use `ZepEntityReader` from `lightrag_entity_reader.py`
- **Async in Flask**: `lightrag_client.py` bridges async LightRAG to sync Flask via `asyncio.run()`
- **Rate limiting**: Adaptive AIMD controller in `lightrag_client.py` (cloud APIs only)

## Important Notes

- Old `zep_*.py` files exist but are NOT imported — kept for reference only
- `lightrag_clientx.py` is a scratch/debug file — not used in production
- Graph caches have TTL eviction (60s) to prevent memory leaks
- Simulation runs as subprocess for isolation; IPC via `simulation_ipc.py`
- Path traversal protection: all user-provided IDs validated via `_validate_id()`
- SQLite databases in simulation dirs use context managers (no connection leaks)

## Running Tests

```bash
cd backend && uv run pytest
```

Test infra configured (pytest + pytest-asyncio) but no test suites written yet.

## Do NOT

- Hardcode model names — always read from Config/env
- Use bare `except:` — use `except Exception:` and log
- Return tracebacks in API responses (log server-side only)
- Skip `_validate_id()` on any endpoint that uses IDs in file paths
- Import from `zep_*.py` files — use `lightrag_*.py` equivalents
