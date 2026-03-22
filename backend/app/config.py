"""
Configuration management.
Priority (highest → lowest): .env file → settings.json → hardcoded defaults.
"""

import os
from dotenv import load_dotenv

# Load .env file from project root
project_root_env = os.path.join(os.path.dirname(__file__), '../../.env')

if os.path.exists(project_root_env):
    load_dotenv(project_root_env, override=True)
else:
    load_dotenv(override=True)


class Config:
    """Flask configuration class"""

    # Flask
    SECRET_KEY = os.environ.get('SECRET_KEY', 'mirofish-secret-key')
    DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'

    # JSON - disable ASCII escaping for Unicode support
    JSON_AS_ASCII = False

    # LLM configuration (OpenAI SDK format, points to Ollama by default)
    LLM_API_KEY = os.environ.get('LLM_API_KEY', 'ollama')  # Ollama accepts any non-empty string
    LLM_BASE_URL = os.environ.get('LLM_BASE_URL', 'http://localhost:11434/v1')
    LLM_MODEL_NAME = os.environ.get('LLM_MODEL_NAME', 'qwen2.5:7b')
    LLM_QUOTA_WAIT_ENABLED = os.environ.get('LLM_QUOTA_WAIT_ENABLED', 'True').lower() == 'true'
    LLM_QUOTA_POLL_SECONDS = float(os.environ.get('LLM_QUOTA_POLL_SECONDS', '10'))
    # 0 means wait indefinitely until quota becomes available again.
    LLM_QUOTA_MAX_WAIT_SECONDS = float(os.environ.get('LLM_QUOTA_MAX_WAIT_SECONDS', '0'))

    # Embedding & LightRAG configuration
    # EMBED_MODEL is provider-agnostic: set to an Ollama model (e.g. nomic-embed-text)
    # for local, or an OpenAI model (e.g. text-embedding-3-small) for cloud.
    # The provider is auto-detected from LLM_BASE_URL.
    OLLAMA_BASE_URL = os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434')
    EMBED_MODEL = os.environ.get('EMBED_MODEL',
                                 os.environ.get('OLLAMA_EMBED_MODEL', 'nomic-embed-text'))
    LIGHTRAG_DATA_DIR = os.path.join(os.path.dirname(__file__), '../data/lightrag_graphs')
    LIGHTRAG_MAX_GRAPH_NODES = int(os.environ.get('LIGHTRAG_MAX_GRAPH_NODES', '1000'))

    # File upload
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB
    UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '../uploads')
    ALLOWED_EXTENSIONS = {'pdf', 'md', 'txt', 'markdown'}

    # Text processing (larger chunks = fewer LLM calls + better extraction context)
    DEFAULT_CHUNK_SIZE = 1000
    DEFAULT_CHUNK_OVERLAP = 100

    # OASIS simulation
    OASIS_DEFAULT_MAX_ROUNDS = int(os.environ.get('OASIS_DEFAULT_MAX_ROUNDS', '10'))
    OASIS_SIMULATION_DATA_DIR = os.path.join(os.path.dirname(__file__), '../uploads/simulations')

    # OASIS platform available actions
    OASIS_TWITTER_ACTIONS = [
        'CREATE_POST', 'LIKE_POST', 'REPOST', 'FOLLOW', 'DO_NOTHING', 'QUOTE_POST'
    ]
    OASIS_REDDIT_ACTIONS = [
        'LIKE_POST', 'DISLIKE_POST', 'CREATE_POST', 'CREATE_COMMENT',
        'LIKE_COMMENT', 'DISLIKE_COMMENT', 'SEARCH_POSTS', 'SEARCH_USER',
        'TREND', 'REFRESH', 'DO_NOTHING', 'FOLLOW', 'MUTE'
    ]

    # Report Agent
    REPORT_AGENT_MAX_TOOL_CALLS = int(os.environ.get('REPORT_AGENT_MAX_TOOL_CALLS', '5'))
    REPORT_AGENT_MAX_REFLECTION_ROUNDS = int(os.environ.get('REPORT_AGENT_MAX_REFLECTION_ROUNDS', '2'))
    REPORT_AGENT_TEMPERATURE = float(os.environ.get('REPORT_AGENT_TEMPERATURE', '0.5'))

    @classmethod
    def reload_from_settings(cls):
        """
        Reload configuration with .env as the highest priority source.
        Priority: .env > settings.json > hardcoded defaults.
        settings.json only fills keys that are NOT explicitly set in .env.
        """
        from .settings import load_settings, migrate_from_env
        migrate_from_env()
        settings = load_settings()
        # Only apply settings.json value when the env var is absent from the environment.
        # load_dotenv() already populated os.environ from .env, so checking os.environ
        # is sufficient to determine whether .env defined a key.
        if not os.environ.get('LLM_API_KEY') and settings.get("llm_api_key"):
            cls.LLM_API_KEY = settings["llm_api_key"]
        if not os.environ.get('LLM_BASE_URL') and settings.get("llm_base_url"):
            cls.LLM_BASE_URL = settings["llm_base_url"]
        if not os.environ.get('LLM_MODEL_NAME') and settings.get("llm_model_name"):
            cls.LLM_MODEL_NAME = settings["llm_model_name"]
        if not os.environ.get('EMBED_MODEL') and not os.environ.get('OLLAMA_EMBED_MODEL'):
            embed = settings.get("embed_model") or settings.get("ollama_embed_model")
            if embed:
                cls.EMBED_MODEL = embed

    @classmethod
    def validate(cls):
        """Validate required configuration."""
        errors = []
        if not cls.LLM_BASE_URL:
            errors.append("LLM_BASE_URL is not configured")
        if not cls.LLM_MODEL_NAME:
            errors.append("LLM_MODEL_NAME is not configured")
        return errors


# Auto-load settings on import
Config.reload_from_settings()
