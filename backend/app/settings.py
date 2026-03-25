"""
Settings persistence layer.
Stores configuration in a JSON file, with fallback to .env values.
"""

import json
import logging
import os
import sys

_settings_logger = logging.getLogger('mirofish.settings')

SETTINGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
SETTINGS_PATH = os.path.join(SETTINGS_DIR, 'settings.json')

DEFAULT_SETTINGS = {
    "llm_api_key": "ollama",
    "llm_base_url": "http://localhost:11434/v1",
    "llm_model_name": "qwen2.5:7b",
    "embed_model": "nomic-embed-text",
    "llm_boost_api_key": "",
    "llm_boost_base_url": "",
    "llm_boost_model_name": "",
    "configured": False,
}


def load_settings():
    """Load settings from JSON file. Returns defaults if file doesn't exist."""
    if not os.path.exists(SETTINGS_PATH):
        return dict(DEFAULT_SETTINGS)
    try:
        with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
            stored = json.load(f)
        # Migrate old key name
        if "ollama_embed_model" in stored and "embed_model" not in stored:
            stored["embed_model"] = stored.pop("ollama_embed_model")
        merged = dict(DEFAULT_SETTINGS)
        merged.update(stored)
        return merged
    except json.JSONDecodeError as e:
        _settings_logger.warning(
            f"Corrupted settings.json ({e}), backing up and returning defaults"
        )
        try:
            backup_path = SETTINGS_PATH + '.corrupt'
            if os.path.exists(SETTINGS_PATH):
                os.replace(SETTINGS_PATH, backup_path)
                _settings_logger.info(f"Corrupted file backed up to {backup_path}")
        except OSError:
            pass
        return dict(DEFAULT_SETTINGS)
    except OSError as e:
        _settings_logger.warning(f"Could not read settings.json ({e}), returning defaults")
        return dict(DEFAULT_SETTINGS)


def save_settings(settings):
    """Save settings to JSON file (atomic write)."""
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    import tempfile
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=SETTINGS_DIR,
        suffix='.tmp'
    )
    try:
        with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=2, ensure_ascii=False)
        # Atomic rename (on Windows, need to remove target first)
        if os.path.exists(SETTINGS_PATH):
            os.replace(tmp_path, SETTINGS_PATH)
        else:
            os.rename(tmp_path, SETTINGS_PATH)
        # Restrict permissions on non-Windows (settings may contain API keys)
        if sys.platform != 'win32':
            try:
                os.chmod(SETTINGS_PATH, 0o600)
            except OSError:
                pass
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def is_configured():
    """Check if the app has been configured with valid API keys."""
    settings = load_settings()
    return settings.get("configured", False)


def migrate_from_env():
    """Auto-migrate from .env values if settings.json doesn't exist yet."""
    if os.path.exists(SETTINGS_PATH):
        return

    llm_base_url = os.environ.get('LLM_BASE_URL', '')
    llm_model = os.environ.get('LLM_MODEL_NAME', '')

    if llm_base_url and llm_model:
        settings = dict(DEFAULT_SETTINGS)
        settings["llm_api_key"] = os.environ.get('LLM_API_KEY', 'ollama')
        settings["llm_base_url"] = llm_base_url
        settings["llm_model_name"] = llm_model
        settings["embed_model"] = os.environ.get('EMBED_MODEL',
                                                 os.environ.get('OLLAMA_EMBED_MODEL', 'nomic-embed-text'))
        settings["llm_boost_api_key"] = os.environ.get('LLM_BOOST_API_KEY', '')
        settings["llm_boost_base_url"] = os.environ.get('LLM_BOOST_BASE_URL', '')
        settings["llm_boost_model_name"] = os.environ.get('LLM_BOOST_MODEL_NAME', '')
        settings["configured"] = True
        save_settings(settings)
