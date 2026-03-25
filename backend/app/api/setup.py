"""
Setup API - Configuration management endpoints.
Provides a web-based alternative to editing .env files.
"""

from flask import request, jsonify
from . import setup_bp
from ..config import Config
from ..settings import load_settings, save_settings


@setup_bp.route('/status', methods=['GET'])
def get_status():
    """Check if the application is configured."""
    settings = load_settings()
    return jsonify({
        'configured': settings.get('configured', False),
        'has_llm_key': bool(settings.get('llm_api_key')),
        'llm_base_url': settings.get('llm_base_url', ''),
        'llm_model_name': settings.get('llm_model_name', ''),
        'embed_model': settings.get('embed_model', 'nomic-embed-text'),
    })


@setup_bp.route('/save', methods=['POST'])
def save_config():
    """Save configuration to settings.json and reload Config."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    settings = load_settings()
    for key in ['llm_api_key', 'llm_base_url', 'llm_model_name',
                'embed_model',
                'llm_boost_api_key', 'llm_boost_base_url', 'llm_boost_model_name']:
        if key in data:
            settings[key] = data[key]

    # Configured when base URL and model name are set (API key is optional for Ollama)
    settings['configured'] = bool(settings.get('llm_base_url')) and bool(settings.get('llm_model_name'))
    save_settings(settings)
    Config.reload_from_settings()

    return jsonify({'success': True, 'configured': settings['configured']})


@setup_bp.route('/validate', methods=['POST'])
def validate_config():
    """Validate LLM connection by making a test API call."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    results = {'llm': False, 'errors': []}

    llm_key = data.get('llm_api_key', 'ollama') or 'ollama'
    llm_base_url = data.get('llm_base_url', 'http://localhost:11434/v1')
    llm_model = data.get('llm_model_name', 'qwen2.5:7b')

    try:
        from openai import OpenAI
        client = OpenAI(api_key=llm_key, base_url=llm_base_url)
        client.chat.completions.create(
            model=llm_model,
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=5,
        )
        results['llm'] = True
    except Exception as e:
        error_msg = str(e)
        # Sanitize: don't expose internal URLs or keys
        if 'api_key' in error_msg.lower() or 'apikey' in error_msg.lower() or 'api-key' in error_msg.lower():
            error_msg = "LLM validation failed: authentication error"
        results['errors'].append(f"LLM validation failed: {error_msg}")

    results['valid'] = results['llm']
    return jsonify(results)
