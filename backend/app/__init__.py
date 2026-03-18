"""
MiroFish Backend - Flask application factory.
"""

import os
import warnings

# Suppress multiprocessing resource_tracker warnings from third-party libs
warnings.filterwarnings("ignore", message=".*resource_tracker.*")

from flask import Flask, request
from flask_cors import CORS

from .config import Config
from .utils.logger import setup_logger, get_logger, configure_werkzeug_logging


def create_app(config_class=Config):
    """Flask application factory."""
    app = Flask(__name__)
    app.config.from_object(config_class)

    # JSON encoding: support Unicode characters directly
    if hasattr(app, 'json') and hasattr(app.json, 'ensure_ascii'):
        app.json.ensure_ascii = False

    # Setup logging
    logger = setup_logger('mirofish')

    # Silence Werkzeug HTTP access logs — app-level logs are more informative
    configure_werkzeug_logging()

    # Only log startup info once (avoid duplicate logs in debug/reload mode)
    is_reloader_process = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
    debug_mode = app.config.get('DEBUG', False)
    should_log_startup = not debug_mode or is_reloader_process

    if should_log_startup:
        logger.info("MiroFish backend initializing...")

    # Enable CORS
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # Register simulation process cleanup handler
    from .services.simulation_runner import SimulationRunner
    SimulationRunner.register_cleanup()

    # Register blueprints
    from .api import graph_bp, simulation_bp, report_bp, setup_bp
    app.register_blueprint(graph_bp, url_prefix='/api/graph')
    app.register_blueprint(simulation_bp, url_prefix='/api/simulation')
    app.register_blueprint(report_bp, url_prefix='/api/report')
    app.register_blueprint(setup_bp, url_prefix='/api/setup')

    # Health check
    @app.route('/health')
    def health():
        return {'status': 'ok', 'service': 'MiroFish Backend'}

    # Activity logging middleware — log meaningful API calls to the console
    _NOISY_PATHS = {'/health', '/api/simulation/'}  # prefixes to skip
    _POLLING_SUFFIXES = (
        '/run-status', '/progress', '/agent-log', '/console-log',
        '/profiles/stream', '/config/stream', '/prepare/status',
    )

    @app.before_request
    def log_activity():
        path = request.path
        method = request.method

        # Skip polling endpoints (called every second by the UI)
        if any(path.endswith(s) for s in _POLLING_SUFFIXES):
            return
        # Skip health checks and static GET requests on list endpoints
        if path == '/health':
            return

        # Only log mutating requests and important GETs
        if method in ('POST', 'DELETE', 'PUT', 'PATCH'):
            body = request.get_json(silent=True) or {}
            sim_id = body.get('simulation_id', '')
            detail = f" [{sim_id}]" if sim_id else ""
            act_logger = get_logger('mirofish.api')
            act_logger.info(f"→ {method} {path}{detail}")

    if should_log_startup:
        logger.info("MiroFish backend ready — awaiting requests")

    return app
