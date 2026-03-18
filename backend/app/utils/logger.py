"""
Logging Configuration Module
Provides unified log management with output to both console and file.
Console output is always flushed immediately so logs appear in real time
when the backend is run via concurrently / npm start.
"""

import os
import sys
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler


def _ensure_utf8_stdout():
    """
    Ensure stdout/stderr use UTF-8 encoding.
    Fixes encoding issues in Windows console.
    """
    if sys.platform == 'win32':
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')


# Log directory
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')


class FlushingStreamHandler(logging.StreamHandler):
    """
    A StreamHandler that flushes after every emit.
    Required so log lines appear immediately when stdout is piped
    (e.g. through concurrently in npm start).
    """
    def emit(self, record):
        super().emit(record)
        self.flush()


def setup_logger(name: str = 'mirofish', level: int = logging.DEBUG) -> logging.Logger:
    """
    Set up a logger with both file and console handlers.

    Args:
        name: Logger name
        level: Log level

    Returns:
        Configured logger
    """
    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent log propagation to root logger to avoid duplicate output
    logger.propagate = False

    # If handlers already exist, do not add again
    if logger.handlers:
        return logger

    # Log formats
    detailed_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    simple_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    )

    # 1. File handler — detailed logs (named by date, with rotation)
    log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, log_filename),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)

    # 2. Console handler — INFO and above, flushed immediately after every line
    _ensure_utf8_stdout()
    console_handler = FlushingStreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_logger(name: str = 'mirofish') -> logging.Logger:
    """
    Get a logger (creates one if it does not exist).

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger


def configure_werkzeug_logging():
    """
    Configure Werkzeug's request logger.

    Suppresses the noisy per-request access lines (127.0.0.1 GET /api/...)
    and keeps only WARNING+ messages so the console stays readable.
    Meaningful app-level activity is already emitted via mirofish.* loggers.
    """
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.WARNING)


# Create default logger
logger = setup_logger()

# Silence Werkzeug request spam — app-level logs are more informative
configure_werkzeug_logging()


# Convenience methods
def debug(msg, *args, **kwargs):
    logger.debug(msg, *args, **kwargs)

def info(msg, *args, **kwargs):
    logger.info(msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    logger.warning(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    logger.error(msg, *args, **kwargs)

def critical(msg, *args, **kwargs):
    logger.critical(msg, *args, **kwargs)
