"""
MiroFish Backend entry point.
"""

import os
import sys

# Force unbuffered output — ensures logs appear immediately in the console
# even when output is piped through concurrently or another tool
os.environ['PYTHONUNBUFFERED'] = '1'

# Fix Windows console UTF-8 encoding
if sys.platform == 'win32':
    os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from app.config import Config


def print_banner(host: str, port: int):
    """Print a startup banner to the console."""
    print("", flush=True)
    print("╔══════════════════════════════════════════════════╗", flush=True)
    print("║          MiroFish Simulation Engine              ║", flush=True)
    print("╠══════════════════════════════════════════════════╣", flush=True)
    print(f"║  Backend API  →  http://{host}:{port}           ║", flush=True)
    print(f"║  Frontend UI  →  http://localhost:3000          ║", flush=True)
    print(f"║  Setup page   →  http://localhost:3000/setup    ║", flush=True)
    print("╚══════════════════════════════════════════════════╝", flush=True)
    print("", flush=True)


def main():
    """Main entry point."""
    # Validate configuration (warn but don't exit - setup page will handle it)
    errors = Config.validate()
    if errors:
        print("⚠  Configuration incomplete:", flush=True)
        for err in errors:
            print(f"   • {err}", flush=True)
        print("   → Visit http://localhost:3000/setup to configure\n", flush=True)

    # Create application
    app = create_app()

    # Runtime configuration
    host = os.environ.get('FLASK_HOST', '0.0.0.0')
    port = int(os.environ.get('FLASK_PORT', 5001))

    print_banner(host, port)

    # Start server — disable Werkzeug reloader and interactive debugger
    # to prevent debug PIN prompts and multi-process port conflicts.
    app.run(host=host, port=port, debug=False, use_reloader=False, use_debugger=False, threaded=True)


if __name__ == '__main__':
    main()
