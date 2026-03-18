"""
MiroFish Backend entry point.
"""

import os
import sys

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


def main():
    """Main entry point."""
    # Validate configuration (warn but don't exit - setup page will handle it)
    errors = Config.validate()
    if errors:
        print("Warning: Configuration incomplete:")
        for err in errors:
            print(f"  - {err}")
        print("\nThe app will start anyway. Configure via the setup page at http://localhost:3000/setup")

    # Create application
    app = create_app()

    # Runtime configuration
    host = os.environ.get('FLASK_HOST', '0.0.0.0')
    port = int(os.environ.get('FLASK_PORT', 5001))

    # Start server — disable Werkzeug reloader and interactive debugger
    # to prevent debug PIN prompts and multi-process port conflicts.
    app.run(host=host, port=port, debug=False, use_reloader=False, use_debugger=False, threaded=True)


if __name__ == '__main__':
    main()
