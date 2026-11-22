"""
Pipvane v3 - Master Application Entry Point.
===========================================
Executes the Pipvane Master Orchestration Daemon.
"""

import sys
from pathlib import Path

# Ensure root directory is on sys.path
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from engine.main import main

if __name__ == "__main__":
    sys.exit(main())
