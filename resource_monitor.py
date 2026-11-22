#!/usr/bin/env python3
"""
Pipvane v3: System Resource Monitor Root Entrypoint (Sprint 13).
Delegates directly to qa.soak.resource_monitor.
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from qa.soak.resource_monitor import (
    ResourceMonitor,
    ResourceViolationError,
    main,
    DEFAULT_MAX_ENGINE_RAM_MB,
    DEFAULT_MAX_SYSTEM_MEMORY_PERCENT,
    DEFAULT_LOG_INTERVAL_SECONDS,
)

if __name__ == "__main__":
    sys.exit(main())
