"""
Pipvane v3 Anti-Split-Brain Failover Simulation Module (qa/chaos/failover_simulation.py).

Proxies to the root failover_simulation.py module for seamless execution
from either repository root or the qa/chaos test harness package.
"""

from __future__ import annotations

import os
import sys

# Ensure root directory is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from failover_simulation import (  # noqa: E402
    NodeRole,
    NodeState,
    SplitBrainViolationError,
    VirtualClock,
    WebSocketLeaseCoordinator,
    MockLocalAgent,
    FailoverSimulationOrchestrator,
    main,
)

__all__ = [
    "NodeRole",
    "NodeState",
    "SplitBrainViolationError",
    "VirtualClock",
    "WebSocketLeaseCoordinator",
    "MockLocalAgent",
    "FailoverSimulationOrchestrator",
    "main",
]

if __name__ == "__main__":
    main()
