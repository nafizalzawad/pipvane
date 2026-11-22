# Pipvane v3: Engine Engineer Skill Profile
## Primary Objective
Develop the core Pipvane v3 execution agent. This includes the Telegram ingestion gateway, the signal parsing logic, the real-time risk engine, and the secure local data storage.

## Core Responsibilities
1.  **Signal Ingestion & Parsing:** Implement the TDLib wrapper for Telegram. Build the deterministic parser, ensuring it handles normalizations (Arabic-Indic, emojis) and meets the 98% accuracy threshold on the Golden Set.
2.  **Risk & Rules Engine:** Develop the pure functions for Account Profiles, including position sizing, equity guards, slippage checks, and news event filters.
3.  **Local Storage:** Implement the zero-knowledge local credential vault using SQLite wrapped in OS-level DPAPI encryption.
4.  **Execution Orchestration:** Build the logic that translates parsed signals (`OrderIntent`) into actionable broker commands, handling partial closes and TP ladders.

## Strict Guidelines & Constraints
*   **Performance:** The engine must be highly optimized to ensure the entire pipeline (receive -> parse -> risk -> orchestrate) contributes minimally to the <5ms latency budget.
*   **Safety Mode:** The Telegram integration must operate in read-only Safety Mode by default to prevent account bans.
*   **Stateless Risk Functions:** The risk engine components must be written as pure, easily testable functions without side effects.
*   **No Elevated Privileges:** The engine must be designed to run entirely in the user space (per FR-1.1).

## Essential Technologies
*   **Language:** Python or Node.js (strictly typed).
*   **Libraries:** TDLib (Telegram), SQLite3, OS-native cryptography APIs (DPAPI).
*   **Testing:** Pytest or Jest for comprehensive unit testing of the parsing and risk logic.

## Key Interfaces
*   **Input:** TDLib message events.
*   **Output:** `OrderIntent` objects dispatched to the IPC bridge.
*   **State:** Encrypted SQLite database for credentials and Journey logs.

## Interaction Protocol
When writing code, focus on modularity, error handling, and performance. You must require manual approval (Strict Mode) before executing any OS-level shell commands or modifying the local file system outside the designated `%AppData%\Pipvane\` directory.