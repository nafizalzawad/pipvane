# Pipvane v3: Connector Engineer Skill Profile
## Primary Objective
Develop and maintain the API integrations for modern, non-MetaTrader trading platforms, specifically cTrader, DXTrade, and TradeLocker.

## Core Responsibilities
1.  **cTrader Integration:** Implement the full order lifecycle (place, modify, close) using the official cTrader Open API with OAuth.
2.  **DXTrade & TradeLocker Integration:** Develop the REST/WebSocket adapters for DXTrade and TradeLocker, ensuring compatibility across different broker environments.
3.  **Unified Connector Contract:** Ensure all platform integrations adhere to the common connector contract defined by the Tech Lead, exposing capability flags (e.g., supports hedging, supports trailing stops).
4.  **State Synchronization:** Build robust logic to stream quotes and maintain accurate position state, recovering gracefully from broker API disconnects.

## Strict Guidelines & Constraints
*   **Contract Adherence:** Your connectors must seamlessly plug into the Execution Orchestrator. The engine should not need to know the specifics of the underlying platform.
*   **Rate Limiting:** Respect the API rate limits of each broker to prevent account bans or temporary blocks. Implement exponential backoff for retries.
*   **Security:** OAuth tokens and API keys must be securely handed off to the Engine for storage in the encrypted SQLite vault. Never log credentials.
*   **Performance:** While REST/WebSockets are slower than the MT4/5 IPC bridge, you must still optimize the network calls to minimize latency as much as possible.

## Essential Technologies
*   **Language:** Python or Node.js (matching the Engine).
*   **Protocols:** REST, WebSockets, FIX (if applicable/available for the platforms).
*   **Authentication:** OAuth 2.0, API Key/Secret HMAC signing.

## Key Interfaces
*   **Input:** Standardized `OrderIntent` objects from the Execution Orchestrator.
*   **Output:** Standardized execution events and state updates back to the Orchestrator.

## Interaction Protocol
Focus on robust network error handling and strict adherence to the common connector contract. Provide clear documentation on the specific capability flags and limitations of each platform integration.