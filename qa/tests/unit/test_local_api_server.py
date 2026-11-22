"""
Unit tests for LocalAPIServer (Sprint 9 Directive).

Validates:
1. Strict Localhost Binding & Rejection of external interfaces.
2. DNS Rebinding protection via Host header inspection.
3. Cryptographic session token generation, persistence, and verification.
4. REST endpoints: /api/status, /api/accounts, /api/settings, /api/logs.
5. High-frequency WebSocket telemetry feed for trade alerts and IPC latency metrics.
6. Server lifecycle, graceful shutdown, and context manager support.
"""

import asyncio
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
import websockets

from engine.local_api_server import (
    APIServerConfig,
    LocalAPIServer,
    LocalBindingViolationError,
    SessionTokenError,
)


@pytest.fixture
def temp_token_file(tmp_path: Path) -> Path:
    """Fixture providing a temporary session.token path."""
    return tmp_path / "pipvane_session.token"


@pytest.fixture
def running_server(temp_token_file: Path):
    """Fixture spinning up an active LocalAPIServer on dynamic ephemeral ports."""
    server = LocalAPIServer(
        host="127.0.0.1",
        port=0,
        ws_port=0,
        token_path=temp_token_file,
    )
    server.start(timeout=5.0)
    try:
        yield server
    finally:
        server.stop(timeout=3.0)


# =============================================================================
# 1. Strict Localhost Binding & Security Tests
# =============================================================================

def test_binding_violation_on_non_loopback_host(temp_token_file: Path):
    """LocalAPIServer must strictly reject non-127.0.0.1 addresses."""
    with pytest.raises(LocalBindingViolationError):
        LocalAPIServer(host="0.0.0.0", token_path=temp_token_file)

    with pytest.raises(LocalBindingViolationError):
        LocalAPIServer(host="192.168.1.100", token_path=temp_token_file)

    with pytest.raises(LocalBindingViolationError):
        APIServerConfig(host="0.0.0.0")


def test_dns_rebinding_protection(running_server: LocalAPIServer):
    """Requests with non-localhost Host headers must be rejected with HTTP 403."""
    url = f"{running_server.get_http_url()}/api/status"
    req = urllib.request.Request(
        url,
        headers={
            "Host": "evil-attacker-site.com",
            "X-Session-Token": running_server.get_session_token(),
        },
    )

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)

    assert exc_info.value.code == 403
    body = json.loads(exc_info.value.read().decode())
    assert "DNS rebinding" in body.get("message", "")


def test_cors_headers_loopback_only(running_server: LocalAPIServer):
    """CORS headers must be granted for localhost/127.0.0.1 and omitted for evil origins."""
    url = f"{running_server.get_http_url()}/api/status"

    # Permitted origin (Next.js default dev port)
    req_good = urllib.request.Request(
        url,
        headers={
            "Origin": "http://localhost:3000",
            "X-Session-Token": running_server.get_session_token(),
        },
    )
    with urllib.request.urlopen(req_good) as resp:
        assert resp.headers.get("Access-Control-Allow-Origin") == "http://localhost:3000"

    # Malicious origin
    req_bad = urllib.request.Request(
        url,
        headers={
            "Origin": "http://malicious-website.io",
            "X-Session-Token": running_server.get_session_token(),
        },
    )
    with urllib.request.urlopen(req_bad) as resp:
        assert resp.headers.get("Access-Control-Allow-Origin") is None


# =============================================================================
# 2. Session Token Generation & Authentication Tests
# =============================================================================

def test_session_token_lifecycle_and_file_persistence(temp_token_file: Path):
    """Validate token generation, disk persistence on start, and cleanup on stop."""
    server = LocalAPIServer(
        host="127.0.0.1",
        port=0,
        ws_port=0,
        token_path=temp_token_file,
    )
    token = server.get_session_token()
    assert len(token) == 64  # 32 bytes hex-encoded = 64 characters
    assert not temp_token_file.exists()

    server.start()
    try:
        assert temp_token_file.exists()
        persisted = temp_token_file.read_text(encoding="utf-8").strip()
        assert persisted == token
    finally:
        server.stop()

    # Token file should be wiped upon clean server stop
    assert not temp_token_file.exists()


def test_unauthenticated_request_rejected(running_server: LocalAPIServer):
    """Endpoints under /api/* must return HTTP 401 when token is missing."""
    url = f"{running_server.get_http_url()}/api/status"
    req = urllib.request.Request(url)

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)

    assert exc_info.value.code == 401
    body = json.loads(exc_info.value.read().decode())
    assert body.get("error") == "Unauthorized"


def test_invalid_token_rejected(running_server: LocalAPIServer):
    """Endpoints must reject forged or mismatched session tokens."""
    url = f"{running_server.get_http_url()}/api/status"
    req = urllib.request.Request(
        url,
        headers={"X-Session-Token": "bad_token_99999999999999999999999999999999"},
    )

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)

    assert exc_info.value.code == 401


def test_authentication_methods(running_server: LocalAPIServer):
    """Session token can be supplied via X-Session-Token, Authorization Bearer, or ?token=."""
    token = running_server.get_session_token()
    base_url = running_server.get_http_url()

    # 1. Via X-Session-Token header
    req1 = urllib.request.Request(
        f"{base_url}/api/status",
        headers={"X-Session-Token": token},
    )
    with urllib.request.urlopen(req1) as resp:
        assert resp.status == 200

    # 2. Via Authorization: Bearer <token>
    req2 = urllib.request.Request(
        f"{base_url}/api/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req2) as resp:
        assert resp.status == 200

    # 3. Via Query parameter ?token=<token>
    req3 = urllib.request.Request(f"{base_url}/api/status?token={token}")
    with urllib.request.urlopen(req3) as resp:
        assert resp.status == 200


# =============================================================================
# 3. REST Endpoints Tests
# =============================================================================

def test_get_status_endpoint(running_server: LocalAPIServer):
    """GET /api/status returns engine health, telegram safety, and license status."""
    token = running_server.get_session_token()
    req = urllib.request.Request(
        f"{running_server.get_http_url()}/api/status",
        headers={"X-Session-Token": token},
    )

    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode())

    assert data["engine_online"] is True
    assert data["coordinator_status"] in ("active", "grace_period", "offline")
    assert "ipc_latency_ms" in data
    assert "websocket_url" in data
    assert data["telegram"]["safety_mode"] is True
    assert data["circuit_breaker"]["tripped"] is False
    assert data["license"]["valid"] is True
    assert data["license"]["account_limit"] >= 1


def test_get_accounts_endpoint(running_server: LocalAPIServer):
    """GET /api/accounts returns list of registered broker terminals."""
    token = running_server.get_session_token()

    # Add a mock terminal
    running_server.register_terminal({
        "id": "term-test-99",
        "name": "Test MetaTrader 5 Bridge",
        "type": "MT5",
        "version": "Build 4450",
        "status": "connected",
        "latency_ms": 0.75,
        "account_number": "123456",
        "server": "MetaQuotes-Demo",
        "balance": 50000.0,
        "equity": 50120.0,
        "margin": 150.0,
        "free_margin": 49970.0,
        "open_positions": 1,
    })

    req = urllib.request.Request(
        f"{running_server.get_http_url()}/api/accounts",
        headers={"X-Session-Token": token},
    )

    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode())

    terminals = data.get("terminals", [])
    assert any(t["id"] == "term-test-99" for t in terminals)

    # Update terminal state and verify update
    running_server.update_terminal_state("term-test-99", {"equity": 50500.0, "open_positions": 2})

    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
        updated = next(t for t in data["terminals"] if t["id"] == "term-test-99")
        assert updated["equity"] == 50500.0
        assert updated["open_positions"] == 2


def test_post_settings_endpoint_valid(running_server: LocalAPIServer):
    """POST /api/settings updates operational risk parameters when valid."""
    token = running_server.get_session_token()
    payload = {
        "sizing_mode": "equity_percent",
        "fixed_lot": 0.25,
        "equity_percent": 1.5,
        "max_drawdown_guard": 5.0,
        "slippage_tolerance_pips": 2.0,
        "daily_loss_limit_usd": 750.0,
        "max_open_trades": 8,
    }

    req = urllib.request.Request(
        f"{running_server.get_http_url()}/api/settings",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Session-Token": token,
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode())
        assert data["success"] is True
        assert data["settings"]["fixed_lot"] == 0.25
        assert data["settings"]["daily_loss_limit_usd"] == 750.0


def test_post_settings_endpoint_validation_failure(running_server: LocalAPIServer):
    """POST /api/settings rejects invalid parameters with HTTP 400."""
    token = running_server.get_session_token()
    invalid_payload = {
        "sizing_mode": "invalid_mode_xyz",
        "fixed_lot": -0.5,  # must be positive
        "equity_percent": 150.0,  # exceeds 100%
    }

    req = urllib.request.Request(
        f"{running_server.get_http_url()}/api/settings",
        data=json.dumps(invalid_payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Session-Token": token,
        },
        method="POST",
    )

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)

    assert exc_info.value.code == 400
    data = json.loads(exc_info.value.read().decode())
    assert data.get("error") == "Validation Error"
    assert len(data.get("details", [])) >= 2


def test_get_logs_endpoint_and_pagination(running_server: LocalAPIServer):
    """GET /api/logs returns paginated and filtered log entries."""
    token = running_server.get_session_token()

    # Insert test logs
    for i in range(10):
        level = "ERROR" if i % 3 == 0 else "INFO"
        running_server.add_log(level, f"Log message #{i}", context={"idx": i})

    # 1. Fetch all with limit=5
    req = urllib.request.Request(
        f"{running_server.get_http_url()}/api/logs?limit=5&offset=0",
        headers={"X-Session-Token": token},
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode())
        assert len(data["logs"]) == 5
        assert data["total"] >= 10

    # 2. Filter by level=ERROR
    req_err = urllib.request.Request(
        f"{running_server.get_http_url()}/api/logs?level=ERROR",
        headers={"X-Session-Token": token},
    )
    with urllib.request.urlopen(req_err) as resp:
        assert resp.status == 200
        data_err = json.loads(resp.read().decode())
        assert all(entry["level"] == "ERROR" for entry in data_err["logs"])
        assert len(data_err["logs"]) >= 3


# =============================================================================
# 4. WebSocket Telemetry Feed Tests
# =============================================================================

@pytest.mark.asyncio
async def test_websocket_authentication_and_rejection(running_server: LocalAPIServer):
    """WebSocket connection without token or with invalid token must be rejected."""
    ws_url = running_server.get_ws_url()

    # 1. Reject missing token
    with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_missing:
        async with websockets.connect(ws_url) as ws:
            await ws.recv()
    assert exc_missing.value.rcvd.code == 1008

    # 2. Reject invalid token
    with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_invalid:
        async with websockets.connect(f"{ws_url}?token=invalid_hex_token_9999") as ws:
            await ws.recv()
    assert exc_invalid.value.rcvd.code == 1008

    # 3. Accept valid token via query string
    token = running_server.get_session_token()
    async with websockets.connect(f"{ws_url}?token={token}") as ws:
        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
        parsed = json.loads(msg)
        assert parsed.get("event") == "connection_established"


@pytest.mark.asyncio
async def test_websocket_ping_pong(running_server: LocalAPIServer):
    """WebSocket clients can send ping action and receive pong response."""
    token = running_server.get_session_token()
    ws_url = f"{running_server.get_ws_url()}?token={token}"

    async with websockets.connect(ws_url) as ws:
        # Drain welcome message
        await asyncio.wait_for(ws.recv(), timeout=2.0)

        # Send ping
        client_ts = time.time()
        await ws.send(json.dumps({"action": "ping", "timestamp": client_ts}))

        resp_raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
        resp = json.loads(resp_raw)
        assert resp.get("event") == "pong"
        assert resp.get("client_time") == client_ts


@pytest.mark.asyncio
async def test_websocket_trade_alert_and_latency_broadcast(running_server: LocalAPIServer):
    """Verify live trade alerts and IPC latency metrics are broadcast to connected clients."""
    token = running_server.get_session_token()
    ws_url = f"{running_server.get_ws_url()}?token={token}"

    async with websockets.connect(ws_url) as ws:
        # Drain initial welcome
        await asyncio.wait_for(ws.recv(), timeout=2.0)

        # 1. Broadcast Trade Alert
        trade_data = {
            "id": "TRD-99881",
            "symbol": "XAUUSD",
            "side": "BUY",
            "lots": 0.5,
            "entry": 2645.20,
            "sl": 2635.00,
            "tp_ladder": [2655.00, 2670.00],
            "status": "FILLED",
            "latency_ms": 0.82,
        }
        running_server.broadcast_trade_alert(trade_data)

        msg_trade = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
        assert msg_trade.get("event") == "trade_alert"
        assert msg_trade["data"]["symbol"] == "XAUUSD"
        assert msg_trade["data"]["latency_ms"] == 0.82

        # 2. Broadcast Latency Metric
        latency_data = {
            "terminal_id": "term-1",
            "latency_us": 640,
            "latency_ms": 0.64,
            "timestamp": time.time(),
        }
        running_server.broadcast_latency_metric(latency_data)

        msg_lat = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
        assert msg_lat.get("event") == "latency_metric"
        assert msg_lat["data"]["latency_us"] == 640


# =============================================================================
# 5. Context Manager & Lifecycle Tests
# =============================================================================

def test_context_manager_support(temp_token_file: Path):
    """LocalAPIServer can be cleanly managed using python 'with' statement."""
    with LocalAPIServer(
        host="127.0.0.1",
        port=0,
        ws_port=0,
        token_path=temp_token_file,
    ) as srv:
        assert srv.is_running
        assert temp_token_file.exists()

        # Check HTTP connectivity
        req = urllib.request.Request(
            f"{srv.get_http_url()}/api/status",
            headers={"X-Session-Token": srv.get_session_token()},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200

    # Outside context manager, should be stopped and token removed
    assert not srv.is_running
    assert not temp_token_file.exists()
