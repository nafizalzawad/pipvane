"""
TDLib Client Interface, Native Ctypes Wrapper, and Deterministic Mock Client.

Provides the official TDLib JSON client abstraction per TDLib specification.
Supports native `tdjson.dll` execution as well as a full offline MockTDLibClient
for Strict Mode testing and CI verification without live network dependencies.
"""

from __future__ import annotations

import abc
import json
import queue
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger("pipvane.engine.tdlib_client")


class TDLibError(Exception):
    """Base exception for TDLib errors."""
    pass


class TDLibLibraryNotFoundError(TDLibError):
    """Raised when native tdjson library cannot be located on the host."""
    pass


class TDLibClientInterface(abc.ABC):
    """Abstract interface defining the TDLib JSON client contract."""

    @abc.abstractmethod
    def send(self, request: Dict[str, Any]) -> None:
        """Asynchronously dispatches a request dictionary to TDLib."""
        pass

    @abc.abstractmethod
    def receive(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Blocks for up to timeout seconds waiting for an incoming update or response."""
        pass

    @abc.abstractmethod
    def execute(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Synchronously executes a pure TDLib request and returns response immediately."""
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """Destroys the TDLib client and releases native resources."""
        pass


class NativeTDLibClient(TDLibClientInterface):
    """
    Official TDLib JSON wrapper using Win32 ctypes to bind to tdjson.dll.
    Conforms to the official TDLib JSON interface:
    - td_json_client_create
    - td_json_client_send
    - td_json_client_receive
    - td_json_client_execute
    - td_json_client_destroy
    """

    def __init__(self, library_path: str = "tdjson.dll") -> None:
        import ctypes
        try:
            self._lib = ctypes.CDLL(library_path)
        except OSError as e:
            raise TDLibLibraryNotFoundError(
                f"Failed to load native TDLib library from '{library_path}': {e}"
            ) from e

        # td_json_client_create
        self._lib.td_json_client_create.restype = ctypes.c_void_p
        self._lib.td_json_client_create.argtypes = []

        # td_json_client_send
        self._lib.td_json_client_send.restype = None
        self._lib.td_json_client_send.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

        # td_json_client_receive
        self._lib.td_json_client_receive.restype = ctypes.c_char_p
        self._lib.td_json_client_receive.argtypes = [ctypes.c_void_p, ctypes.c_double]

        # td_json_client_execute
        self._lib.td_json_client_execute.restype = ctypes.c_char_p
        self._lib.td_json_client_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

        # td_json_client_destroy
        self._lib.td_json_client_destroy.restype = None
        self._lib.td_json_client_destroy.argtypes = [ctypes.c_void_p]

        self._handle = self._lib.td_json_client_create()
        if not self._handle:
            raise TDLibError("Failed to create native TDLib client handle")

    def send(self, request: Dict[str, Any]) -> None:
        if not self._handle:
            raise TDLibError("Client is closed")
        data = json.dumps(request).encode("utf-8")
        self._lib.td_json_client_send(self._handle, data)

    def receive(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        if not self._handle:
            return None
        res_ptr = self._lib.td_json_client_receive(self._handle, float(timeout))
        if not res_ptr:
            return None
        return json.loads(res_ptr.decode("utf-8"))

    def execute(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._handle:
            return None
        data = json.dumps(request).encode("utf-8")
        res_ptr = self._lib.td_json_client_execute(self._handle, data)
        if not res_ptr:
            return None
        return json.loads(res_ptr.decode("utf-8"))

    def close(self) -> None:
        if self._handle:
            handle = self._handle
            self._handle = None
            self._lib.td_json_client_destroy(handle)


class MockTDLibClient(TDLibClientInterface):
    """
    In-memory mock TDLib client conforming to the TDLib protocol.
    Enables Strict Mode testing of:
    - Authentication state machine (Phone, 2FA, QR code)
    - Channel message ingestion and backfill history queries
    - Connection states and reconnect loops
    Without initiating any external network socket connections.
    """

    def __init__(self, initial_auth_state: str = "authorizationStateWaitTdlibParameters") -> None:
        self.sent_requests: List[Dict[str, Any]] = []
        self._inbox: queue.Queue[Dict[str, Any]] = queue.Queue()
        self._closed = False
        self.current_auth_state = initial_auth_state
        self.mock_chat_histories: Dict[int, List[Dict[str, Any]]] = {}

        # Emit initial auth state
        if initial_auth_state:
            self._inbox.put({
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": initial_auth_state}
            })

    def send(self, request: Dict[str, Any]) -> None:
        if self._closed:
            raise TDLibError("Client is closed")

        self.sent_requests.append(request)
        req_type = request.get("@type", "")
        extra = request.get("@extra")

        # Mock TDLib internal state machine responses
        if req_type == "setTdlibParameters":
            self.current_auth_state = "authorizationStateWaitPhoneNumber"
            self._inbox.put({
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": self.current_auth_state}
            })
            if extra is not None:
                self._inbox.put({"@type": "ok", "@extra": extra})

        elif req_type == "setAuthenticationPhoneNumber":
            self.current_auth_state = "authorizationStateWaitCode"
            self._inbox.put({
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": self.current_auth_state}
            })
            if extra is not None:
                self._inbox.put({"@type": "ok", "@extra": extra})

        elif req_type == "checkAuthenticationCode":
            code = request.get("code", "")
            if code == "2FA":
                self.current_auth_state = "authorizationStateWaitPassword"
            else:
                self.current_auth_state = "authorizationStateReady"
            self._inbox.put({
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": self.current_auth_state}
            })
            if extra is not None:
                self._inbox.put({"@type": "ok", "@extra": extra})

        elif req_type == "checkAuthenticationPassword":
            self.current_auth_state = "authorizationStateReady"
            self._inbox.put({
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": self.current_auth_state}
            })
            if extra is not None:
                self._inbox.put({"@type": "ok", "@extra": extra})

        elif req_type == "requestQrCodeAuthentication":
            self.current_auth_state = "authorizationStateWaitOtherDeviceConfirmation"
            self._inbox.put({
                "@type": "updateAuthorizationState",
                "authorization_state": {
                    "@type": self.current_auth_state,
                    "link": "tg://login?token=MOCK_SECURE_TOKEN_XYZ12345"
                }
            })
            if extra is not None:
                self._inbox.put({"@type": "ok", "@extra": extra})

        elif req_type == "getChatHistory":
            chat_id = request.get("chat_id")
            from_msg_id = request.get("from_message_id", 0)
            limit = request.get("limit", 50)

            history = self.mock_chat_histories.get(chat_id, [])
            # Return slice of messages
            # In TDLib, if from_msg_id == 0, returns latest messages
            if from_msg_id == 0:
                selected = history[:limit]
            else:
                # Messages older or starting from from_msg_id
                selected = [m for m in history if m.get("id", 0) <= from_msg_id][:limit]

            response = {
                "@type": "messages",
                "total_count": len(selected),
                "messages": selected,
            }
            if extra is not None:
                response["@extra"] = extra
            self._inbox.put(response)

        elif req_type in ("close", "logOut"):
            self.current_auth_state = "authorizationStateClosed"
            self._inbox.put({
                "@type": "updateAuthorizationState",
                "authorization_state": {"@type": "authorizationStateClosed"}
            })
            if extra is not None:
                self._inbox.put({"@type": "ok", "@extra": extra})

    def receive(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        if self._closed:
            return None
        try:
            return self._inbox.get(block=True, timeout=max(0.001, timeout))
        except queue.Empty:
            return None

    def execute(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        req_type = request.get("@type", "")
        if req_type == "getTextEntities":
            return {"@type": "formattedText", "text": request.get("text", ""), "entities": []}
        return {"@type": "ok"}

    def close(self) -> None:
        self._closed = True

    # --- Test Helper Methods ---

    def simulate_incoming_update(self, update: Dict[str, Any]) -> None:
        """Injects an update into the receive inbox (e.g., updateNewMessage)."""
        self._inbox.put(update)

    def set_mock_chat_history(self, chat_id: int, messages: List[Dict[str, Any]]) -> None:
        """Sets pre-canned messages for getChatHistory tests."""
        self.mock_chat_histories[chat_id] = messages
