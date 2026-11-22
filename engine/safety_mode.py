"""
Pipvane v3 Strict Safety Mode Guard.

Enforces a strictly read-only MTProto ingestion pipeline to protect user accounts
from spam flags, rate limits, and Telegram bans.
Completely prohibits and intercepts any outbound write, messaging, reaction,
channel joining, or mutating TDLib API calls.
"""

from __future__ import annotations

import logging
from typing import Set

logger = logging.getLogger("pipvane.engine.safety_mode")


class SafetyModeViolationError(PermissionError):
    """Raised when an outbound write or mutating Telegram operation is attempted."""
    pass


# Explicit blacklist of mutating / outbound TDLib methods
FORBIDDEN_TDLIB_METHODS: Set[str] = {
    # Outbound messaging
    "sendmessage",
    "sendchataction",
    "forwardmessages",
    "sendbotstartmessage",
    "sendinlinequeryresultmessage",
    "resendmessages",
    "sendpaymentform",
    # Reactions
    "setmessagereactions",
    "addreaction",
    "removereaction",
    # Channel/Chat membership mutations
    "joinchat",
    "joinchatbyinvitelink",
    "leavechat",
    "addchatmember",
    "addchatmembers",
    "deletechat",
    "deletechathistory",
    "deletechatmessagesfromuser",
    # Message mutations
    "deletemessages",
    "editmessagetext",
    "editmessagecaption",
    "editmessagereplymarkup",
    "editmessagemedia",
    "pinchatmessage",
    "unpinchatmessage",
    "unpinallchatmessages",
    # Chat property mutations
    "setchattitle",
    "setchatphoto",
    "setchatdraftmessage",
    "setchatpermissions",
    "setchatdescription",
    "setchatmemberstatus",
    # Chat creation
    "createprivatechat",
    "createbasicgroupchat",
    "createsupergroupchat",
    "createsecretchat",
    "createnewsupergroupchat",
    "createnewbasicgroupchat",
    "createnewsecretchat",
    # Bot interactions
    "answercallbackquery",
    "answerinlinequery",
    "answerprecheckoutquery",
    "answershippingquery",
    "sendcustomrequest",
}

# Whitelist of explicitly allowed read-only or initialization TDLib methods
ALLOWED_TDLIB_METHODS: Set[str] = {
    # Initialization and auth
    "settdlibparameters",
    "setauthenticationphonenumber",
    "checkauthenticationcode",
    "checkauthenticationpassword",
    "requestqrcodeauthentication",
    "setdatabaseencryptionkey",
    "checkdatabaseencryptionkey",
    "getauthorizationstate",
    # Read-only ingestion
    "getchat",
    "getchats",
    "getchathistory",
    "getmessage",
    "getmessages",
    "getuser",
    "getme",
    "getsupergroup",
    "getbasicgroup",
    "getchatadministrators",
    "getchatmember",
    "searchchatmessages",
    "getconnectionstate",
    # Lifecycle
    "logout",
    "close",
    "destroy",
    "getoption",
    "setoption",  # used for tdlib internal options (e.g., logging verbosity)
}


def assert_safety_mode_allows(method: str) -> None:
    """
    Validates that a TDLib method is permitted under Strict Safety Mode.
    Raises SafetyModeViolationError if the method attempts to write, post,
    react, join, leave, or modify any Telegram chat state.
    """
    normalized = method.strip().lower()

    # Check explicit forbidden list
    if normalized in FORBIDDEN_TDLIB_METHODS:
        raise SafetyModeViolationError(
            f"Safety Mode Violation: Method '{method}' is strictly forbidden. Outbound messages, reactions, "
            f"and channel joins/leaves are disabled by policy to prevent account bans."
        )

    # Prefix inspection for write/mutating heuristics
    mutating_prefixes = ("send", "post", "delete", "edit", "join", "leave", "pin", "unpin", "react", "create")
    if any(normalized.startswith(prefix) for prefix in mutating_prefixes):
        if normalized not in ALLOWED_TDLIB_METHODS:
            raise SafetyModeViolationError(
                f"Safety Mode Violation: Mutating method '{method}' matches prohibited action pattern. "
                f"Outbound modifications are strictly disabled."
            )
