"""
BetaFlo Middleman Server
========================
A lightweight HTTP server that sits between your App Inventor app and
FloLogic's cloud. Your app sends simple GET requests to this server;
this server handles the complex SignalR WebSocket protocol with FloLogic.

Endpoints (all are plain HTTP GET so App Inventor's WebClient works):

  GET /status
        Returns current valve mode, flow state, home limit, away limit,
        bypass time, and temperature as JSON.

  GET /mode?value=home|away|bypass|off
        Changes the valve mode.
        - home    → HOME mode
        - away    → AWAY mode
        - bypass  → BYPASS mode
        - off     → SHUTOFF mode

  GET /set_home?minutes=7
        Sets the Home mode flow time limit (integer minutes).

  GET /set_away?minutes=3
        Sets the Away mode flow time limit (float minutes).

  GET /set_bypass?minutes=90
        Sets the Bypass time (integer minutes).

  GET /set_auto_away?hours=24
        Sets the Auto-Away delay (integer hours).

  GET /set_sensitivity?value=0.5
        Sets the flow sensitivity (float oz/min, range 0.5–48).

All endpoints return JSON: {"ok": true} on success or {"ok": false, "error": "..."}.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
_LOGGER = logging.getLogger("betaflo")

# ── FloLogic cloud constants ──────────────────────────────────────────────────
HUB_URL = "https://hub-cloudapps-prod.azurewebsites.net/signalr"
RECORD_SEPARATOR = "\x1e"
DEVICE_NAME = "BetaFlo-Server"
DEVICE_CODE = "AND-betaflo-001"
DEVICE_TOKEN = "betaflo-token"

# ── Valve mode map ────────────────────────────────────────────────────────────
VALVE_MODES = {
    "home": 1,
    "away": 2,
    "bypass": 4,
    "off": 8,
    "shutoff": 8,
    "disabled": 16,
}
MODE_NAMES = {v: k for k, v in VALVE_MODES.items()}


# ─────────────────────────────────────────────────────────────────────────────
# SignalR helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _negotiate(session: aiohttp.ClientSession, hub_url: str) -> str:
    """POST /negotiate and return the connection token."""
    resp = await session.post(hub_url.rstrip("/") + "/negotiate")
    resp.raise_for_status()
    payload = await resp.json()
    token = payload.get("connectionToken") or payload.get("connectionId")
    if not token:
        raise RuntimeError("SignalR negotiate did not return a token")
    return token


async def _open_ws(
    session: aiohttp.ClientSession,
    hub_url: str,
    token: str,
) -> aiohttp.ClientWebSocketResponse:
    """Open the SignalR WebSocket and send the handshake."""
    ws_url = hub_url.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = f"{ws_url}?id={quote(token, safe='')}"
    ws = await session.ws_connect(ws_url)
    # SignalR JSON protocol handshake
    await ws.send_str(
        json.dumps({"protocol": "json", "version": 1}) + RECORD_SEPARATOR
    )
    return ws


def _frame(target: str, *arguments: Any) -> str:
    """Build a SignalR invocation frame."""
    return (
        json.dumps({"type": 1, "target": target, "arguments": list(arguments)},
                   separators=(",", ":"))
        + RECORD_SEPARATOR
    )


async def _read_event(
    ws: aiohttp.ClientWebSocketResponse,
    want: str,
    timeout: float = 30,
) -> list[Any]:
    """Read frames until we see the named event; return its arguments."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for {want!r}")
        msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
            raise RuntimeError(f"WebSocket closed while waiting for {want!r}")
        if msg.type != aiohttp.WSMsgType.TEXT:
            continue
        for raw in msg.data.split(RECORD_SEPARATOR):
            raw = raw.strip()
            if not raw:
                continue
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if frame.get("type") == 1 and frame.get("target") == want:
                return frame.get("arguments", [])


async def _login(ws: aiohttp.ClientWebSocketResponse, email: str, password: str):
    """Login and return (user, valve)."""
    # Send login
    await ws.send_str(_frame("Login", email, password, DEVICE_NAME, None))

    # Collect both LoggedIn and ValveSent (whichever order they arrive)
    user = None
    valve = None
    deadline = asyncio.get_event_loop().time() + 30
    while user is None or valve is None:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
        if msg.type != aiohttp.WSMsgType.TEXT:
            continue
        for raw in msg.data.split(RECORD_SEPARATOR):
            raw = raw.strip()
            if not raw:
                continue
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if frame.get("type") != 1:
                continue
            target = frame.get("target", "")
            args = frame.get("arguments", [])
            if target == "LoggedIn" and args:
                user = args[0]
            elif target == "ValveSent" and args:
                valve = args[0]

    if user is None:
        raise RuntimeError("FloLogic login did not return a user — check credentials")
    if valve is None:
        # Try RefreshValveArray
        await ws.send_str(_frame("RefreshValveArray", user))
        args = await _read_event(ws, "ValveArraySent", timeout=20)
        devices = args[0] if args else []
        valve = devices[0] if devices else None
    if valve is None:
        raise RuntimeError("FloLogic login did not return a valve")
    return user, valve


# ─────────────────────────────────────────────────────────────────────────────
# Main FloLogic session: login → do something → close
# ─────────────────────────────────────────────────────────────────────────────

async def _with_flologic(email: str, password: str, action):
    """
    Open a SignalR session, log in, run action(ws, user, valve), close.
    action is an async callable that receives (ws, user, valve).
    Returns whatever action returns.
    """
    async with aiohttp.ClientSession() as session:
        token = await _negotiate(session, HUB_URL)
        ws = await _open_ws(session, HUB_URL, token)
        try:
            user, valve = await _login(ws, email, password)
            return await action(ws, user, valve)
        finally:
            with suppress(Exception):
                await ws.close()


async def _send_state_change(ws, user, valve, fields: dict[str, Any]) -> None:
    """Send a RequestStateChange and wait for StateChangeResult."""
    command = {
        "active": True,
        "created": datetime.now(UTC).isoformat(),
        "userId": user["id"],
        "valveId": valve["id"],
        **fields,
    }
    await ws.send_str(_frame("RequestStateChange", user, valve, command))
    await _read_event(ws, "StateChangeResult", timeout=45)


async def fetch_status(email: str, password: str) -> dict[str, Any]:
    """Fetch current valve status."""
    async def action(ws, user, valve):
        return {
            "ok": True,
            "mode": _mode_name(valve.get("mode")),
            "flow_state": valve.get("flowState"),
            "home_limit_minutes": valve.get("homeIntervalTime"),
            "away_limit_minutes": valve.get("awayIntervalTime"),
            "bypass_time_minutes": valve.get("bypassTime"),
            "auto_away_hours": valve.get("autoAwayTime"),
            "temperature": valve.get("temperature"),
            "drip_rate": valve.get("dripRate"),
            "valve_name": (
                valve.get("valveFriendlyName")
                or valve.get("combinedName")
                or valve.get("name")
                or "FloLogic"
            ),
        }
    return await _with_flologic(email, password, action)


async def set_mode(email: str, password: str, mode: str) -> dict[str, Any]:
    """Set valve mode."""
    mode = mode.lower()
    if mode not in VALVE_MODES:
        return {"ok": False, "error": f"Unknown mode '{mode}'. Use: home/away/bypass/off"}
    mode_value = VALVE_MODES[mode]

    async def action(ws, user, valve):
        await _send_state_change(ws, user, valve, {"mode": mode_value})
        return {"ok": True, "mode": mode}

    return await _with_flologic(email, password, action)


async def set_home_limit(email: str, password: str, minutes: int) -> dict[str, Any]:
    """Set home flow time limit in minutes."""
    async def action(ws, user, valve):
        await _send_state_change(ws, user, valve, {"homeIntervalTime": int(minutes)})
        return {"ok": True, "home_limit_minutes": int(minutes)}
    return await _with_flologic(email, password, action)


async def set_away_limit(email: str, password: str, minutes: float) -> dict[str, Any]:
    """Set away flow time limit in minutes."""
    async def action(ws, user, valve):
        await _send_state_change(ws, user, valve, {"awayIntervalTime": float(minutes)})
        return {"ok": True, "away_limit_minutes": float(minutes)}
    return await _with_flologic(email, password, action)


async def set_bypass_time(email: str, password: str, minutes: int) -> dict[str, Any]:
    """Set bypass time in minutes."""
    async def action(ws, user, valve):
        await _send_state_change(ws, user, valve, {"bypassTime": int(minutes)})
        return {"ok": True, "bypass_time_minutes": int(minutes)}
    return await _with_flologic(email, password, action)


async def set_auto_away(email: str, password: str, hours: int) -> dict[str, Any]:
    """Set auto-away delay in hours."""
    async def action(ws, user, valve):
        await _send_state_change(ws, user, valve, {"autoAwayTime": int(hours)})
        return {"ok": True, "auto_away_hours": int(hours)}
    return await _with_flologic(email, password, action)


async def set_sensitivity(email: str, password: str, value: float) -> dict[str, Any]:
    """Set flow sensitivity in oz/min (0.5–48)."""
    async def action(ws, user, valve):
        await _send_state_change(ws, user, valve, {"dripRate": float(value)})
        return {"ok": True, "sensitivity": float(value)}
    return await _with_flologic(email, password, action)


def _mode_name(mode_value) -> str:
    if mode_value is None:
        return "unknown"
    try:
        v = int(mode_value)
    except (TypeError, ValueError):
        return "unknown"
    # exact match first
    if v in MODE_NAMES:
        return MODE_NAMES[v]
    # flag check
    if v & 8:
        return "shutoff"
    if v & 4:
        return "bypass"
    if v & 2:
        return "away"
    if v & 1:
        return "home"
    return f"unknown_{v}"


# ─────────────────────────────────────────────────────────────────────────────
# HTTP handlers
# ─────────────────────────────────────────────────────────────────────────────

def _get_credentials(request: web.Request):
    """Pull email/password from query string or env."""
    email = (
        request.rel_url.query.get("email")
        or os.environ.get("FLOLOGIC_EMAIL", "")
    )
    password = (
        request.rel_url.query.get("password")
        or os.environ.get("FLOLOGIC_PASSWORD", "")
    )
    if not email or not password:
        raise ValueError(
            "FloLogic credentials missing. "
            "Set FLOLOGIC_EMAIL and FLOLOGIC_PASSWORD env vars, "
            "or pass ?email=...&password=... in the URL."
        )
    return email, password


def _json_response(data: dict) -> web.Response:
    return web.Response(
        text=json.dumps(data),
        content_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )


async def handle_status(request: web.Request) -> web.Response:
    try:
        email, password = _get_credentials(request)
        result = await fetch_status(email, password)
        return _json_response(result)
    except Exception as exc:
        _LOGGER.exception("Error in /status")
        return _json_response({"ok": False, "error": str(exc)})


async def handle_mode(request: web.Request) -> web.Response:
    try:
        email, password = _get_credentials(request)
        mode = request.rel_url.query.get("value", "")
        if not mode:
            return _json_response({"ok": False, "error": "Missing ?value= parameter"})
        result = await set_mode(email, password, mode)
        return _json_response(result)
    except Exception as exc:
        _LOGGER.exception("Error in /mode")
        return _json_response({"ok": False, "error": str(exc)})


async def handle_set_home(request: web.Request) -> web.Response:
    try:
        email, password = _get_credentials(request)
        raw = request.rel_url.query.get("minutes")
        if raw is None:
            return _json_response({"ok": False, "error": "Missing ?minutes= parameter"})
        result = await set_home_limit(email, password, int(float(raw)))
        return _json_response(result)
    except Exception as exc:
        _LOGGER.exception("Error in /set_home")
        return _json_response({"ok": False, "error": str(exc)})


async def handle_set_away(request: web.Request) -> web.Response:
    try:
        email, password = _get_credentials(request)
        raw = request.rel_url.query.get("minutes")
        if raw is None:
            return _json_response({"ok": False, "error": "Missing ?minutes= parameter"})
        result = await set_away_limit(email, password, float(raw))
        return _json_response(result)
    except Exception as exc:
        _LOGGER.exception("Error in /set_away")
        return _json_response({"ok": False, "error": str(exc)})


async def handle_set_bypass(request: web.Request) -> web.Response:
    try:
        email, password = _get_credentials(request)
        raw = request.rel_url.query.get("minutes")
        if raw is None:
            return _json_response({"ok": False, "error": "Missing ?minutes= parameter"})
        result = await set_bypass_time(email, password, int(float(raw)))
        return _json_response(result)
    except Exception as exc:
        _LOGGER.exception("Error in /set_bypass")
        return _json_response({"ok": False, "error": str(exc)})


async def handle_set_auto_away(request: web.Request) -> web.Response:
    try:
        email, password = _get_credentials(request)
        raw = request.rel_url.query.get("hours")
        if raw is None:
            return _json_response({"ok": False, "error": "Missing ?hours= parameter"})
        result = await set_auto_away(email, password, int(float(raw)))
        return _json_response(result)
    except Exception as exc:
        _LOGGER.exception("Error in /set_auto_away")
        return _json_response({"ok": False, "error": str(exc)})


async def handle_set_sensitivity(request: web.Request) -> web.Response:
    try:
        email, password = _get_credentials(request)
        raw = request.rel_url.query.get("value")
        if raw is None:
            return _json_response({"ok": False, "error": "Missing ?value= parameter"})
        result = await set_sensitivity(email, password, float(raw))
        return _json_response(result)
    except Exception as exc:
        _LOGGER.exception("Error in /set_sensitivity")
        return _json_response({"ok": False, "error": str(exc)})


async def handle_ping(request: web.Request) -> web.Response:
    return _json_response({"ok": True, "message": "BetaFlo server is running"})


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ping", handle_ping)
    app.router.add_get("/status", handle_status)
    app.router.add_get("/mode", handle_mode)
    app.router.add_get("/set_home", handle_set_home)
    app.router.add_get("/set_away", handle_set_away)
    app.router.add_get("/set_bypass", handle_set_bypass)
    app.router.add_get("/set_auto_away", handle_set_auto_away)
    app.router.add_get("/set_sensitivity", handle_set_sensitivity)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    _LOGGER.info("BetaFlo server starting on port %d", port)
    email_check = os.environ.get("FLOLOGIC_EMAIL", "")
    if not email_check:
        _LOGGER.warning(
            "FLOLOGIC_EMAIL env var not set — "
            "you must pass ?email=...&password=... in every request, "
            "or set env vars before starting."
        )
    web.run_app(create_app(), port=port)
