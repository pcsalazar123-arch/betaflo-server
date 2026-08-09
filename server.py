"""
BetaFlo Middleman Server
========================
Translates simple HTTP GET requests from your App Inventor app into
SignalR WebSocket calls to FloLogic's cloud.

Endpoints:
  GET /ping                          - Health check
  GET /status                        - Current valve state
  GET /mode?value=home|away|bypass|off
  GET /set_home?minutes=7
  GET /set_away?minutes=3
  GET /set_bypass?minutes=90
  GET /set_auto_away?hours=24
  GET /set_sensitivity?value=0.5

All return JSON: {"ok": true} on success or {"ok": false, "error": "..."}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import aiohttp
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_LOGGER = logging.getLogger("betaflo")

# ── FloLogic constants ────────────────────────────────────────────────────────
HUB_URL = "https://hub-cloudapps-prod.azurewebsites.net/signalr"
RECORD_SEPARATOR = "\x1e"

# These headers are required — FloLogic ignores/times out without them
DEFAULT_HEADERS = {
    "userDeviceCode": "AND-betaflo-001",
    "userDeviceToken": "betaflo-token",
    "relogToken": "",
    "OsPlatform": "Android",
    "AppVer": "homeassistant",
    "DeviceName": "BetaFlo",
}

VALVE_MODES = {"home": 1, "away": 2, "bypass": 4, "off": 8, "shutoff": 8}
MODE_NAMES  = {1: "home", 2: "away", 4: "bypass", 8: "shutoff", 16: "disabled"}


# ─────────────────────────────────────────────────────────────────────────────
# SignalR low-level helpers
# ─────────────────────────────────────────────────────────────────────────────

def _frame(target: str, *arguments: Any) -> str:
    return json.dumps({"type": 1, "target": target, "arguments": list(arguments)},
                      separators=(",", ":")) + RECORD_SEPARATOR


async def _negotiate(session: aiohttp.ClientSession, headers: dict) -> str:
    resp = await session.post(HUB_URL + "/negotiate", headers=headers)
    if resp.status in (401, 403):
        raise RuntimeError("FloLogic rejected credentials (401/403)")
    resp.raise_for_status()
    payload = await resp.json()
    token = payload.get("connectionToken") or payload.get("connectionId")
    if not token:
        raise RuntimeError("SignalR negotiate did not return a token")
    return token


async def _open_ws(session: aiohttp.ClientSession, token: str,
                   headers: dict) -> aiohttp.ClientWebSocketResponse:
    ws_url = HUB_URL.replace("https://", "wss://") + f"?id={quote(token, safe='')}"
    ws = await session.ws_connect(ws_url, headers=headers)
    # SignalR JSON protocol handshake
    await ws.send_str(json.dumps({"protocol": "json", "version": 1}) + RECORD_SEPARATOR)
    return ws


async def _read_frames(ws: aiohttp.ClientWebSocketResponse,
                       timeout: float = 5.0) -> list[dict]:
    """Read all available frames within timeout seconds."""
    frames = []
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
        except asyncio.TimeoutError:
            break
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED):
            break
        if msg.type != aiohttp.WSMsgType.TEXT:
            continue
        for raw in msg.data.split(RECORD_SEPARATOR):
            raw = raw.strip()
            if not raw:
                continue
            try:
                frames.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
    return frames


async def _wait_for_event(ws: aiohttp.ClientWebSocketResponse,
                          want: str, timeout: float = 30.0) -> list[Any]:
    """Wait for a specific SignalR event and return its arguments."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for '{want}'")
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Timed out waiting for '{want}'")
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED):
            raise RuntimeError(f"WebSocket closed while waiting for '{want}'")
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


# ─────────────────────────────────────────────────────────────────────────────
# FloLogic session: negotiate → websocket → login → action → close
# ─────────────────────────────────────────────────────────────────────────────

async def _login(ws, email: str, password: str, headers: dict):
    """Send Login and collect user + valve objects."""
    # Discard the handshake acknowledgement (empty frame)
    await _read_frames(ws, timeout=3.0)

    user = None
    valve = None
    relog_token = ""

    # Send login with required headers
    await ws.send_str(_frame("Login", email, password, headers["DeviceName"], None))

    # Collect LoggedIn and ValveSent — they may arrive in either order
    deadline = asyncio.get_event_loop().time() + 30
    while (user is None or valve is None):
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
        except asyncio.TimeoutError:
            break
        if msg.type not in (aiohttp.WSMsgType.TEXT,):
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
                relog_token = user.get("relogToken", "")
            elif target == "ValveSent" and args:
                valve = args[0]

    if user is None:
        raise RuntimeError(
            "FloLogic login timed out — check FLOLOGIC_EMAIL and FLOLOGIC_PASSWORD")

    if valve is None:
        # Some accounts need RefreshValveArray
        await ws.send_str(_frame("RefreshValveArray", user))
        args = await _wait_for_event(ws, "ValveArraySent", timeout=20)
        devices = args[0] if args else []
        valve = devices[0] if devices else None

    if valve is None:
        raise RuntimeError("FloLogic did not return a valve")

    return user, valve, relog_token


class _FloLogicSession:
    """
    Keeps one logged-in FloLogic connection alive across requests instead of
    doing a full negotiate -> connect -> login cycle every single time.

    That full login cycle was taking ~30s per request AND was the most
    likely cause of the official FloLogic app losing its session (repeatedly
    re-authenticating the same account looks like a new device grabbing the
    session each time). Reusing one connection avoids both problems.

    A lock serializes calls so two overlapping HTTP requests can't stomp on
    the same websocket at once.
    """

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._user = None
        self._valve = None
        self._headers = None
        self._lock = asyncio.Lock()

    def _connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    async def _connect(self, email: str, password: str):
        t0 = time.monotonic()
        await self._close()
        headers = {**DEFAULT_HEADERS}
        session = aiohttp.ClientSession()
        try:
            t1 = time.monotonic()
            token = await _negotiate(session, headers)
            t2 = time.monotonic()
            ws = await _open_ws(session, token, headers)
            t3 = time.monotonic()
            user, valve, relog_token = await _login(ws, email, password, headers)
            t4 = time.monotonic()
            headers["relogToken"] = relog_token
        except Exception:
            with suppress(Exception):
                await session.close()
            raise
        self._session, self._ws = session, ws
        self._user, self._valve, self._headers = user, valve, headers
        _LOGGER.info(
            "FloLogic: new session established | close=%.2fs negotiate=%.2fs "
            "ws_open=%.2fs login=%.2fs total=%.2fs",
            t1 - t0, t2 - t1, t3 - t2, t4 - t3, t4 - t0,
        )

    async def _close(self):
        if self._ws is not None:
            with suppress(Exception):
                await self._ws.close()
        if self._session is not None:
            with suppress(Exception):
                await self._session.close()
        self._session = self._ws = self._user = self._valve = None

    async def run(self, email: str, password: str, action):
        t_start = time.monotonic()
        async with self._lock:
            reused = self._connected()
            if not reused:
                _LOGGER.info("FloLogic: no live session (connected=%s) — reconnecting",
                              reused)
                await self._connect(email, password)
            else:
                _LOGGER.info("FloLogic: reusing existing session")
            try:
                # Refresh valve snapshot so callers see current state
                t_action0 = time.monotonic()
                result = await action(self._ws, self._user, self._valve)
                _LOGGER.info("FloLogic: action took %.2fs (reused=%s), total run() %.2fs",
                             time.monotonic() - t_action0, reused, time.monotonic() - t_start)
                return result
            except Exception:
                # Connection may have gone stale (FloLogic timed it out,
                # network blip, etc). Reconnect once and retry the action.
                _LOGGER.warning("FloLogic: session appears stale, reconnecting")
                await self._connect(email, password)
                t_action0 = time.monotonic()
                result = await action(self._ws, self._user, self._valve)
                _LOGGER.info("FloLogic: retry action took %.2fs, total run() %.2fs",
                             time.monotonic() - t_action0, time.monotonic() - t_start)
                return result


_flologic_session = _FloLogicSession()


async def _with_flologic(email: str, password: str, action):
    """Run action(ws, user, valve) against the shared, reused FloLogic session."""
    return await _flologic_session.run(email, password, action)


async def _state_change(ws, user, valve, fields: dict) -> None:
    command = {
        "active": True,
        "created": datetime.now(UTC).isoformat(),
        "userId": user["id"],
        "valveId": valve["id"],
        **fields,
    }
    await ws.send_str(_frame("RequestStateChange", user, valve, command))
    # We don't wait for FloLogic's StateChangeResult confirmation — in practice
    # it often never arrives on this connection (likely because FloLogic only
    # pushes it to the session it considers "active", e.g. the official app),
    # even though the command itself reliably goes through. Waiting up to 45s
    # for it was causing false-negative errors AND holding this session open
    # far longer than necessary, which was a likely contributor to kicking the
    # official FloLogic app's session. A short flush delay is enough.
    await asyncio.sleep(1.5)
    # Patch our cached valve dict in place so a /status call right after this
    # reflects the change, since we no longer re-fetch on every request.
    valve.update(fields)


def _mode_name(v) -> str:
    try:
        v = int(v)
    except (TypeError, ValueError):
        return "unknown"
    if v in MODE_NAMES:
        return MODE_NAMES[v]
    if v & 8: return "shutoff"
    if v & 4: return "bypass"
    if v & 2: return "away"
    if v & 1: return "home"
    return f"unknown_{v}"


# ─────────────────────────────────────────────────────────────────────────────
# FloLogic actions
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_status(email, password):
    async def action(ws, user, valve):
        return {
            "ok": True,
            "mode": _mode_name(valve.get("mode")),
            "home_limit_minutes": valve.get("homeIntervalTime"),
            "away_limit_minutes": valve.get("awayIntervalTime"),
            "bypass_time_minutes": valve.get("bypassTime"),
            "auto_away_hours": valve.get("autoAwayTime"),
            "temperature": valve.get("temperature"),
            "valve_name": (valve.get("valveFriendlyName")
                           or valve.get("combinedName") or "FloLogic"),
        }
    return await _with_flologic(email, password, action)


async def set_mode(email, password, mode: str):
    mode = mode.lower()
    if mode not in VALVE_MODES:
        return {"ok": False, "error": f"Unknown mode '{mode}'. Use: home/away/bypass/off"}
    async def action(ws, user, valve):
        await _state_change(ws, user, valve, {"mode": VALVE_MODES[mode]})
        return {"ok": True, "mode": mode}
    return await _with_flologic(email, password, action)


async def set_home_limit(email, password, minutes: int):
    async def action(ws, user, valve):
        await _state_change(ws, user, valve, {"homeIntervalTime": int(minutes)})
        return {"ok": True, "home_limit_minutes": int(minutes)}
    return await _with_flologic(email, password, action)


async def set_away_limit(email, password, minutes: float):
    async def action(ws, user, valve):
        await _state_change(ws, user, valve, {"awayIntervalTime": float(minutes)})
        return {"ok": True, "away_limit_minutes": float(minutes)}
    return await _with_flologic(email, password, action)


async def set_bypass_time(email, password, minutes: int):
    async def action(ws, user, valve):
        await _state_change(ws, user, valve, {"bypassTime": int(minutes)})
        return {"ok": True, "bypass_time_minutes": int(minutes)}
    return await _with_flologic(email, password, action)


async def set_auto_away(email, password, hours: int):
    async def action(ws, user, valve):
        await _state_change(ws, user, valve, {"autoAwayTime": int(hours)})
        return {"ok": True, "auto_away_hours": int(hours)}
    return await _with_flologic(email, password, action)


async def set_sensitivity(email, password, value: float):
    async def action(ws, user, valve):
        await _state_change(ws, user, valve, {"dripRate": float(value)})
        return {"ok": True, "sensitivity": float(value)}
    return await _with_flologic(email, password, action)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP handlers
# ─────────────────────────────────────────────────────────────────────────────

def _creds(request: web.Request):
    email    = request.rel_url.query.get("email")    or os.environ.get("FLOLOGIC_EMAIL", "")
    password = request.rel_url.query.get("password") or os.environ.get("FLOLOGIC_PASSWORD", "")
    if not email or not password:
        raise ValueError("Set FLOLOGIC_EMAIL and FLOLOGIC_PASSWORD env vars on Render")
    return email, password


def _ok(data: dict) -> web.Response:
    return web.Response(text=json.dumps(data), content_type="application/json",
                        headers={"Access-Control-Allow-Origin": "*"})


async def handle_ping(r):    return _ok({"ok": True, "message": "BetaFlo server is running"})
async def handle_status(r):
    try:    return _ok(await fetch_status(*_creds(r)))
    except Exception as e: _LOGGER.exception("Error in /status"); return _ok({"ok": False, "error": str(e)})

async def handle_mode(r):
    try:
        mode = r.rel_url.query.get("value", "")
        if not mode: return _ok({"ok": False, "error": "Missing ?value="})
        return _ok(await set_mode(*_creds(r), mode))
    except Exception as e: _LOGGER.exception("Error in /mode"); return _ok({"ok": False, "error": str(e)})

async def handle_set_home(r):
    try:
        raw = r.rel_url.query.get("minutes")
        if raw is None: return _ok({"ok": False, "error": "Missing ?minutes="})
        return _ok(await set_home_limit(*_creds(r), int(float(raw))))
    except Exception as e: _LOGGER.exception("Error in /set_home"); return _ok({"ok": False, "error": str(e)})

async def handle_set_away(r):
    try:
        raw = r.rel_url.query.get("minutes")
        if raw is None: return _ok({"ok": False, "error": "Missing ?minutes="})
        return _ok(await set_away_limit(*_creds(r), float(raw)))
    except Exception as e: _LOGGER.exception("Error in /set_away"); return _ok({"ok": False, "error": str(e)})

async def handle_set_bypass(r):
    try:
        raw = r.rel_url.query.get("minutes")
        if raw is None: return _ok({"ok": False, "error": "Missing ?minutes="})
        return _ok(await set_bypass_time(*_creds(r), int(float(raw))))
    except Exception as e: _LOGGER.exception("Error in /set_bypass"); return _ok({"ok": False, "error": str(e)})

async def handle_set_auto_away(r):
    try:
        raw = r.rel_url.query.get("hours")
        if raw is None: return _ok({"ok": False, "error": "Missing ?hours="})
        return _ok(await set_auto_away(*_creds(r), int(float(raw))))
    except Exception as e: _LOGGER.exception("Error in /set_auto_away"); return _ok({"ok": False, "error": str(e)})

async def handle_set_sensitivity(r):
    try:
        raw = r.rel_url.query.get("value")
        if raw is None: return _ok({"ok": False, "error": "Missing ?value="})
        return _ok(await set_sensitivity(*_creds(r), float(raw)))
    except Exception as e: _LOGGER.exception("Error in /set_sensitivity"); return _ok({"ok": False, "error": str(e)})


def create_app():
    app = web.Application()
    app.router.add_get("/ping",              handle_ping)
    app.router.add_get("/status",            handle_status)
    app.router.add_get("/mode",              handle_mode)
    app.router.add_get("/set_home",          handle_set_home)
    app.router.add_get("/set_away",          handle_set_away)
    app.router.add_get("/set_bypass",        handle_set_bypass)
    app.router.add_get("/set_auto_away",     handle_set_auto_away)
    app.router.add_get("/set_sensitivity",   handle_set_sensitivity)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    _LOGGER.info("BetaFlo server starting on port %d", port)
    web.run_app(create_app(), port=port)
