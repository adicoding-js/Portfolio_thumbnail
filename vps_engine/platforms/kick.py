from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import websockets

from ..config import settings
from ..detector import ChatMessage

logger = logging.getLogger("pinkwire.kick")

KICK_PUSHER_URL = (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
    "?protocol=7&client=js&version=8.4.0-rc2&flash=false"
)


class KickPlatform:
    """Official Kick OAuth 2.1 Public API + Kick Pusher WebSocket connector."""

    def __init__(self) -> None:
        self.client_id = settings.kick_client_id
        self.client_secret = settings.kick_client_secret
        self._app_token: Optional[str] = None
        self._token_expiry: float = 0.0

    async def _ensure_app_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        if not self.client_id or not self.client_secret:
            return None
        if self._app_token and time.time() < self._token_expiry - 60:
            return self._app_token

        url = "https://id.kick.com/oauth/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            async with session.post(
                url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                payload = await resp.json()
                self._app_token = payload["access_token"]
                self._token_expiry = time.time() + int(payload.get("expires_in", 3600))
                return self._app_token
        except Exception as exc:
            logger.warning("Kick OAuth token fetch failed: %s", exc)
            return None

    async def get_channel_info(self, session: aiohttp.ClientSession, slug: str) -> Optional[Dict[str, Any]]:
        token = await self._ensure_app_token(session)
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            async with session.get(
                "https://api.kick.com/public/v1/channels",
                headers=headers,
                params={"slug": slug.lower()},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = (await resp.json()).get("data", [])
                    if data:
                        ch = data[0]
                        stream = ch.get("stream") or {}
                        return {
                            "slug": slug.lower(),
                            "is_live": bool(stream.get("is_live")),
                            "chatroom_id": ch.get("chatroom_id") or ch.get("broadcaster_user_id"),
                            "viewer_count": stream.get("viewer_count", 0),
                            "stream_url": f"https://kick.com/{slug.lower()}",
                        }
        except Exception as exc:
            logger.debug("Kick public API lookup error for %s: %s", slug, exc)

        try:
            async with session.get(
                f"https://kick.com/api/v2/channels/{slug.lower()}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                if resp.status == 200:
                    ch = await resp.json()
                    livestream = ch.get("livestream")
                    return {
                        "slug": slug.lower(),
                        "is_live": livestream is not None and bool(livestream.get("is_live", True)),
                        "chatroom_id": (ch.get("chatroom") or {}).get("id"),
                        "viewer_count": (livestream or {}).get("viewer_count", 0),
                        "stream_url": f"https://kick.com/{slug.lower()}",
                    }
        except Exception:
            pass
        return None

    async def get_live_channels(self, session: aiohttp.ClientSession, slugs: List[str]) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        for slug in slugs:
            info = await self.get_channel_info(session, slug)
            if info and info.get("is_live"):
                results[slug.lower()] = info
        return results

    async def listen_chat(
        self,
        handle: str,
        chatroom_id: int,
        on_message: Callable[[ChatMessage], None],
        stop_event: asyncio.Event,
    ) -> None:
        backoff = 2
        while not stop_event.is_set():
            try:
                async with websockets.connect(KICK_PUSHER_URL, ping_interval=None) as ws:
                    sub_msg = {
                        "event": "pusher:subscribe",
                        "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
                    }
                    await ws.send(json.dumps(sub_msg))
                    backoff = 2
                    last_ping = time.time()
                    logger.info("Connected Kick Pusher chat: %s (room %s)", handle, chatroom_id)

                    while not stop_event.is_set():
                        if time.time() - last_ping >= 30.0:
                            await ws.send(json.dumps({"event": "pusher:ping", "data": {}}))
                            last_ping = time.time()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        frame = json.loads(raw)
                        event_name = frame.get("event", "")
                        if event_name == "App\\Events\\ChatMessageEvent":
                            payload = json.loads(frame.get("data", "{}"))
                            sender = payload.get("sender") or {}
                            identity = sender.get("identity") or {}
                            on_message(
                                ChatMessage(
                                    timestamp=time.time(),
                                    username=sender.get("username", "kick_user"),
                                    text=payload.get("content", ""),
                                    color=identity.get("color") or "#53FC18",
                                )
                            )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Kick Pusher %s disconnected (%s); retrying in %ss", handle, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(60, backoff * 2)
