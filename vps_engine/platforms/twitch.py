from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import websockets

from ..config import settings
from ..detector import ChatMessage

logger = logging.getLogger("pinkwire.twitch")

IRC_PRIVMSG_RE = re.compile(
    r"^(?:@(?P<tags>[^ ]+) )?:(?P<user>[^!]+)![^ ]+ PRIVMSG #(?P<channel>[^ ]+) :(?P<msg>.*)$"
)


class TwitchPlatform:
    """Official Twitch Helix API + Twitch IRC WebSocket connector."""

    def __init__(self) -> None:
        self.client_id = settings.twitch_client_id
        self.client_secret = settings.twitch_client_secret
        self.user_oauth_token = settings.twitch_user_oauth_token
        self._app_token: Optional[str] = None
        self._token_expiry: float = 0.0

    async def _ensure_app_token(self, session: aiohttp.ClientSession) -> str:
        if self._app_token and time.time() < self._token_expiry - 60:
            return self._app_token

        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        async with session.post(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            self._app_token = data["access_token"]
            self._token_expiry = time.time() + int(data.get("expires_in", 3600))
            return self._app_token

    async def _headers(self, session: aiohttp.ClientSession, use_user_token: bool = False) -> Dict[str, str]:
        token = self.user_oauth_token if (use_user_token and self.user_oauth_token) else await self._ensure_app_token(session)
        return {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {token}",
        }

    async def get_live_streams(self, session: aiohttp.ClientSession, logins: List[str]) -> Dict[str, Dict[str, Any]]:
        if not logins or not self.client_id:
            return {}
        headers = await self._headers(session)
        params = [("user_login", login.lower()) for login in logins[:100]]
        async with session.get(
            "https://api.twitch.tv/helix/streams",
            headers=headers,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                logger.warning("Twitch get_live_streams HTTP %s", resp.status)
                return {}
            data = await resp.json()
            return {item["user_login"].lower(): item for item in data.get("data", [])}

    async def get_top_streams_for_game(
        self, session: aiohttp.ClientSession, game_name: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        if not self.client_id:
            return []
        headers = await self._headers(session)
        async with session.get(
            "https://api.twitch.tv/helix/games",
            headers=headers,
            params={"name": game_name},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return []
            games = (await resp.json()).get("data", [])
            if not games:
                return []
            game_id = games[0]["id"]

        async with session.get(
            "https://api.twitch.tv/helix/streams",
            headers=headers,
            params={"game_id": game_id, "first": str(limit), "language": "en"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return []
            return (await resp.json()).get("data", [])

    async def capture_clip(
        self,
        session: aiohttp.ClientSession,
        handle: str,
        broadcaster_id: Optional[str],
        output_mp4: Path,
        duration_sec: int = 45,
    ) -> bool:
        if self.user_oauth_token and broadcaster_id:
            try:
                headers = await self._headers(session, use_user_token=True)
                async with session.post(
                    "https://api.twitch.tv/helix/clips",
                    headers=headers,
                    params={"broadcaster_id": broadcaster_id},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status in (200, 202):
                        clip_id = (await resp.json())["data"][0]["id"]
                        await asyncio.sleep(6)
                        clip_url = f"https://clips.twitch.tv/{clip_id}"
                        proc = await asyncio.create_subprocess_exec(
                            "yt-dlp", "-q", "--no-warnings",
                            "-f", "best[height<=1080]/best",
                            "-o", str(output_mp4), clip_url,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        _, stderr = await proc.communicate()
                        if proc.returncode != 0:
                            logger.error(
                                "yt-dlp failed for %s (exit %d): %s",
                                clip_url, proc.returncode, stderr.decode(errors="ignore")
                            )
                        if output_mp4.exists() and output_mp4.stat().st_size > 10_000:
                            logger.info("Captured Twitch Helix clip %s for %s", clip_id, handle)
                            return True
            except Exception as exc:
                logger.warning("Helix clip failed for %s (%s); falling back to HLS edge.", handle, exc)

        clip_len = min(60, max(20, duration_sec))
        cmd = (
            f"streamlink --twitch-disable-ads --stdout 'https://twitch.tv/{handle}' 480p,720p60,720p,best | "
            f"ffmpeg -y -hide_banner -loglevel error -i pipe:0 -t {clip_len} "
            f"-c copy -movflags +faststart '{output_mp4}'"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(
                "Twitch HLS edge streamlink/ffmpeg capture failed for %s (exit %d): %s",
                handle, proc.returncode, stderr.decode(errors="ignore")
            )
        return output_mp4.exists() and output_mp4.stat().st_size > 10_000

    async def listen_chat(
        self,
        handle: str,
        on_message: Callable[[ChatMessage], None],
        stop_event: asyncio.Event,
    ) -> None:
        channel = handle.lower()
        backoff = 2
        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    "wss://irc-ws.chat.twitch.tv:443",
                    ping_interval=30,
                    ping_timeout=15,
                ) as ws:
                    await ws.send("CAP REQ :twitch.tv/tags twitch.tv/commands")
                    await ws.send("PASS SCHMOOPIIE")
                    await ws.send(f"NICK justinfan{int(time.time()) % 90000 + 10000}")
                    await ws.send(f"JOIN #{channel}")
                    backoff = 2
                    logger.info("Connected Twitch IRC chat: #%s", channel)

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        for line in raw.splitlines():
                            if line.startswith("PING"):
                                await ws.send("PONG :tmi.twitch.tv")
                                continue
                            match = IRC_PRIVMSG_RE.match(line)
                            if not match:
                                continue
                            tags_raw = match.group("tags") or ""
                            tags = dict(t.split("=", 1) for t in tags_raw.split(";") if "=" in t)
                            color = tags.get("color") or "#FF3385"
                            display = tags.get("display-name") or match.group("user")
                            on_message(
                                ChatMessage(
                                    timestamp=time.time(),
                                    username=display,
                                    text=match.group("msg"),
                                    color=color,
                                )
                            )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Twitch IRC #%s disconnected (%s); retrying in %ss", channel, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(60, backoff * 2)
