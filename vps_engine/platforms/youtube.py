from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import pytchat

from ..config import resolve_bin, settings
from ..detector import ChatMessage

logger = logging.getLogger("pinkwire.youtube")


class YouTubePlatform:
    """
    Official YouTube Data API v3 for live broadcast discovery + pytchat for zero-quota 24/7 chat.
    Uses yt-dlp --download-sections to exploit native YouTube Live DVR rewind.
    """

    def __init__(self) -> None:
        self.api_key = settings.youtube_api_key
        self._last_check: Dict[str, float] = {}
        self._cached_live: Dict[str, Optional[Dict[str, Any]]] = {}

    async def check_channel_live(
        self, session: aiohttp.ClientSession, channel_id: str, min_interval_sec: int = 180
    ) -> Optional[Dict[str, Any]]:
        now = time.time()
        if channel_id in self._last_check and (now - self._last_check[channel_id]) < min_interval_sec:
            return self._cached_live.get(channel_id)

        self._last_check[channel_id] = now

        url = f"https://www.youtube.com/channel/{channel_id}/live"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True
            ) as resp:
                if resp.status != 200:
                    logger.warning("YouTube live check HTTP %s for channel %s", resp.status, channel_id)
                    return self._cached_live.get(channel_id)
                html = await resp.text()

                if '"isLive":true' not in html and '"isLive": true' not in html:
                    self._cached_live[channel_id] = None
                    return None

                vid_match = re.search(r'"videoId":"([a-zA-Z0-9_-]{11})"', html)
                if not vid_match:
                    self._cached_live[channel_id] = None
                    return None

                video_id = vid_match.group(1)
                title = "Live Stream"
                title_match = re.search(r"<title>(.*?)(?: - YouTube)?</title>", html)
                if title_match:
                    clean_title = title_match.group(1).strip()
                    if clean_title:
                        title = clean_title

                info = {
                    "channel_id": channel_id,
                    "video_id": video_id,
                    "title": title,
                    "started_at": time.time(),
                }
                self._cached_live[channel_id] = info
                return info
        except Exception as exc:
            logger.warning("YouTube live check failed for %s: %s", channel_id, exc)
            return self._cached_live.get(channel_id)

    async def search_top_live(
        self, session: aiohttp.ClientSession, query: str, limit: int = 3
    ) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "q": query,
            "eventType": "live",
            "type": "video",
            "order": "viewCount",
            "maxResults": str(limit),
            "key": self.api_key,
        }
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return []
                items = (await resp.json()).get("items", [])
                return [
                    {
                        "channel_id": it["snippet"]["channelId"],
                        "channel_title": it["snippet"]["channelTitle"],
                        "video_id": it["id"]["videoId"],
                        "title": it["snippet"]["title"],
                    }
                    for it in items
                ]
        except Exception:
            return []

    async def capture_dvr_clip(
        self,
        video_id: str,
        output_mp4: Path,
        duration_sec: int = 70,
    ) -> bool:
        url = f"https://www.youtube.com/watch?v={video_id}"
        section = f"*-{duration_sec}-inf"
        proc = await asyncio.create_subprocess_exec(
            resolve_bin("yt-dlp"),
            "-q", "--no-warnings",
            "--live-from-start",
            "--download-sections", section,
            "-f", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]/best",
            "--merge-output-format", "mp4",
            "-o", str(output_mp4),
            url,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning("YouTube DVR clip failed for %s: %s", video_id, stderr.decode(errors="ignore"))
            return False
        return output_mp4.exists() and output_mp4.stat().st_size > 10_000

    async def listen_chat(
        self,
        video_id: str,
        on_message: Callable[[ChatMessage], None],
        stop_event: asyncio.Event,
    ) -> None:
        def _poll_loop() -> None:
            backoff = 3
            while not stop_event.is_set():
                try:
                    chat = pytchat.create(video_id=video_id, interruptable=False)
                    backoff = 3
                    logger.info("Connected YouTube pytchat: %s", video_id)
                    while chat.is_alive() and not stop_event.is_set():
                        for c in chat.get().sync_items():
                            on_message(
                                ChatMessage(
                                    timestamp=time.time(),
                                    username=c.author.name,
                                    text=c.message,
                                    color="#FF4E45",
                                )
                            )
                        time.sleep(0.5)
                    chat.terminate()
                except Exception as exc:
                    logger.warning("YouTube pytchat %s error (%s); retrying in %ss", video_id, exc, backoff)
                    time.sleep(backoff)
                    backoff = min(60, backoff * 2)

        await asyncio.to_thread(_poll_loop)
