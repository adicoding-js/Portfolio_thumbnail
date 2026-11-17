from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp
import uvicorn

from .ai_triage import AITriageEngine
from .api import create_app
from .config import settings
from .detector import SpikeDetector, SpikeEvent
from .platforms.kick import KickPlatform
from .platforms.twitch import TwitchPlatform
from .platforms.youtube import YouTubePlatform
from .ring_buffer import RingBufferManager, enforce_storage_guard, generate_contact_sheet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pinkwire.engine")


@dataclass
class ActiveWatcher:
    stream_key: str
    streamer_name: str
    platform: str
    handle: str
    channel_id: str
    priority: int
    detector: SpikeDetector
    stop_event: asyncio.Event
    task: asyncio.Task
    meta: Dict[str, Any]


class LiveClipperEngine:
    """24/7 Ultra-Lightweight Orchestrator tuned for Hack Club Nest (2GB RAM / 16GB Disk)."""

    def __init__(self) -> None:
        self.twitch = TwitchPlatform()
        self.kick = KickPlatform()
        self.youtube = YouTubePlatform()
        self.ai_triage = AITriageEngine()
        self.ring_buffers = RingBufferManager()

        self.watchers: Dict[str, ActiveWatcher] = {}
        self.capture_queue: asyncio.Queue[SpikeEvent] = asyncio.Queue(maxsize=20)
        self.capture_semaphore = asyncio.Semaphore(1)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = True

    def _on_spike_ready(self, event: SpikeEvent) -> None:
        logger.info(
            "SPIKE DETECTED [%s:%s] %.2fx (%.1f mps, %d unique chatters)",
            event.platform, event.handle, event.spike_multiplier, event.peak_mps, event.unique_chatters,
        )
        try:
            self.capture_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Capture queue full; dropping spike for %s", event.stream_key)

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        await self.ai_triage.initialize(self._session)

        asyncio.create_task(self._discovery_loop(), name="discovery_loop")
        asyncio.create_task(self._detector_tick_loop(), name="detector_tick_loop")
        asyncio.create_task(self._capture_worker_loop(), name="capture_worker_loop")
        logger.info("PinkWire LiveClipper Engine started (max_watchers=%d, max_ring_buffers=%d)",
                    settings.max_chat_watchers, settings.max_ring_buffers)

    async def stop(self) -> None:
        self._running = False
        for key in list(self.watchers.keys()):
            await self._detach_watcher(key)
        await self.ring_buffers.stop_all()
        if self._session:
            await self._session.close()

    async def _attach_watcher(
        self,
        platform: str,
        handle: str,
        streamer_name: str,
        channel_id: str,
        priority: int,
        meta: Dict[str, Any],
    ) -> None:
        stream_key = f"{platform}:{handle.lower()}"
        if stream_key in self.watchers:
            self.watchers[stream_key].meta.update(meta)
            return
        if len(self.watchers) >= settings.max_chat_watchers:
            return

        defaults = settings.profiles.get("capture_defaults", {})
        active_game = settings.profiles.get("active_game", "gta_vi")

        detector = SpikeDetector(
            stream_key=stream_key,
            streamer_name=streamer_name,
            platform=platform,
            handle=handle,
            channel_id=channel_id,
            game_id=active_game,
            lexicon=settings.lexicon,
            on_spike_ready=self._on_spike_ready,
            pre_buffer_sec=int(defaults.get("pre_buffer_sec", 60)),
            post_buffer_sec=int(defaults.get("post_buffer_sec", 10)),
            max_extend_sec=int(defaults.get("max_extend_sec", 120)),
            cooldown_sec=int(defaults.get("cooldown_sec", 20)),
        )

        stop_event = asyncio.Event()
        if platform == "twitch":
            coro = self.twitch.listen_chat(handle, detector.ingest, stop_event)
            await self.ring_buffers.ensure_buffer(
                stream_key, f"https://twitch.tv/{handle}", priority=priority, platform="twitch"
            )
        elif platform == "kick":
            chatroom_id = meta.get("chatroom_id")
            if not chatroom_id:
                return
            coro = self.kick.listen_chat(handle, int(chatroom_id), detector.ingest, stop_event)
            await self.ring_buffers.ensure_buffer(
                stream_key, meta["stream_url"], priority=priority, platform="kick"
            )
        elif platform == "youtube":
            video_id = meta.get("video_id")
            if not video_id:
                return
            coro = self.youtube.listen_chat(video_id, detector.ingest, stop_event)
        else:
            return

        task = asyncio.create_task(coro, name=f"chat_{stream_key}")
        self.watchers[stream_key] = ActiveWatcher(
            stream_key=stream_key,
            streamer_name=streamer_name,
            platform=platform,
            handle=handle,
            channel_id=channel_id,
            priority=priority,
            detector=detector,
            stop_event=stop_event,
            task=task,
            meta=meta,
        )
        logger.info("Attached chat watcher: %s (%s)", stream_key, streamer_name)

    async def _detach_watcher(self, stream_key: str) -> None:
        w = self.watchers.pop(stream_key, None)
        if not w:
            return
        w.stop_event.set()
        w.task.cancel()
        if w.platform in ("kick", "twitch"):
            await self.ring_buffers.release_buffer(w.stream_key)
            await self.ring_buffers.release_buffer(w.handle.lower())
        logger.info("Detached offline watcher: %s", stream_key)

    async def _discovery_loop(self) -> None:
        assert self._session is not None
        while self._running:
            try:
                live_keys = set()
                twitch_roster: List[Dict[str, Any]] = []
                kick_roster: List[Dict[str, Any]] = []
                yt_roster: List[Dict[str, Any]] = []

                for ch_id, ch_data in settings.profiles.get("channels", {}).items():
                    for r in ch_data.get("roster", []):
                        entry = {**r, "channel_id": ch_id}
                        if r["platform"] == "twitch":
                            twitch_roster.append(entry)
                        elif r["platform"] == "kick":
                            kick_roster.append(entry)
                        elif r["platform"] == "youtube":
                            yt_roster.append(entry)

                twitch_live = await self.twitch.get_live_streams(
                    self._session, [r["handle"] for r in twitch_roster]
                )
                for r in twitch_roster:
                    h = r["handle"].lower()
                    if h in twitch_live:
                        live_keys.add(f"twitch:{h}")
                        await self._attach_watcher(
                            "twitch", h, r["name"], r["channel_id"], r.get("priority", 2),
                            {"broadcaster_id": twitch_live[h]["user_id"], "viewers": twitch_live[h]["viewer_count"]},
                        )

                kick_live = await self.kick.get_live_channels(
                    self._session, [r["handle"] for r in kick_roster]
                )
                for r in kick_roster:
                    h = r["handle"].lower()
                    if h in kick_live:
                        live_keys.add(f"kick:{h}")
                        await self._attach_watcher(
                            "kick", h, r["name"], r["channel_id"], r.get("priority", 2), kick_live[h]
                        )

                for r in yt_roster:
                    info = await self.youtube.check_channel_live(self._session, r["handle"])
                    if info:
                        h = r["handle"]
                        live_keys.add(f"youtube:{h.lower()}")
                        await self._attach_watcher(
                            "youtube", h, r["name"], r["channel_id"], r.get("priority", 2), info
                        )

                if settings.profiles.get("auto_track_top_streams", False):
                    active_game_id = settings.profiles.get("active_game", "gta_vi")
                    game_cfg = settings.profiles.get("games", {}).get(active_game_id, {})
                    slots = settings.max_chat_watchers - len(live_keys)
                    if slots > 0:
                        cat_name = game_cfg.get("twitch_category_name", "Grand Theft Auto VI")
                        raw_top = await self.twitch.get_top_streams_for_game(self._session, cat_name, limit=15)
                        # Require viewer_count >= 150 to ignore dead streams
                        active_top = [s for s in raw_top if s.get("viewer_count", 0) >= 150]

                        # If active game has fewer than 5 live streams with >= 150 viewers,
                        # automatically fall back to "Just Chatting" and "Grand Theft Auto V" top streams
                        if len(active_top) < 5:
                            for fb_cat in ("Just Chatting", "Grand Theft Auto V"):
                                if len(active_top) >= 10:
                                    break
                                fb_streams = await self.twitch.get_top_streams_for_game(self._session, fb_cat, limit=10)
                                for s in fb_streams:
                                    if s.get("viewer_count", 0) >= 150:
                                        if not any(it["user_login"].lower() == s["user_login"].lower() for it in active_top):
                                            active_top.append(s)

                        for s in active_top:
                            h = s["user_login"].lower()
                            key = f"twitch:{h}"
                            if key not in live_keys and len(live_keys) < settings.max_chat_watchers:
                                live_keys.add(key)
                                await self._attach_watcher(
                                    "twitch", h, s["user_name"], "pinkwire_official", 3,
                                    {"broadcaster_id": s["user_id"], "viewers": s["viewer_count"], "auto_discovered": True},
                                )

                for key in list(self.watchers.keys()):
                    if key not in live_keys:
                        await self._detach_watcher(key)

            except Exception as exc:
                logger.error("Discovery loop error: %s", exc)

            await asyncio.sleep(60)

    async def _detector_tick_loop(self) -> None:
        while self._running:
            now = time.time()
            for w in list(self.watchers.values()):
                w.detector.tick(now)
            await asyncio.sleep(1.0)

    async def _capture_worker_loop(self) -> None:
        assert self._session is not None
        while self._running:
            event = await self.capture_queue.get()
            async with self.capture_semaphore:
                try:
                    await self._process_spike(event)
                except Exception as exc:
                    logger.error("Failed processing spike %s: %s", event.stream_key, exc)
                finally:
                    self.capture_queue.task_done()

    async def _process_spike(self, event: SpikeEvent) -> None:
        assert self._session is not None
        clip_id = f"{int(time.time())}_{event.platform}_{event.handle.lower()[:12]}_{uuid.uuid4().hex[:6]}"
        mp4_path = settings.clips_dir / f"{clip_id}.mp4"
        sheet_path = settings.clips_dir / f"{clip_id}.sheet.jpg"
        chat_path = settings.clips_dir / f"{clip_id}.chat.json"
        meta_path = settings.clips_dir / f"{clip_id}.meta.json"

        game_cfg = settings.profiles.get("games", {}).get(event.game_id, {})
        game_name = game_cfg.get("display_name", event.game_id)

        triage = await self.ai_triage.evaluate_spike(
            session=self._session,
            streamer_name=event.streamer_name,
            game_name=game_name,
            spike_multiplier=event.spike_multiplier,
            peak_messages=event.peak_messages,
        )

        if not triage.get("is_clip_worthy", False):
            logger.info(
                "AI Triage rejected spike for %s (score=%s, reason=%s) — skipping disk write.",
                event.streamer_name, triage.get("viral_score"), triage.get("reject_reason"),
            )
            return

        watcher = self.watchers.get(event.stream_key)
        meta_info = watcher.meta if watcher else {}
        clip_duration = int((event.spike_end - event.spike_start) + event.pre_buffer_sec + event.post_buffer_sec)

        captured = False
        if event.platform == "twitch":
            # Check if rolling ring-buffer is active for this twitch stream (captures 60s buildup)
            if self.ring_buffers.get_buffer(event.stream_key) or self.ring_buffers.get_buffer(event.handle.lower()):
                captured = await self.ring_buffers.extract_clip(
                    event.stream_key, mp4_path, duration_sec=min(80, clip_duration)
                )
                if not captured:
                    captured = await self.ring_buffers.extract_clip(
                        event.handle.lower(), mp4_path, duration_sec=min(80, clip_duration)
                    )
            # Fall back to live HLS edge capture if stream wasn't buffered
            if not captured:
                captured = await self.twitch.capture_clip(
                    self._session,
                    event.handle,
                    meta_info.get("broadcaster_id"),
                    mp4_path,
                    duration_sec=clip_duration,
                )
        elif event.platform == "youtube":
            video_id = meta_info.get("video_id")
            if video_id:
                captured = await self.youtube.capture_dvr_clip(video_id, mp4_path, duration_sec=clip_duration)
        elif event.platform == "kick":
            captured = await self.ring_buffers.extract_clip(
                event.stream_key, mp4_path, duration_sec=min(80, clip_duration)
            )
            if not captured:
                captured = await self.ring_buffers.extract_clip(
                    event.handle.lower(), mp4_path, duration_sec=min(80, clip_duration)
                )

        if not captured:
            logger.warning("Capture failed for %s; discarding spike.", event.stream_key)
            mp4_path.unlink(missing_ok=True)
            return

        await generate_contact_sheet(mp4_path, sheet_path)

        chat_path.write_text(json.dumps(event.chat_messages, ensure_ascii=False), encoding="utf-8")

        ch_cfg = settings.profiles.get("channels", {}).get(event.channel_id, {})
        tags = list(dict.fromkeys(ch_cfg.get("default_tags", []) + game_cfg.get("extra_tags", [])))

        metadata = {
            "clip_id": clip_id,
            "created_at": time.time(),
            "streamer_name": event.streamer_name,
            "platform": event.platform,
            "handle": event.handle,
            "channel_id": event.channel_id,
            "channel_name": ch_cfg.get("name", event.channel_id),
            "channel_handle": ch_cfg.get("handle", ""),
            "game_id": event.game_id,
            "game_name": game_name,
            "tags": tags,
            "viral_score": triage["viral_score"],
            "moment_type": triage["moment_type"],
            "moment_summary": triage["moment_summary"],
            "suggested_hook": triage["suggested_hook"],
            "spike_multiplier": event.spike_multiplier,
            "peak_mps": event.peak_mps,
            "baseline_mps": event.baseline_mps,
            "unique_chatters": event.unique_chatters,
            "peak_chat_sample": event.peak_messages[:12],
            "file_size_mb": round(mp4_path.stat().st_size / (1024 * 1024), 2),
            "sheet_size_kb": round(sheet_path.stat().st_size / 1024, 1) if sheet_path.exists() else 0,
            "starred": False,
        }
        meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

        enforce_storage_guard(settings.clips_dir, settings.max_vps_storage_mb)
        logger.info("Saved clip %s (score=%d, %.2f MB)", clip_id, metadata["viral_score"], metadata["file_size_mb"])

    def get_watchdog_status(self) -> Dict[str, Any]:
        used_bytes = sum(p.stat().st_size for p in settings.clips_dir.glob("*") if p.is_file())
        active_ring = set(self.ring_buffers.active_handles())

        roster_status: List[Dict[str, Any]] = []
        seen_keys = set()

        for ch_id, ch_data in settings.profiles.get("channels", {}).items():
            for r in ch_data.get("roster", []):
                key = f"{r['platform']}:{r['handle'].lower()}"
                seen_keys.add(key)
                w = self.watchers.get(key)
                if w:
                    st = w.detector.get_status()
                    st["live"] = True
                    st["ring_buffer"] = (r["handle"].lower() in active_ring or key in active_ring)
                    roster_status.append(st)
                else:
                    roster_status.append({
                        "stream_key": key,
                        "streamer_name": r["name"],
                        "platform": r["platform"],
                        "handle": r["handle"],
                        "channel_id": ch_id,
                        "live": False,
                        "current_mps": 0.0,
                        "baseline_mps": 0.0,
                        "unique_chatters_10s": 0,
                        "in_spike": False,
                        "ring_buffer": False,
                    })

        for key, w in self.watchers.items():
            if key not in seen_keys:
                st = w.detector.get_status()
                st["live"] = True
                st["auto_discovered"] = True
                st["ring_buffer"] = (w.handle.lower() in active_ring or key in active_ring)
                roster_status.append(st)

        return {
            "active_game": settings.profiles.get("active_game", "gta_vi"),
            "auto_track_top_streams": settings.profiles.get("auto_track_top_streams", False),
            "ai_model": self.ai_triage.selected_model,
            "active_watchers": len(self.watchers),
            "max_watchers": settings.max_chat_watchers,
            "active_ring_buffers": list(active_ring),
            "max_ring_buffers": settings.max_ring_buffers,
            "storage_used_mb": round(used_bytes / (1024 * 1024), 1),
            "storage_limit_mb": settings.max_vps_storage_mb,
            "queue_size": self.capture_queue.qsize(),
            "streamers": roster_status,
        }

    async def force_test_clip(self, stream_key: str) -> Optional[Dict[str, Any]]:
        """
        Trigger an immediate 30s capture + 3-frame contact sheet + metadata write on any LIVE stream (bypassing AI rejection).
        """
        key = stream_key.lower().strip()
        watcher = self.watchers.get(key)
        if not watcher:
            for k, w in self.watchers.items():
                if k.lower() == key:
                    watcher = w
                    key = k
                    break
        if not watcher:
            logger.warning("Force test clip requested for non-live stream: %s", stream_key)
            return None

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

        clip_id = f"{int(time.time())}_test_{watcher.platform}_{watcher.handle.lower()[:12]}_{uuid.uuid4().hex[:6]}"
        mp4_path = settings.clips_dir / f"{clip_id}.mp4"
        sheet_path = settings.clips_dir / f"{clip_id}.sheet.jpg"
        chat_path = settings.clips_dir / f"{clip_id}.chat.json"
        meta_path = settings.clips_dir / f"{clip_id}.meta.json"

        clip_duration = 30
        captured = False

        if watcher.platform == "twitch":
            # Extract from ring-buffer if active (faster, retains pre-buffered frames), else live HLS edge
            if self.ring_buffers.get_buffer(key) or self.ring_buffers.get_buffer(watcher.handle.lower()):
                captured = await self.ring_buffers.extract_clip(key, mp4_path, duration_sec=clip_duration)
                if not captured:
                    captured = await self.ring_buffers.extract_clip(
                        watcher.handle.lower(), mp4_path, duration_sec=clip_duration
                    )
            if not captured:
                captured = await self.twitch.capture_clip(
                    self._session,
                    watcher.handle,
                    watcher.meta.get("broadcaster_id"),
                    mp4_path,
                    duration_sec=clip_duration,
                )
        elif watcher.platform == "kick":
            captured = await self.ring_buffers.extract_clip(
                watcher.handle.lower(), mp4_path, duration_sec=clip_duration
            )
        elif watcher.platform == "youtube":
            video_id = watcher.meta.get("video_id")
            if video_id:
                captured = await self.youtube.capture_dvr_clip(video_id, mp4_path, duration_sec=clip_duration)

        if not captured or not mp4_path.exists():
            logger.warning("Force test clip capture failed for %s", stream_key)
            mp4_path.unlink(missing_ok=True)
            return None

        await generate_contact_sheet(mp4_path, sheet_path)

        recent_msgs = [
            {
                "t": round(m.timestamp - (time.time() - clip_duration), 2),
                "ts": m.timestamp,
                "user": m.username,
                "text": m.text,
                "color": m.color,
                "badges": m.badges,
            }
            for m in list(watcher.detector._chat_history)[-50:]
        ]
        chat_path.write_text(json.dumps(recent_msgs, ensure_ascii=False), encoding="utf-8")

        game_cfg = settings.profiles.get("games", {}).get(watcher.detector.game_id, {})
        game_name = game_cfg.get("display_name", watcher.detector.game_id)
        ch_cfg = settings.profiles.get("channels", {}).get(watcher.channel_id, {})
        tags = list(dict.fromkeys(ch_cfg.get("default_tags", []) + game_cfg.get("extra_tags", [])))

        metadata = {
            "clip_id": clip_id,
            "created_at": time.time(),
            "streamer_name": watcher.streamer_name,
            "platform": watcher.platform,
            "handle": watcher.handle,
            "channel_id": watcher.channel_id,
            "channel_name": ch_cfg.get("name", watcher.channel_id),
            "channel_handle": ch_cfg.get("handle", ""),
            "game_id": watcher.detector.game_id,
            "game_name": game_name,
            "tags": tags,
            "viral_score": 99,
            "moment_type": "test_clip",
            "moment_summary": f"Manual test clip captured for {watcher.streamer_name} ({watcher.platform.upper()}).",
            "suggested_hook": f"TEST CLIP: {watcher.streamer_name.upper()} LIVE",
            "spike_multiplier": 1.0,
            "peak_mps": round(watcher.detector.current_mps, 2),
            "baseline_mps": round(watcher.detector.baseline_mps, 2),
            "unique_chatters": watcher.detector.unique_chatters_10s,
            "peak_chat_sample": [m.text for m in list(watcher.detector._chat_history)[-12:] if not m.is_system],
            "file_size_mb": round(mp4_path.stat().st_size / (1024 * 1024), 2),
            "sheet_size_kb": round(sheet_path.stat().st_size / 1024, 1) if sheet_path.exists() else 0,
            "starred": False,
            "test_clip": True,
        }
        meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
        enforce_storage_guard(settings.clips_dir, settings.max_vps_storage_mb)
        logger.info("Force test clip completed for %s: %s (%.2f MB)", stream_key, clip_id, metadata["file_size_mb"])
        return metadata


async def run() -> None:
    engine = LiveClipperEngine()
    app = create_app(engine)

    @app.on_event("startup")
    async def _startup() -> None:
        await engine.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await engine.stop()

    config = uvicorn.Config(
        app,
        host=settings.vps_host,
        port=settings.vps_port,
        log_level="info",
        workers=1,
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(run())
