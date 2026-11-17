from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, Optional

from .config import resolve_bin, settings

logger = logging.getLogger("pinkwire.ring_buffer")


class RollingRingBuffer:
    """
    Lightweight 480p/720p rolling .ts ring-buffer for Twitch or Kick streams.
    Uses streamlink -> ffmpeg -c copy (near-zero CPU, ~30-40MB disk for 80s window).
    """

    def __init__(
        self,
        handle: str,
        stream_url: str,
        buffer_root: Path,
        platform: str = "kick",
        priority: int = 2,
    ) -> None:
        self.handle = handle.lower()
        self.stream_url = stream_url
        self.platform = platform.lower()
        self.priority = priority
        self.stream_key = f"{self.platform}:{self.handle}"
        self.dir = buffer_root / f"{self.platform}_{self.handle}"
        self.dir.mkdir(parents=True, exist_ok=True)
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._running = False
        self.started_at: float = 0.0

    @property
    def is_running(self) -> bool:
        return self._running and self._proc is not None and self._proc.returncode is None

    async def start(self) -> None:
        if self.is_running:
            return
        self._clean_dir()
        self._running = True
        self.started_at = time.time()

        seg_pattern = str(self.dir / "seg_%03d.ts")
        s_bin = resolve_bin("streamlink")
        ff_bin = resolve_bin("ffmpeg")
        extra_flags = "--twitch-disable-ads " if self.platform == "twitch" else ""
        cmd = (
            f"'{s_bin}' {extra_flags}--stdout --retry-streams 5 --retry-open 3 "
            f"'{self.stream_url}' 720p60,720p,best | "
            f"'{ff_bin}' -hide_banner -loglevel error -i pipe:0 -c copy "
            f"-f segment -segment_time 5 -segment_wrap 140 -reset_timestamps 1 '{seg_pattern}'"
        )
        self._proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info(
            "Started rolling ring-buffer for %s [%s] (priority %d)",
            self.stream_key, self.platform.upper(), self.priority,
        )

    async def stop(self) -> None:
        self._running = False
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except Exception:
                self._proc.kill()
        self._proc = None
        self._clean_dir()
        logger.info("Stopped ring-buffer for %s", self.stream_key)

    def _clean_dir(self) -> None:
        if self.dir.exists():
            for f in self.dir.glob("*.ts"):
                try:
                    f.unlink()
                except OSError:
                    pass

    async def extract_clip(self, output_mp4: Path, duration_sec: int = 600) -> bool:
        segments = sorted(self.dir.glob("seg_*.ts"), key=lambda p: p.stat().st_mtime)
        if not segments:
            logger.warning("No ring-buffer segments found for %s", self.stream_key)
            return False

        if len(segments) > 2:
            segments = segments[:-1]

        needed = max(2, int(duration_sec / 5) + 1)
        chosen = segments[-needed:]

        concat_file = self.dir / f"concat_{int(time.time())}.txt"
        try:
            with concat_file.open("w", encoding="utf-8") as f:
                for seg in chosen:
                    f.write(f"file '{seg.resolve()}'\n")

            proc = await asyncio.create_subprocess_exec(
                resolve_bin("ffmpeg"), "-y", "-hide_banner", "-loglevel", "error",
                "-f", "concat", "-safe", "0", "-i", str(concat_file),
                "-c", "copy", "-movflags", "+faststart", str(output_mp4),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.error(
                    "Ring-buffer concat failed for %s (exit %d): %s",
                    self.stream_key, proc.returncode, stderr.decode(errors="ignore"),
                )
                return False
            return output_mp4.exists() and output_mp4.stat().st_size > 10_000
        finally:
            if concat_file.exists():
                concat_file.unlink(missing_ok=True)


KickRingBuffer = RollingRingBuffer


class RingBufferManager:
    """Strictly enforces MAX_RING_BUFFERS=4 to protect Hack Club Nest RAM & 16GB disk."""

    def __init__(self) -> None:
        self.max_buffers = settings.max_ring_buffers
        self.buffers: Dict[str, RollingRingBuffer] = {}

    def _resolve_key(self, handle_or_key: str, platform: str = "kick") -> str:
        if ":" in handle_or_key:
            return handle_or_key.lower()
        return f"{platform.lower()}:{handle_or_key.lower()}"

    def get_buffer(self, handle_or_key: str) -> Optional[RollingRingBuffer]:
        key = handle_or_key.lower()
        if key in self.buffers:
            return self.buffers[key]
        for buf in self.buffers.values():
            if buf.handle == key or buf.stream_key == key:
                return buf
        return None

    async def ensure_buffer(
        self,
        handle_or_key: str,
        stream_url: str,
        priority: int = 2,
        platform: str = "kick",
    ) -> bool:
        key = self._resolve_key(handle_or_key, platform=platform)
        if key in self.buffers and self.buffers[key].is_running:
            self.buffers[key].priority = priority
            return True

        if len(self.buffers) >= self.max_buffers:
            worst_key = None
            worst_prio = -1
            for k, b in self.buffers.items():
                if b.priority > worst_prio:
                    worst_prio = b.priority
                    worst_key = k

            if worst_key and worst_prio > priority:
                logger.info(
                    "Evicting lower priority ring-buffer %s (prio %d) for %s (prio %d)",
                    worst_key, worst_prio, key, priority,
                )
                await self.release_buffer(worst_key)
            else:
                return False

        plat = platform
        handle = handle_or_key
        if ":" in handle_or_key:
            plat, handle = handle_or_key.split(":", 1)

        buf = RollingRingBuffer(
            handle=handle,
            stream_url=stream_url,
            buffer_root=settings.ring_buffer_dir,
            platform=plat,
            priority=priority,
        )
        await buf.start()
        self.buffers[key] = buf
        return True

    async def release_buffer(self, handle_or_key: str) -> None:
        key = handle_or_key.lower()
        buf = self.buffers.pop(key, None)
        if not buf:
            for k, b in list(self.buffers.items()):
                if b.handle == key or b.stream_key == key:
                    buf = self.buffers.pop(k)
                    break
        if buf:
            await buf.stop()

    async def extract_clip(self, handle_or_key: str, output_mp4: Path, duration_sec: int = 600) -> bool:
        buf = self.get_buffer(handle_or_key)
        if not buf or not buf.is_running:
            return False
        return await buf.extract_clip(output_mp4, duration_sec=duration_sec)

    async def stop_all(self) -> None:
        for handle in list(self.buffers.keys()):
            await self.release_buffer(handle)

    def active_handles(self) -> list[str]:
        res = []
        for b in self.buffers.values():
            if b.is_running:
                res.append(b.handle)
                res.append(b.stream_key)
        return list(dict.fromkeys(res))


async def generate_contact_sheet(mp4_path: Path, out_jpg: Path) -> bool:
    """Generate a 3-frame JPEG contact sheet (<50KB) for 5G Data-Saver preview."""
    probe = await asyncio.create_subprocess_exec(
        resolve_bin("ffprobe"), "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", str(mp4_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await probe.communicate()
    try:
        duration = max(3.0, float(stdout.decode().strip()))
    except ValueError:
        duration = 30.0

    fps_filter = f"fps=3/{duration:.2f},scale=320:-1,tile=3x1"
    proc = await asyncio.create_subprocess_exec(
        resolve_bin("ffmpeg"), "-y", "-hide_banner", "-loglevel", "error",
        "-i", str(mp4_path),
        "-vf", fps_filter,
        "-frames:v", "1",
        "-q:v", "9",
        str(out_jpg),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.error(
            "Contact sheet ffmpeg failed for %s (exit %d): %s",
            mp4_path.name, proc.returncode, stderr.decode(errors="ignore")
        )
    return out_jpg.exists() and out_jpg.stat().st_size > 0


def is_hype_shielded(meta: Dict[str, Any]) -> bool:
    """
    Returns True if clip is protected by the Multi-Factor Hype Shield:
    - Manually Starred (★)
    - Exceptional virality: viral_score >= 85
    - Massive chat explosion: multiplier >= 3.5x AND unique_chatters >= 40
    - Extended high-intensity moment: duration >= 45s AND multiplier >= 2.5x
    - Certified entertaining moment (clutch, fail, rage, funny, shock) with viral_score >= 75
    """
    if meta.get("starred", False):
        return True
    if int(meta.get("viral_score", 0)) >= 85:
        return True

    multiplier = float(meta.get("spike_multiplier", 1.0))
    chatters = int(meta.get("unique_chatters", 0))
    if multiplier >= 3.5 and chatters >= 40:
        return True

    duration = float(meta.get("duration_sec", 0))
    if duration >= 45.0 and multiplier >= 2.5:
        return True

    moment_type = str(meta.get("moment_type", "")).lower()
    if moment_type in ("clutch", "fail", "rage", "funny", "shock") and int(meta.get("viral_score", 0)) >= 75:
        return True

    return False


def enforce_storage_guard(clips_dir: Path, max_mb: int) -> int:
    """
    Enforces MAX_VPS_STORAGE_MB by pruning clips in increasing order of viral score
    (lowest virality goes first). Hype-shielded and starred clips are protected.
    """
    import json

    def dir_size_mb() -> float:
        total = sum(p.stat().st_size for p in clips_dir.glob("*") if p.is_file())
        return total / (1024 * 1024)

    pruned = 0
    if dir_size_mb() <= max_mb:
        return 0

    meta_files = list(clips_dir.glob("*.meta.json"))
    clip_records = []
    for meta_path in meta_files:
        try:
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            clip_records.append((meta_path, meta))
        except Exception:
            continue

    # Split into prunable candidates and hype-shielded clips
    prunable = []
    shielded = []
    for meta_path, meta in clip_records:
        if is_hype_shielded(meta):
            shielded.append((meta_path, meta))
        else:
            prunable.append((meta_path, meta))

    # Sort prunable in increasing order of viral_score (lowest virality first), then oldest created_at
    prunable.sort(key=lambda item: (item[1].get("viral_score", 0), item[1].get("created_at", 0)))

    target_mb = max_mb * 0.90
    for meta_path, meta in prunable:
        if dir_size_mb() <= target_mb:
            break
        clip_id = meta.get("clip_id") or meta_path.name.replace(".meta.json", "")
        for suffix in (".mp4", ".full.mp4", ".meta.json", ".chat.json", ".sheet.jpg", ".chat.webm", ".burned.mp4"):
            target = clips_dir / f"{clip_id}{suffix}"
            if target.exists():
                target.unlink(missing_ok=True)
        pruned += 1
        logger.info(
            "Storage guard pruned low-virality clip: %s (score=%s, mult=%.1fx)",
            clip_id, meta.get("viral_score"), float(meta.get("spike_multiplier", 1.0))
        )

    # Emergency safety fallback: if disk is STILL overflowing, prune unstarred shielded clips with lowest score
    if dir_size_mb() > max_mb:
        emergency_candidates = [it for it in shielded if not it[1].get("starred", False)]
        emergency_candidates.sort(key=lambda item: (item[1].get("viral_score", 0), item[1].get("created_at", 0)))
        for meta_path, meta in emergency_candidates:
            if dir_size_mb() <= target_mb:
                break
            clip_id = meta.get("clip_id") or meta_path.name.replace(".meta.json", "")
            for suffix in (".mp4", ".full.mp4", ".meta.json", ".chat.json", ".sheet.jpg", ".chat.webm", ".burned.mp4"):
                target = clips_dir / f"{clip_id}{suffix}"
                if target.exists():
                    target.unlink(missing_ok=True)
            pruned += 1
            logger.warning(
                "Emergency storage guard pruned shielded clip: %s (score=%s)",
                clip_id, meta.get("viral_score")
            )

    return pruned
