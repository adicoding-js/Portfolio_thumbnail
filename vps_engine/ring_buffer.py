from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, Optional

from .config import settings

logger = logging.getLogger("pinkwire.ring_buffer")


class KickRingBuffer:
    """
    Ultra-lightweight 720p rolling .ts ring-buffer for a single Kick stream.
    Uses streamlink -> ffmpeg -c copy (near-zero CPU, ~35MB disk for 90s window).
    """

    def __init__(self, handle: str, stream_url: str, buffer_root: Path) -> None:
        self.handle = handle
        self.stream_url = stream_url
        self.dir = buffer_root / f"kick_{handle}"
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
        cmd = (
            f"streamlink --stdout --retry-streams 5 --retry-open 3 "
            f"'{self.stream_url}' 720p,720p60,best | "
            f"ffmpeg -hide_banner -loglevel error -i pipe:0 -c copy "
            f"-f segment -segment_time 5 -segment_wrap 18 -reset_timestamps 1 '{seg_pattern}'"
        )
        self._proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info("Started 720p Kick ring-buffer for %s", self.handle)

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
        logger.info("Stopped Kick ring-buffer for %s", self.handle)

    def _clean_dir(self) -> None:
        if self.dir.exists():
            for f in self.dir.glob("*.ts"):
                try:
                    f.unlink()
                except OSError:
                    pass

    async def extract_clip(self, output_mp4: Path, duration_sec: int = 75) -> bool:
        segments = sorted(self.dir.glob("seg_*.ts"), key=lambda p: p.stat().st_mtime)
        if not segments:
            logger.warning("No ring-buffer segments found for kick:%s", self.handle)
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
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                "-f", "concat", "-safe", "0", "-i", str(concat_file),
                "-c", "copy", "-movflags", "+faststart", str(output_mp4),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.error("Ring-buffer concat failed for %s: %s", self.handle, stderr.decode(errors="ignore"))
                return False
            return output_mp4.exists() and output_mp4.stat().st_size > 10_000
        finally:
            if concat_file.exists():
                concat_file.unlink(missing_ok=True)


class RingBufferManager:
    """Strictly enforces MAX_RING_BUFFERS=2 to protect Hack Club Nest RAM & 16GB disk."""

    def __init__(self) -> None:
        self.max_buffers = settings.max_ring_buffers
        self.buffers: Dict[str, KickRingBuffer] = {}

    async def ensure_buffer(self, handle: str, stream_url: str, priority: int = 2) -> bool:
        if handle in self.buffers and self.buffers[handle].is_running:
            return True
        if len(self.buffers) >= self.max_buffers:
            return False
        buf = KickRingBuffer(handle, stream_url, settings.ring_buffer_dir)
        await buf.start()
        self.buffers[handle] = buf
        return True

    async def release_buffer(self, handle: str) -> None:
        buf = self.buffers.pop(handle, None)
        if buf:
            await buf.stop()

    async def extract_clip(self, handle: str, output_mp4: Path, duration_sec: int = 75) -> bool:
        buf = self.buffers.get(handle)
        if not buf or not buf.is_running:
            return False
        return await buf.extract_clip(output_mp4, duration_sec=duration_sec)

    async def stop_all(self) -> None:
        for handle in list(self.buffers.keys()):
            await self.release_buffer(handle)

    def active_handles(self) -> list[str]:
        return [h for h, b in self.buffers.items() if b.is_running]


async def generate_contact_sheet(mp4_path: Path, out_jpg: Path) -> bool:
    """Generate a 3-frame JPEG contact sheet (<50KB) for 5G Data-Saver preview."""
    probe = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
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
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", str(mp4_path),
        "-vf", fps_filter,
        "-frames:v", "1",
        "-q:v", "9",
        str(out_jpg),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    return out_jpg.exists() and out_jpg.stat().st_size > 0


def enforce_storage_guard(clips_dir: Path, max_mb: int) -> int:
    """Enforces MAX_VPS_STORAGE_MB by deleting oldest unstarred clips first."""
    import json

    def dir_size_mb() -> float:
        total = sum(p.stat().st_size for p in clips_dir.glob("*") if p.is_file())
        return total / (1024 * 1024)

    pruned = 0
    if dir_size_mb() <= max_mb:
        return 0

    meta_files = sorted(clips_dir.glob("*.meta.json"), key=lambda p: p.stat().st_mtime)
    for meta_path in meta_files:
        if dir_size_mb() <= max_mb * 0.90:
            break
        try:
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            if meta.get("starred", False):
                continue
        except Exception:
            pass

        clip_id = meta_path.name.replace(".meta.json", "")
        for suffix in (".mp4", ".meta.json", ".chat.json", ".sheet.jpg", ".chat.webm", ".burned.mp4"):
            target = clips_dir / f"{clip_id}{suffix}"
            if target.exists():
                target.unlink(missing_ok=True)
        pruned += 1
        logger.info("Storage guard pruned old unstarred clip: %s", clip_id)

    return pruned
