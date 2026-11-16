from __future__ import annotations

import asyncio
import json
import logging
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger("pinkwire.chat_renderer")


class EmoteCache:
    """Fetches and caches global 7TV, BTTV, and FFZ emotes on disk."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.cache_dir / "emote_index.json"
        self.emote_map: Dict[str, str] = {}
        if self.index_path.exists():
            try:
                self.emote_map = json.loads(self.index_path.read_text(encoding="utf-8"))
            except Exception:
                self.emote_map = {}

    async def refresh_global_index(self) -> None:
        if self.emote_map:
            return
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            try:
                async with session.get("https://7tv.io/v3/emote-sets/global") as r:
                    if r.status == 200:
                        data = await r.json()
                        for e in data.get("emotes", []):
                            self.emote_map[e["name"]] = f"https://cdn.7tv.app/emote/{e['id']}/1x.png"
            except Exception:
                pass
            try:
                async with session.get("https://api.betterttv.net/3/cached/emotes/global") as r:
                    if r.status == 200:
                        for e in await r.json():
                            self.emote_map[e["code"]] = f"https://cdn.betterttv.net/emote/{e['id']}/1x.png"
            except Exception:
                pass
            try:
                async with session.get("https://api.frankerfacez.com/v1/set/global") as r:
                    if r.status == 200:
                        data = await r.json()
                        for s in data.get("sets", {}).values():
                            for e in s.get("emoticons", []):
                                urls = e.get("urls", {})
                                url = urls.get("1") or next(iter(urls.values()), None)
                                if url:
                                    self.emote_map[e["name"]] = f"https:{url}" if url.startswith("//") else url
            except Exception:
                pass

        self.index_path.write_text(json.dumps(self.emote_map), encoding="utf-8")

    async def get_emote_image(self, code: str, session: aiohttp.ClientSession) -> Optional[Image.Image]:
        url = self.emote_map.get(code)
        if not url:
            return None
        safe_name = "".join(c if c.isalnum() else "_" for c in code) + ".png"
        local_path = self.cache_dir / safe_name
        if not local_path.exists():
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        local_path.write_bytes(await resp.read())
                    else:
                        return None
            except Exception:
                return None
        try:
            img = Image.open(local_path).convert("RGBA")
            img.thumbnail((24, 24))
            return img
        except Exception:
            return None


def _hex_to_rgba(hex_color: str) -> Tuple[int, int, int, int]:
    h = hex_color.lstrip("#")
    if len(h) == 6:
        return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), 255)
    return (255, 51, 133, 255)


async def render_chat_overlay(
    chat_json_path: Path,
    output_path: Path,
    emote_cache_dir: Path,
    source_mp4: Optional[Path] = None,
    burn_in: bool = False,
    width: int = 360,
    height: int = 640,
    fps: int = 10,
) -> Path:
    messages: List[Dict[str, Any]] = json.loads(chat_json_path.read_text(encoding="utf-8"))
    if not messages:
        raise ValueError("No chat messages available to render.")

    duration = max(5.0, max(float(m.get("t", 0.0)) for m in messages) + 2.0)
    cache = EmoteCache(emote_cache_dir)
    await cache.refresh_global_index()

    emote_imgs: Dict[str, Image.Image] = {}
    unique_tokens = {tok for m in messages for tok in m.get("text", "").split()}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        for tok in unique_tokens:
            if tok in cache.emote_map:
                img = await cache.get_emote_image(tok, session)
                if img:
                    emote_imgs[tok] = img

    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", 16)
        font_reg = ImageFont.truetype("DejaVuSans.ttf", 16)
    except OSError:
        font = ImageFont.load_default()
        font_reg = ImageFont.load_default()

    overlay_webm = output_path if not burn_in else output_path.with_suffix(".tmp_chat.webm")

    ffmpeg_cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-f", "rawvideo", "-vcodec", "rawvideo",
        "-s", f"{width}x{height}", "-pix_fmt", "rgba", "-r", str(fps),
        "-i", "-",
        "-c:v", "libvpx-vp9", "-pix_fmt", "yuva420p", "-b:v", "600k",
        str(overlay_webm),
    ]

    proc = subprocess.Popen(ffmpeg_cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    total_frames = int(duration * fps)

    try:
        for frame_idx in range(total_frames):
            t = frame_idx / fps
            visible = [m for m in messages if float(m.get("t", 0.0)) <= t][-18:]

            canvas = Image.new("RGBA", (width, height), (12, 12, 18, 130))
            draw = ImageDraw.Draw(canvas)

            y = height - 28
            for msg in reversed(visible):
                if y < 10:
                    break
                user = str(msg.get("user", "anon"))[:18]
                color = _hex_to_rgba(str(msg.get("color", "#FF3385")))
                text = str(msg.get("text", ""))[:90]

                x = 10
                draw.text((x, y), f"{user}: ", fill=color, font=font)
                x += int(draw.textlength(f"{user}: ", font=font))

                for tok in text.split():
                    if tok in emote_imgs and x + 26 < width - 8:
                        canvas.paste(emote_imgs[tok], (x, y - 3), emote_imgs[tok])
                        x += 28
                    else:
                        tok_w = int(draw.textlength(tok + " ", font=font_reg))
                        if x + tok_w > width - 8:
                            break
                        draw.text((x, y), tok + " ", fill=(245, 245, 250, 255), font=font_reg)
                        x += tok_w
                y -= 30

            assert proc.stdin is not None
            proc.stdin.write(canvas.tobytes())

        assert proc.stdin is not None
        proc.stdin.close()
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"FFmpeg chat render failed: {proc.stderr.read().decode(errors='ignore')}")
    except Exception:
        proc.kill()
        raise

    if burn_in and source_mp4 and source_mp4.exists():
        burn_proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-i", str(source_mp4),
            "-i", str(overlay_webm),
            "-filter_complex", "[0:v][1:v]overlay=W-w-16:H-h-16:shortest=1",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "24",
            "-c:a", "copy", "-movflags", "+faststart",
            str(output_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await burn_proc.communicate()
        overlay_webm.unlink(missing_ok=True)
        if burn_proc.returncode != 0:
            raise RuntimeError(f"Burn-in failed: {stderr.decode(errors='ignore')}")
        return output_path

    return overlay_webm
