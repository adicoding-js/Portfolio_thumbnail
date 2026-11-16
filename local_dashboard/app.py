from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel

ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

DOWNLOADS_DIR = ROOT_DIR / "downloads"
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_VPS_URL = os.getenv("VPS_URL", f"http://localhost:{os.getenv('VPS_PORT', '8765')}").rstrip("/")
DEFAULT_API_SECRET = os.getenv("API_SECRET", "pinkwire_nest_secret_2026")
MAX_LOCAL_STORAGE_MB = int(os.getenv("MAX_LOCAL_STORAGE_MB", "8000"))

app = FastAPI(title="PinkWire LiveClipper Local Dashboard")


class DownloadRequest(BaseModel):
    vps_url: str = DEFAULT_VPS_URL
    api_secret: str = DEFAULT_API_SECRET


class RenderRequest(BaseModel):
    vps_url: str = DEFAULT_VPS_URL
    api_secret: str = DEFAULT_API_SECRET
    burn_in: bool = False


def _enforce_local_cap() -> None:
    def total_mb() -> float:
        return sum(p.stat().st_size for p in DOWNLOADS_DIR.glob("*") if p.is_file()) / (1024 * 1024)

    if total_mb() <= MAX_LOCAL_STORAGE_MB:
        return
    files = sorted(DOWNLOADS_DIR.glob("*.mp4"), key=lambda p: p.stat().st_mtime)
    for mp4 in files:
        if total_mb() <= MAX_LOCAL_STORAGE_MB * 0.85:
            break
        stem = mp4.stem
        for ext in (".mp4", ".meta.json", ".chat.json", ".chat.webm", ".burned.mp4"):
            (DOWNLOADS_DIR / f"{stem}{ext}").unlink(missing_ok=True)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    html_path = Path(__file__).resolve().parent / "static" / "index.html"
    return html_path.read_text(encoding="utf-8")


@app.get("/local/config")
async def get_local_config() -> Dict[str, Any]:
    used_mb = sum(p.stat().st_size for p in DOWNLOADS_DIR.glob("*") if p.is_file()) / (1024 * 1024)
    return {
        "default_vps_url": DEFAULT_VPS_URL,
        "default_api_secret": DEFAULT_API_SECRET,
        "local_used_mb": round(used_mb, 1),
        "local_limit_mb": MAX_LOCAL_STORAGE_MB,
        "downloaded_ids": [p.stem for p in DOWNLOADS_DIR.glob("*.mp4")],
    }


@app.get("/proxy/sheet/{clip_id}")
async def proxy_sheet(
    clip_id: str,
    vps_url: str = Query(default=DEFAULT_VPS_URL),
    api_secret: str = Query(default=DEFAULT_API_SECRET),
) -> Response:
    url = f"{vps_url.rstrip('/')}/api/clips/{clip_id}/file/sheet"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={"X-API-Secret": api_secret}) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=resp.status, detail="Sheet not found")
            return Response(content=await resp.read(), media_type="image/jpeg")


@app.post("/local/download/{clip_id}")
async def download_clip_bundle(clip_id: str, req: DownloadRequest) -> Dict[str, Any]:
    _enforce_local_cap()
    base = req.vps_url.rstrip("/")
    headers = {"X-API-Secret": req.api_secret}
    saved = []

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
        for kind, suffix in (("mp4", ".mp4"), ("meta", ".meta.json"), ("chat", ".chat.json")):
            url = f"{base}/api/clips/{clip_id}/file/{kind}"
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    if kind == "mp4":
                        raise HTTPException(status_code=resp.status, detail="Failed to download MP4")
                    continue
                target = DOWNLOADS_DIR / f"{clip_id}{suffix}"
                with target.open("wb") as f:
                    async for chunk in resp.content.iter_chunked(65536):
                        f.write(chunk)
                saved.append(target.name)

    return {"clip_id": clip_id, "saved_files": saved, "folder": str(DOWNLOADS_DIR)}


@app.post("/local/render-chat/{clip_id}")
async def request_chat_render(clip_id: str, req: RenderRequest) -> Dict[str, Any]:
    base = req.vps_url.rstrip("/")
    headers = {"X-API-Secret": req.api_secret}

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
        async with session.post(
            f"{base}/api/clips/{clip_id}/render-chat",
            headers=headers,
            json={"burn_in": req.burn_in},
        ) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=resp.status, detail=await resp.text())
            info = await resp.json()

        kind = info["kind"]
        filename = info["filename"]
        async with session.get(f"{base}/api/clips/{clip_id}/file/{kind}", headers=headers) as dl:
            dl.raise_for_status()
            target = DOWNLOADS_DIR / filename
            with target.open("wb") as f:
                async for chunk in dl.content.iter_chunked(65536):
                    f.write(chunk)

    return {"clip_id": clip_id, "rendered_file": str(target), "kind": kind}


@app.get("/local/play/{clip_id}")
async def play_local_clip(clip_id: str) -> FileResponse:
    path = DOWNLOADS_DIR / f"{clip_id}.mp4"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Clip not downloaded locally yet")
    return FileResponse(path, media_type="video/mp4")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8080)
