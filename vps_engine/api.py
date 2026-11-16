from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .chat_renderer import render_chat_overlay
from .config import settings

if TYPE_CHECKING:
    from .main import LiveClipperEngine


class ToggleAutoTrackBody(BaseModel):
    enabled: bool


class SetActiveGameBody(BaseModel):
    game_id: str


class StarClipBody(BaseModel):
    starred: bool


class RenderChatBody(BaseModel):
    burn_in: bool = False


def create_app(engine: "LiveClipperEngine") -> FastAPI:
    app = FastAPI(title="PinkWire LiveClipper VPS API", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def verify_secret(x_api_secret: Optional[str] = Header(default=None), secret: Optional[str] = Query(default=None)) -> None:
        token = x_api_secret or secret
        if token != settings.api_secret:
            raise HTTPException(status_code=401, detail="Invalid API secret")

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {"status": "ok", "service": "pinkwire-liveclipper-vps"}

    @app.get("/api/status", dependencies=[Depends(verify_secret)])
    async def get_status() -> Dict[str, Any]:
        return engine.get_watchdog_status()

    @app.get("/api/profiles", dependencies=[Depends(verify_secret)])
    async def get_profiles() -> Dict[str, Any]:
        return settings.profiles

    @app.post("/api/settings/auto-track", dependencies=[Depends(verify_secret)])
    async def set_auto_track(body: ToggleAutoTrackBody) -> Dict[str, Any]:
        settings.profiles["auto_track_top_streams"] = body.enabled
        settings.save_profiles()
        return {"auto_track_top_streams": body.enabled}

    @app.post("/api/settings/active-game", dependencies=[Depends(verify_secret)])
    async def set_active_game(body: SetActiveGameBody) -> Dict[str, Any]:
        if body.game_id not in settings.profiles.get("games", {}):
            raise HTTPException(status_code=404, detail="Game profile not found")
        settings.profiles["active_game"] = body.game_id
        settings.save_profiles()
        return {"active_game": body.game_id}

    @app.get("/api/clips", dependencies=[Depends(verify_secret)])
    async def list_clips(
        channel: Optional[str] = None,
        game: Optional[str] = None,
        platform: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clips: List[Dict[str, Any]] = []
        for meta_file in settings.clips_dir.glob("*.meta.json"):
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            if channel and meta.get("channel_id") != channel:
                continue
            if game and meta.get("game_id") != game:
                continue
            if platform and meta.get("platform") != platform:
                continue
            clips.append(meta)

        clips.sort(key=lambda c: (c.get("viral_score", 0), c.get("created_at", 0)), reverse=True)
        return clips

    @app.post("/api/clips/{clip_id}/star", dependencies=[Depends(verify_secret)])
    async def star_clip(clip_id: str, body: StarClipBody) -> Dict[str, Any]:
        meta_path = settings.clips_dir / f"{clip_id}.meta.json"
        if not meta_path.exists():
            raise HTTPException(status_code=404, detail="Clip not found")
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["starred"] = body.starred
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return meta

    @app.delete("/api/clips/{clip_id}", dependencies=[Depends(verify_secret)])
    async def delete_clip(clip_id: str) -> Dict[str, str]:
        for suffix in (".mp4", ".meta.json", ".chat.json", ".sheet.jpg", ".chat.webm", ".burned.mp4"):
            p = settings.clips_dir / f"{clip_id}{suffix}"
            if p.exists():
                p.unlink(missing_ok=True)
        return {"deleted": clip_id}

    @app.get("/api/clips/{clip_id}/file/{kind}", dependencies=[Depends(verify_secret)])
    async def download_clip_file(clip_id: str, kind: str) -> FileResponse:
        mapping = {
            "mp4": (f"{clip_id}.mp4", "video/mp4"),
            "meta": (f"{clip_id}.meta.json", "application/json"),
            "chat": (f"{clip_id}.chat.json", "application/json"),
            "sheet": (f"{clip_id}.sheet.jpg", "image/jpeg"),
            "overlay": (f"{clip_id}.chat.webm", "video/webm"),
            "burned": (f"{clip_id}.burned.mp4", "video/mp4"),
        }
        if kind not in mapping:
            raise HTTPException(status_code=400, detail="Invalid file kind")
        filename, media_type = mapping[kind]
        path = settings.clips_dir / filename
        if not path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(path, media_type=media_type, filename=filename)

    @app.post("/api/clips/{clip_id}/render-chat", dependencies=[Depends(verify_secret)])
    async def render_chat(clip_id: str, body: RenderChatBody) -> Dict[str, Any]:
        chat_path = settings.clips_dir / f"{clip_id}.chat.json"
        mp4_path = settings.clips_dir / f"{clip_id}.mp4"
        if not chat_path.exists():
            raise HTTPException(status_code=404, detail="Chat JSON not found")

        out_path = settings.clips_dir / (f"{clip_id}.burned.mp4" if body.burn_in else f"{clip_id}.chat.webm")
        await render_chat_overlay(
            chat_json_path=chat_path,
            output_path=out_path,
            emote_cache_dir=settings.emote_cache_dir,
            source_mp4=mp4_path if body.burn_in else None,
            burn_in=body.burn_in,
        )
        return {
            "clip_id": clip_id,
            "kind": "burned" if body.burn_in else "overlay",
            "filename": out_path.name,
            "size_kb": round(out_path.stat().st_size / 1024, 1),
        }

    @app.websocket("/ws/live")
    async def ws_live(websocket: WebSocket, secret: str = Query(default="")) -> None:
        if secret != settings.api_secret:
            await websocket.close(code=4001)
            return
        await websocket.accept()
        try:
            while True:
                await websocket.send_json(engine.get_watchdog_status())
                await asyncio.sleep(2.0)
        except WebSocketDisconnect:
            pass

    return app
