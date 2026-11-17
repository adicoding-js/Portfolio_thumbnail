import json
import os
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

# Ensure virtualenv bin, ~/.local/bin, /usr/local/bin are always in PATH for subprocesses
py_bin_dir = str(Path(sys.executable).parent)
extra_paths = [
    py_bin_dir,
    str(Path.home() / ".local" / "bin"),
    "/usr/local/bin",
    "/usr/bin",
]
path_env = os.environ.get("PATH", "")
parts = path_env.split(os.pathsep) if path_env else []
for ep in reversed(extra_paths):
    if ep and ep not in parts:
        parts.insert(0, ep)
os.environ["PATH"] = os.pathsep.join(parts)


def resolve_bin(name: str) -> str:
    """Find binary path prioritizing current Python environment's bin folder, ~/.local/bin, then system PATH."""
    py_dir = Path(sys.executable).parent
    candidates = [
        py_dir / name,
        py_dir / f"{name}.exe",
        Path.home() / ".local" / "bin" / name,
        Path("/usr/local/bin") / name,
        Path("/usr/bin") / name,
    ]
    for c in candidates:
        if c.is_file() and os.access(c, os.X_OK):
            return str(c)
    found = shutil.which(name)
    return found or name


@dataclass
class Settings:
    # Platform API credentials
    twitch_client_id: str = os.getenv("TWITCH_CLIENT_ID", "")
    twitch_client_secret: str = os.getenv("TWITCH_CLIENT_SECRET", "")
    twitch_user_oauth_token: str = os.getenv("TWITCH_USER_OAUTH_TOKEN", "")

    kick_client_id: str = os.getenv("KICK_CLIENT_ID", "")
    kick_client_secret: str = os.getenv("KICK_CLIENT_SECRET", "")

    youtube_api_key: str = os.getenv("YOUTUBE_API_KEY", "")

    # Hack Club AI Proxy
    hackclub_api_key: str = os.getenv("HACKCLUB_API_KEY", "")
    hackclub_base_url: str = os.getenv("HACKCLUB_BASE_URL", "https://ai.hackclub.com/proxy/v1").rstrip("/")

    # Server & limits
    vps_host: str = os.getenv("VPS_HOST", "0.0.0.0")
    vps_port: int = int(os.getenv("VPS_PORT", "8765"))
    api_secret: str = os.getenv("API_SECRET", "pinkwire_nest_secret_2026")
    max_chat_watchers: int = int(os.getenv("MAX_CHAT_WATCHERS", "15"))
    max_ring_buffers: int = int(os.getenv("MAX_RING_BUFFERS", "4"))
    max_vps_storage_mb: int = int(os.getenv("MAX_VPS_STORAGE_MB", "6000"))
    enable_audio_confirm: bool = os.getenv("ENABLE_AUDIO_CONFIRM", "false").lower() == "true"

    # Paths
    root_dir: Path = ROOT_DIR
    config_dir: Path = ROOT_DIR / "config"
    data_dir: Path = ROOT_DIR / "data"
    clips_dir: Path = ROOT_DIR / "data" / "clips"
    ring_buffer_dir: Path = ROOT_DIR / "data" / "ring_buffers"
    emote_cache_dir: Path = ROOT_DIR / "data" / "emote_cache"

    profiles: Dict[str, Any] = field(default_factory=dict)
    lexicon: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.clips_dir.mkdir(parents=True, exist_ok=True)
        self.ring_buffer_dir.mkdir(parents=True, exist_ok=True)
        self.emote_cache_dir.mkdir(parents=True, exist_ok=True)
        self.reload_configs()

    def reload_configs(self) -> None:
        profiles_path = self.config_dir / "profiles.json"
        lexicon_path = self.config_dir / "hype_lexicon.json"
        with profiles_path.open("r", encoding="utf-8") as f:
            self.profiles = json.load(f)
        with lexicon_path.open("r", encoding="utf-8") as f:
            self.lexicon = json.load(f)

    def save_profiles(self) -> None:
        profiles_path = self.config_dir / "profiles.json"
        with profiles_path.open("w", encoding="utf-8") as f:
            json.dump(self.profiles, f, indent=2, ensure_ascii=False)


settings = Settings()
