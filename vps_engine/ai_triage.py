from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

from .config import settings

logger = logging.getLogger("pinkwire.ai_triage")

PREFERRED_MODELS = [
    "openai/gpt-6-luna",
    "deepseek/deepseek-v4.1-flash",
    "z-ai/glm-5.3-flash",
    "anthropic/claude-haiku-5.5",
]


class AITriageEngine:
    """Ultra-low-cost (<$0.001/spike) AI clip triage via Hack Club AI Proxy."""

    def __init__(self) -> None:
        self.base_url = settings.hackclub_base_url
        self.api_key = settings.hackclub_api_key
        self.selected_model: Optional[str] = None

    async def initialize(self, session: aiohttp.ClientSession) -> str:
        if self.selected_model:
            return self.selected_model

        if not self.api_key or self.api_key == "your_hackclub_api_key_here":
            self.selected_model = PREFERRED_MODELS[0]
            logger.warning("HACKCLUB_API_KEY not configured; defaulting model to %s", self.selected_model)
            return self.selected_model

        url = f"{self.base_url}/models"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                resp.raise_for_status()
                payload = await resp.json()
                available = [m["id"] for m in payload.get("data", []) if "id" in m]
        except Exception as exc:
            logger.error("Failed to fetch Hack Club Proxy models (%s); using fallback.", exc)
            self.selected_model = PREFERRED_MODELS[0]
            return self.selected_model

        for pref in PREFERRED_MODELS:
            if pref in available:
                self.selected_model = pref
                logger.info("AI Triage selected preferred model: %s", pref)
                return pref

        for model_id in available:
            lower = model_id.lower()
            if any(tag in lower for tag in ("flash", "mini", "haiku", "nano")):
                self.selected_model = model_id
                logger.info("AI Triage selected fallback fast model: %s", model_id)
                return model_id

        self.selected_model = available[0] if available else PREFERRED_MODELS[0]
        logger.info("AI Triage selected default model: %s", self.selected_model)
        return self.selected_model

    @staticmethod
    def _deduplicate_messages(messages: List[str], limit: int = 60) -> List[str]:
        seen = set()
        result: List[str] = []
        for msg in messages:
            cleaned = " ".join(msg.strip().split())
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(cleaned[:140])
            if len(result) >= limit:
                break
        return result

    async def evaluate_spike(
        self,
        session: aiohttp.ClientSession,
        streamer_name: str,
        game_name: str,
        spike_multiplier: float,
        peak_messages: List[str],
    ) -> Dict[str, Any]:
        if not self.selected_model:
            await self.initialize(session)

        sample = self._deduplicate_messages(peak_messages, limit=60)

        if not self.api_key or self.api_key == "your_hackclub_api_key_here":
            score = min(95, int(45 + spike_multiplier * 12))
            return {
                "is_clip_worthy": score >= 55,
                "viral_score": score,
                "moment_type": "hype",
                "moment_summary": f"Chat spiked {spike_multiplier:.1f}x during {streamer_name}'s {game_name} stream.",
                "suggested_hook": f"WAIT UNTIL YOU SEE WHAT {streamer_name.upper()} JUST DID",
                "reject_reason": "" if score >= 55 else "Below heuristic threshold",
            }

        system_prompt = (
            "You are a viral gaming clip editor for PinkWire. "
            "Analyze the chat spike sample and decide if this is a genuine clip-worthy moment "
            "(funny fail, insane clutch, shocking reaction, drama, rage, or unexpected event) "
            "vs a false positive (raid, sub train, giveaway, poll, or bot spam). "
            "Respond ONLY with valid JSON matching this exact schema:\n"
            "{\n"
            '  "is_clip_worthy": boolean,\n'
            '  "viral_score": integer (0-100),\n'
            '  "moment_type": string (e.g. "clutch", "fail", "rage", "funny", "shock", "false_positive"),\n'
            '  "moment_summary": string (1-2 concise sentences),\n'
            '  "suggested_hook": string (punchy 5-10 word short-form title hook),\n'
            '  "reject_reason": string (empty string if is_clip_worthy is true)\n'
            "}"
        )

        user_payload = {
            "streamer": streamer_name,
            "game": game_name,
            "spike_multiplier": round(spike_multiplier, 2),
            "chat_sample": sample,
        }

        body = {
            "model": self.selected_model,
            "temperature": 0.2,
            "max_tokens": 1000,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            async with session.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=body,
                timeout=aiohttp.ClientTimeout(total=25),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                choice = data.get("choices", [{}])[0]
                message = choice.get("message", {})
                raw_content = (message.get("content") or "").strip()
                if not raw_content:
                    raise ValueError(f"Empty content in AI proxy response: {data}")
                if raw_content.startswith("```"):
                    raw_content = raw_content.split("```")[1]
                    if raw_content.startswith("json"):
                        raw_content = raw_content[4:]
                parsed = json.loads(raw_content.strip())
                return {
                    "is_clip_worthy": bool(parsed.get("is_clip_worthy", False)),
                    "viral_score": max(0, min(100, int(parsed.get("viral_score", 0)))),
                    "moment_type": str(parsed.get("moment_type", "unknown")),
                    "moment_summary": str(parsed.get("moment_summary", "")),
                    "suggested_hook": str(parsed.get("suggested_hook", "")),
                    "reject_reason": str(parsed.get("reject_reason", "")),
                }
        except Exception as exc:
            logger.error("AI Triage request failed (%s); keeping clip conservatively.", exc)
            return {
                "is_clip_worthy": True,
                "viral_score": min(85, int(40 + spike_multiplier * 10)),
                "moment_type": "unverified_spike",
                "moment_summary": f"AI triage unavailable; {spike_multiplier:.1f}x chat spike detected.",
                "suggested_hook": f"{streamer_name} chat goes crazy!",
                "reject_reason": "",
            }

    async def analyze_narrative_context(
        self,
        session: aiohttp.ClientSession,
        audio_path: Path,
        streamer_name: str,
        game_name: str,
        spike_offset_sec: float,
        total_audio_duration_sec: float,
        peak_messages: List[str],
    ) -> Dict[str, Any]:
        """
        Multimodal Audio analysis using Gemini via Hack Club AI Proxy.
        Listens to the 10-minute stream audio snippet, finds the exact setup start,
        climax, and aftermath, returning precise trim timestamps and a spoken hook.
        """
        default_setup = max(0.0, round(spike_offset_sec - 60.0, 1))
        default_end = min(total_audio_duration_sec, round(spike_offset_sec + 35.0, 1))
        fallback = {
            "setup_start_sec": default_setup,
            "climax_sec": round(spike_offset_sec, 1),
            "reaction_end_sec": default_end,
            "spoken_hook": f"WAIT UNTIL YOU SEE WHAT {streamer_name.upper()} JUST DID",
            "context_summary": f"Chat exploded around {int(spike_offset_sec)}s into {streamer_name}'s stream.",
            "moment_type": "hype",
            "viral_score": 80,
        }

        if not audio_path.exists() or audio_path.stat().st_size < 1000:
            logger.warning("Audio file missing or too small (%s); using heuristic trim.", audio_path)
            return fallback

        if not self.api_key or self.api_key == "your_hackclub_api_key_here":
            logger.info("Hack Club API key not set; using heuristic narrative trim.")
            return fallback

        try:
            audio_bytes = audio_path.read_bytes()
            audio_b64 = base64.b64encode(audio_bytes).decode("ascii")

            system_prompt = (
                "You are an elite short-form video editor for PinkWire (TikTok, Shorts, Reels). "
                f"You are listening to an audio recording from {streamer_name}'s {game_name} live stream. "
                f"The total audio duration is {int(total_audio_duration_sec)} seconds. "
                f"Around second {int(spike_offset_sec)}, the live chat EXPLODED with reactions.\n\n"
                "Your objective is to find the NARRATIVE CONTEXT of this moment:\n"
                "1. `setup_start_sec`: The exact second where the streamer starts the joke, bet, dare, "
                f"story, challenge, or gameplay buildup leading up to the climax. Must be BEFORE second {int(spike_offset_sec)} "
                "(typically 25 to 80 seconds prior).\n"
                "2. `climax_sec`: The exact second of the explosion/peak event (crash, clutch, fail, scream, revelation).\n"
                "3. `reaction_end_sec`: The second where the streamer's reaction or chat banter naturally winds down "
                "(typically 15 to 45 seconds after the climax).\n"
                "4. Ensure total duration (reaction_end_sec - setup_start_sec) is between 45 and 120 seconds.\n"
                "5. `spoken_hook`: A punchy 5-9 word viral hook in ALL CAPS quoting or capturing the streamer's actual spoken setup words.\n"
                "6. `context_summary`: 1-2 concise sentences explaining what happened and why it's funny/hype.\n"
                "7. `moment_type`: One of 'clutch', 'fail', 'rage', 'funny', 'shock', 'drama'.\n"
                "8. `viral_score`: Integer 0-100 indicating viral potential.\n\n"
                "Respond ONLY with valid JSON matching this schema:\n"
                "{\n"
                '  "setup_start_sec": float,\n'
                '  "climax_sec": float,\n'
                '  "reaction_end_sec": float,\n'
                '  "spoken_hook": string,\n'
                '  "context_summary": string,\n'
                '  "moment_type": string,\n'
                '  "viral_score": integer\n'
                "}"
            )

            sample_msgs = self._deduplicate_messages(peak_messages, limit=30)
            user_text = (
                f"Streamer: {streamer_name}\n"
                f"Game: {game_name}\n"
                f"Spike occurred around: second {int(spike_offset_sec)} (of {int(total_audio_duration_sec)}s)\n"
                f"Chat reaction sample: {', '.join(sample_msgs[:15])}\n"
                "Listen to the audio, locate the narrative setup and punchline, and return JSON timestamps."
            )

            body = {
                "model": "google/gemini-2.5-flash",
                "temperature": 0.2,
                "max_tokens": 1000,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": user_text},
                            {"type": "input_audio", "input_audio": {"data": audio_b64, "format": "mp3"}},
                        ],
                    },
                ],
            }

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }

            async with session.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=body,
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                choice = data.get("choices", [{}])[0]
                content = (choice.get("message", {}).get("content") or "").strip()
                if content.startswith("```"):
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                parsed = json.loads(content.strip())

                setup_start = float(parsed.get("setup_start_sec", default_setup))
                climax = float(parsed.get("climax_sec", spike_offset_sec))
                reaction_end = float(parsed.get("reaction_end_sec", default_end))

                # Sanity boundary checks
                setup_start = max(0.0, min(setup_start, spike_offset_sec - 5.0))
                reaction_end = max(climax + 5.0, min(total_audio_duration_sec, reaction_end))
                dur = reaction_end - setup_start
                if dur < 30.0:
                    setup_start = max(0.0, spike_offset_sec - 45.0)
                    reaction_end = min(total_audio_duration_sec, spike_offset_sec + 25.0)
                elif dur > 150.0:
                    setup_start = max(0.0, spike_offset_sec - 75.0)
                    reaction_end = min(total_audio_duration_sec, spike_offset_sec + 45.0)

                return {
                    "setup_start_sec": round(setup_start, 1),
                    "climax_sec": round(climax, 1),
                    "reaction_end_sec": round(reaction_end, 1),
                    "spoken_hook": str(parsed.get("spoken_hook", fallback["spoken_hook"])).strip(),
                    "context_summary": str(parsed.get("context_summary", fallback["context_summary"])).strip(),
                    "moment_type": str(parsed.get("moment_type", "funny")),
                    "viral_score": max(50, min(100, int(parsed.get("viral_score", 85)))),
                }
        except Exception as exc:
            logger.error("Gemini narrative audio analysis failed (%s); using fallback timestamps.", exc)
            return fallback
