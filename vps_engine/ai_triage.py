from __future__ import annotations

import json
import logging
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
            "max_tokens": 260,
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
                raw_content = data["choices"][0]["message"]["content"].strip()
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
