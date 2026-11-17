from __future__ import annotations

import re
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class ChatMessage:
    timestamp: float
    username: str
    text: str
    color: str = "#FF3385"
    badges: List[str] = field(default_factory=list)
    is_system: bool = False


@dataclass
class SpikeEvent:
    stream_key: str
    streamer_name: str
    platform: str
    handle: str
    channel_id: str
    game_id: str
    spike_start: float
    spike_end: float
    pre_buffer_sec: int
    post_buffer_sec: int
    peak_mps: float
    baseline_mps: float
    spike_multiplier: float
    unique_chatters: int
    chat_messages: List[Dict[str, Any]]
    peak_messages: List[str]


class HypeScorer:
    """Computes weighted hype score and filters commands, polls, system spam, and copy-pasta."""

    def __init__(self, lexicon: Dict[str, Any]) -> None:
        self.tiers: List[Tuple[float, List[str], List[re.Pattern]]] = []
        for tier_data in lexicon.get("tiers", {}).values():
            weight = float(tier_data["weight"])
            raw_terms = tier_data["terms"]
            emoji_terms = [t for t in raw_terms if not any(c.isalnum() for c in t)]
            word_patterns = [
                re.compile(rf"(?<!\w){re.escape(t.lower())}(?!\w)")
                for t in raw_terms
                if any(c.isalnum() for c in t)
            ]
            self.tiers.append((weight, emoji_terms, word_patterns))
        self.tiers.sort(key=lambda x: x[0], reverse=True)

        noise = lexicon.get("noise", {})
        self.command_prefixes = tuple(noise.get("command_prefixes", ["!"]))
        self.ignored_commands = set(c.lower() for c in noise.get("ignored_commands", []))
        self.single_polls = set(p.lower() for p in noise.get("single_token_polls", []))
        self.system_patterns = [p.lower() for p in noise.get("system_patterns", [])]
        self.copypasta_window = float(noise.get("copypasta_window_sec", 5.0))
        self.copypasta_threshold = float(noise.get("copypasta_similarity_threshold", 0.70))

    @staticmethod
    def _normalize(text: str) -> str:
        return re.sub(r"\s+", " ", text.strip().lower())

    def score(self, msg: ChatMessage, recent_norm_texts: Deque[Tuple[float, str]]) -> float:
        if msg.is_system:
            return 0.0

        norm = self._normalize(msg.text)
        if not norm:
            return 0.0

        first_tok = norm.split(" ", 1)[0]
        if first_tok.startswith(self.command_prefixes) or first_tok in self.ignored_commands:
            return 0.0

        if norm in self.single_polls:
            return 0.0

        for pat in self.system_patterns:
            if pat in norm:
                return 0.0

        cutoff = msg.timestamp - self.copypasta_window
        while recent_norm_texts and recent_norm_texts[0][0] < cutoff:
            recent_norm_texts.popleft()

        recent_norm_texts.append((msg.timestamp, norm))
        if len(recent_norm_texts) >= 8:
            counts = Counter(t for _, t in recent_norm_texts)
            most_common_text, count = counts.most_common(1)[0]
            if most_common_text == norm and (count / len(recent_norm_texts)) > self.copypasta_threshold:
                return 0.0

        for weight, emojis, patterns in self.tiers:
            for e in emojis:
                if e in msg.text:
                    return weight
            for p in patterns:
                if p.search(norm):
                    return weight

        return 1.0


class SpikeDetector:
    """
    Hybrid rolling-baseline + unique-chatter + weighted-lexicon spike detector.
    Supports pre/post buffers, dynamic extend/merge up to 120s, and 20s cooldown.
    """

    def __init__(
        self,
        stream_key: str,
        streamer_name: str,
        platform: str,
        handle: str,
        channel_id: str,
        game_id: str,
        lexicon: Dict[str, Any],
        on_spike_ready: Callable[[SpikeEvent], None],
        pre_buffer_sec: int = 60,
        post_buffer_sec: int = 10,
        max_extend_sec: int = 120,
        cooldown_sec: int = 20,
        trigger_multiplier: float = 2.6,
        extend_multiplier: float = 1.8,
        min_baseline_mps: float = 0.8,
        warmup_sec: float = 30.0,
    ) -> None:
        self.stream_key = stream_key
        self.streamer_name = streamer_name
        self.platform = platform
        self.handle = handle
        self.channel_id = channel_id
        self.game_id = game_id
        self.on_spike_ready = on_spike_ready

        self.pre_buffer_sec = max(15, min(90, pre_buffer_sec))
        self.post_buffer_sec = max(5, min(30, post_buffer_sec))
        self.max_extend_sec = max_extend_sec
        self.cooldown_sec = cooldown_sec
        self.trigger_multiplier = trigger_multiplier
        self.extend_multiplier = extend_multiplier
        self.min_baseline_mps = min_baseline_mps
        self.warmup_sec = warmup_sec

        self.scorer = HypeScorer(lexicon)
        self.started_at: Optional[float] = None

        self._recent_norm_texts: Deque[Tuple[float, str]] = deque()
        self._scored_events: Deque[Tuple[float, float, str]] = deque()
        self._baseline_samples: Deque[Tuple[float, float]] = deque()
        self._chat_history: Deque[ChatMessage] = deque()

        self._in_spike = False
        self._spike_start = 0.0
        self._last_high_ts = 0.0
        self._peak_mps = 0.0
        self._peak_multiplier = 0.0
        self._baseline_at_trigger = 0.0
        self._cooldown_until = 0.0
        self._last_eval_ts = 0.0

        self.current_mps: float = 0.0
        self.baseline_mps: float = 0.0
        self.unique_chatters_10s: int = 0

    def ingest(self, msg: ChatMessage) -> Optional[SpikeEvent]:
        now = msg.timestamp
        if self.started_at is None:
            self.started_at = now

        self._chat_history.append(msg)
        hist_cutoff = now - 180.0
        while self._chat_history and self._chat_history[0].timestamp < hist_cutoff:
            self._chat_history.popleft()

        weight = self.scorer.score(msg, self._recent_norm_texts)
        if weight > 0.0:
            self._scored_events.append((now, weight, msg.username))

        win_cutoff = now - 10.0
        while self._scored_events and self._scored_events[0][0] < win_cutoff:
            self._scored_events.popleft()

        if now - self._last_eval_ts >= 0.5:
            self._last_eval_ts = now
            return self.tick(now)
        return None

    def tick(self, now: Optional[float] = None) -> Optional[SpikeEvent]:
        now = now or time.time()
        if self.started_at is None:
            return None

        win_cutoff = now - 10.0
        while self._scored_events and self._scored_events[0][0] < win_cutoff:
            self._scored_events.popleft()

        weighted_sum = sum(w for _, w, _ in self._scored_events)
        raw_count = len(self._scored_events)
        self.current_mps = raw_count / 10.0
        weighted_mps = weighted_sum / 10.0
        unique_users = {u for _, _, u in self._scored_events}
        self.unique_chatters_10s = len(unique_users)

        if not self._in_spike:
            self._baseline_samples.append((now, weighted_mps))
            base_cutoff = now - 120.0
            while self._baseline_samples and self._baseline_samples[0][0] < base_cutoff:
                self._baseline_samples.popleft()

        if self._baseline_samples:
            self.baseline_mps = sum(v for _, v in self._baseline_samples) / len(self._baseline_samples)
        else:
            self.baseline_mps = 0.0

        if (now - self.started_at) < self.warmup_sec:
            return None
        effective_baseline = max(self.baseline_mps, self.min_baseline_mps)
        multiplier = weighted_mps / effective_baseline if effective_baseline > 0 else 0.0

        if not self._in_spike:
            if now < self._cooldown_until:
                return None

            # Adaptive thresholds based on baseline chat velocity
            if self.baseline_mps <= 2.5:
                req_multiplier = 2.0
                req_unique = 4
            else:
                req_multiplier = 2.4
                req_unique = 6

            if (
                multiplier >= req_multiplier
                and self.current_mps >= self.min_baseline_mps
                and self.unique_chatters_10s >= req_unique
            ):
                self._in_spike = True
                self._spike_start = now
                self._last_high_ts = now
                self._peak_mps = self.current_mps
                self._peak_multiplier = multiplier
                self._baseline_at_trigger = effective_baseline
        else:
            if self.current_mps > self._peak_mps:
                self._peak_mps = self.current_mps
            if multiplier > self._peak_multiplier:
                self._peak_multiplier = multiplier

            if multiplier >= self.extend_multiplier:
                self._last_high_ts = now

            elapsed_spike = now - self._spike_start
            post_elapsed = now - self._last_high_ts

            if post_elapsed >= self.post_buffer_sec or elapsed_spike >= self.max_extend_sec:
                return self._finalize_spike(now)

        return None

    def _finalize_spike(self, now: float) -> SpikeEvent:
        spike_start = self._spike_start
        spike_end = min(now, spike_start + self.max_extend_sec)
        clip_window_start = spike_start - self.pre_buffer_sec

        window_msgs = [
            {
                "t": round(m.timestamp - clip_window_start, 2),
                "ts": m.timestamp,
                "user": m.username,
                "text": m.text,
                "color": m.color,
                "badges": m.badges,
            }
            for m in self._chat_history
            if clip_window_start <= m.timestamp <= spike_end
        ]

        peak_msgs = [
            m.text
            for m in self._chat_history
            if (spike_start - 5.0) <= m.timestamp <= spike_end and not m.is_system
        ]

        unique_in_spike = len({
            m.username
            for m in self._chat_history
            if spike_start <= m.timestamp <= spike_end
        })

        event = SpikeEvent(
            stream_key=self.stream_key,
            streamer_name=self.streamer_name,
            platform=self.platform,
            handle=self.handle,
            channel_id=self.channel_id,
            game_id=self.game_id,
            spike_start=spike_start,
            spike_end=spike_end,
            pre_buffer_sec=self.pre_buffer_sec,
            post_buffer_sec=self.post_buffer_sec,
            peak_mps=round(self._peak_mps, 2),
            baseline_mps=round(self._baseline_at_trigger, 2),
            spike_multiplier=round(self._peak_multiplier, 2),
            unique_chatters=unique_in_spike,
            chat_messages=window_msgs,
            peak_messages=peak_msgs,
        )

        self._in_spike = False
        self._cooldown_until = now + self.cooldown_sec

        self.on_spike_ready(event)
        return event

    def get_status(self) -> Dict[str, Any]:
        return {
            "stream_key": self.stream_key,
            "streamer_name": self.streamer_name,
            "platform": self.platform,
            "handle": self.handle,
            "channel_id": self.channel_id,
            "game_id": self.game_id,
            "current_mps": round(self.current_mps, 2),
            "baseline_mps": round(self.baseline_mps, 2),
            "unique_chatters_10s": self.unique_chatters_10s,
            "in_spike": self._in_spike,
            "warmed_up": self.started_at is not None and (time.time() - self.started_at) >= self.warmup_sec,
        }
