"""
Offline verification suite for SpikeDetector (baseline warm-up, noise/copypasta rejection,
weighted hype trigger, dynamic extend/merge, and cooldown) + optional AI Triage smoke test.
"""
import asyncio
import json
from pathlib import Path

import aiohttp

from vps_engine.ai_triage import AITriageEngine
from vps_engine.detector import ChatMessage, SpikeDetector, SpikeEvent


def run_detector_simulation() -> SpikeEvent:
    lexicon = json.loads((Path("config/hype_lexicon.json")).read_text(encoding="utf-8"))
    captured_events: list[SpikeEvent] = []

    detector = SpikeDetector(
        stream_key="twitch:kaicenat",
        streamer_name="Kai Cenat",
        platform="twitch",
        handle="kaicenat",
        channel_id="pinkwire_official",
        game_id="gta_vi",
        lexicon=lexicon,
        on_spike_ready=lambda ev: captured_events.append(ev),
        pre_buffer_sec=60,
        post_buffer_sec=10,
        max_extend_sec=120,
        cooldown_sec=20,
    )

    t = 1_700_000_000.0

    for sec in range(75):
        for i in range(2):
            detector.ingest(ChatMessage(t + sec + i * 0.4, f"viewer_{sec}_{i}", "just vibing in vice city"))

    assert len(captured_events) == 0, "Should not trigger during calm baseline"

    t_noise = t + 76
    for i in range(25):
        detector.ingest(ChatMessage(t_noise + i * 0.1, f"spammer_{i}", "!drops"))
        detector.ingest(ChatMessage(t_noise + i * 0.12, f"pasta_{i}", "FREE GTA6 BETA LINK IN BIO"))
        detector.ingest(ChatMessage(t_noise + i * 0.15, f"voter_{i}", "1"))
    assert len(captured_events) == 0, "Noise/commands/copypasta should not trigger a spike"

    t_spike = t + 90
    hype_lines = [
        "CLIP IT CLIP IT", "NO WAY LMAOOO 💀", "AINTNOWAY HE JUST DID THAT",
        "PUT ME IN THE VID", "OMEGALUL", "WTF BRO WHAT 😭", "CLIP THAT RIGHT NOW",
        "HOLY CLUTCH", "HI YOUTUBE", "CAUGHT IN 4K 💀",
    ]
    for sec in range(16):
        for i in range(10):
            detector.ingest(
                ChatMessage(t_spike + sec + i * 0.08, f"chatter_{sec}_{i}", hype_lines[(sec + i) % len(hype_lines)])
            )

    t_cool = t_spike + 17
    for sec in range(14):
        detector.tick(t_cool + sec)

    assert len(captured_events) == 1, f"Expected 1 merged spike, got {len(captured_events)}"
    ev = captured_events[0]
    print(
        f"[PASS] Detector triggered 1 merged spike: multiplier={ev.spike_multiplier}x, "
        f"peak_mps={ev.peak_mps}, unique_chatters={ev.unique_chatters}, "
        f"duration={ev.spike_end - ev.spike_start:.1f}s"
    )
    return ev


async def run_triage_test(event: SpikeEvent) -> None:
    triage = AITriageEngine()
    async with aiohttp.ClientSession() as session:
        model = await triage.initialize(session)
        result = await triage.evaluate_spike(
            session=session,
            streamer_name=event.streamer_name,
            game_name="GTA VI",
            spike_multiplier=event.spike_multiplier,
            peak_messages=event.peak_messages,
        )
        print(f"[PASS] AI Triage ({model}): {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    spike = run_detector_simulation()
    asyncio.run(run_triage_test(spike))
