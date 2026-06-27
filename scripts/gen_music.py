#!/usr/bin/env python3
"""
Generate four royalty-free synthesised music clips for the highlight video feature.
Output: server/music/{lively,lyrical,serene,dynamic}.wav  (30 s each, mono 44.1 kHz)

Run once from the project root:
    python scripts/gen_music.py

To replace a clip with a real music file, drop a WAV or MP3 with the same base name
into server/music/ and rename it to match (lively.wav, lyrical.wav, serene.wav,
dynamic.wav).  The server streams whatever file is present; it does not require WAV.
"""
import math
import struct
import wave
from pathlib import Path

SAMPLE_RATE = 44100
DURATION    = 30.0          # seconds per clip
OUT_DIR     = Path(__file__).parent.parent / "server" / "music"

# ── Note frequency table (Hz, equal temperament, A4 = 440) ──────────────────
FREQ: dict[str, float] = {
    "R": 0,
    "C3": 130.81, "D3": 146.83, "E3": 164.81, "F3": 174.61,
    "F#3": 184.997, "G3": 196.00, "A3": 220.00, "B3": 246.94,
    "C4": 261.63, "D4": 293.66, "E4": 329.63, "F4": 349.23,
    "F#4": 369.99, "G4": 392.00, "A4": 440.00, "B4": 493.88,
    "C5": 523.25, "D5": 587.33, "E5": 659.25, "F5": 698.46,
    "F#5": 739.99, "G5": 783.99, "A5": 880.00,
}


def _note(freq: float, dur: float, vol: float, timbre: str) -> list[int]:
    n       = int(dur * SAMPLE_RATE)
    attack  = min(int(0.015 * SAMPLE_RATE), max(1, n // 6))
    release = min(int(0.10  * SAMPLE_RATE), max(1, n // 3))
    buf: list[int] = []
    for i in range(n):
        if freq == 0:
            buf.append(0)
            continue
        t = i / SAMPLE_RATE
        if   i < attack:       env = i / attack
        elif i >= n - release: env = (n - i) / release
        else:                  env = 1.0
        if timbre == "bass":
            v = (  math.sin(2 * math.pi * freq       * t)
                 + 0.60 * math.sin(2 * math.pi * freq * 2 * t)
                 + 0.20 * math.sin(2 * math.pi * freq * 3 * t)) / 1.80
        elif timbre == "flute":
            v = (  math.sin(2 * math.pi * freq       * t)
                 + 0.12 * math.sin(2 * math.pi * freq * 2 * t)) / 1.12
        else:   # piano
            v = (  math.sin(2 * math.pi * freq       * t)
                 + 0.45 * math.sin(2 * math.pi * freq * 2 * t)
                 + 0.20 * math.sin(2 * math.pi * freq * 3 * t)
                 + 0.08 * math.sin(2 * math.pi * freq * 4 * t)) / 1.73
        buf.append(int(max(-32767, min(32767, v * env * vol * 32767))))
    return buf


def _sequence(notes: list[tuple], bpm: float, vol: float, timbre: str) -> list[int]:
    """Render note sequence [(name, beats), …] repeating until DURATION seconds."""
    beat   = 60.0 / bpm
    target = int(DURATION * SAMPLE_RATE)
    buf: list[int] = []
    while len(buf) < target:
        for name, beats in notes:
            dur      = beats * beat
            play_dur = dur * 0.92   # 8% gap avoids clicks between notes
            rest_dur = dur - play_dur
            buf += _note(FREQ[name], play_dur, vol, timbre)
            buf += _note(0,          rest_dur, 0,   timbre)
    buf = buf[:target]
    # 1-second fade-out
    fade_n = SAMPLE_RATE
    for i in range(max(0, target - fade_n), target):
        buf[i] = int(buf[i] * (target - i) / fade_n)
    return buf


def _mix(a: list[int], b: list[int]) -> list[int]:
    length = max(len(a), len(b))
    return [
        max(-32767, min(32767,
            (a[i] if i < len(a) else 0) + (b[i] if i < len(b) else 0)))
        for i in range(length)
    ]


def _save(samples: list[int], path: Path) -> None:
    with wave.open(str(path), "w") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(struct.pack(f"<{len(samples)}h", *samples))
    print(f"  {path.name:<20} {len(samples) // SAMPLE_RATE}s  "
          f"{path.stat().st_size // 1024} KB")


# ── Clip definitions ─────────────────────────────────────────────────────────

CLIPS: dict[str, dict] = {

    # 輕快 — C major pentatonic, bouncy and bright, 128 BPM
    "lively": dict(
        bpm=128,
        melody=dict(vol=0.28, timbre="piano", notes=[
            ("C5", 0.5), ("E5", 0.5), ("G5", 1.0), ("E5", 0.5), ("C5", 0.5),
            ("G4", 1.0), ("A4", 0.5), ("C5", 0.5), ("E5", 1.0), ("C5", 0.5),
            ("A4", 0.5), ("E5", 0.5), ("G5", 0.5), ("A5", 1.0), ("G5", 0.5),
            ("E5", 0.5), ("C5", 0.5), ("E5", 0.5), ("G5", 1.0), ("C5", 1.0),
        ]),
        bass=dict(vol=0.18, timbre="bass", notes=[
            ("C3", 2.0), ("G3", 2.0),
            ("C3", 2.0), ("A3", 2.0),
            ("F3", 2.0), ("C3", 2.0),
            ("G3", 2.0), ("C3", 2.0),
        ]),
    ),

    # 抒情 — A minor, flowing and sentimental, 108 BPM
    "lyrical": dict(
        bpm=108,
        melody=dict(vol=0.26, timbre="flute", notes=[
            ("A4", 2.0), ("C5", 1.0), ("E5", 2.0), ("C5", 1.0),
            ("E5", 1.5), ("D5", 1.5), ("C5", 3.0),
            ("B4", 2.0), ("G4", 1.0), ("A4", 2.0), ("E4", 1.0),
            ("A4", 1.5), ("G4", 1.5), ("A4", 3.0),
        ]),
        bass=dict(vol=0.16, timbre="bass", notes=[
            ("A3", 3.0), ("E3", 3.0),
            ("E3", 3.0), ("A3", 3.0),
            ("G3", 3.0), ("D3", 3.0),
            ("E3", 3.0), ("A3", 3.0),
        ]),
    ),

    # 幽美 — D major pentatonic, serene and elegant, 96 BPM
    "serene": dict(
        bpm=96,
        melody=dict(vol=0.22, timbre="flute", notes=[
            ("D5", 4.0), ("F#5", 2.0), ("A5", 4.0),
            ("F#5", 2.0), ("D5", 4.0), ("B4", 2.0),
            ("A4", 4.0), ("F#4", 2.0), ("D4", 4.0),
            ("A4", 2.0), ("D5", 4.0), ("D5", 2.0),
        ]),
        bass=dict(vol=0.14, timbre="bass", notes=[
            ("D3", 4.0), ("A3", 4.0),
            ("F#3", 4.0), ("D3", 4.0),
            ("D3", 4.0), ("A3", 4.0),
            ("A3", 4.0), ("D3", 4.0),
        ]),
    ),

    # 動感 — C major, driving and energetic, 148 BPM
    "dynamic": dict(
        bpm=148,
        melody=dict(vol=0.28, timbre="piano", notes=[
            ("C5", 0.25), ("C5", 0.25), ("G5", 0.50), ("C5", 0.25), ("E5", 0.25),
            ("G5", 0.50), ("E5", 0.25), ("C5", 0.25), ("G5", 0.50), ("E5", 0.50),
            ("G5", 0.25), ("A5", 0.25), ("G5", 0.50), ("E5", 0.25), ("C5", 0.25),
            ("E5", 0.50), ("G5", 0.25), ("A5", 0.25), ("G5", 0.50), ("C5", 0.50),
        ]),
        bass=dict(vol=0.22, timbre="bass", notes=[
            ("C3", 0.5), ("G3", 0.5), ("C3", 0.5), ("G3", 0.5),
            ("C3", 0.5), ("G3", 0.5), ("C3", 0.5), ("E3", 0.5),
            ("F3", 0.5), ("C3", 0.5), ("F3", 0.5), ("G3", 0.5),
            ("G3", 0.5), ("C3", 0.5), ("G3", 0.5), ("C3", 0.5),
        ]),
    ),
}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating music clips → {OUT_DIR}\n")
    labels = {"lively": "輕快", "lyrical": "抒情", "serene": "幽美", "dynamic": "動感"}
    for name, cfg in CLIPS.items():
        bpm    = cfg["bpm"]
        melody = _sequence(cfg["melody"]["notes"], bpm, cfg["melody"]["vol"], cfg["melody"]["timbre"])
        bass   = _sequence(cfg["bass"]["notes"],   bpm, cfg["bass"]["vol"],   cfg["bass"]["timbre"])
        mixed  = _mix(melody, bass)
        out    = OUT_DIR / f"{name}.wav"
        _save(mixed, out)
        print(f"    {labels[name]}  ({bpm} BPM)")
    print("\nDone — restart the server to serve the new tracks.")


if __name__ == "__main__":
    main()
