#!/usr/bin/env python3
"""
Facebook Dual Live Stream — Xtream Codes / IPTV Edition
────────────────────────────────────────────────────────
Designed for sources like: http://host/user/pass/stream_id
These are Xtream Codes IPTV endpoints that serve raw MPEG-TS over HTTP.

Key problems with this source type and how we solve them:
  ① Timestamp discontinuities / PTS resets mid-stream
     → -fflags +genpts+igndts+discardcorrupt+nobuffer
     → -copytb 1  preserves timebase during copy
     → setpts=N/FR/TB filter resets timestamps cleanly on encode path
  ② Audio codec may be MP3 (not AAC) — FLV/RTMPS rejects MP3
     → Always re-encode audio to AAC regardless of copy_mode
     → -c:v copy -c:a aac  (video copy, audio transcode only — near-zero CPU)
  ③ Server drops connection without warning (IPTV servers are flaky)
     → -reconnect 1 -reconnect_at_eof 1 -reconnect_streamed 1
     → Python watchdog with exponential back-off restarts FFmpeg instantly
  ④ HLS vs MPEG-TS ambiguity (same URL, different content by server)
     → probe_source() sniffs Content-Type before building the FFmpeg command
     → Picks the right demuxer (-f mpegts vs letting FFmpeg auto-detect HLS)
  ⑤ Weak CPU — must avoid re-encoding video
     → Video: always copy (0 CPU for video)
     → Audio: aac re-encode only (tiny CPU — audio is < 5% of load)
     → Worst case (broken video stream): ultrafast libx264 fallback

Requirements:  pip install requests
Usage:         python fb_live_stream.py
"""

import subprocess
import sys
import threading
import time

import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
PAGE_ID    = "me"
PAGE_TOKEN = "EAAKXMxkBFCIBRex3DB9VODA7mqfEedZCwQIqFNZBF7u1ZAeZA3ZAgCQ9LgZClqmSMi8BrKlN4eaEw57tuRwoogY6ejDhDX5QN8w3V27jiwDxLRKGylOkz7PIogd0TS5h6NiUQMcIiLcdGKgqmgqj8OJGkVObsUdIwLJRVoGPKSsFKSq8uP85YGV5I9WZBY6nGCPcTlA7BeKAQt4eZBcz7yZA57YUtpKSUU5cKvZA0BlYNxZAVaFyWZB2P4ZBzS6WGgKs46mFhBQMNRKssutms8aUe9shSYuKfd3FTigZDZD"   # pages_manage_posts + pages_read_engagement

# Xtream Codes IPTV source URL format:
#   http://host/username/password/stream_id
# Both lives can share the same source or use different stream IDs.
LIVE_A = {
    "title":       "Matrix Nejma — Live 1",
    "description": "Stream 1 — Matrix Nejma",
    "source":      "http://dhoomtv.xyz/8zpo3GsVY7/beneficial2concern/652350",
}

LIVE_B = {
    "title":       "Matrix Nejma — Live 2",
    "description": "Stream 2 — Matrix Nejma",
    "source":      "http://dhoomtv.xyz/8zpo3GsVY7/beneficial2concern/652351",
    # ↑ Change stream_id at the end for a different channel
}

GRAPH_API = "https://graph.facebook.com/v25.0"

# Watchdog tuning
MAX_FAST_CRASHES  = 12    # give up after this many quick consecutive crashes
CRASH_WINDOW_SECS = 20    # crash counts as "fast" if process lived less than this
# ─────────────────────────────────────────────────────────────────────────────


# ── Source type detection ─────────────────────────────────────────────────────

def probe_source(url: str) -> str:
    """
    Sniff the HTTP Content-Type to determine what the server is actually serving.
    Returns one of: 'mpegts' | 'hls' | 'http' | 'other'
    Falls back to 'mpegts' on any error (safest assumption for Xtream Codes).
    """
    if not url.startswith("http"):
        return "other"
    try:
        r = requests.head(url, timeout=8, allow_redirects=True)
        ct = r.headers.get("Content-Type", "").lower()
        if "mpegurl" in ct or url.endswith(".m3u8"):
            return "hls"
        if "mpeg" in ct or "octet-stream" in ct or "video" in ct:
            return "mpegts"
        # No extension, no clear Content-Type → Xtream Codes default = mpegts
        return "mpegts"
    except Exception:
        return "mpegts"   # safest fallback for IPTV


# ── FFmpeg command builder ────────────────────────────────────────────────────

def build_ffmpeg_cmd(rtmps_url: str, source: str) -> list:
    """
    Build a maximally resilient FFmpeg command for an Xtream Codes / IPTV source.

    Strategy:
      Video → -c:v copy          (zero CPU — never re-encode video)
      Audio → -c:a aac           (tiny CPU — always transcode to ensure AAC)

    This handles the most common IPTV problem: video is H.264 (copyable) but
    audio is MP3 or AC3 which FLV/RTMPS rejects. We transcode only audio.

    Input resilience flags:
      -fflags +genpts           regenerate PTS from DTS (fixes missing PTS)
      -fflags +igndts           ignore broken DTS values
      -fflags +discardcorrupt   drop corrupt packets silently
      -fflags +nobuffer         reduce input buffering latency
      -err_detect ignore_err    continue past non-fatal decode errors
      -max_error_rate 1.0       survive 100% packet error rate
      -thread_queue_size 8192   large queue absorbs source jitter/gaps
      -probesize 10M            large probe so FFmpeg finds streams in long headers
      -analyzeduration 5000000  5s analysis window for slow-starting streams

    HTTP reconnect flags (survive server drops without restarting FFmpeg):
      -reconnect 1              reconnect if connection drops
      -reconnect_at_eof 1       reconnect when server sends EOF (channel change)
      -reconnect_streamed 1     reconnect even on non-seekable streams
      -reconnect_delay_max 5    max 5 s between reconnect attempts

    Output resilience:
      -max_interleave_delta 0   no interleave flush based on timestamps
      -flvflags no_duration_filesize  prevent FLV header overflow on bad ts
      -avoid_negative_ts make_zero   shift timestamps so nothing is negative
      -copyts                   preserve source timestamps (keeps copy in sync)
      -start_at_zero            shift so stream starts at t=0 not random epoch
    """
    source_type = probe_source(source) if source.startswith("http") else "other"

    # ── Common resilience flags applied to every HTTP source ─────────────────
    http_input_flags = [
        "-reconnect",           "1",
        "-reconnect_at_eof",    "1",
        "-reconnect_streamed",  "1",
        "-reconnect_delay_max", "5",
        "-timeout",             "10000000",     # 10 s read timeout (µs)
        "-fflags",              "+genpts+igndts+discardcorrupt+nobuffer",
        "-err_detect",          "ignore_err",
        "-max_error_rate",      "1.0",
        "-thread_queue_size",   "8192",
        "-probesize",           "10000000",     # 10 MB probe
        "-analyzeduration",     "5000000",      # 5 s analysis
    ]

    # ── Build source input args ───────────────────────────────────────────────
    if source == "test":
        src_args = [
            "-re",
            "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=30",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100",
        ]
    elif source_type == "hls":
        src_args = (
            http_input_flags
            + ["-allowed_extensions", "ALL"]   # allow all HLS segment extensions
            + ["-i", source]
        )
    elif source_type == "mpegts":
        # Force mpegts demuxer — more reliable than auto-detect for Xtream Codes
        src_args = (
            http_input_flags
            + ["-f", "mpegts"]
            + ["-i", source]
        )
    elif source.startswith("rtsp://") or source.startswith("rtsps://"):
        src_args = [
            "-rtsp_transport",    "tcp",
            "-stimeout",          "10000000",
            "-fflags",            "+genpts+igndts+discardcorrupt+nobuffer",
            "-err_detect",        "ignore_err",
            "-max_error_rate",    "1.0",
            "-thread_queue_size", "8192",
            "-i", source,
        ]
    else:
        # Local file / device
        src_args = [
            "-fflags",            "+genpts+igndts+discardcorrupt",
            "-thread_queue_size", "8192",
            "-i", source,
        ]

    # ── Codec args ────────────────────────────────────────────────────────────
    if source == "test":
        # Test card: must encode both (raw lavfi → x264 + aac)
        codec_args = [
            "-c:v",         "libx264",
            "-preset",      "ultrafast",
            "-tune",        "zerolatency",
            "-crf",         "28",
            "-b:v",         "0",
            "-maxrate",     "3500k",
            "-bufsize",     "2000k",
            "-g",           "60",
            "-keyint_min",  "60",
            "-sc_threshold","0",
            "-vf",          "format=yuv420p",
            "-c:a",         "aac",
            "-b:a",         "128k",
            "-ar",          "44100",
            "-ac",          "2",
            "-avoid_negative_ts", "make_zero",
        ]
    else:
        # IPTV source: copy video (0 CPU), re-encode audio to AAC (tiny CPU)
        # -copytb 1 keeps the video timebase stable during copy
        # Audio: aac_adtstoasc bitstream filter strips ADTS headers that
        #        some IPTV sources wrap AAC in — required for FLV container
        codec_args = [
            "-map",         "0:v:0",        # first video stream only
            "-map",         "0:a:0",        # first audio stream only
            "-c:v",         "copy",
            "-copytb",      "1",
            "-c:a",         "aac",
            "-b:a",         "128k",
            "-ar",          "44100",
            "-ac",          "2",
            "-bsf:a",       "aac_adtstoasc",    # fix ADTS-wrapped AAC for FLV
            "-copyts",
            "-start_at_zero",
            "-avoid_negative_ts", "make_zero",
        ]

    # ── Output flags ──────────────────────────────────────────────────────────
    output_flags = [
        "-max_interleave_delta", "0",
        "-f",                    "flv",
        "-flvflags",             "no_duration_filesize",
        rtmps_url,
    ]

    return (
        ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-stats"]
        + src_args
        + codec_args
        + output_flags
    )


# ── Stream watchdog ───────────────────────────────────────────────────────────

def stream_worker(rtmps_url: str, source: str, label: str,
                  ready_event: threading.Event, stop_event: threading.Event):
    """
    Resilient FFmpeg watchdog for one live stream.

    Crash classification:
      Fast crash  (lived < CRASH_WINDOW_SECS) → source / network problem
                  → exponential back-off, count toward MAX_FAST_CRASHES
      Slow crash  (lived ≥ CRASH_WINDOW_SECS) → normal server drop / EOF
                  → restart immediately, reset crash counter

    After MAX_FAST_CRASHES consecutive fast crashes the watchdog stops
    to avoid hammering a dead source on a weak CPU.
    """
    fast_crashes = 0
    first_run    = True

    while not stop_event.is_set():
        if fast_crashes >= MAX_FAST_CRASHES:
            print(
                f"\n[{label}] ⛔  {MAX_FAST_CRASHES} consecutive fast crashes.\n"
                f"[{label}]    Source '{source}' is not recoverable.\n"
                f"[{label}]    Verify the URL is live, then restart the script."
            )
            return

        cmd = build_ffmpeg_cmd(rtmps_url, source)
        print(f"\n[{label}] ▶  Starting  (streak={fast_crashes}/{MAX_FAST_CRASHES})")

        start = time.time()
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=None)
        except FileNotFoundError:
            print(f"[{label}] ❌  ffmpeg not found — install FFmpeg and retry.")
            stop_event.set()
            return
        except Exception as exc:
            print(f"[{label}] ❌  Popen failed: {exc}. Retrying in 5 s…")
            time.sleep(5)
            continue

        if first_run:
            ready_event.set()
            first_run = False

        while proc.poll() is None:
            if stop_event.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break
            time.sleep(1)

        if stop_event.is_set():
            print(f"[{label}] ■  Stopped cleanly.")
            return

        lived = time.time() - start
        rc    = proc.returncode

        if lived < CRASH_WINDOW_SECS:
            fast_crashes += 1
            wait = min(3 * (2 ** (fast_crashes - 1)), 60)   # 3, 6, 12, 24, 48, 60…
            print(
                f"[{label}] ⚡  Fast exit after {lived:.1f}s (code={rc})  "
                f"streak={fast_crashes}/{MAX_FAST_CRASHES}  "
                f"waiting {wait:.0f}s…"
            )
            for _ in range(int(wait)):
                if stop_event.is_set():
                    return
                time.sleep(1)
        else:
            fast_crashes = 0
            print(f"[{label}] 🔄  EOF/disconnect after {lived:.1f}s — restarting now…")


# ── DASH preview poller ───────────────────────────────────────────────────────

def dash_preview_worker(live_id: str, label: str, stop_event: threading.Event):
    """
    Poll GET /{live_id}?fields=dash_preview_url,ingest_streams independently
    for each live video. Prints the unique dash_preview_url per live video
    as soon as Facebook populates it (~15–30 s after FFmpeg connects).
    """
    url    = f"{GRAPH_API}/{live_id}"
    params = {"access_token": PAGE_TOKEN,
              "fields": "id,dash_preview_url,ingest_streams"}

    print(f"[{label}|DASH] Waiting for preview URL  (id={live_id})…")
    last_preview = None

    while not stop_event.is_set():
        try:
            data = requests.get(url, params=params, timeout=15).json()

            if "error" in data:
                print(f"[{label}|DASH] API error: {data['error'].get('message')}")
            else:
                preview = data.get("dash_preview_url")
                streams = data.get("ingest_streams", [])

                if preview and preview != last_preview:
                    last_preview = preview
                    bar = "═" * 72
                    print(f"\n{bar}")
                    print(f"  📺  [{label}]  DASH PREVIEW  —  open in VLC or dash.js")
                    print(f"{bar}")
                    print(f"  {preview}")
                    print(f"{bar}\n")
                elif not preview:
                    print(f"[{label}|DASH] Not ready yet, retry in 10 s…")

                if streams:
                    print(f"  ┌─ [{label}] Ingest Health {'─'*48}┐")
                    for s in streams:
                        role   = "MASTER " if s.get("is_master") else "standby"
                        sid    = s.get("stream_id", "?")
                        h      = s.get("stream_health") or {}
                        vbr    = h.get("video_bitrate")
                        fps    = h.get("video_framerate")
                        abr    = h.get("audio_bitrate")
                        res    = (f"{int(h['video_width'])}x{int(h['video_height'])}"
                                  if h.get("video_width") else "n/a")
                        print(f"  │  [{role}] id={sid}")
                        if vbr:
                            print(f"  │    Video : {res}  {vbr/1000:.0f} kbps  {fps:.1f} fps")
                        if abr:
                            print(f"  │    Audio : {abr/1000:.0f} kbps")
                        print("  │")
                    print(f"  └{'─'*70}┘\n")

        except Exception as exc:
            print(f"[{label}|DASH] Error: {exc}")

        interval = 30 if last_preview else 10
        for _ in range(interval):
            if stop_event.is_set():
                return
            time.sleep(1)


# ── Facebook API helpers ──────────────────────────────────────────────────────

def create_live_video(cfg: dict, label: str) -> dict:
    url     = f"{GRAPH_API}/{PAGE_ID}/live_videos"
    payload = {
        "access_token": PAGE_TOKEN,
        "title":        cfg["title"],
        "description":  cfg["description"],
        "status":       "UNPUBLISHED",
    }
    print(f"[{label}] Creating live video…")
    r = requests.post(url, data=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        print(f"[{label}] FB error: {data['error']}")
        sys.exit(1)
    print(f"[{label}] ✓  id={data['id']}")
    print(f"[{label}]    RTMPS → {data.get('secure_stream_url', 'N/A')}")
    return data


def end_live_video(live_id: str, label: str):
    url = f"{GRAPH_API}/{live_id}"
    r   = requests.post(url,
            data={"access_token": PAGE_TOKEN, "end_live_video": True},
            timeout=15)
    msg = "ended." if r.ok else f"error: {r.text}"
    print(f"[{label}] Live {live_id} {msg}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sep = "─" * 72

    # 1 — Probe sources before hitting the Facebook API
    for cfg, lbl in [(LIVE_A, "LIVE-A"), (LIVE_B, "LIVE-B")]:
        src = cfg["source"]
        if src.startswith("http"):
            st = probe_source(src)
            print(f"[{lbl}] Source probed → type={st}  url={src}")

    print(f"\n{sep}")

    # 2 — Create both live videos
    live_a = create_live_video(LIVE_A, "LIVE-A")
    print(sep)
    live_b = create_live_video(LIVE_B, "LIVE-B")
    print(f"{sep}\n")

    id_a, rtmps_a = live_a["id"], live_a.get("secure_stream_url")
    id_b, rtmps_b = live_b["id"], live_b.get("secure_stream_url")

    if not rtmps_a or not rtmps_b:
        print("[!] Missing RTMPS URL — check token permissions.")
        sys.exit(1)

    # 3 — Launch both FFmpeg processes simultaneously
    #     Facebook deactivates an UNPUBLISHED live if it receives no data
    #     within ~30 s, so we start both threads at the same time.
    stop_event = threading.Event()
    ready_a    = threading.Event()
    ready_b    = threading.Event()
    threads    = []

    t_a = threading.Thread(
        target=stream_worker,
        args=(rtmps_a, LIVE_A["source"], "LIVE-A", ready_a, stop_event),
        daemon=True,
    )
    t_b = threading.Thread(
        target=stream_worker,
        args=(rtmps_b, LIVE_B["source"], "LIVE-B", ready_b, stop_event),
        daemon=True,
    )

    print("[*] Launching both FFmpeg processes simultaneously…")
    t_a.start()
    t_b.start()
    threads += [t_a, t_b]

    ready_a.wait(timeout=30)
    ready_b.wait(timeout=30)
    print("[*] Both streams are pushing data.\n")

    # 4 — DASH preview poller per live video
    time.sleep(5)
    da = threading.Thread(target=dash_preview_worker,
                          args=(id_a, "LIVE-A", stop_event), daemon=True)
    db = threading.Thread(target=dash_preview_worker,
                          args=(id_b, "LIVE-B", stop_event), daemon=True)
    da.start(); db.start()
    threads += [da, db]

    # 5 — Block until Ctrl+C
    try:
        print("[*] Streaming both lives…  Ctrl+C to stop.\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[*] Shutting down…")
        stop_event.set()

    for t in threads:
        t.join(timeout=10)

    # 6 — End both Facebook live videos
    end_live_video(id_a, "LIVE-A")
    end_live_video(id_b, "LIVE-B")
    print("[*] Done.")


if __name__ == "__main__":
    main()