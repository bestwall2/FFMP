#!/usr/bin/env python3
"""
Matrix Nejma — Facebook Dual Live Manager
Flask backend: manages up to 3 simultaneous UNPUBLISHED live videos per token,
auto-rotates every 4 hours, persists session, serves the web UI.

Install:  pip install flask requests
Run:      python mt.py
UI:       http://localhost:5000
"""

import json
import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests as req
from flask import Flask, jsonify, render_template_string, request, send_from_directory

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_LIVE_PER_TOKEN = 3          # Facebook hard limit per token
MAX_STREAM_SECS    = 4 * 3600   # rotate after 4 hours
CRASH_WINDOW       = 20         # seconds — fast-crash threshold
MAX_FAST_CRASHES   = 12
GRAPH_API          = "https://graph.facebook.com/v25.0"
STATE_FILE         = Path(__file__).parent / "state.json"

app = Flask(__name__)

# ── Global state (protected by state_lock) ────────────────────────────────────
state_lock = threading.Lock()

# config: per-slot cards with token/source pairs
#   cards: [{token, source, source_label, title}]
# streams: {stream_id: StreamWorker}
config = {"cards": [], "max_lives": 0}
streams = {}   # label → StreamWorker
token_group_restart_enabled = False  # Global setting for token group restart


# ═════════════════════════════════════════════════════════════════════════════
# Facebook API helpers
# ═════════════════════════════════════════════════════════════════════════════

def fb_create_live(token: str, page_id: str, title: str, desc: str) -> dict:
    payload = {
        "access_token": token,
        "title":        title,
        "description":  desc,
        "published":    False,
        "status":       "UNPUBLISHED",
    }
    r = req.post(f"{GRAPH_API}/{page_id}/live_videos", data=payload, timeout=30)
    try:
        r.raise_for_status()
    except req.exceptions.HTTPError as exc:
        body = r.text
        try:
            error_data = r.json().get("error", {})
        except Exception:
            error_data = {}
        detail = error_data.get("message") or body or str(exc)
        code = error_data.get("code")
        subcode = error_data.get("error_subcode")
        msg = detail
        if code is not None:
            msg += f" (code={code}"
            if subcode is not None:
                msg += f", subcode={subcode}"
            msg += ")"
        raise RuntimeError(msg) from exc

    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", str(data["error"])))
    return data


def fb_end_live(token: str, live_id: str):
    try:
        req.post(f"{GRAPH_API}/{live_id}", data={
            "access_token":   token,
            "end_live_video": True,
        }, timeout=15)
    except Exception:
        pass


def fb_get_live(token: str, live_id: str) -> dict:
    r = req.get(f"{GRAPH_API}/{live_id}", params={
        "access_token": token,
        "fields":       "id,dash_preview_url,ingest_streams,status",
    }, timeout=15)
    return r.json()


def fb_get_page_id(token: str) -> str:
    r = req.get(f"{GRAPH_API}/me", params={
        "access_token": token,
        "fields": "id",
    }, timeout=10)
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", "Invalid token"))
    return data["id"]


def probe_source(url: str) -> str:
    if not url.startswith("http"):
        return "other"
    try:
        r  = req.head(url, timeout=6, allow_redirects=True)
        ct = r.headers.get("Content-Type", "").lower()
        if "mpegurl" in ct or url.endswith(".m3u8"):
            return "hls"
        return "mpegts"
    except Exception:
        return "mpegts"


# ═════════════════════════════════════════════════════════════════════════════
# FFmpeg command builder
# ═════════════════════════════════════════════════════════════════════════════

def build_cmd(rtmps_url: str, source: str) -> list:
    http_flags = [
        "-reconnect",           "1",
        "-reconnect_at_eof",    "1",
        "-reconnect_streamed",  "1",
        "-reconnect_delay_max", "5",
        "-timeout",             "10000000",
        "-fflags",              "+genpts+igndts+discardcorrupt+nobuffer",
        "-err_detect",          "ignore_err",
        "-max_error_rate",      "1.0",
        "-thread_queue_size",   "8192",
        "-probesize",           "10000000",
        "-analyzeduration",     "5000000",
    ]

    stype = probe_source(source) if source.startswith("http") else "other"

    if source == "test":
        src = ["-re", "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=30",
               "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100"]
    elif stype == "hls":
        src = http_flags + ["-allowed_extensions", "ALL", "-i", source]
    elif stype == "mpegts":
        src = http_flags + ["-f", "mpegts", "-i", source]
    elif source.startswith("rtsp://"):
        src = ["-rtsp_transport", "tcp", "-stimeout", "10000000",
               "-fflags", "+genpts+igndts+discardcorrupt+nobuffer",
               "-err_detect", "ignore_err", "-max_error_rate", "1.0",
               "-thread_queue_size", "8192", "-i", source]
    else:
        src = ["-fflags", "+genpts+igndts+discardcorrupt",
               "-thread_queue_size", "8192", "-i", source]

    if source == "test":
        codec = ["-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                 "-crf", "28", "-b:v", "0", "-maxrate", "3500k", "-bufsize", "2000k",
                 "-g", "60", "-keyint_min", "60", "-sc_threshold", "0",
                 "-vf", "format=yuv420p",
                 "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
                 "-avoid_negative_ts", "make_zero"]
    else:
        codec = ["-map", "0:v:0", "-map", "0:a:0",
                 "-c:v", "copy", "-copytb", "1",
                 "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
                 "-bsf:a", "aac_adtstoasc",
                 "-copyts", "-start_at_zero", "-avoid_negative_ts", "make_zero"]

    out = ["-max_interleave_delta", "0", "-f", "flv",
           "-flvflags", "no_duration_filesize", rtmps_url]

    return ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-stats"] + src + codec + out


# ═════════════════════════════════════════════════════════════════════════════
# StreamWorker — one live video lifecycle
# ═════════════════════════════════════════════════════════════════════════════

class StreamWorker:
    """
    Manages one Facebook live video slot:
      - Creates the live video on Facebook
      - Runs FFmpeg, restarts on crash
      - After 4 h: ends the live, creates a new one, continues
      - Pending source changes (from UI) are applied at rotation time
    """

    def __init__(self, label: str, token: str, page_id: str,
                 source: str, source_label: str):
        self.label        = label
        self.token        = token
        self.page_id      = page_id
        self.source       = source
        self.source_label = source_label

        # Pending source — applied at next rotation
        self._pending_source       = None
        self._pending_source_label = None
        self._pending_lock         = threading.Lock()

        # Token group restart coordination
        self._pending_restart = False
        self._restart_time    = None

        # Public status (read by API)
        self.live_id      = None
        self.rtmps_url    = None
        self.dash_url     = None
        self.status       = "idle"        # idle|starting|live|rotating|error|stopped
        self.error        = None
        self.elapsed_secs = 0.0
        self.slot_start   = None          # when current 4h slot started
        self.rotation_n   = 0             # how many rotations done

        self._stop        = threading.Event()
        self._thread      = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    def queue_source_change(self, source: str, label: str):
        """Called from UI when user changes source. Applied at next rotation."""
        with self._pending_lock:
            self._pending_source       = source
            self._pending_source_label = label

    def _apply_pending(self):
        with self._pending_lock:
            if self._pending_source:
                self.source       = self._pending_source
                self.source_label = self._pending_source_label
                self._pending_source       = None
                self._pending_source_label = None

    def to_dict(self) -> dict:
        with self._pending_lock:
            pending = self._pending_source_label
        return {
            "label":         self.label,
            "source_label":  self.source_label,
            "source":        self.source,
            "pending_source":pending,
            "live_id":       self.live_id,
            "rtmps_url":     self.rtmps_url,
            "dash_url":      self.dash_url,
            "status":        self.status,
            "error":         self.error,
            "elapsed_secs":  round(self.elapsed_secs, 1),
            "slot_remaining":round(max(0, MAX_STREAM_SECS - self.elapsed_secs), 1),
            "rotation_n":    self.rotation_n,
        }

    def _handle_token_group_restart(self):
        """Check if we should restart all streams with the same token."""
        global token_group_restart_enabled, streams
        if not token_group_restart_enabled:
            return False  # Individual restart

        # Find all streams with the same token
        same_token_streams = []
        with state_lock:
            for label, worker in streams.items():
                if worker.token == self.token and worker != self:
                    same_token_streams.append(worker)

        if not same_token_streams:
            return False  # No other streams with same token

        log(f"[{self.label}] Token group restart triggered — {len(same_token_streams) + 1} streams with same token")

        # Set a restart flag on all streams with same token
        all_same_token = same_token_streams + [self]
        for worker in all_same_token:
            worker._pending_restart = True
            worker._restart_time = time.time() + 45

        log(f"[{self.label}] All streams scheduled for coordinated restart in 45s")
        return True

    def _restart_ffmpeg(self):
        """Restart FFmpeg process without ending the live video."""
        log(f"[{self.label}] Restarting FFmpeg process...")
        self.status = "restarting"
        time.sleep(2)  # Brief pause
        # The _run_slot loop will continue and restart FFmpeg naturally

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _run(self):
        while not self._stop.is_set():
            self._apply_pending()
            coordinated_restart = self._run_slot()
            if self._stop.is_set():
                break
            if coordinated_restart:
                # Coordinated restart - don't rotate, just continue with same live
                log(f"[{self.label}] Coordinated restart complete, continuing with same live")
                continue
            # Rotate: end current live, start new one
            self.rotation_n  += 1
            self.elapsed_secs = 0.0
            self.status = "rotating"
            log(f"[{self.label}] Rotation #{self.rotation_n} — creating new live…")
            if self.live_id:
                fb_end_live(self.token, self.live_id)
                self.live_id  = None
                self.rtmps_url = None
                self.dash_url  = None

        self.status = "stopped"
        if self.live_id:
            fb_end_live(self.token, self.live_id)
        log(f"[{self.label}] Stopped.")

    def _run_slot(self):
        """Create one live video, stream until 4h or stop signal. Returns True if coordinated restart."""
        # Create live video
        self.status = "starting"
        try:
            data = fb_create_live(
                self.token, self.page_id,
                f"Matrix Nejma — {self.label}",
                f"Stream {self.label} — Matrix Nejma",
            )
            self.live_id  = data["id"]
            self.rtmps_url = data.get("secure_stream_url", "")
            self.error    = None
            log(f"[{self.label}] Live created id={self.live_id}")
        except Exception as exc:
            self.status = "error"
            self.error  = str(exc)
            log(f"[{self.label}] Could not create live: {exc}")
            # Wait before retry
            for _ in range(30):
                if self._stop.is_set():
                    return False
                time.sleep(1)
            return False

        # Start DASH poller
        dash_stop = threading.Event()
        threading.Thread(target=self._dash_poller, args=(dash_stop,), daemon=True).start()

        # Stream with watchdog
        self.slot_start  = time.time()
        self.elapsed_secs = 0.0
        fast_crashes = 0

        while not self._stop.is_set():
            # Check 4h rotation trigger
            self.elapsed_secs = time.time() - self.slot_start
            if self.elapsed_secs >= MAX_STREAM_SECS:
                log(f"[{self.label}] 4h reached — rotating.")
                dash_stop.set()
                return False

            # Check for coordinated token group restart
            if self._pending_restart and time.time() >= self._restart_time:
                log(f"[{self.label}] Executing coordinated restart...")
                self._pending_restart = False
                self._restart_time = None
                # Force a restart by breaking out of the FFmpeg loop
                dash_stop.set()
                return True

            if fast_crashes >= MAX_FAST_CRASHES:
                self.status = "error"
                self.error  = f"{MAX_FAST_CRASHES} consecutive fast crashes"
                log(f"[{self.label}] Too many crashes — giving up on this slot.")
                break

            cmd   = build_cmd(self.rtmps_url, self.source)
            start = time.time()
            self.status = "live"

            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            except FileNotFoundError:
                self.status = "error"
                self.error  = "ffmpeg not found"
                self._stop.set()
                break
            except Exception as exc:
                self.error = str(exc)
                time.sleep(5)
                continue

            while proc.poll() is None:
                if self._stop.is_set():
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    break
                self.elapsed_secs = time.time() - self.slot_start
                if self.elapsed_secs >= MAX_STREAM_SECS:
                    proc.terminate()
                    break
                time.sleep(1)

            # Read any remaining output from FFmpeg
            try:
                stdout_data, stderr_data = proc.communicate(timeout=2)
                if stderr_data:
                    stderr_lines = stderr_data.decode('utf-8', errors='replace').strip()
                    if stderr_lines:
                        # Log FFmpeg errors for debugging
                        for line in stderr_lines.split('\n')[:5]:  # Limit to first 5 lines
                            if line.strip():
                                log(f"[{self.label}] FFmpeg: {line.strip()}")
            except Exception:
                pass  # Ignore if we can't read output

            lived = time.time() - start
            if lived < CRASH_WINDOW:
                fast_crashes += 1
                exit_code = proc.returncode if proc.returncode is not None else "unknown"
                self.status = "restarting"
                log(f"[{self.label}] Fast crash ({lived:.0f}s, exit={exit_code}) streak={fast_crashes}")

                # Check for token group restart
                if self._handle_token_group_restart():
                    # Token group restart handled, exit this slot
                    dash_stop.set()
                    return True

                # Individual restart

                # Individual restart
                wait = min(3 * (2 ** (fast_crashes - 1)), 60)
                log(f"[{self.label}] Individual restart in {wait:.0f}s...")
                for _ in range(int(wait)):
                    if self._stop.is_set():
                        break
                    time.sleep(1)
            else:
                fast_crashes = 0
                log(f"[{self.label}] Disconnect ({lived:.0f}s) — restarting FFmpeg…")

        dash_stop.set()
        return False

    def _dash_poller(self, stop_ev: threading.Event):
        """Poll Facebook for dash_preview_url until stop signal."""
        last = None
        while not stop_ev.is_set() and not self._stop.is_set():
            if not self.live_id or self.status not in {"starting", "live", "rotating"}:
                time.sleep(5)
                continue

            try:
                data = fb_get_live(self.token, self.live_id)
                url  = data.get("dash_preview_url")
                if url and url != last:
                    self.dash_url = url
                    last = url
                    log(f"[{self.label}] DASH: {url}")
            except Exception:
                pass
            interval = 30 if last else 10
            for _ in range(interval):
                if stop_ev.is_set() or self._stop.is_set():
                    break
                time.sleep(1)


# ═════════════════════════════════════════════════════════════════════════════
# Logging
# ═════════════════════════════════════════════════════════════════════════════

log_lines = []
log_lock  = threading.Lock()

def log(msg: str):
    ts  = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with log_lock:
        log_lines.append(line)
        if len(log_lines) > 500:
            log_lines.pop(0)
    print(line)


# ═════════════════════════════════════════════════════════════════════════════
# API routes
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return send_from_directory(str(Path(__file__).parent), "index.html")


@app.route("/api/status")
def api_status():
    with state_lock:
        running = {k: v.to_dict() for k, v in streams.items()}
    with log_lock:
        recent_logs = list(log_lines[-100:])
    return jsonify({
        "config":  config,
        "streams": running,
        "logs":    recent_logs,
        "running": bool(streams),
    })


def _normalize_card(raw: dict) -> dict:
    token = str(raw.get("token", "")).strip()
    source = str(raw.get("source", raw.get("url", ""))).strip()
    source_label = str(raw.get("source_label", raw.get("label", ""))).strip()
    title = str(raw.get("title", "")).strip() or source_label or source or "Untitled Stream"
    return {
        "token": token,
        "source": source,
        "source_label": source_label,
        "title": title,
    }


def _normalize_cards(raw_cards):
    if not isinstance(raw_cards, list):
        return []
    return [_normalize_card(c) for c in raw_cards]


@app.route("/api/config", methods=["POST"])
def api_config():
    """Save per-slot token/source config. Does not start streams."""
    data = request.get_json(silent=True) or {}
    with state_lock:
        if data.get("cards") is not None:
            cards = data.get("cards", [])
        else:
            token = str(data.get("token", "")).strip()
            cards = []
            for src in data.get("sources", []):
                cards.append(_normalize_card({"token": token, **src}))

        config["cards"] = _normalize_cards(cards)
        config["max_lives"] = len(config["cards"])
        if "tokenGroupRestartEnabled" in data:
            config["token_group_restart_enabled"] = bool(data["tokenGroupRestartEnabled"])
            global token_group_restart_enabled
            token_group_restart_enabled = config["token_group_restart_enabled"]
    _save_state()
    return jsonify({"ok": True})


@app.route("/api/start", methods=["POST"])
def api_start():
    """
    Start streaming. Assigns each slot its own token/source.
    If streams are already running, new sources are queued for next rotation.
    """
    global token_group_restart_enabled
    data = request.get_json(silent=True) or {}
    cards = data.get("cards")
    if cards is None:
        with state_lock:
            cards = config.get("cards", [])
    else:
        cards = _normalize_cards(cards)

    token_group_restart_enabled = data.get("tokenGroupRestartEnabled", token_group_restart_enabled)

    if not cards:
        return jsonify({"ok": False, "error": "No stream cards configured"}), 400

    # Validate tokens and page IDs
    page_ids = {}
    token_counts = {}
    for card in cards:
        if not card.get("token"):
            return jsonify({"ok": False, "error": "One or more slots have no token configured"}), 400
        if not card.get("source"):
            return jsonify({"ok": False, "error": "One or more slots have no source configured"}), 400
        token = card["token"]
        token_counts[token] = token_counts.get(token, 0) + 1

    for token, count in token_counts.items():
        if count > MAX_LIVE_PER_TOKEN:
            return jsonify({"ok": False, "error": f"Token limit exceeded: {count} slots use the same token (max {MAX_LIVE_PER_TOKEN})"}), 400

    try:
        for card in cards:
            token = card["token"]
            if token not in page_ids:
                page_ids[token] = fb_get_page_id(token)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    log(f"Tokens validated — page_ids={list(page_ids.values())}")

    with state_lock:
        if streams:
            # Already running: queue source changes for each slot
            for i, (label, worker) in enumerate(streams.items()):
                card = cards[i % len(cards)]
                worker.queue_source_change(card["source"], card["source_label"])
            log("Already running — source changes queued for next rotation.")
            return jsonify({"ok": True, "queued": True})

        # Fresh start: create workers simultaneously
        workers = {}
        for i, card in enumerate(cards):
            label = f"Live-{i+1}"
            w = StreamWorker(
                label,
                card["token"],
                page_ids[card["token"]],
                card["source"],
                card["source_label"],
            )
            workers[label] = w

        # Start all threads at once
        for w in workers.values():
            w.start()

        # Give Facebook a moment then wait for all to be past "starting"
        time.sleep(0.5)
        streams.update(workers)

    log(f"Started {len(cards)} live stream(s).")
    return jsonify({"ok": True, "queued": False})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    with state_lock:
        for w in streams.values():
            w.stop()
        streams.clear()
    log("All streams stopped.")
    return jsonify({"ok": True})


@app.route("/api/logs")
def api_logs():
    with log_lock:
        return jsonify({"logs": list(log_lines[-200:])})


@app.route("/api/validate_token", methods=["POST"])
def api_validate_token():
    token = request.json.get("token", "").strip()
    try:
        page_id = fb_get_page_id(token)
        return jsonify({"ok": True, "page_id": page_id})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


# ═════════════════════════════════════════════════════════════════════════════
# State persistence
# ═════════════════════════════════════════════════════════════════════════════

def _save_state():
    try:
        STATE_FILE.write_text(json.dumps(config, indent=2))
    except Exception:
        pass


def _load_state():
    global config
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text())
            if isinstance(saved, dict):
                if saved.get("cards") is not None:
                    config["cards"] = _normalize_cards(saved.get("cards", []))
                elif saved.get("token") is not None and saved.get("sources") is not None:
                    token = str(saved.get("token", "")).strip()
                    config["cards"] = _normalize_cards([
                        {"token": token, **src} for src in saved.get("sources", [])
                    ])
                else:
                    config.update(saved)
                config["max_lives"] = len(config.get("cards", []))
                if saved.get("token_group_restart_enabled") is not None:
                    config["token_group_restart_enabled"] = bool(saved["token_group_restart_enabled"])
                elif saved.get("tokenGroupRestartEnabled") is not None:
                    config["token_group_restart_enabled"] = bool(saved["tokenGroupRestartEnabled"])
                global token_group_restart_enabled
                token_group_restart_enabled = config.get("token_group_restart_enabled", False)
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# Entry point
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    _load_state()
    log("Matrix Nejma Live Manager starting…")
    log("UI → http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)