#!/usr/bin/env python3
"""
Matrix Nejma — Facebook Live Manager (fixed silent-kill detection)
─────────────────────────────────────────────────────────────────
Root cause of "stream looks alive in logs but DASH is dead":
  Facebook silently kills one live video when two lives share a token
  and one goes inactive. FFmpeg keeps pushing to a dead RTMPS endpoint
  and reports no error — so logs look clean but the broadcast is gone.

Fix:
  A health-check thread polls GET /{live_id}?fields=status every 20 s.
  If Facebook reports status != LIVE (e.g. VOD, PROCESSING, or error)
  while FFmpeg is still running, we kill FFmpeg and force a restart
  which re-creates the live video from scratch.

  Additionally: if two slots share the same token, they are started
  with a 3-second stagger so Facebook doesn't race-condition the
  simultaneous creation.
"""

import json
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import requests as req
from flask import Flask, jsonify, request, send_from_directory

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_LIVE_PER_TOKEN  = 3
MAX_STREAM_SECS     = 4 * 3600
CRASH_WINDOW        = 20
MAX_FAST_CRASHES    = 12
GRAPH_API           = "https://graph.facebook.com/v25.0"
STATE_FILE          = Path(__file__).parent / "state.json"
HEALTH_POLL_SECS    = 20   # how often to ask Facebook "is this live still alive?"
HEALTH_GRACE_SECS   = 60   # don't health-check until this many seconds after FFmpeg goes live
SAME_TOKEN_STAGGER  = 15   # seconds between live_video creations sharing the same token
                           # Facebook needs ~10-15s to open the RTMPS ingest slot server-side
INGEST_VERIFY_SECS  = 90   # max seconds to wait for ingest slot to become ready
INGEST_POLL_SECS    = 5    # poll interval during ingest verification
TLS_WAIT_SECS       = 30   # extra wait before retrying after TLS fatal alert

app = Flask(__name__)

state_lock = threading.Lock()
config  = {"cards": [], "max_lives": 0}
streams = {}   # label → StreamWorker
token_group_restart_enabled = False


# ═════════════════════════════════════════════════════════════════════════════
# Logging
# ═════════════════════════════════════════════════════════════════════════════

log_lines = []
log_lock  = threading.Lock()

def log(msg: str):
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with log_lock:
        log_lines.append(line)
        if len(log_lines) > 500:
            log_lines.pop(0)
    print(line)


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
        try:
            err = r.json().get("error", {})
        except Exception:
            err = {}
        detail = err.get("message") or r.text or str(exc)
        code   = err.get("code")
        sub    = err.get("error_subcode")
        msg    = detail
        if code:
            msg += f" (code={code}"
            if sub:
                msg += f", subcode={sub}"
            msg += ")"
        raise RuntimeError(msg) from exc
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", str(data["error"])))
    return data


def fb_end_live(token: str, live_id: str):
    try:
        req.post(f"{GRAPH_API}/{live_id}",
                 data={"access_token": token, "end_live_video": True},
                 timeout=15)
    except Exception:
        pass


def fb_get_live(token: str, live_id: str) -> dict:
    r = req.get(f"{GRAPH_API}/{live_id}", params={
        "access_token": token,
        "fields":       "id,dash_preview_url,ingest_streams,status",
    }, timeout=15)
    return r.json()


def fb_get_page_id(token: str) -> str:
    r = req.get(f"{GRAPH_API}/me",
                params={"access_token": token, "fields": "id"},
                timeout=10)
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", "Invalid token"))
    return data["id"]


def fb_verify_ingest(token: str, live_id: str, label: str) -> bool:
    """
    Poll GET /{live_id}?fields=ingest_streams until Facebook marks the ingest
    slot as ready (is_master=true and stream_health is populated), OR until
    INGEST_VERIFY_SECS elapses.

    Why: after create_live returns a secure_stream_url, Facebook still needs
    time to spin up the RTMPS ingest server behind that URL.  If FFmpeg tries
    to connect before it is ready, the TLS handshake fails immediately with
    "TLS fatal alert" because there is nothing listening yet.  Waiting here
    means FFmpeg always connects to an already-open socket.

    Returns True if ready, False if we timed out (caller should still try —
    worst case FFmpeg gets one TLS error then succeeds on retry).
    """
    deadline = time.time() + INGEST_VERIFY_SECS
    attempt  = 0
    while time.time() < deadline:
        attempt += 1
        try:
            r = req.get(f"{GRAPH_API}/{live_id}", params={
                "access_token": token,
                "fields":       "id,status,ingest_streams",
            }, timeout=10)
            data    = r.json()
            status  = data.get("status", "")
            streams = data.get("ingest_streams", [])

            # Any non-UNPUBLISHED status after creation = ready or gone
            if status == "LIVE":
                log(f"[{label}] Ingest ready (status=LIVE) after {attempt} polls")
                return True

            # Check ingest_streams array — at least one entry = slot is open
            if streams:
                log(f"[{label}] Ingest slot open ({len(streams)} stream(s)) after {attempt} polls")
                return True

            log(f"[{label}] Waiting for ingest slot… status={status} attempt={attempt}")
        except Exception as exc:
            log(f"[{label}] Ingest verify error: {exc}")

        for _ in range(INGEST_POLL_SECS):
            time.sleep(1)

    log(f"[{label}] Ingest verify timed out after {INGEST_VERIFY_SECS}s — proceeding anyway")
    return False


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
        src = ["-re",
               "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=30",
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

    return (["ffmpeg", "-hide_banner", "-loglevel", "warning", "-stats"]
            + src + codec + out)


# ═════════════════════════════════════════════════════════════════════════════
# StreamWorker
# ═════════════════════════════════════════════════════════════════════════════

class StreamWorker:
    def __init__(self, label: str, token: str, page_id: str,
                 source: str, source_label: str):
        self.label        = label
        self.token        = token
        self.page_id      = page_id
        self.source       = source
        self.source_label = source_label

        self._pending_source       = None
        self._pending_source_label = None
        self._pending_lock         = threading.Lock()

        self.live_id      = None
        self.rtmps_url    = None
        self.dash_url     = None
        self.status       = "idle"
        self.error        = None
        self.elapsed_secs = 0.0
        self.slot_start   = None
        self.rotation_n   = 0

        # Health-check signal: set this to kill the current FFmpeg proc
        # because Facebook killed the live video silently
        self._fb_killed   = threading.Event()
        self._current_proc = None
        self._proc_lock    = threading.Lock()

        self._stop   = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        with self._proc_lock:
            if self._current_proc and self._current_proc.poll() is None:
                try:
                    self._current_proc.terminate()
                except Exception:
                    pass

    def queue_source_change(self, source: str, label: str):
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
            "label":          self.label,
            "source_label":   self.source_label,
            "source":         self.source,
            "pending_source": pending,
            "live_id":        self.live_id,
            "rtmps_url":      self.rtmps_url,
            "dash_url":       self.dash_url,
            "status":         self.status,
            "error":          self.error,
            "elapsed_secs":   round(self.elapsed_secs, 1),
            "slot_remaining": round(max(0, MAX_STREAM_SECS - self.elapsed_secs), 1),
            "rotation_n":     self.rotation_n,
        }

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _run(self):
        while not self._stop.is_set():
            self._apply_pending()
            self._run_slot()
            if self._stop.is_set():
                break
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
        """One 4-hour slot: create live → stream → health-watch → rotate."""
        self.status = "starting"
        self._fb_killed.clear()

        # ── Create live video ─────────────────────────────────────────────────
        try:
            data = fb_create_live(
                self.token, self.page_id,
                f"Matrix Nejma — {self.label}",
                f"Stream {self.label} — Matrix Nejma",
            )
            self.live_id   = data["id"]
            self.rtmps_url = data.get("secure_stream_url", "")
            self.error     = None
            log(f"[{self.label}] Live created id={self.live_id}")
        except Exception as exc:
            self.status = "error"
            self.error  = str(exc)
            log(f"[{self.label}] Could not create live: {exc}")
            for _ in range(30):
                if self._stop.is_set():
                    return
                time.sleep(1)
            return

        # ── Wait for Facebook to open the RTMPS ingest slot ──────────────────
        # Without this, FFmpeg hits "TLS fatal alert" because the ingest server
        # is not ready yet. We poll ingest_streams until it appears.
        log(f"[{self.label}] Verifying ingest slot is open before starting FFmpeg…")
        fb_verify_ingest(self.token, self.live_id, self.label)

        # ── Start background threads ──────────────────────────────────────────
        dash_stop   = threading.Event()
        health_stop = threading.Event()
        threading.Thread(target=self._dash_poller,
                         args=(dash_stop,), daemon=True).start()
        threading.Thread(target=self._health_checker,
                         args=(health_stop,), daemon=True).start()

        # ── FFmpeg watchdog loop ──────────────────────────────────────────────
        self.slot_start   = time.time()
        self.elapsed_secs = 0.0
        fast_crashes      = 0

        while not self._stop.is_set():
            self.elapsed_secs = time.time() - self.slot_start

            # 4-hour rotation trigger
            if self.elapsed_secs >= MAX_STREAM_SECS:
                log(f"[{self.label}] 4h reached — rotating.")
                break

            # Facebook silently killed the live video
            if self._fb_killed.is_set():
                log(f"[{self.label}] Facebook killed the live — re-creating…")
                self._fb_killed.clear()
                # End dead live, create fresh one
                if self.live_id:
                    fb_end_live(self.token, self.live_id)
                health_stop.set()
                dash_stop.set()
                try:
                    data = fb_create_live(
                        self.token, self.page_id,
                        f"Matrix Nejma — {self.label}",
                        f"Stream {self.label} — Matrix Nejma",
                    )
                    self.live_id   = data["id"]
                    self.rtmps_url = data.get("secure_stream_url", "")
                    self.error     = None
                    fast_crashes   = 0
                    log(f"[{self.label}] New live id={self.live_id}")
                    fb_verify_ingest(self.token, self.live_id, self.label)
                    # Restart background threads with new live_id
                    dash_stop   = threading.Event()
                    health_stop = threading.Event()
                    self._fb_killed.clear()
                    threading.Thread(target=self._dash_poller,
                                     args=(dash_stop,), daemon=True).start()
                    threading.Thread(target=self._health_checker,
                                     args=(health_stop,), daemon=True).start()
                    continue
                except Exception as exc:
                    self.error  = str(exc)
                    self.status = "error"
                    log(f"[{self.label}] Re-create failed: {exc}")
                    time.sleep(10)
                    continue

            if fast_crashes >= MAX_FAST_CRASHES:
                self.status = "error"
                self.error  = f"{MAX_FAST_CRASHES} consecutive fast crashes"
                log(f"[{self.label}] Too many crashes — pausing slot.")
                # Don't exit slot loop — keep waiting so health-check can
                # detect when Facebook kills/restores and we can recover
                time.sleep(30)
                fast_crashes = 0
                continue

            # ── Launch FFmpeg ─────────────────────────────────────────────────
            cmd   = build_cmd(self.rtmps_url, self.source)
            start = time.time()
            self.status = "live"

            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
                with self._proc_lock:
                    self._current_proc = proc
            except FileNotFoundError:
                self.status = "error"
                self.error  = "ffmpeg not found"
                self._stop.set()
                break
            except Exception as exc:
                self.error = str(exc)
                time.sleep(5)
                continue

            # ── Wait for FFmpeg to exit ───────────────────────────────────────
            while proc.poll() is None:
                if self._stop.is_set():
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    break

                if self._fb_killed.is_set():
                    # Health check found live is dead — kill FFmpeg now
                    log(f"[{self.label}] Killing FFmpeg (live was silently killed by FB)")
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

            with self._proc_lock:
                self._current_proc = None

            # Collect stderr for diagnostics and TLS detection
            tls_error = False
            try:
                _, stderr_data = proc.communicate(timeout=2)
                if stderr_data:
                    decoded = stderr_data.decode("utf-8", errors="replace")
                    for line in decoded.split("\n")[:8]:
                        if line.strip():
                            log(f"[{self.label}] FFmpeg: {line.strip()}")
                    # TLS fatal alert = ingest slot not open yet on Facebook's side
                    if "TLS fatal alert" in decoded or "tls_alert" in decoded.lower():
                        tls_error = True
            except Exception:
                pass

            if self._stop.is_set():
                break

            # Skip crash counting if FB killed it (not a real crash)
            if self._fb_killed.is_set():
                continue

            lived = time.time() - start

            if tls_error and lived < CRASH_WINDOW:
                # TLS error: ingest not ready. Wait longer, then re-verify.
                # Don't count against fast_crash streak — this is FB latency.
                log(f"[{self.label}] TLS fatal alert — ingest slot not ready yet. "
                    f"Waiting {TLS_WAIT_SECS}s then re-verifying…")
                self.status = "starting"
                for _ in range(TLS_WAIT_SECS):
                    if self._stop.is_set() or self._fb_killed.is_set():
                        break
                    time.sleep(1)
                if not self._stop.is_set() and not self._fb_killed.is_set():
                    fb_verify_ingest(self.token, self.live_id, self.label)
                continue  # retry FFmpeg without incrementing fast_crashes

            elif lived < CRASH_WINDOW:
                fast_crashes += 1
                wait = min(3 * (2 ** (fast_crashes - 1)), 60)
                self.status = "restarting"
                log(f"[{self.label}] Fast exit {lived:.0f}s streak={fast_crashes} wait={wait:.0f}s")
                for _ in range(int(wait)):
                    if self._stop.is_set() or self._fb_killed.is_set():
                        break
                    time.sleep(1)
            else:
                fast_crashes = 0
                log(f"[{self.label}] Disconnect ({lived:.0f}s) — restarting FFmpeg…")

        dash_stop.set()
        health_stop.set()

    # ── Facebook health checker ───────────────────────────────────────────────

    def _health_checker(self, stop_ev: threading.Event):
        """
        Poll GET /{live_id}?fields=status every HEALTH_POLL_SECS.

        Facebook 'status' values:
          LIVE          — actively receiving data, all good
          UNPUBLISHED   — created but not yet receiving data (normal at start)
          LIVE_STOPPED  — ended normally
          VOD           — converted to VOD (live ended)
          PROCESSING    — being processed (rare)

        If we see anything other than LIVE or UNPUBLISHED after the grace
        period while FFmpeg thinks it is running, set _fb_killed so the
        watchdog kills and re-creates.
        """
        # Wait for grace period before first check
        for _ in range(HEALTH_GRACE_SECS):
            if stop_ev.is_set() or self._stop.is_set():
                return
            time.sleep(1)

        log(f"[{self.label}] Health checker active (poll every {HEALTH_POLL_SECS}s)")

        while not stop_ev.is_set() and not self._stop.is_set():
            live_id = self.live_id
            if not live_id or self.status not in ("live", "restarting"):
                time.sleep(HEALTH_POLL_SECS)
                continue

            try:
                data   = fb_get_live(self.token, live_id)
                fb_status = data.get("status", "")

                if "error" in data:
                    err_code = data["error"].get("code", 0)
                    err_msg  = data["error"].get("message", "")
                    # Code 100 = object doesn't exist = live was deleted
                    if err_code in (100, 803):
                        log(f"[{self.label}] Health: live {live_id} no longer exists "
                            f"(code={err_code}) — triggering re-create")
                        self._fb_killed.set()
                    else:
                        log(f"[{self.label}] Health: API error {err_msg}")
                elif fb_status in ("LIVE_STOPPED", "VOD", "PROCESSING"):
                    log(f"[{self.label}] Health: Facebook status={fb_status} "
                        f"but FFmpeg is running — SILENT KILL detected, re-creating")
                    self._fb_killed.set()
                elif fb_status == "LIVE":
                    # All good — also update DASH URL opportunistically
                    dash = data.get("dash_preview_url")
                    if dash and dash != self.dash_url:
                        self.dash_url = dash
                        log(f"[{self.label}] DASH updated: {dash}")
                else:
                    # UNPUBLISHED or unknown — log but don't panic
                    log(f"[{self.label}] Health: status={fb_status} (ok, not live yet)")

            except Exception as exc:
                log(f"[{self.label}] Health check error: {exc}")

            for _ in range(HEALTH_POLL_SECS):
                if stop_ev.is_set() or self._stop.is_set():
                    return
                time.sleep(1)

    # ── DASH preview poller ───────────────────────────────────────────────────

    def _dash_poller(self, stop_ev: threading.Event):
        last = None
        while not stop_ev.is_set() and not self._stop.is_set():
            if not self.live_id or self.status not in ("starting", "live", "rotating"):
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
    token        = str(raw.get("token", "")).strip()
    source       = str(raw.get("source", raw.get("url", ""))).strip()
    source_label = str(raw.get("source_label", raw.get("label", ""))).strip()
    title        = str(raw.get("title", "")).strip() or source_label or "Untitled"
    return {"token": token, "source": source,
            "source_label": source_label, "title": title}


def _normalize_cards(raw):
    if not isinstance(raw, list):
        return []
    return [_normalize_card(c) for c in raw]


@app.route("/api/config", methods=["POST"])
def api_config():
    data = request.get_json(silent=True) or {}
    with state_lock:
        cards = data.get("cards")
        if cards is not None:
            config["cards"] = _normalize_cards(cards)
        else:
            token = str(data.get("token", "")).strip()
            config["cards"] = _normalize_cards(
                [{"token": token, **s} for s in data.get("sources", [])])
        config["max_lives"] = len(config["cards"])
        if "tokenGroupRestartEnabled" in data:
            config["token_group_restart_enabled"] = bool(data["tokenGroupRestartEnabled"])
            global token_group_restart_enabled
            token_group_restart_enabled = config["token_group_restart_enabled"]
    _save_state()
    return jsonify({"ok": True})


@app.route("/api/start", methods=["POST"])
def api_start():
    global token_group_restart_enabled
    data  = request.get_json(silent=True) or {}
    cards = _normalize_cards(data.get("cards") or config.get("cards", []))

    if not cards:
        return jsonify({"ok": False, "error": "No stream cards configured"}), 400

    token_group_restart_enabled = data.get("tokenGroupRestartEnabled",
                                            token_group_restart_enabled)

    # Validate all tokens first
    page_ids     = {}
    token_counts = {}
    for card in cards:
        if not card["token"] or not card["source"]:
            return jsonify({"ok": False,
                            "error": "One or more slots missing token or source"}), 400
        token_counts[card["token"]] = token_counts.get(card["token"], 0) + 1

    for t, cnt in token_counts.items():
        if cnt > MAX_LIVE_PER_TOKEN:
            return jsonify({"ok": False,
                            "error": f"Token used in {cnt} slots (max {MAX_LIVE_PER_TOKEN})"}), 400

    try:
        for card in cards:
            t = card["token"]
            if t not in page_ids:
                page_ids[t] = fb_get_page_id(t)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    log(f"Tokens validated — {len(page_ids)} unique token(s), {len(cards)} slot(s)")

    with state_lock:
        if streams:
            for i, (label, worker) in enumerate(streams.items()):
                card = cards[i % len(cards)]
                worker.queue_source_change(card["source"], card["source_label"])
            log("Already running — source changes queued for next rotation.")
            return jsonify({"ok": True, "queued": True})

        # Build workers
        workers = {}
        for i, card in enumerate(cards):
            label = f"Live-{i+1}"
            workers[label] = StreamWorker(
                label, card["token"], page_ids[card["token"]],
                card["source"], card["source_label"],
            )

        # ── Stagger start for slots sharing the same token ────────────────────
        # Facebook rejects simultaneous live_video creation from the same token.
        # We group by token and insert SAME_TOKEN_STAGGER seconds between
        # workers that share a token, while still starting different-token
        # workers in parallel.
        token_last_start = {}  # token → last start time

        def start_worker(w: StreamWorker):
            t = w.token
            now = time.time()
            last = token_last_start.get(t, 0)
            wait = max(0, SAME_TOKEN_STAGGER - (now - last))
            if wait > 0:
                log(f"[{w.label}] Staggering {wait:.1f}s (same token as previous slot)")
                time.sleep(wait)
            token_last_start[t] = time.time()
            w.start()

        start_threads = []
        for w in workers.values():
            t = threading.Thread(target=start_worker, args=(w,), daemon=True)
            start_threads.append(t)
            t.start()

        # Wait for all starters to finish (max ~15s for 3 same-token slots)
        for t in start_threads:
            t.join(timeout=20)

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
    token = (request.json or {}).get("token", "").strip()
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
    global config, token_group_restart_enabled
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text())
            if isinstance(saved, dict):
                if saved.get("cards") is not None:
                    config["cards"] = _normalize_cards(saved["cards"])
                elif saved.get("token") and saved.get("sources"):
                    token = str(saved["token"]).strip()
                    config["cards"] = _normalize_cards(
                        [{"token": token, **s} for s in saved["sources"]])
                config["max_lives"] = len(config.get("cards", []))
                tgr = (saved.get("token_group_restart_enabled")
                       or saved.get("tokenGroupRestartEnabled"))
                if tgr is not None:
                    config["token_group_restart_enabled"] = bool(tgr)
                    token_group_restart_enabled = bool(tgr)
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    _load_state()
    log("Matrix Nejma Live Manager starting…")
    log(f"Health checker: polls every {HEALTH_POLL_SECS}s, grace period {HEALTH_GRACE_SECS}s")
    log("UI → http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)