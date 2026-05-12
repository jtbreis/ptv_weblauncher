import json
import os
import secrets
import shutil
import subprocess
import threading
import time
import uuid
from collections import deque
import requests
import yaml
from flask import Flask, render_template, request, jsonify, Response, stream_with_context, redirect, url_for, abort

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

CONFIG_PATH = os.environ.get("JOBS_CONFIG", "/config/jobs.yaml")
REPO_ROOT = os.environ.get("REPO_ROOT", "/repo")
# Writable path for persisted run history (survives restarts if mounted).
_default_hist = os.path.join(os.path.dirname(CONFIG_PATH) or "/tmp", "run_history.json")
RUN_HISTORY_PATH = os.environ.get("RUN_HISTORY_PATH", _default_hist)

HISTORY_LOCK = threading.Lock()
MAX_HISTORY_RECORDS = 2000
LOG_TAIL_MAX_CHARS = 48000
MAX_QUEUE_ITEMS = 200

# FIFO job queue — one worker runs queued jobs sequentially (after prior finishes).
_queue_deque: deque = deque()
_queue_lock = threading.Lock()
_queue_wakeup = threading.Condition(_queue_lock)
_queue_worker_started = threading.Event()


def _queue_worker_loop() -> None:
    while True:
        with _queue_wakeup:
            while len(_queue_deque) == 0:
                _queue_wakeup.wait()
            item = _queue_deque.popleft()

        cfg = load_config()
        job_cfg = next((j for j in cfg["jobs"] if j["id"] == item["job_id"]), None)
        if not job_cfg:
            print(f"ptv_weblauncher: queue skip unknown job_id {item.get('job_id')}", flush=True)
            continue

        run_id = str(uuid.uuid4())
        env_vars = dict(item.get("env") or {})
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with jobs_lock:
            jobs[run_id] = {
                "job_id": item["job_id"],
                "job_name": job_cfg["name"],
                "status": "pending",
                "output_lines": [f"[queue] Started from queue (queue_id={item['queue_id'][:8]}…)",],
                "exit_code": None,
                "duration": None,
                "started_at": now,
                "finished_at": None,
                "pid": None,
                "env": env_vars,
                "from_queue": True,
                "queue_id": item["queue_id"],
            }
        execute_job_run(run_id, job_cfg, env_vars, cfg)


def _ensure_queue_worker() -> None:
    if _queue_worker_started.is_set():
        return
    with _queue_lock:
        if _queue_worker_started.is_set():
            return
        t = threading.Thread(target=_queue_worker_loop, name="queue-worker", daemon=True)
        t.start()
        _queue_worker_started.set()


def _subprocess_env(extra: dict) -> dict:
    """Child env: job vars merged into the process env, then PATH is guaranteed to include system bins.

    Job `env` from the client is applied before PATH sanitization so a user/API-supplied PATH cannot
    strip /usr/bin (where docker.io installs the CLI); that mismatch caused `docker: command not found`
    in children even when the Flask process could resolve docker.
    """
    env = os.environ.copy()
    merged: dict[str, str] = {}
    for k, v in extra.items():
        if v is None or v == "":
            continue
        merged[k] = str(v)
    env.update(merged)
    path = env.get("PATH", "")
    parts = [x for x in path.split(":") if x]
    for prefix in ("/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"):
        if prefix not in parts:
            parts.insert(0, prefix)
    env["PATH"] = ":".join(parts)
    return env


def _docker_cli_on_path(env: dict) -> str | None:
    """Return path to docker executable, or None. Updates env PATH if found in a standard location."""
    path = env.get("PATH", "")
    found = shutil.which("docker", path=path)
    if found:
        return found
    for candidate in ("/usr/local/bin/docker", "/usr/bin/docker"):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            bindir = os.path.dirname(candidate)
            if bindir not in path.split(":"):
                env["PATH"] = bindir + ":" + path
            return candidate
    return None


# In-memory job store: run_id -> {status, output_lines, env, ...}
jobs = {}
jobs_lock = threading.Lock()


def _log_docker_cli_at_import() -> None:
    """One-line signal in container logs: same PATH logic as job children."""
    probe = _subprocess_env({})
    found = _docker_cli_on_path(probe)
    if found:
        print(f"ptv_weblauncher: docker CLI OK ({found})", flush=True)
    else:
        print(
            "ptv_weblauncher: WARNING: docker CLI not found. "
            "Rebuild the launcher image (Dockerfile installs docker-ce-cli + docker-compose-plugin).",
            flush=True,
        )


_log_docker_cli_at_import()


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _load_history_file() -> list:
    try:
        with open(RUN_HISTORY_PATH, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except (json.JSONDecodeError, OSError) as e:
        print(f"ptv_weblauncher: WARN could not load history {RUN_HISTORY_PATH}: {e}", flush=True)
        return []


def _append_history_record(record: dict) -> None:
    """Append one terminal run to JSON history (newest last; trim to MAX_HISTORY_RECORDS)."""
    record = dict(record)
    with HISTORY_LOCK:
        try:
            hist = _load_history_file()
        except Exception:
            hist = []
        hist.append(record)
        hist = hist[-MAX_HISTORY_RECORDS:]
        parent = os.path.dirname(RUN_HISTORY_PATH)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # Write in place: os.replace(tmp, path) fails with Errno 16 (EBUSY) when `path` is a Docker
        # bind-mounted single file (rename onto the mount target is not allowed).
        with open(RUN_HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(hist, f, indent=2)
            f.flush()
            os.fsync(f.fileno())


def _history_record_by_id(run_id: str) -> dict | None:
    for rec in reversed(_load_history_file()):
        if rec.get("run_id") == run_id:
            return rec
    return None


def _log_tail(lines: list[str]) -> str:
    text = "\n".join(lines[-800:])
    if len(text) > LOG_TAIL_MAX_CHARS:
        return text[-LOG_TAIL_MAX_CHARS:]
    return text


def _finalize_run(run_id: str, job_cfg: dict, cfg: dict, env_vars: dict, status: str, exit_code: int | None, duration: int | None) -> None:
    finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
    from_queue = False
    queue_trace_id = None
    with jobs_lock:
        j = jobs.get(run_id)
        lines = list(j.get("output_lines", [])) if j else []
        started_at = j.get("started_at", finished_at) if j else finished_at
        if j:
            from_queue = bool(j.get("from_queue"))
            queue_trace_id = j.get("queue_id")

    record = {
        "run_id": run_id,
        "job_id": job_cfg["id"],
        "job_name": job_cfg["name"],
        "status": status,
        "exit_code": exit_code,
        "duration": duration,
        "started_at": started_at,
        "finished_at": finished_at,
        "env": dict(env_vars),
        "script": job_cfg.get("script"),
        "args": job_cfg.get("args", []),
        "log_tail": _log_tail(lines),
        "from_queue": from_queue,
        "queue_id": queue_trace_id,
    }
    with jobs_lock:
        if run_id in jobs:
            jobs[run_id]["finished_at"] = finished_at
            jobs[run_id]["log_tail"] = record["log_tail"]
    _append_history_record(record)
    send_ntfy(cfg, job_cfg["name"], status, exit_code if exit_code is not None else -1, duration or 0)


def send_ntfy(cfg, job_name, status, exit_code, duration):
    try:
        ntfy_cfg = cfg.get("ntfy", {})
        topic = ntfy_cfg.get("topic", "")
        server = ntfy_cfg.get("server", "https://ntfy.sh")
        if not topic:
            return
        icon = "✅" if status == "done" else "❌"
        requests.post(
            f"{server}/{topic}",
            data=f"{icon} {job_name} {'finished' if status == 'done' else 'FAILED'} (exit {exit_code}) in {duration}s",
            headers={"Title": f"Job: {job_name}", "Priority": "default"},
            timeout=5
        )
    except Exception as e:
        print(f"ntfy error: {e}")


def execute_job_run(run_id, job_cfg, env_vars, cfg):
    """Run one job in the current thread (blocking until subprocess exits)."""
    with jobs_lock:
        jobs[run_id]["status"] = "running"

    start = time.time()
    script = job_cfg["script"]
    args = job_cfg.get("args", [])
    cmd = [script] + args

    env = _subprocess_env(env_vars)
    env["REPO_ROOT"] = REPO_ROOT

    def append_line(line):
        with jobs_lock:
            jobs[run_id]["output_lines"].append(line)

    docker_bin = _docker_cli_on_path(env)
    if not docker_bin:
        for line in (
            "ERROR: `docker` was not found (Docker CLI missing from PATH).",
            "Job scripts call `docker compose`; the machine running this Flask app needs the Docker client.",
            "Fix: run the launcher from ptv_weblauncher/docker-compose.yml (image installs docker-ce-cli and mounts /var/run/docker.sock),",
            "or install Docker on the host and ensure `docker` is on PATH before starting python app.py.",
        ):
            append_line(line)
        duration = int(time.time() - start)
        with jobs_lock:
            jobs[run_id]["status"] = "failed"
            jobs[run_id]["exit_code"] = 127
            jobs[run_id]["duration"] = duration
        _finalize_run(run_id, job_cfg, cfg, env_vars, "failed", 127, duration)
        return

    env["DOCKER"] = os.path.realpath(docker_bin)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=REPO_ROOT,
            text=True,
            bufsize=1
        )
        with jobs_lock:
            jobs[run_id]["pid"] = proc.pid

        for line in proc.stdout:
            append_line(line.rstrip())

        proc.wait()
        exit_code = proc.returncode
        duration = int(time.time() - start)
        status = "done" if exit_code == 0 else "failed"

        with jobs_lock:
            jobs[run_id]["status"] = status
            jobs[run_id]["exit_code"] = exit_code
            jobs[run_id]["duration"] = duration

        _finalize_run(run_id, job_cfg, cfg, env_vars, status, exit_code, duration)

    except Exception as e:
        duration = int(time.time() - start)
        with jobs_lock:
            jobs[run_id]["status"] = "failed"
            jobs[run_id]["exit_code"] = -1
            jobs[run_id]["duration"] = duration
            jobs[run_id]["output_lines"].append(f"ERROR: {e}")
        _finalize_run(run_id, job_cfg, cfg, env_vars, "failed", -1, duration)


def run_job_thread(run_id, job_cfg, env_vars, cfg):
    execute_job_run(run_id, job_cfg, env_vars, cfg)


@app.route("/")
def index():
    cfg = load_config()
    return render_template("index.html", jobs=cfg["jobs"], nav_active="launch")


@app.route("/running")
def running_page():
    return render_template("running.html", nav_active="running")


@app.route("/history")
def history_page():
    return render_template("history.html", nav_active="history", run_history_path=RUN_HISTORY_PATH)


@app.route("/queue")
def queue_page():
    return render_template("queue.html", nav_active="queue")


def _extra_env_keys(job_cfg: dict, env_used: dict) -> dict:
    names = {e["name"] for e in (job_cfg.get("env") or [])}
    return {k: v for k, v in env_used.items() if k not in names}


@app.route("/runs/<run_id>")
def run_detail_page(run_id):
    cfg = load_config()
    with jobs_lock:
        live = jobs.get(run_id)
    hist = _history_record_by_id(run_id) if not live or live.get("status") in ("done", "failed") else None
    # In-flight: only in memory
    if live and live.get("status") not in ("done", "failed"):
        job_cfg = next((j for j in cfg["jobs"] if j["id"] == live["job_id"]), None)
        if not job_cfg:
            abort(404)
        env_used = dict(live.get("env") or {})
        return render_template(
            "run_detail.html",
            nav_active="running",
            run_id=run_id,
            job_cfg=job_cfg,
            env_used=env_used,
            extra_env=_extra_env_keys(job_cfg, env_used),
            status=live.get("status"),
            exit_code=live.get("exit_code"),
            duration=live.get("duration"),
            started_at=live.get("started_at"),
            finished_at=live.get("finished_at"),
            log_tail="\n".join(live.get("output_lines") or []),
            is_live=True,
        )
    rec = hist or live
    if not rec:
        abort(404)
    job_id = rec.get("job_id")
    job_cfg = next((j for j in cfg["jobs"] if j["id"] == job_id), None)
    if not job_cfg:
        abort(404)
    log_tail = rec.get("log_tail") or ""
    if not log_tail and isinstance(rec.get("output_lines"), list):
        log_tail = "\n".join(rec["output_lines"])
    env_used = dict(rec.get("env") or {})
    return render_template(
        "run_detail.html",
        nav_active="history",
        run_id=run_id,
        job_cfg=job_cfg,
        env_used=env_used,
        extra_env=_extra_env_keys(job_cfg, env_used),
        status=rec.get("status"),
        exit_code=rec.get("exit_code"),
        duration=rec.get("duration"),
        started_at=rec.get("started_at"),
        finished_at=rec.get("finished_at"),
        log_tail=log_tail,
        is_live=False,
    )


@app.route("/api/launch", methods=["POST"])
def launch():
    data = request.json or {}
    job_id_req = data.get("job_id")
    env_vars = data.get("env", {})
    use_queue = bool(data.get("queue"))

    cfg = load_config()
    job_cfg = next((j for j in cfg["jobs"] if j["id"] == job_id_req), None)
    if not job_cfg:
        return jsonify({"error": "Job not found"}), 404

    if use_queue:
        with _queue_lock:
            waiting_before = len(_queue_deque)
            runner_busy = False
            with jobs_lock:
                for v in jobs.values():
                    if v.get("from_queue") and v.get("status") in ("pending", "running"):
                        runner_busy = True
                        break
            if waiting_before + (1 if runner_busy else 0) >= MAX_QUEUE_ITEMS:
                return jsonify({"error": f"Queue full (max {MAX_QUEUE_ITEMS} waiting or in-flight)"}), 400

            qid = str(uuid.uuid4())
            item = {
                "queue_id": qid,
                "job_id": job_id_req,
                "job_name": job_cfg["name"],
                "env": dict(env_vars),
                "queued_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            _queue_deque.append(item)
            waiting_line_position = len(_queue_deque)
            jobs_ahead = (1 if runner_busy else 0) + waiting_line_position - 1
            _queue_wakeup.notify()

        _ensure_queue_worker()
        return jsonify({
            "queued": True,
            "queue_id": qid,
            "waiting_line_position": waiting_line_position,
            "jobs_completed_before_yours_est": jobs_ahead,
            "job_id": job_id_req,
            "job_name": job_cfg["name"],
            "runner_busy": runner_busy,
        })

    run_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[run_id] = {
            "job_id": job_id_req,
            "job_name": job_cfg["name"],
            "status": "pending",
            "output_lines": [],
            "exit_code": None,
            "duration": None,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": None,
            "pid": None,
            "env": dict(env_vars),
            "from_queue": False,
        }

    t = threading.Thread(target=run_job_thread, args=(run_id, job_cfg, env_vars, cfg), daemon=True)
    t.start()

    return jsonify({"run_id": run_id, "queued": False})


@app.route("/api/queue", methods=["GET"])
def api_queue_list():
    with _queue_lock:
        waiting = list(_queue_deque)

    running = None
    with jobs_lock:
        for rid, v in jobs.items():
            if not v.get("from_queue"):
                continue
            if v.get("status") in ("pending", "running"):
                running = {
                    "run_id": rid,
                    "queue_id": v.get("queue_id"),
                    "job_id": v.get("job_id"),
                    "job_name": v["job_name"],
                    "status": v.get("status"),
                    "started_at": v.get("started_at"),
                    "pid": v.get("pid"),
                }
                break

    return jsonify({
        "waiting": waiting,
        "running": running,
        "waiting_count": len(waiting),
    })


@app.route("/api/queue/<queue_id>", methods=["DELETE"])
def api_queue_remove(queue_id):
    with _queue_lock:
        kept = [x for x in _queue_deque if x["queue_id"] != queue_id]
        if len(kept) == len(_queue_deque):
            return jsonify({"error": "Queue item not found or already running"}), 404
        _queue_deque.clear()
        _queue_deque.extend(kept)
    return jsonify({"ok": True})


@app.route("/api/runs")
def list_runs():
    with jobs_lock:
        result = {}
        for k, v in jobs.items():
            result[k] = {
                "job_id": v.get("job_id"),
                "job_name": v["job_name"],
                "status": v["status"],
                "exit_code": v.get("exit_code"),
                "duration": v.get("duration"),
                "started_at": v.get("started_at"),
                "finished_at": v.get("finished_at"),
                "env": v.get("env") or {},
            }
    return jsonify(result)


@app.route("/api/runs/active")
def list_active_runs():
    with jobs_lock:
        out = []
        for run_id, v in jobs.items():
            if v.get("status") in ("pending", "running"):
                out.append({
                    "run_id": run_id,
                    "job_id": v.get("job_id"),
                    "job_name": v["job_name"],
                    "status": v["status"],
                    "started_at": v.get("started_at"),
                    "pid": v.get("pid"),
                    "env": v.get("env") or {},
                    "from_queue": bool(v.get("from_queue")),
                    "queue_id": v.get("queue_id"),
                })
    return jsonify({"items": out})


@app.route("/api/history")
def api_history():
    items = list(reversed(_load_history_file()))
    return jsonify({"items": items})


@app.route("/api/runs/<run_id>")
def run_status(run_id):
    with jobs_lock:
        job = jobs.get(run_id)
    if not job:
        rec = _history_record_by_id(run_id)
        if not rec:
            return jsonify({"error": "Not found"}), 404
        return jsonify({
            "job_id": rec.get("job_id"),
            "job_name": rec.get("job_name"),
            "status": rec.get("status"),
            "exit_code": rec.get("exit_code"),
            "duration": rec.get("duration"),
            "started_at": rec.get("started_at"),
            "finished_at": rec.get("finished_at"),
            "pid": None,
            "env": rec.get("env") or {},
            "from_history": True,
        })
    return jsonify({
        "job_id": job.get("job_id"),
        "job_name": job["job_name"],
        "status": job["status"],
        "exit_code": job.get("exit_code"),
        "duration": job.get("duration"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "pid": job.get("pid"),
        "env": job.get("env") or {},
        "from_history": False,
    })


@app.route("/api/runs/<run_id>/logs")
def stream_logs(run_id):
    def generate():
        sent = 0
        while True:
            with jobs_lock:
                job = jobs.get(run_id)
            if not job:
                yield f"data: [run not found]\n\n"
                break
            lines = job["output_lines"]
            while sent < len(lines):
                yield f"data: {lines[sent]}\n\n"
                sent += 1
            if job["status"] in ("done", "failed"):
                yield f"data: [FINISHED: exit {job.get('exit_code', '?')} in {job.get('duration','?')}s]\n\n"
                break
            time.sleep(0.3)

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
