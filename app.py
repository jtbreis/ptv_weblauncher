import json
import os
import re
import secrets
import shutil
import signal
import subprocess
import threading
import time
import uuid
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import yaml
from flask import Flask, render_template, request, jsonify, Response, stream_with_context, redirect, url_for, abort

from data_runs import apply_write_protect, build_base_overview, build_case_detail, default_config

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.config["TEMPLATES_AUTO_RELOAD"] = os.environ.get("FLASK_TEMPLATES_AUTO_RELOAD", "1") == "1"

CONFIG_PATH = os.environ.get("JOBS_CONFIG", "/config/jobs.yaml")
_repo_env = os.environ.get("REPO_ROOT")
if _repo_env:
    REPO_ROOT = _repo_env
else:
    _launcher_dir = os.path.dirname(os.path.abspath(__file__))
    _parent_repo = os.path.dirname(_launcher_dir)
    if os.path.isdir(os.path.join(_parent_repo, "processed_data")) or os.path.isdir(os.path.join(_parent_repo, "data")):
        REPO_ROOT = _parent_repo
    else:
        REPO_ROOT = "/repo"
# Writable path for persisted run history (survives restarts if mounted).
_default_hist = os.path.join(os.path.dirname(CONFIG_PATH) or "/tmp", "run_history.json")
RUN_HISTORY_PATH = os.environ.get("RUN_HISTORY_PATH", _default_hist)
# In-flight docker jobs (container name + metadata) so restarts can reattach instead of
# recording a false failed/-15 while STM keeps running on the host.
_default_active = os.path.join(os.path.dirname(CONFIG_PATH) or "/tmp", "active_runs.json")
ACTIVE_RUNS_PATH = os.environ.get("ACTIVE_RUNS_PATH", _default_active)

_PTV_CONTAINER_ID_RE = re.compile(r"^PTV_CONTAINER_ID=([0-9a-fA-F]+)\s*$")
_ACTIVE_RUNS_LOCK = threading.Lock()

# Job timestamps in Pacific (PST/PDT). Override with TZ=... in docker-compose if needed.
_DISPLAY_TZ = ZoneInfo(os.environ.get("TZ", "America/Los_Angeles"))


def _now_str() -> str:
    return datetime.now(_DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S")


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
        now = _now_str()
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
                "proc": None,
                "cancel_requested": False,
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


def _save_history_file(hist: list) -> None:
    """Persist history list (oldest first)."""
    parent = os.path.dirname(RUN_HISTORY_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    # Write in place: os.replace(tmp, path) fails with Errno 16 (EBUSY) when `path` is a Docker
    # bind-mounted single file (rename onto the mount target is not allowed).
    with open(RUN_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(hist, f, indent=2)
        f.flush()
        os.fsync(f.fileno())


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
        _save_history_file(hist)


def _delete_history_record(run_id: str) -> bool:
    """Remove a run from persisted history. Returns True if a record was removed."""
    with HISTORY_LOCK:
        hist = _load_history_file()
        new_hist = [r for r in hist if r.get("run_id") != run_id]
        if len(new_hist) == len(hist):
            return False
        _save_history_file(new_hist)
        return True


def _history_record_by_id(run_id: str) -> dict | None:
    for rec in reversed(_load_history_file()):
        if rec.get("run_id") == run_id:
            return rec
    return None


def _history_overview_fields(rec: dict) -> dict:
    """Add case / base path for history list views (from stored env)."""
    env = rec.get("env") or {}
    out = dict(rec)
    out["case"] = (env.get("CASE") or "").strip()
    out["base_path"] = (env.get("OUTPUT_BASE") or env.get("INPUT_BASE") or "").strip()
    return out


def _log_tail(lines: list[str]) -> str:
    text = "\n".join(lines[-800:])
    if len(text) > LOG_TAIL_MAX_CHARS:
        return text[-LOG_TAIL_MAX_CHARS:]
    return text


def _finalize_run(run_id: str, job_cfg: dict, cfg: dict, env_vars: dict, status: str, exit_code: int | None, duration: int | None) -> None:
    finished_at = _now_str()
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
    _remove_active_run(run_id)
    _append_history_record(record)
    send_ntfy(cfg, job_cfg["name"], status, exit_code if exit_code is not None else -1, duration or 0)


def _container_name_for_run(run_id: str) -> str:
    # Docker names: [a-zA-Z0-9][a-zA-Z0-9_.-]* — UUID hyphens are fine.
    return f"ptv-{run_id}"


def _load_active_runs() -> list:
    try:
        with open(ACTIVE_RUNS_PATH, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except (json.JSONDecodeError, OSError) as e:
        print(f"ptv_weblauncher: WARN could not load active runs {ACTIVE_RUNS_PATH}: {e}", flush=True)
        return []


def _save_active_runs(items: list) -> None:
    parent = os.path.dirname(ACTIVE_RUNS_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(ACTIVE_RUNS_PATH, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)
        f.flush()
        os.fsync(f.fileno())


def _upsert_active_run(record: dict) -> None:
    with _ACTIVE_RUNS_LOCK:
        items = [r for r in _load_active_runs() if r.get("run_id") != record.get("run_id")]
        items.append(record)
        _save_active_runs(items)


def _remove_active_run(run_id: str) -> None:
    with _ACTIVE_RUNS_LOCK:
        items = [r for r in _load_active_runs() if r.get("run_id") != run_id]
        _save_active_runs(items)


def _docker_run(docker_bin: str, args: list[str], timeout: float | None = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        [docker_bin, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _docker_container_state(docker_bin: str, name_or_id: str) -> dict | None:
    """Return {running: bool, exit_code: int|None, id: str} or None if missing."""
    fmt = "{{.Id}}|{{.State.Running}}|{{.State.ExitCode}}"
    try:
        cp = _docker_run(docker_bin, ["inspect", "-f", fmt, name_or_id], timeout=30)
    except (subprocess.TimeoutExpired, OSError):
        return None
    if cp.returncode != 0:
        return None
    parts = (cp.stdout or "").strip().split("|")
    if len(parts) != 3:
        return None
    running = parts[1].lower() == "true"
    try:
        exit_code = int(parts[2])
    except ValueError:
        exit_code = None
    return {"id": parts[0], "running": running, "exit_code": exit_code}


def _docker_rm(docker_bin: str, name_or_id: str) -> None:
    try:
        _docker_run(docker_bin, ["rm", "-f", name_or_id], timeout=60)
    except (subprocess.TimeoutExpired, OSError):
        pass


def _docker_stop(docker_bin: str, name_or_id: str) -> bool:
    try:
        cp = _docker_run(docker_bin, ["stop", "-t", "30", name_or_id], timeout=120)
        return cp.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _kill_process_group(proc: subprocess.Popen) -> None:
    """Terminate shell job and children (e.g. docker compose build)."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.terminate()
        except OSError:
            pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except OSError:
                pass
        proc.wait(timeout=5)


def stop_run(run_id: str) -> tuple[bool, str]:
    """Request stop for a pending or running job. Returns (ok, message)."""
    with jobs_lock:
        job = jobs.get(run_id)
        if not job:
            return False, "Run not found"
        if job.get("status") in ("done", "failed", "cancelled"):
            return False, "Run already finished"
        job["cancel_requested"] = True
        proc = job.get("proc")
        container = job.get("container_name") or job.get("container_id")
        docker_bin = job.get("docker_bin")

    if container and docker_bin:
        if _docker_stop(docker_bin, container):
            return True, "Stop signalled to Docker container"
        return True, "Stop requested (container stop may have already exited)"

    if proc is not None and proc.poll() is None:
        _kill_process_group(proc)
        return True, "Stop signalled"

    return True, "Stop requested"


def send_ntfy(cfg, job_name, status, exit_code, duration):
    try:
        ntfy_cfg = cfg.get("ntfy", {})
        topic = ntfy_cfg.get("topic", "")
        server = ntfy_cfg.get("server", "https://ntfy.sh")
        if not topic:
            return
        if status == "done":
            icon, label = "✅", "finished"
        elif status == "cancelled":
            icon, label = "⏹", "cancelled"
        else:
            icon, label = "❌", "FAILED"
        requests.post(
            f"{server}/{topic}",
            data=f"{icon} {job_name} {label} (exit {exit_code}) in {duration}s",
            headers={"Title": f"Job: {job_name}", "Priority": "default"},
            timeout=5
        )
    except Exception as e:
        print(f"ntfy error: {e}")


def _persist_active_snapshot(run_id: str) -> None:
    with jobs_lock:
        j = jobs.get(run_id)
        if not j or j.get("status") not in ("pending", "running"):
            return
        record = {
            "run_id": run_id,
            "job_id": j.get("job_id"),
            "job_name": j.get("job_name"),
            "status": j.get("status"),
            "started_at": j.get("started_at"),
            "env": dict(j.get("env") or {}),
            "from_queue": bool(j.get("from_queue")),
            "queue_id": j.get("queue_id"),
            "container_name": j.get("container_name"),
            "container_id": j.get("container_id"),
            "output_lines": list(j.get("output_lines") or [])[-400:],
        }
    _upsert_active_run(record)


def _follow_docker_logs_and_wait(
    run_id: str,
    docker_bin: str,
    container_ref: str,
    start: float,
    job_cfg: dict,
    cfg: dict,
    env_vars: dict,
    append_line,
) -> None:
    """Stream docker logs until the container exits, then finalize the run."""
    append_line(f"[docker] Following logs for {container_ref}")
    log_proc = None
    try:
        log_proc = subprocess.Popen(
            [docker_bin, "logs", "-f", "--timestamps", container_ref],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        with jobs_lock:
            jobs[run_id]["proc"] = log_proc
            jobs[run_id]["pid"] = log_proc.pid

        assert log_proc.stdout is not None
        for line in log_proc.stdout:
            append_line(line.rstrip())
            with jobs_lock:
                if jobs[run_id].get("cancel_requested"):
                    break
        log_proc.wait(timeout=30)
    except Exception as e:
        append_line(f"[docker] log follow ended: {e}")
    finally:
        if log_proc is not None and log_proc.poll() is None:
            try:
                log_proc.terminate()
                log_proc.wait(timeout=5)
            except Exception:
                try:
                    log_proc.kill()
                except Exception:
                    pass

    # Wait until container is not running (logs -f can race slightly).
    exit_code = None
    for _ in range(120):
        state = _docker_container_state(docker_bin, container_ref)
        if state is None:
            append_line("[docker] Container no longer present after log follow.")
            exit_code = -1
            break
        if not state["running"]:
            exit_code = state.get("exit_code")
            break
        with jobs_lock:
            if jobs[run_id].get("cancel_requested"):
                _docker_stop(docker_bin, container_ref)
        time.sleep(1)
    else:
        state = _docker_container_state(docker_bin, container_ref)
        exit_code = state.get("exit_code") if state else -1

    _docker_rm(docker_bin, container_ref)

    duration = int(time.time() - start)
    with jobs_lock:
        cancelled = jobs[run_id].get("cancel_requested")
        if cancelled:
            status = "cancelled"
            append_line("[cancelled] Run stopped by user.")
        else:
            status = "done" if exit_code == 0 else "failed"
        jobs[run_id]["status"] = status
        jobs[run_id]["exit_code"] = exit_code
        jobs[run_id]["duration"] = duration
        jobs[run_id]["proc"] = None

    _finalize_run(run_id, job_cfg, cfg, env_vars, status, exit_code, duration)


def _execute_local_job(run_id, job_cfg, env_vars, cfg, env, start, append_line):
    """Foreground subprocess for non-docker (or fallback) jobs."""
    script = job_cfg["script"]
    args = job_cfg.get("args", [])
    cmd = [script] + args
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=REPO_ROOT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        with jobs_lock:
            jobs[run_id]["pid"] = proc.pid
            jobs[run_id]["proc"] = proc
            cancel_now = jobs[run_id].get("cancel_requested")
        if cancel_now:
            _kill_process_group(proc)

        assert proc.stdout is not None
        for line in proc.stdout:
            append_line(line.rstrip())
            with jobs_lock:
                if jobs[run_id].get("cancel_requested"):
                    break

        with jobs_lock:
            cancelled = jobs[run_id].get("cancel_requested")
        if cancelled and proc.poll() is None:
            _kill_process_group(proc)
        proc.wait()
        exit_code = proc.returncode
        duration = int(time.time() - start)

        with jobs_lock:
            cancelled = jobs[run_id].get("cancel_requested")
            if cancelled:
                status = "cancelled"
                append_line("[cancelled] Run stopped by user.")
            else:
                status = "done" if exit_code == 0 else "failed"
            jobs[run_id]["status"] = status
            jobs[run_id]["exit_code"] = exit_code
            jobs[run_id]["duration"] = duration
            jobs[run_id]["proc"] = None

        _finalize_run(run_id, job_cfg, cfg, env_vars, status, exit_code, duration)

    except Exception as e:
        duration = int(time.time() - start)
        with jobs_lock:
            jobs[run_id]["status"] = "failed"
            jobs[run_id]["exit_code"] = -1
            jobs[run_id]["duration"] = duration
            jobs[run_id]["output_lines"].append(f"ERROR: {e}")
        _finalize_run(run_id, job_cfg, cfg, env_vars, "failed", -1, duration)


def _execute_docker_detached_job(run_id, job_cfg, env_vars, cfg, env, docker_bin, start, append_line):
    """Start compose job detached, then track the container until it exits.

    This keeps long STM/tracking containers alive if the weblauncher process is
    restarted (SIGTERM on the launcher no longer kills the pipeline container).
    """
    container_name = _container_name_for_run(run_id)
    env = dict(env)
    env["DETACHED"] = "1"
    env["PTV_RUN_ID"] = run_id
    env["PTV_CONTAINER_NAME"] = container_name

    with jobs_lock:
        jobs[run_id]["container_name"] = container_name
        jobs[run_id]["docker_bin"] = docker_bin

    # Remove a leftover container with the same name from a previous crash.
    existing = _docker_container_state(docker_bin, container_name)
    if existing is not None:
        append_line(f"[docker] Removing leftover container {container_name}")
        _docker_rm(docker_bin, container_name)

    script = job_cfg["script"]
    args = job_cfg.get("args", [])
    cmd = [script] + args
    container_id = None

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=REPO_ROOT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        with jobs_lock:
            jobs[run_id]["pid"] = proc.pid
            jobs[run_id]["proc"] = proc
            cancel_now = jobs[run_id].get("cancel_requested")
        if cancel_now:
            _kill_process_group(proc)

        assert proc.stdout is not None
        for line in proc.stdout:
            text = line.rstrip()
            append_line(text)
            m = _PTV_CONTAINER_ID_RE.match(text)
            if m:
                container_id = m.group(1)
                with jobs_lock:
                    jobs[run_id]["container_id"] = container_id
                _persist_active_snapshot(run_id)
            with jobs_lock:
                if jobs[run_id].get("cancel_requested"):
                    break

        with jobs_lock:
            cancelled = jobs[run_id].get("cancel_requested")
        if cancelled and proc.poll() is None:
            _kill_process_group(proc)
        proc.wait()
        launch_rc = proc.returncode
        with jobs_lock:
            jobs[run_id]["proc"] = None
    except Exception as e:
        duration = int(time.time() - start)
        with jobs_lock:
            jobs[run_id]["status"] = "failed"
            jobs[run_id]["exit_code"] = -1
            jobs[run_id]["duration"] = duration
            jobs[run_id]["output_lines"].append(f"ERROR: {e}")
        _finalize_run(run_id, job_cfg, cfg, env_vars, "failed", -1, duration)
        return

    with jobs_lock:
        cancelled = jobs[run_id].get("cancel_requested")
    if cancelled:
        ref = container_id or container_name
        state = _docker_container_state(docker_bin, ref)
        if state and state["running"]:
            _docker_stop(docker_bin, ref)
        _docker_rm(docker_bin, ref)
        duration = int(time.time() - start)
        with jobs_lock:
            jobs[run_id]["status"] = "cancelled"
            jobs[run_id]["exit_code"] = None
            jobs[run_id]["duration"] = duration
        append_line("[cancelled] Run stopped by user.")
        _finalize_run(run_id, job_cfg, cfg, env_vars, "cancelled", None, duration)
        return

    ref = container_id or container_name
    state = _docker_container_state(docker_bin, ref)
    if state is None:
        duration = int(time.time() - start)
        append_line(
            "[docker] ERROR: launch script finished but container was not found. "
            f"(launch exit {launch_rc}; expected name {container_name})"
        )
        with jobs_lock:
            jobs[run_id]["status"] = "failed"
            jobs[run_id]["exit_code"] = launch_rc if launch_rc else 1
            jobs[run_id]["duration"] = duration
        _finalize_run(run_id, job_cfg, cfg, env_vars, "failed", launch_rc if launch_rc else 1, duration)
        return

    if not container_id:
        container_id = state["id"]
        with jobs_lock:
            jobs[run_id]["container_id"] = container_id

    append_line(f"[docker] Pipeline container running as {container_name} ({container_id[:12]})")
    _persist_active_snapshot(run_id)

    if not state["running"]:
        exit_code = state.get("exit_code")
        _docker_rm(docker_bin, ref)
        duration = int(time.time() - start)
        status = "done" if exit_code == 0 else "failed"
        with jobs_lock:
            jobs[run_id]["status"] = status
            jobs[run_id]["exit_code"] = exit_code
            jobs[run_id]["duration"] = duration
        _finalize_run(run_id, job_cfg, cfg, env_vars, status, exit_code, duration)
        return

    _follow_docker_logs_and_wait(
        run_id, docker_bin, container_name, start, job_cfg, cfg, env_vars, append_line
    )


def execute_job_run(run_id, job_cfg, env_vars, cfg):
    """Run one job in the current thread (blocking until subprocess/container exits)."""
    with jobs_lock:
        if jobs[run_id].get("cancel_requested"):
            jobs[run_id]["status"] = "cancelled"
            jobs[run_id]["exit_code"] = None
            jobs[run_id]["duration"] = 0
        else:
            jobs[run_id]["status"] = "running"

    with jobs_lock:
        if jobs[run_id].get("status") == "cancelled":
            jobs[run_id]["output_lines"].append("[cancelled] Stopped before start.")
            _finalize_run(run_id, job_cfg, cfg, env_vars, "cancelled", None, 0)
            return

    start = time.time()
    env = _subprocess_env(env_vars)
    env["REPO_ROOT"] = REPO_ROOT
    env["PTV_WEBLAUNCHER"] = "1"

    def append_line(line):
        with jobs_lock:
            jobs[run_id]["output_lines"].append(line)

    docker_bin = _docker_cli_on_path(env)
    requires_docker = job_cfg.get("requires_docker", True)
    if requires_docker and not docker_bin:
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

    if docker_bin:
        env["DOCKER"] = os.path.realpath(docker_bin)

    # Detached docker tracking for compose-based pipeline jobs (default requires_docker).
    if requires_docker and docker_bin:
        _execute_docker_detached_job(
            run_id, job_cfg, env_vars, cfg, env, docker_bin, start, append_line
        )
        return

    _execute_local_job(run_id, job_cfg, env_vars, cfg, env, start, append_line)


def run_job_thread(run_id, job_cfg, env_vars, cfg):
    execute_job_run(run_id, job_cfg, env_vars, cfg)


def _reattach_active_runs() -> None:
    """On startup, resume monitoring for docker jobs that outlived the launcher."""
    items = _load_active_runs()
    if not items:
        return

    cfg = load_config()
    env_probe = _subprocess_env({})
    docker_bin = _docker_cli_on_path(env_probe)
    if not docker_bin:
        print(
            "ptv_weblauncher: active runs found but docker CLI missing; cannot reattach.",
            flush=True,
        )
        return

    for rec in items:
        run_id = rec.get("run_id")
        if not run_id:
            continue
        job_id = rec.get("job_id")
        job_cfg = next((j for j in cfg["jobs"] if j["id"] == job_id), None)
        if not job_cfg:
            print(f"ptv_weblauncher: drop active run {run_id}: unknown job_id {job_id}", flush=True)
            _remove_active_run(run_id)
            continue

        container_name = rec.get("container_name") or _container_name_for_run(run_id)
        container_id = rec.get("container_id")
        ref = container_id or container_name
        env_vars = dict(rec.get("env") or {})
        state = _docker_container_state(docker_bin, ref)

        with jobs_lock:
            if run_id in jobs and jobs[run_id].get("status") in ("pending", "running"):
                continue
            jobs[run_id] = {
                "job_id": job_id,
                "job_name": job_cfg["name"],
                "status": "running",
                "output_lines": list(rec.get("output_lines") or [])
                + [f"[docker] Reattached after weblauncher restart ({_now_str()})"],
                "exit_code": None,
                "duration": None,
                "started_at": rec.get("started_at") or _now_str(),
                "finished_at": None,
                "pid": None,
                "proc": None,
                "cancel_requested": False,
                "env": env_vars,
                "from_queue": bool(rec.get("from_queue")),
                "queue_id": rec.get("queue_id"),
                "container_name": container_name,
                "container_id": container_id,
                "docker_bin": docker_bin,
            }

        if state is None:
            with jobs_lock:
                jobs[run_id]["output_lines"].append(
                    "[docker] Container missing on reattach; marking failed."
                )
                jobs[run_id]["status"] = "failed"
                jobs[run_id]["exit_code"] = -1
                jobs[run_id]["duration"] = 0
            _finalize_run(run_id, job_cfg, cfg, env_vars, "failed", -1, 0)
            continue

        if not state["running"]:
            exit_code = state.get("exit_code")
            _docker_rm(docker_bin, ref)
            status = "done" if exit_code == 0 else "failed"
            with jobs_lock:
                jobs[run_id]["output_lines"].append(
                    f"[docker] Container already exited on reattach (exit {exit_code})."
                )
                jobs[run_id]["status"] = status
                jobs[run_id]["exit_code"] = exit_code
                jobs[run_id]["duration"] = 0
            _finalize_run(run_id, job_cfg, cfg, env_vars, status, exit_code, 0)
            continue

        print(f"ptv_weblauncher: reattaching to running container {container_name}", flush=True)

        def _started_epoch(started_at: str | None) -> float:
            if not started_at:
                return time.time()
            try:
                return datetime.strptime(started_at, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=_DISPLAY_TZ
                ).timestamp()
            except ValueError:
                return time.time()

        start = _started_epoch(rec.get("started_at"))

        def _monitor(rid=run_id, jcfg=job_cfg, ev=env_vars, cname=container_name, st=start):
            def append_line(line, _rid=rid):
                with jobs_lock:
                    if _rid in jobs:
                        jobs[_rid]["output_lines"].append(line)

            _follow_docker_logs_and_wait(
                rid, docker_bin, cname, st, jcfg, cfg, ev, append_line
            )

        t = threading.Thread(target=_monitor, name=f"reattach-{run_id[:8]}", daemon=True)
        t.start()


_reattach_started = threading.Event()


def _ensure_reattach() -> None:
    if _reattach_started.is_set():
        return
    _reattach_started.set()
    try:
        _reattach_active_runs()
    except Exception as e:
        print(f"ptv_weblauncher: reattach error: {e}", flush=True)


_ensure_reattach()
_ensure_queue_worker()


@app.route("/")
def index():
    return render_template("runs_overview.html", nav_active="data_runs")


@app.route("/launch")
def launch_page():
    cfg = load_config()
    return render_template("index.html", jobs=cfg["jobs"], nav_active="launch")


@app.route("/running")
def running_page():
    return render_template("running.html", nav_active="running")


@app.route("/history")
def history_page():
    return render_template("history.html", nav_active="history", run_history_path=RUN_HISTORY_PATH)


@app.route("/data-runs")
def data_runs_page():
    return redirect(url_for("index"))


@app.route("/queue")
def queue_page():
    return render_template("queue.html", nav_active="queue")


@app.route("/pdf-tracking")
def pdf_tracking_page():
    return render_template("pdf_tracking.html", nav_active="pdf_tracking")


@app.route("/error-sensitivity-tracking")
def error_sensitivity_tracking_page():
    cfg = load_config()
    jobs = {j["id"]: j for j in cfg["jobs"]}
    return render_template(
        "sensitivity_page.html",
        nav_active="error_sensitivity",
        page_title="STM Error Sensitivity",
        page_description=(
            "Sweep stereomatching error thresholds, run graph tracking for each ceiling, "
            "and compare tracks per 4-frame snapshot plus velocity/acceleration statistics."
        ),
        sweep_job=jobs["error_sensitivity_tracking"],
        analysis_job=jobs["error_sensitivity_analysis"],
        analysis_subdir="STM_error_sensitivity",
    )


@app.route("/box-size-sensitivity-tracking")
def box_size_sensitivity_tracking_page():
    cfg = load_config()
    jobs = {j["id"]: j for j in cfg["jobs"]}
    return render_template(
        "sensitivity_page.html",
        nav_active="box_size_sensitivity",
        page_title="Box Size Sensitivity",
        page_description=(
            "Sweep box_size_track from a small to larger hop limit, run graph tracking at "
            "each value, and compare tracks per 4-frame snapshot plus velocity/acceleration statistics."
        ),
        sweep_job=jobs["box_size_sensitivity_tracking"],
        analysis_job=jobs["box_size_sensitivity_analysis"],
        analysis_subdir="box_size_sensitivity",
    )


@app.route("/box-relaxed-tracking")
def box_relaxed_tracking_page():
    return render_template("box_relaxed_tracking.html", nav_active="box_relaxed")


@app.route("/runs/<run_id>")
def run_detail_page(run_id):
    cfg = load_config()
    with jobs_lock:
        live = jobs.get(run_id)
    hist = _history_record_by_id(run_id) if not live or live.get("status") in ("done", "failed", "cancelled") else None
    # In-flight: only in memory
    if live and live.get("status") not in ("done", "failed", "cancelled"):
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
                "queued_at": _now_str(),
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
            "started_at": _now_str(),
            "finished_at": None,
            "pid": None,
            "proc": None,
            "cancel_requested": False,
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
                    "container_name": v.get("container_name"),
                    "env": v.get("env") or {},
                    "from_queue": bool(v.get("from_queue")),
                    "queue_id": v.get("queue_id"),
                })
    return jsonify({"items": out})


@app.route("/api/history")
def api_history():
    items = [_history_overview_fields(r) for r in reversed(_load_history_file())]
    return jsonify({"items": items})


@app.route("/api/data/defaults")
def api_data_defaults():
    return jsonify(default_config(REPO_ROOT))


@app.after_request
def _no_cache_data_api(response):
    if request.path.startswith("/api/data") or request.path in ("/", "/data-runs"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        response.headers["Pragma"] = "no-cache"
    return response


@app.route("/api/data/overview")
def api_data_overview():
    cfg = default_config(REPO_ROOT)
    base = (request.args.get("base") or cfg.get("default_base") or "").strip()
    if not base:
        return jsonify({"error": "base is required"}), 400
    payload = build_base_overview(repo_root=REPO_ROOT, base=base)
    payload["generated_at"] = _now_str()
    return jsonify(payload)


@app.route("/api/data/cases/<path:case>")
def api_data_case_detail(case):
    cfg = default_config(REPO_ROOT)
    base = (request.args.get("base") or cfg.get("default_base") or "").strip()
    if not base:
        return jsonify({"error": "base is required"}), 400
    case = case.strip()
    if not case:
        return jsonify({"error": "case is required"}), 400
    payload = build_case_detail(repo_root=REPO_ROOT, case=case, base=base)
    payload["generated_at"] = _now_str()
    return jsonify(payload)


@app.route("/api/data/write-protect", methods=["POST"])
def api_data_write_protect():
    """Write-protect or unlock pipeline artifact files (chmod -w / chmod u+w)."""
    data = request.json or {}
    base = (data.get("base") or "").strip()
    case = (data.get("case") or "").strip()
    artifact = (data.get("artifact") or "").strip()
    run = (data.get("run") or "").strip() or None
    protect = bool(data.get("write_protect", True))

    if not base:
        cfg = default_config(REPO_ROOT)
        base = (cfg.get("default_base") or "").strip()
    if not base or not case or not artifact:
        return jsonify({"error": "base, case, and artifact are required"}), 400

    try:
        result = apply_write_protect(
            repo_root=REPO_ROOT,
            base=base,
            case=case,
            artifact=artifact,
            protect=protect,
            run=run,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    if not result.get("changed"):
        return jsonify(result), 404 if result.get("error") else 400
    if result.get("partial"):
        return jsonify(result), 207
    return jsonify(result)


@app.route("/api/history/<run_id>", methods=["DELETE"])
def api_history_delete(run_id):
    if _delete_history_record(run_id):
        return jsonify({"ok": True, "run_id": run_id})
    return jsonify({"error": "Run not found in history"}), 404


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


@app.route("/api/runs/<run_id>/stop", methods=["POST"])
def stop_run_api(run_id):
    ok, message = stop_run(run_id)
    if not ok:
        code = 404 if "not found" in message.lower() else 400
        return jsonify({"error": message}), code
    return jsonify({"ok": True, "message": message})


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
            if job["status"] in ("done", "failed", "cancelled"):
                st = job["status"]
                yield (
                    f"data: [FINISHED: {st} exit {job.get('exit_code', '?')} "
                    f"in {job.get('duration', '?')}s]\n\n"
                )
                break
            time.sleep(0.3)

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
