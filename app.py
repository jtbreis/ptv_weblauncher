import os
import secrets
import subprocess
import threading
import time
import uuid
import requests
import yaml
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

CONFIG_PATH = os.environ.get("JOBS_CONFIG", "/config/jobs.yaml")
REPO_ROOT = os.environ.get("REPO_ROOT", "/repo")


def _subprocess_env(extra: dict) -> dict:
    """Child env: host PATH + job vars. Ensures common bin dirs (e.g. /usr/bin/docker) are searched."""
    env = os.environ.copy()
    path = env.get("PATH", "")
    parts = [x for x in path.split(":") if x]
    for prefix in ("/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"):
        if prefix not in parts:
            parts.insert(0, prefix)
    env["PATH"] = ":".join(parts)
    env.update({k: v for k, v in extra.items() if v})
    return env


# In-memory job store: job_id -> {status, output_lines, proc, ...}
jobs = {}
jobs_lock = threading.Lock()

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

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

def run_job_thread(job_id, job_cfg, env_vars, cfg):
    with jobs_lock:
        jobs[job_id]["status"] = "running"

    start = time.time()
    script = job_cfg["script"]
    args = job_cfg.get("args", [])
    cmd = [script] + args

    env = _subprocess_env(env_vars)
    env["REPO_ROOT"] = REPO_ROOT

    def append_line(line):
        with jobs_lock:
            jobs[job_id]["output_lines"].append(line)

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
            jobs[job_id]["pid"] = proc.pid

        for line in proc.stdout:
            append_line(line.rstrip())

        proc.wait()
        exit_code = proc.returncode
        duration = int(time.time() - start)
        status = "done" if exit_code == 0 else "failed"

        with jobs_lock:
            jobs[job_id]["status"] = status
            jobs[job_id]["exit_code"] = exit_code
            jobs[job_id]["duration"] = duration

        send_ntfy(cfg, job_cfg["name"], status, exit_code, duration)

    except Exception as e:
        with jobs_lock:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["output_lines"].append(f"ERROR: {e}")
        send_ntfy(cfg, job_cfg["name"], "failed", -1, int(time.time() - start))


@app.route("/")
def index():
    cfg = load_config()
    return render_template("index.html", jobs=cfg["jobs"])

@app.route("/api/launch", methods=["POST"])
def launch():
    data = request.json
    job_id_req = data.get("job_id")
    env_vars = data.get("env", {})

    cfg = load_config()
    job_cfg = next((j for j in cfg["jobs"] if j["id"] == job_id_req), None)
    if not job_cfg:
        return jsonify({"error": "Job not found"}), 404

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
            "pid": None
        }

    t = threading.Thread(target=run_job_thread, args=(run_id, job_cfg, env_vars, cfg), daemon=True)
    t.start()

    return jsonify({"run_id": run_id})

@app.route("/api/runs")
def list_runs():
    with jobs_lock:
        result = {k: {x: v[x] for x in ["job_name","status","exit_code","duration","started_at"]} for k, v in jobs.items()}
    return jsonify(result)

@app.route("/api/runs/<run_id>")
def run_status(run_id):
    with jobs_lock:
        job = jobs.get(run_id)
    if not job:
        return jsonify({"error": "Not found"}), 404
    return jsonify({k: job[k] for k in ["job_name","status","exit_code","duration","started_at","pid"]})

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
