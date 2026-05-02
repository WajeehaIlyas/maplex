# communication/server.py
# Flask HTTP server exposing Master functionality as REST endpoints.
# Phase 2 adds: POST /task/submit  (single standalone task)

from flask import Flask, request, jsonify
import logging, sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.master import Master
from core.task   import Task, TaskType
from core.job    import Job
import config

log = logging.getLogger("server")
app = Flask(__name__)
master = Master()

# ── Worker endpoints ──────────────────────────────────────────────────────────

@app.route("/worker/register", methods=["POST"])
def register_worker():
    d = request.get_json()
    return jsonify(master.register_worker(
        d["worker_id"], d.get("host","127.0.0.1"), d.get("port",0)))

@app.route("/worker/heartbeat", methods=["POST"])
def heartbeat():
    return jsonify(master.heartbeat(request.get_json()["worker_id"]))

@app.route("/worker/task", methods=["GET"])
def get_task():
    wid = request.args.get("worker_id")
    if not wid:
        return jsonify({"error": "worker_id required"}), 400
    return jsonify(master.get_next_task(wid))

@app.route("/worker/result", methods=["POST"])
def submit_result():
    d = request.get_json()
    return jsonify(master.submit_result(
        d["task_id"], d.get("result"), d.get("success", True), d.get("error")))

# ── Job endpoints ─────────────────────────────────────────────────────────────

@app.route("/job/submit", methods=["POST"])
def submit_job():
    d     = request.get_json()
    tasks = [Task(TaskType(t["task_type"]), t["payload"])
             for t in d.get("tasks", [])]
    job   = Job(name=d.get("name","unnamed"), input_data=None, tasks=tasks)
    j     = master.submit_job(job)
    return jsonify({"job_id": j.job_id, "status": j.status.value,
                    "total_tasks": j.total_tasks})

@app.route("/job/<job_id>", methods=["GET"])
def job_status(job_id):
    s = master.get_job_status(job_id)
    return jsonify(s) if s else (jsonify({"error":"not found"}), 404)

# ── Standalone task endpoint (used by Pipeline) ───────────────────────────────

@app.route("/task/submit", methods=["POST"])
def submit_task():
    """Submit a single MAP or REDUCE task outside of a Job object."""
    d    = request.get_json()
    task = Task(TaskType(d["task_type"]), d["payload"])
    master.submit_tasks([task])
    return jsonify({"task_id": task.task_id, "status": task.status.value})

@app.route("/task/<task_id>", methods=["GET"])
def task_status(task_id):
    s = master.get_task_status(task_id)
    return jsonify(s) if s else (jsonify({"error":"not found"}), 404)

# ── Monitoring endpoints ──────────────────────────────────────────────────────

@app.route("/status",  methods=["GET"])
def system_status(): return jsonify(master.get_stats())

@app.route("/workers", methods=["GET"])
def list_workers():  return jsonify(master.list_workers())

@app.route("/results", methods=["GET"])
def all_results():   return jsonify(master.get_all_results())

@app.route("/health",  methods=["GET"])
def health():        return jsonify({"status": "ok"})

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[SERVER] Master on {config.MASTER_HOST}:{config.MASTER_PORT}")
    app.run(host=config.MASTER_HOST, port=config.MASTER_PORT, debug=False)
