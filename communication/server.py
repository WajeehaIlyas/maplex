# communication/server.py
# Flask HTTP server that exposes the Master's functionality as REST endpoints.
# Run this process to start the master node.

from flask import Flask, request, jsonify
import logging
import sys
import os

# allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.master import Master
from core.task   import Task, TaskType
from core.job    import Job
import config

log = logging.getLogger("server")
app = Flask(__name__)

# Single shared Master instance
master = Master()


# ── Worker endpoints ──────────────────────────────────────────────────────────

@app.route("/worker/register", methods=["POST"])
def register_worker():
    data = request.get_json()
    result = master.register_worker(
        worker_id = data["worker_id"],
        host      = data.get("host", "127.0.0.1"),
        port      = data.get("port", 0),
    )
    return jsonify(result)


@app.route("/worker/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json()
    return jsonify(master.heartbeat(data["worker_id"]))


@app.route("/worker/task", methods=["GET"])
def get_task():
    worker_id = request.args.get("worker_id")
    if not worker_id:
        return jsonify({"error": "worker_id required"}), 400
    task = master.get_next_task(worker_id)
    return jsonify(task)          # None serialises as JSON null


@app.route("/worker/result", methods=["POST"])
def submit_result():
    data = request.get_json()
    result = master.submit_result(
        task_id = data["task_id"],
        result  = data.get("result"),
        success = data.get("success", True),
        error   = data.get("error"),
    )
    return jsonify(result)


# ── Client / job endpoints ────────────────────────────────────────────────────

@app.route("/job/submit", methods=["POST"])
def submit_job():
    """
    Expects JSON:
    {
      "name": "my-job",
      "tasks": [
        {"task_type": "DUMMY", "payload": [1, 2, 3]},
        …
      ]
    }
    """
    data = request.get_json()
    tasks = [
        Task(task_type=TaskType(t["task_type"]), payload=t["payload"])
        for t in data.get("tasks", [])
    ]
    job = Job(name=data.get("name", "unnamed"), input_data=None, tasks=tasks)
    submitted = master.submit_job(job)
    return jsonify({"job_id": submitted.job_id, "status": submitted.status.value,
                    "total_tasks": submitted.total_tasks})


@app.route("/job/<job_id>", methods=["GET"])
def job_status(job_id):
    status = master.get_job_status(job_id)
    if status is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(status)


@app.route("/task/<task_id>", methods=["GET"])
def task_status(task_id):
    status = master.get_task_status(task_id)
    if status is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(status)


# ── Monitoring endpoints ──────────────────────────────────────────────────────

@app.route("/status", methods=["GET"])
def system_status():
    return jsonify(master.get_stats())


@app.route("/workers", methods=["GET"])
def list_workers():
    return jsonify(master.list_workers())


@app.route("/results", methods=["GET"])
def all_results():
    return jsonify(master.get_all_results())


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[SERVER] Master starting on {config.MASTER_HOST}:{config.MASTER_PORT}")
    app.run(
        host  = config.MASTER_HOST,
        port  = config.MASTER_PORT,
        debug = False,
    )
