#!/usr/bin/env python3
# main.py
# CLI entry point for the Distributed MapReduce system.
#
# Usage:
#   python main.py master              -- start the master HTTP server
#   python main.py worker <id>         -- start a single worker
#   python main.py run                 -- start master + 3 workers + demo job
#   python main.py demo                -- submit a demo job (master must be up)
#   python main.py status              -- print system status

import sys
import time
import multiprocessing
import os

# ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))


def start_master():
    """Start the Flask master server (blocking)."""
    from communication.server import app
    import config
    print(f"[MAIN] Starting master on {config.MASTER_HOST}:{config.MASTER_PORT}")
    app.run(host=config.MASTER_HOST, port=config.MASTER_PORT, debug=False)


def start_worker(worker_id: str, port: int):
    """Start a single worker (blocking poll loop)."""
    import config
    # small stagger so workers don't all hit master simultaneously
    time.sleep(0.3 * int(worker_id.split("-")[-1]) if "-" in worker_id else 0)
    from core.worker import Worker
    w = Worker(worker_id=worker_id, host="127.0.0.1", port=port)
    w.start()


def run_demo():
    """Submit a demo DUMMY job and print results."""
    import config
    from communication.client import MasterClient

    client = MasterClient()

    # wait for master to be ready
    for attempt in range(10):
        try:
            client.health()
            break
        except Exception:
            print(f"[DEMO] Waiting for master… ({attempt+1}/10)")
            time.sleep(1)
    else:
        print("[DEMO] Master not reachable. Is it running?")
        return

    print("\n[DEMO] Submitting job with 6 DUMMY tasks…")
    job_resp = client.submit_job(
        name  = "phase1-demo",
        tasks = [
            {"task_type": "DUMMY", "payload": [1, 2, 3]},
            {"task_type": "DUMMY", "payload": [4, 5, 6]},
            {"task_type": "DUMMY", "payload": [7, 8, 9]},
            {"task_type": "DUMMY", "payload": 10},
            {"task_type": "DUMMY", "payload": [11, 12]},
            {"task_type": "DUMMY", "payload": [13, 14, 15]},
        ],
    )
    job_id = job_resp["job_id"]
    print(f"[DEMO] Job ID: {job_id}")

    # poll until done
    print("[DEMO] Waiting for completion", end="", flush=True)
    for _ in range(60):
        time.sleep(1)
        status = client.get_job_status(job_id)
        print(".", end="", flush=True)
        if status["status"] in ("COMPLETED", "FAILED"):
            break
    print()

    status = client.get_job_status(job_id)
    print(f"\n[DEMO] Job status : {status['status']}")
    print(f"[DEMO] Tasks done : {status['completed']}/{status['total']}")
    print(f"[DEMO] Results    :")
    for i, r in enumerate(status["results"], 1):
        print(f"       Task {i}: {r}")

    print(f"\n[DEMO] System stats:")
    stats = client.system_status()
    for k, v in stats.items():
        print(f"       {k:20s}: {v}")


def print_status():
    from communication.client import MasterClient
    client = MasterClient()
    try:
        stats   = client.system_status()
        workers = client.list_workers()
        print("\n── System Status ─────────────────────────────")
        for k, v in stats.items():
            print(f"  {k:22s}: {v}")
        print("\n── Workers ───────────────────────────────────")
        for w in workers:
            print(f"  {w['worker_id']:20s} status={w['status']:8s} "
                  f"done={w['tasks_done']}")
        if not workers:
            print("  (none registered)")
    except Exception as e:
        print(f"[STATUS] Could not reach master: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import config

    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"

    if cmd == "master":
        start_master()

    elif cmd == "worker":
        wid  = sys.argv[2] if len(sys.argv) > 2 else "worker-0"
        port = int(sys.argv[3]) if len(sys.argv) > 3 else config.WORKER_BASE_PORT
        start_worker(wid, port)

    elif cmd == "run":
        # Start master + workers + demo in separate processes
        print("[MAIN] Launching master + workers…")
        procs = []

        master_proc = multiprocessing.Process(
            target=start_master, daemon=False)
        master_proc.start()
        procs.append(master_proc)

        time.sleep(1.5)   # let master boot

        for i in range(config.NUM_WORKERS):
            p = multiprocessing.Process(
                target=start_worker,
                args=(f"worker-{i}", config.WORKER_BASE_PORT + i),
                daemon=True,
            )
            p.start()
            procs.append(p)
            print(f"[MAIN] Worker-{i} spawned (pid={p.pid})")

        time.sleep(1)
        run_demo()

        print("\n[MAIN] Demo finished. Press Ctrl-C to stop.")
        try:
            master_proc.join()
        except KeyboardInterrupt:
            print("\n[MAIN] Shutting down.")
            for p in procs:
                p.terminate()

    elif cmd == "demo":
        run_demo()

    elif cmd == "status":
        print_status()

    else:
        print("""
Distributed MapReduce — Phase 1

  python main.py master            Start the master HTTP server
  python main.py worker <id> [port]  Start a worker process
  python main.py run               Start everything + run demo job
  python main.py demo              Submit demo job (master must be up)
  python main.py status            Print system status
        """)
