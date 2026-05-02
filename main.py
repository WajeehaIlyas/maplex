#!/usr/bin/env python3
# main.py  —  CLI entry point for the Distributed MapReduce system (Phase 2)
#
# Commands:
#   python main.py master                  start the master HTTP server
#   python main.py worker <id> [port]      start a single worker
#   python main.py run                     master + workers + Phase 1 dummy demo
#   python main.py wordcount <file>        run word-count MapReduce on a text file
#   python main.py logcount  <file>        run log-analysis MapReduce on a log file
#   python main.py demo                    submit Phase 1 dummy job
#   python main.py status                  print system status

import sys, time, os, multiprocessing
sys.path.insert(0, os.path.dirname(__file__))


# ── Process starters ──────────────────────────────────────────────────────────

def start_master():
    from communication.server import app
    import config
    print(f"[MAIN] Master on {config.MASTER_HOST}:{config.MASTER_PORT}")
    app.run(host=config.MASTER_HOST, port=config.MASTER_PORT, debug=False)


def start_worker(worker_id, port):
    import config
    idx = int(worker_id.split("-")[-1]) if worker_id[-1].isdigit() else 0
    time.sleep(0.3 * idx)
    from core.worker import Worker
    Worker(worker_id=worker_id, host="127.0.0.1", port=port).start()


def _wait_for_master(retries=10):
    from communication.client import MasterClient
    client = MasterClient()
    for i in range(retries):
        try:
            client.health()
            return client
        except Exception:
            print(f"[MAIN] Waiting for master… ({i+1}/{retries})")
            time.sleep(1)
    print("[MAIN] Master not reachable.")
    sys.exit(1)


def _spawn_workers(n=None):
    import config
    n = n or config.NUM_WORKERS
    procs = []
    for i in range(n):
        p = multiprocessing.Process(
            target=start_worker,
            args=(f"worker-{i}", config.WORKER_BASE_PORT + i),
            daemon=True,
        )
        p.start()
        procs.append(p)
        print(f"[MAIN] worker-{i} spawned (pid={p.pid})")
    time.sleep(1.2)
    return procs


# ── Phase 1 dummy demo ────────────────────────────────────────────────────────

def run_dummy_demo():
    client = _wait_for_master()
    print("\n[DEMO] Submitting 6 DUMMY tasks…")
    job = client.submit_job("phase1-demo", [
        {"task_type": "DUMMY", "payload": [1, 2, 3]},
        {"task_type": "DUMMY", "payload": [4, 5, 6]},
        {"task_type": "DUMMY", "payload": [7, 8, 9]},
        {"task_type": "DUMMY", "payload": 10},
        {"task_type": "DUMMY", "payload": [11, 12]},
        {"task_type": "DUMMY", "payload": [13, 14, 15]},
    ])
    job_id = job["job_id"]
    print(f"[DEMO] Job ID: {job_id}")
    print("[DEMO] Waiting", end="", flush=True)
    for _ in range(60):
        time.sleep(1); print(".", end="", flush=True)
        s = client.get_job_status(job_id)
        if s["status"] in ("COMPLETED", "FAILED"): break
    print()
    s = client.get_job_status(job_id)
    print(f"[DEMO] Status: {s['status']}  Tasks: {s['completed']}/{s['total']}")
    for i, r in enumerate(s["results"], 1):
        print(f"       Task {i}: {r}")


# ── Phase 2: Word Count ───────────────────────────────────────────────────────

def run_wordcount(filepath):
    from communication.client import MasterClient
    from core.pipeline    import Pipeline
    from jobs.word_count.mapper  import WordCountMapper
    from jobs.word_count.reducer import WordCountReducer
    from mapreduce.mapper import Mapper

    client = _wait_for_master()
    print(f"\n[WORDCOUNT] Reading {filepath}…")
    lines = Mapper.read_file_lines(filepath)
    print(f"[WORDCOUNT] {len(lines)} lines — running MapReduce pipeline…")

    pipeline = Pipeline(
        mapper_cls  = WordCountMapper,
        reducer_cls = WordCountReducer,
        client      = client,
    )
    results = pipeline.run(lines)

    print(f"\n[WORDCOUNT] Results — top 20 words by count:")
    print(f"  {'WORD':<20} COUNT")
    print(f"  {'-'*20} -----")
    for word, count in sorted(results.items(), key=lambda x: -x[1])[:20]:
        print(f"  {word:<20} {count}")

    outpath = os.path.join("data", "outputs", "wordcount_results.txt")
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w") as f:
        for word, count in sorted(results.items(), key=lambda x: -x[1]):
            f.write(f"{word}: {count}\n")
    print(f"\n[WORDCOUNT] Full results saved to {outpath}")


# ── Phase 2: Log Analysis ─────────────────────────────────────────────────────

def run_logcount(filepath):
    from communication.client import MasterClient
    from core.pipeline    import Pipeline
    from jobs.log_analysis.mapper  import LogMapper
    from jobs.log_analysis.reducer import LogReducer
    from mapreduce.mapper import Mapper

    client = _wait_for_master()
    print(f"\n[LOGCOUNT] Reading {filepath}…")
    lines = Mapper.read_file_lines(filepath)
    print(f"[LOGCOUNT] {len(lines)} lines — running MapReduce pipeline…")

    pipeline = Pipeline(
        mapper_cls  = LogMapper,
        reducer_cls = LogReducer,
        client      = client,
    )
    results = pipeline.run(lines)

    print(f"\n[LOGCOUNT] Log level counts:")
    print(f"  {'LEVEL':<12} COUNT")
    print(f"  {'-'*12} -----")
    for level, count in sorted(results.items(), key=lambda x: -x[1]):
        print(f"  {level:<12} {count}")

    outpath = os.path.join("data", "outputs", "logcount_results.txt")
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w") as f:
        for level, count in sorted(results.items(), key=lambda x: -x[1]):
            f.write(f"{level}: {count}\n")
    print(f"\n[LOGCOUNT] Results saved to {outpath}")


# ── Print status ──────────────────────────────────────────────────────────────

def print_status():
    from communication.client import MasterClient
    client = MasterClient()
    try:
        stats   = client.system_status()
        workers = client.list_workers()
        print("\n── System Status ─────────────────────────")
        for k, v in stats.items():
            print(f"  {k:22s}: {v}")
        print("\n── Workers ───────────────────────────────")
        for w in workers:
            print(f"  {w['worker_id']:20s} status={w['status']:8s} done={w['tasks_done']}")
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
        mp = multiprocessing.Process(target=start_master, daemon=False)
        mp.start()
        time.sleep(1.5)
        _spawn_workers()
        run_dummy_demo()
        print("\n[MAIN] Press Ctrl-C to stop.")
        try:
            mp.join()
        except KeyboardInterrupt:
            mp.terminate()

    elif cmd == "wordcount":
        if len(sys.argv) < 3:
            print("Usage: python main.py wordcount <filepath>")
            sys.exit(1)
        run_wordcount(sys.argv[2])

    elif cmd == "logcount":
        if len(sys.argv) < 3:
            print("Usage: python main.py logcount <filepath>")
            sys.exit(1)
        run_logcount(sys.argv[2])

    elif cmd == "demo":
        run_dummy_demo()

    elif cmd == "status":
        print_status()

    else:
        print("""
Distributed MapReduce — Phase 2

  python main.py master                   Start the master HTTP server
  python main.py worker <id> [port]       Start a worker process
  python main.py run                      Start everything + dummy demo

  python main.py wordcount <file>         Run word-count on a text file
  python main.py logcount  <file>         Run log-analysis on a log file

  python main.py demo                     Submit Phase 1 dummy job
  python main.py status                   Print system status
        """)
