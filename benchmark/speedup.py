# benchmark/speedup.py  — rewritten with adaptive chunk sizing
import time, multiprocessing, os, logging
log = logging.getLogger("benchmark")
BAR_WIDTH = 36

class SpeedupBenchmark:
    def __init__(self, job, input_path, worker_counts=None):
        self.job           = job
        self.input_path    = os.path.abspath(input_path)
        self.worker_counts = worker_counts or [1,2,3,4]

    def run(self):
        results = []
        print(f"\n{'═'*60}\n  STRONG SCALING BENCHMARK\n  Job     : {self.job}")
        print(f"  Input   : {self.input_path}\n  Configs : {self.worker_counts} worker(s)\n{'═'*60}\n")
        for n in self.worker_counts:
            print(f"[BENCH] ── Running with {n} worker(s) ──")
            elapsed = self._run_one(n)
            results.append({"workers":n,"elapsed":elapsed})
            print(f"[BENCH]    Elapsed: {elapsed:.3f}s\n")
            time.sleep(1.2)
        baseline = results[0]["elapsed"]
        for r in results:
            r["speedup"]    = round(baseline/r["elapsed"],3)
            r["efficiency"] = round(r["speedup"]/r["workers"],3)
        return results

    def _run_one(self, n_workers):
        import config
        port = config.MASTER_PORT
        mp = multiprocessing.Process(target=_run_master, args=(port,), daemon=False)
        mp.start()
        self._wait_for_master(port)
        procs = []
        for i in range(n_workers):
            p = multiprocessing.Process(target=_run_worker,
                args=(f"bench-worker-{i}", config.WORKER_BASE_PORT+i), daemon=True)
            p.start(); procs.append(p)
            print(f"[BENCH]    Worker bench-worker-{i} started (pid={p.pid})")
        self._wait_for_workers(n_workers, port)
        chunk = self._chunk_size(n_workers)
        unit  = "images" if self.job=="colouranalysis" else "lines"
        print(f"[BENCH]    Chunk size: {chunk} {unit}/task")
        t0 = time.perf_counter()
        self._submit_job(port, chunk)
        elapsed = time.perf_counter() - t0
        for p in procs: p.terminate()
        mp.terminate(); mp.join(timeout=4)
        time.sleep(1.2)
        return elapsed

    def _chunk_size(self, n_workers):
        try:
            if self.job == "colouranalysis":
                from jobs.image_analysis.analysis_pipeline import discover_images
                total = max(1, len(discover_images(self.input_path)))
                return max(1, total//(n_workers*2))
            else:
                total = sum(1 for _ in open(self.input_path))
                return max(10, total//(n_workers*4))
        except: return 50

    def _wait_for_master(self, port, retries=25):
        import requests
        for _ in range(retries):
            try: requests.get(f"http://127.0.0.1:{port}/health",timeout=1); return
            except: time.sleep(0.4)
        raise RuntimeError("Master not reachable")

    def _wait_for_workers(self, n, port, retries=40):
        import requests
        for _ in range(retries):
            try:
                if len(requests.get(f"http://127.0.0.1:{port}/workers",timeout=2).json()) >= n: return
            except: pass
            time.sleep(0.4)
        raise RuntimeError(f"{n} workers did not register")

    def _submit_job(self, port, chunk_size):
        from communication.client import MasterClient
        from mapreduce.mapper import Mapper
        client = MasterClient(base_url=f"http://127.0.0.1:{port}")
        if self.job in ("wordcount","logcount"):
            try:    from mapreduce.pipeline import Pipeline
            except: from core.pipeline import Pipeline
            if self.job == "wordcount":
                from jobs.word_count.mapper  import WordCountMapper as M
                from jobs.word_count.reducer import WordCountReducer as R
            else:
                from jobs.log_analysis.mapper  import LogMapper as M
                from jobs.log_analysis.reducer import LogReducer as R
            lines = Mapper.read_file_lines(self.input_path)
            Pipeline(M, R, client=client, chunk_size=chunk_size).run(lines)
        elif self.job == "colouranalysis":
            from jobs.image_analysis.analysis_pipeline import ImageAnalysisPipeline, discover_images
            ImageAnalysisPipeline(client=client, chunk_size=chunk_size).run(discover_images(self.input_path))
        else: raise ValueError(f"Unknown job: {self.job!r}")

    @staticmethod
    def print_report(results):
        if not results: print("[BENCH] No results."); return
        baseline = results[0]["elapsed"]
        best     = max(results, key=lambda r: r["speedup"])
        print(f"\n{'═'*64}\n  BENCHMARK RESULTS — STRONG SCALING\n{'═'*64}")
        print(f"  {'Workers':<10} {'Time (s)':>10} {'Speedup':>10} {'Efficiency':>12}  Bar")
        print(f"  {'-'*60}")
        for r in results:
            n,t,sp,eff = r["workers"],r["elapsed"],r["speedup"],r["efficiency"]
            bar = "█" * max(2, int(sp/best["speedup"]*BAR_WIDTH))
            flag = "✓" if eff>=0.65 else ("~" if eff>=0.45 else "△")
            print(f"  {n:<10} {t:>10.3f} {sp:>10.2f}x {eff:>11.2f} {flag}  {bar}")
        print(f"{'─'*64}")
        print(f"\n  Baseline (1 worker) : {baseline:.3f}s")
        print(f"  Best ({best['workers']} workers)     : {best['elapsed']:.3f}s")
        print(f"  Max speedup         : {best['speedup']:.2f}x")
        print(f"  Parallel efficiency : {best['efficiency']:.0%}\n")
        eff = best["efficiency"]
        if   eff >= 0.65: print("  ✓ Good efficiency — system scales well. Parallelism confirmed.")
        elif eff >= 0.45: print("  ~ Moderate efficiency — parallelism confirmed. Try larger input.")
        else:             print("  △ Workload too small. Use larger input for cleaner speedup curve.")
        print(f"{'═'*64}\n")

    @staticmethod
    def save_report(results, output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path,"w") as f:
            f.write("STRONG SCALING BENCHMARK RESULTS\n"+"="*50+"\n\n")
            f.write(f"{'Workers':<10} {'Time(s)':>10} {'Speedup':>10} {'Efficiency':>12}\n"+"-"*44+"\n")
            for r in results:
                f.write(f"{r['workers']:<10} {r['elapsed']:>10.3f} {r['speedup']:>10.2f}x {r['efficiency']:>11.2f}\n")
        print(f"[BENCH] Results saved → {output_path}")


def _run_master(port):
    import config, logging
    config.MASTER_PORT = port
    config.MASTER_URL  = f"http://{config.MASTER_HOST}:{port}"
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    logging.getLogger("master").setLevel(logging.ERROR)
    from communication.server import app
    app.run(host=config.MASTER_HOST, port=port, debug=False)

def _run_worker(worker_id, port):
    import time, logging
    logging.getLogger("worker").setLevel(logging.ERROR)
    time.sleep(0.3)
    from core.worker import Worker
    Worker(worker_id=worker_id, host="127.0.0.1", port=port).start()
