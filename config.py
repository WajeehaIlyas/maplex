# config.py
# Central configuration for the distributed MapReduce system.

# ── Master settings ────────────────────────────────────────────────────────────
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
MASTER_URL  = f"http://{MASTER_HOST}:{MASTER_PORT}"

# ── Worker settings ────────────────────────────────────────────────────────────
NUM_WORKERS       = 3          # how many worker processes to spawn
WORKER_BASE_PORT  = 5100       # workers listen on 5100, 5101, 5102, …
POLL_INTERVAL     = 0.5        # seconds between worker polling attempts
MAX_RETRIES       = 5          # how many times a worker retries a failed task

# ── Task settings ──────────────────────────────────────────────────────────────
CHUNK_SIZE        = 5          # lines per map-task chunk (used in Phase 2)
TASK_TIMEOUT      = 30         # seconds before master marks a task as timed-out

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR          = "data"
OUTPUT_DIR        = "data/outputs"
SAMPLE_LOGS_DIR   = "data/sample_logs"
SAMPLE_IMAGES_DIR = "data/sample_images"
