# Maplex — Distributed MapReduce Execution System

Maplex is a simplified distributed MapReduce system built in Python. It demonstrates
core distributed computing concepts — parallel execution, task scheduling, fault
tolerance, and data aggregation — using a lightweight master-worker architecture
that communicates over HTTP.

The system processes datasets in three job types: **text (word count)**, **log files
(level analysis)**, and **images (13 distributed transforms)**.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Running the System](#running-the-system)
- [Available Commands](#available-commands)
- [Image Transforms](#image-transforms)
- [Running Tests](#running-tests)
- [Project Phases](#project-phases)

---

## Features

- Master-worker distributed architecture over HTTP (Flask)
- Full MapReduce pipeline: Map → Shuffle → Reduce
- Word count on text files
- Log analysis supporting 5 log formats (standard, timestamped, Apache, syslog, bracketed)
- Distributed image processing with 13 transforms via Pillow
- Fault tolerance: task timeout detection and automatic re-queuing
- Heartbeat monitoring for worker health
- 220+ automated unit and integration tests

---

## Architecture

```
┌─────────────────────────────────────────┐
│              MASTER NODE                │
│  Task Queue │ Worker Registry │ Results │
│         Flask HTTP Server :5000         │
└──────────────┬──────────────────────────┘
               │  GET /worker/task
               │  POST /worker/result
       ┌───────┼───────┐
       ▼       ▼       ▼
  Worker-0  Worker-1  Worker-2
  (poll)    (poll)    (poll)
```

A single **master node** coordinates everything. **Worker nodes** are stateless —
they register, poll for tasks, execute them, and report back. The pipeline drives
the full Map → Shuffle → Reduce lifecycle through the master's REST API.

---

## Project Structure

```
maplex/
├── core/
│   ├── master.py          # Job scheduling, task queue, worker registry
│   ├── worker.py          # Task polling, execution, result reporting
│   ├── task.py            # Task dataclass and status enums
│   └── job.py             # Job dataclass with progress tracking
├── communication/
│   ├── server.py          # Flask REST server
│   ├── client.py          # HTTP client used by workers and pipeline
│   └── protocol.py        # Message schema helpers
├── mapreduce/
│   ├── mapper.py          # Base Mapper class with input splitting
│   ├── shuffler.py        # Groups intermediate key-value pairs by key
│   ├── reducer.py         # Base Reducer class with result collection
│   └── pipeline.py        # Orchestrates Map → Shuffle → Reduce
├── jobs/
│   ├── word_count/        # WordCountMapper, WordCountReducer
│   ├── log_analysis/      # LogMapper, LogReducer, log_parser
│   └── image_processing/  # ImageMapper, ImageReducer, transforms, image_pipeline
├── data/
│   ├── sample_logs/       # Sample .log and .txt input files
│   ├── sample_images/     # Sample images for testing
│   └── outputs/           # Results and processed images written here
├── tests/                 # All unit and integration tests
├── config.py              # Ports, timeouts, chunk sizes, paths
├── main.py                # CLI entry point
└── requirements.txt
```

---

## Installation

**Requirements:** Python 3.8+

```bash
# Clone or download the project
cd maplex

# Install dependencies
pip install flask requests Pillow

# Verify installation
python3 -c "import flask, requests, PIL; print('All dependencies OK')"
```

---

## Running the System

Maplex requires at least two terminal windows — one for the master and one or more
for workers. Open terminals in the `maplex/` directory.

### Terminal 1 — Start the master

```bash
python3 main.py master
```

You should see:
```
[MAIN] Master on 127.0.0.1:5000
* Running on http://127.0.0.1:5000
```

### Terminal 2 — Start worker 0

```bash
python3 main.py worker worker-0 5100
```

### Terminal 3 — Start worker 1 (optional but recommended)

```bash
python3 main.py worker worker-1 5101
```

### Terminal 4 — Submit a job

```bash
# Word count
python3 main.py wordcount data/sample_logs/text.txt

# Log analysis
python3 main.py logcount data/sample_logs/app.log

# Image processing
python3 main.py imageprocess data/sample_images grayscale
```

### One-command start (master + 3 workers + demo)

```bash
python3 main.py run
```

---

## Available Commands

| Command | Description |
|---|---|
| `python3 main.py master` | Start the master HTTP server |
| `python3 main.py worker <id> [port]` | Start a worker process |
| `python3 main.py run` | Start master + 3 workers + run dummy demo |
| `python3 main.py wordcount <file>` | Run word-count MapReduce on a text file |
| `python3 main.py logcount <file>` | Run log-analysis MapReduce on a log file |
| `python3 main.py imageprocess <dir\|file> <transform>` | Process images |
| `python3 main.py transforms` | List all available image transforms |
| `python3 main.py demo` | Submit a Phase 1 dummy job |
| `python3 main.py status` | Print system status and worker list |

### Image processing with parameters

```bash
# With default parameters
python3 main.py imageprocess data/sample_images edge_detect

# With custom parameters
python3 main.py imageprocess data/sample_images brightness factor=2.0
python3 main.py imageprocess data/sample_images thumbnail size=64
python3 main.py imageprocess data/sample_images rotate degrees=45
python3 main.py imageprocess data/sample_images blur radius=3.0

# Process a single image
python3 main.py imageprocess data/sample_images/photo.jpg sepia
```

---

## Image Transforms

| Transform | Description | Parameters |
|---|---|---|
| `grayscale` | Convert to grayscale | — |
| `brightness` | Adjust brightness | `factor=1.5` |
| `contrast` | Adjust contrast | `factor=1.5` |
| `blur` | Gaussian blur | `radius=2.0` |
| `sharpen` | Unsharp mask sharpen | — |
| `resize` | Resize to exact dimensions | `width=256 height=256` |
| `thumbnail` | Resize into box (preserves ratio) | `size=128` |
| `flip_horizontal` | Mirror left-right | — |
| `flip_vertical` | Flip top-bottom | — |
| `rotate` | Rotate by degrees | `degrees=90` |
| `edge_detect` | Highlight edges | — |
| `sepia` | Warm sepia tone | — |
| `invert` | Colour inversion (negative) | — |

Output images are saved to `data/outputs/images/` named as
`originalname__transform.jpg`. A summary report is written to
`data/outputs/images/report_<transform>.txt`.

---

## Running Tests

No server or workers needed — all tests use Flask's built-in test client and
mocked HTTP calls.

```bash
# Run all 220 tests
python3 -m unittest discover -s tests -p "test_*.py" -v

# Run by phase
python3 -m unittest tests.test_task_job tests.test_master tests.test_server tests.test_worker -v
python3 -m unittest tests.test_mapreduce_units tests.test_word_count tests.test_log_analysis -v
python3 -m unittest tests.test_transforms tests.test_image_mapper_reducer tests.test_image_worker -v

# Run a specific test file
python3 -m unittest tests.test_transforms -v
```

Expected output:
```
Ran 220 tests in 3.2s

OK
```

---

## Project Phases

### Phase 1 — Foundation
Builds the master-worker infrastructure, HTTP communication layer, task/job
dataclasses, and the worker poll loop. Proves parallel execution using a DUMMY
task type that squares numbers.

### Phase 2 — MapReduce Engine
Adds the Mapper, Shuffler, Reducer base classes and the Pipeline orchestrator.
Implements two concrete jobs: word count (emits `(word, 1)` pairs) and log analysis
(parses 5 log formats, emits `(level, 1)` pairs).

### Phase 3 — Image Processing
Extends the engine to handle binary file processing. Workers receive image file
paths, apply Pillow transforms, save outputs, and emit result paths. The reducer
builds a structured summary report. 13 transforms are available.

---

## Configuration

Edit `config.py` to tune system behaviour:

```python
MASTER_HOST    = "127.0.0.1"   # Master listen address
MASTER_PORT    = 5000           # Master listen port
NUM_WORKERS    = 3              # Workers spawned by 'run' command
CHUNK_SIZE     = 5              # Lines per MAP task chunk
TASK_TIMEOUT   = 30             # Seconds before task is re-queued
POLL_INTERVAL  = 0.5            # Worker poll frequency in seconds
MAX_RETRIES    = 5              # Worker retry attempts on connection error
```

---

## Output Files

| Command | Output location |
|---|---|
| `wordcount` | `data/outputs/wordcount_results.txt` |
| `logcount` | `data/outputs/logcount_results.txt` |
| `imageprocess` | `data/outputs/images/<name>__<transform>.jpg` |
| Image report | `data/outputs/images/report_<transform>.txt` |
