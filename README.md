# QueueCTL-CLI-based-Background-Job-Queue-System

**GitHub Repository:** `https://github.com/UdayKurla/QueueCTL-CLI-based-Background-Job-Queue-System`

**QueueCTL** is a minimal, production-grade, CLI-based background job queue system built in Python.

It manages background jobs with worker processes, handles failures with **exponential backoff**, ensures concurrency with persistent storage and atomic locking, and maintains reliability via a **Dead Letter Queue (DLQ)**.

  * **Tech Stack:** Python 3, Click (CLI), `sqlite3` (Persistence), `multiprocessing` (Concurrency).

-----

## 1\. Setup Instructions

### Prerequisites

  * Python 3.8+

### Installation Steps

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/UdayKurla/QueueCTL-CLI-based-Background-Job-Queue-System
    cd QueueCTL-CLI-based-Background-Job-Queue-System
    ```

2.  **Create and Activate Virtual Environment:**

    ```bash
    python -m venv .venv
    # Windows CMD:
    .\.venv\Scripts\activate
    # Linux/macOS:
    # source .venv/bin/activate
    ```

3.  **Install Dependencies:**

    ```bash
    # This installs the 'Click' library:
    pip install -r requirements.txt
    ```

4.  **Database:** The `queuectl.db` SQLite file will be automatically created upon the first execution of any command.

-----

## 2\. Usage Examples (Clean CLI Interface)

All operations are accessible through the hierarchical `python cli.py` interface.

### Enqueuing Jobs

| Command Example | Description |
| :--- | :--- |
| `python cli.py enqueue "{\"id\":\"job-A\", \"command\":\"ECHO hello\"}"` | Add a new job to the queue. |
| `python cli.py enqueue "{\"id\":\"job-B\", \"command\":\"false-cmd\", \"max_retries\":5}"` | Override default `max_retries`. |

### Worker Management

| Command Example | Description |
| :--- | :--- |
| `python cli.py worker start --count 3` | Start multiple worker processes. |
| `python cli.py worker stop` | Stop running workers gracefully. |

### Status and Listing

| Command Example | Description |
| :--- | :--- |
| `python cli.py status` | Show summary of all job states & active workers. |
| `python cli.py list --state pending` | List jobs filtered by state. |

### DLQ Management

| Command Example | Description |
| :--- | :--- |
| `python cli.py dlq list` | View jobs in the Dead Letter Queue. |
| `python cli.py dlq retry fail-job` | Move a job from DLQ back to pending. |

### Configuration

| Command Example | Description |
| :--- | :--- |
| `python cli.py config set retry_base 3` | Set the base for exponential backoff via CLI. |
| `python cli.py config list` | List all current system settings. |

-----

## 3\. Architecture Overview

The system design focuses on meeting the **minimal, production-grade** requirements.

### Data Persistence and Concurrency

  * **Persistence:** All job data persists in a single **`queuectl.db`** file using **SQLite**.
  * **Locking:** The `storage.py:pick_job_for_worker()` function ensures concurrent workers do not process the same job. This is achieved using an **atomic database transaction** (`SELECT` and `UPDATE` within a single commit) to lock the job and prevent race conditions.

### Reliability and Retries

  * **Job Execution:** Workers use Python's `subprocess` to execute commands, with **exit codes determining success or failure**.
  * **Exponential Backoff:** Failed jobs automatically retry. The delay is calculated using the formula:
    $$delay = base^{\text{attempts}}\text{ seconds}$$
    The base is retrieved from persistent configuration.
  * **DLQ:** Jobs transition to the **`dead`** state after exhausting the configurable `max_retries`.

-----

## 4\. Test Scenarios

1.  **Basic job completes successfully**.
2.  **Failed job retries with backoff and moves to DLQ**.
3.  **Multiple workers process jobs without overlap**.
4.  **Invalid commands fail gracefully**.
5.  **Job data survives restart**.

-----
## 5\. Demo Video

  * **Video Link:** https://drive.google.com/file/d/1r7L9hM6C47VK5TkfrXOCCUKVZACOznBG/view?usp=sharing

-----
