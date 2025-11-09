# storage.py

import sqlite3
from job import Job
from datetime import datetime, timezone

# --- Configuration ---
DB_NAME = 'queuectl.db'
SQLITE_TIMEOUT = 5.0 # seconds. Prevents workers from hanging indefinitely on lock contention.

# --- Database Setup ---

def get_db_connection():
    """Returns a connection object to the database with a timeout."""
    conn = sqlite3.connect(DB_NAME, timeout=SQLITE_TIMEOUT) 
    conn.row_factory = sqlite3.Row  
    return conn

def init_db():
    """Initializes the database table for jobs upon module import."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,         
            attempts INTEGER NOT NULL,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            retry_after_time TEXT         
        );
    ''')
    conn.commit()
    conn.close()

init_db() 

# --- Helper Functions (BUG FIX Applied Here) ---

def row_to_job(row) -> Job | None:
    """
    Converts a sqlite3.Row object to a Job object.
    
    FIX: Prevents TypeError by removing 'id' and 'command' from **kwargs 
    before passing to Job.__init__ as they are already positional arguments.
    """
    if row is None:
        return None
    
    # Convert row to dict
    data = dict(row)
    
    # Extract positional arguments using .pop() to remove them from data
    job_id = data.pop('id')
    command = data.pop('command')
    
    # Pass the rest of the data via **kwargs
    return Job(job_id, command, **data)

# --- Core Persistence Functions ---

def enqueue_job(job: Job) -> tuple[bool, str]:
    """Adds a new job to the queue."""
    conn = get_db_connection()
    cursor = conn.cursor()
    data = job.to_dict()

    try:
        cursor.execute('''
            INSERT OR IGNORE INTO jobs 
            (id, command, state, attempts, max_retries, created_at, updated_at, retry_after_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['id'], data['command'], data['state'], data['attempts'], 
            data['max_retries'], data['created_at'], data['updated_at'], data['retry_after_time']
        ))
        conn.commit()
        
        if cursor.rowcount == 0:
            return False, f"Job with ID {job.id} already exists."
        return True, f"Job {job.id} added to queue in '{job.state}' state."
        
    except Exception as e:
        conn.rollback()
        return False, f"Database error during enqueue: {e}"
    finally:
        conn.close()

def pick_job_for_worker() -> Job | None:
    """
    Atomically find a pending job and mark it as processing (Locking required).
    Uses a standard transaction which is reliable with the SQLite timeout.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    job = None

    try:
        # 1. Select the oldest pending job ready for processing
        now_utc = datetime.now(timezone.utc).isoformat()
        
        cursor.execute('''
            SELECT * FROM jobs 
            WHERE state = 'pending' 
            AND (retry_after_time IS NULL OR retry_after_time <= ?)
            ORDER BY created_at ASC 
            LIMIT 1
        ''', (now_utc,))
        
        row = cursor.fetchone()
        
        if row:
            job = row_to_job(row)
            
            # 2. Immediately mark the job as 'processing' (This is the lock)
            job.state = 'processing'
            job.updated_at = now_utc
            
            # This UPDATE is protected by the ongoing transaction
            cursor.execute('''
                UPDATE jobs 
                SET state = ?, updated_at = ? 
                WHERE id = ?
            ''', (job.state, job.updated_at, job.id))
            
            # 3. Commit the transaction, releasing the lock
            conn.commit()
            
    except sqlite3.OperationalError as e:
        # Catches locking errors or timeout failures
        # Note: This is a normal occurrence in high-concurrency SQLite usage
        # and usually just means the worker needs to try again.
        conn.rollback() 
        job = None
    except Exception as e:
        # Other errors (like the previous TypeError before the fix)
        print(f"DEBUG: Unknown error during job pick: {type(e).__name__}: {e}")
        conn.rollback() 
        job = None
    finally:
        conn.close()
        
    return job

def update_job_state(job: Job):
    """Updates a job's state, attempts, and retry time after processing/failure."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    job.updated_at = datetime.now(timezone.utc).isoformat()
    
    cursor.execute('''
        UPDATE jobs 
        SET state = ?, attempts = ?, updated_at = ?, retry_after_time = ?
        WHERE id = ?
    ''', (job.state, job.attempts, job.updated_at, job.retry_after_time, job.id))
    
    conn.commit()
    conn.close()

def list_jobs(state_filter: str | None = None) -> list[Job]:
    """Lists jobs, optionally filtering by state."""
    conn = get_db_connection()
    cursor = conn.cursor()
    jobs = []
    
    if state_filter:
        cursor.execute("SELECT * FROM jobs WHERE state = ? ORDER BY updated_at DESC", (state_filter,))
    else:
        cursor.execute("SELECT * FROM jobs ORDER BY updated_at DESC")
    
    for row in cursor.fetchall():
        jobs.append(row_to_job(row))
        
    conn.close()
    return jobs

def get_job_by_id(job_id: str) -> Job | None:
    """Retrieves a single job by its ID."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cursor.fetchone()
    
    conn.close()
    return row_to_job(row)

def get_job_state_counts() -> dict:
    """Gets the count of jobs in each state for the status command."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT state, COUNT(id) FROM jobs GROUP BY state")
    counts = {row['state']: row['COUNT(id)'] for row in cursor.fetchall()}
    
    conn.close()
    return counts