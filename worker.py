import time
import subprocess
import math
from datetime import datetime, timedelta, timezone
from job import Job
from storage import pick_job_for_worker, update_job_state
from config import get_config

class Worker:
    def __init__(self, worker_id: int, stop_event):
        self.id = worker_id
        self.stop_event = stop_event
        # Read retry base from persistent config, defaulting to 2 [cite: 58, 43]
        self.retry_base = int(get_config('retry_base', default='2')) 

    def run(self):
        """The main loop for the worker process."""
        print(f"Worker {self.id} started. Using retry base: {self.retry_base}")
        
        while not self.stop_event.is_set():
            job = pick_job_for_worker() 
            
            if job:
                self.process_job(job)
            else:
                time.sleep(1)

        print(f"Worker {self.id} received stop signal. Graceful shutdown complete. [cite: 56]")

    def process_job(self, job: Job):
        """Execute the command and handle result/retry logic."""
        try:
            # Execute command [cite: 37]
            result = subprocess.run(job.command, shell=True, check=False, 
                                    capture_output=True, text=True)
            
            # Exit codes determine success or failure [cite: 38]
            if result.returncode == 0:
                self.handle_success(job)
            else:
                self.handle_failure(job)
                
        except Exception as e:
            # Catch errors like command not found or execution issues [cite: 39]
            print(f"Job {job.id} execution failed unexpectedly: {e}")
            self.handle_failure(job, execution_error=True)

    def handle_success(self, job: Job):
        job.state = 'completed'
        job.retry_after_time = None
        update_job_state(job)
        print(f"Job {job.id} completed successfully.")

    def handle_failure(self, job: Job, **kwargs):
        """Handles job failure, retries, and DLQ transition."""
        job.attempts += 1
        
        if job.attempts >= job.max_retries:
            # Move to Dead Letter Queue (DLQ) after max_retries [cite: 45]
            job.state = 'dead'
            job.retry_after_time = None
            print(f"Job {job.id} failed after {job.attempts} attempts. Moving to DLQ. [cite: 11, 72]")
        else:
            # Implement Exponential Backoff [cite: 42]
            job.state = 'pending' # Move back to pending, but set a future retry time
            
            # delay = base ** attempts seconds [cite: 44]
            delay = math.pow(self.retry_base, job.attempts) 
            retry_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
            job.retry_after_time = retry_time.isoformat()
            
            print(f"Job {job.id} failed (attempt {job.attempts}). Retrying in {delay:.2f} seconds.")

        update_job_state(job)