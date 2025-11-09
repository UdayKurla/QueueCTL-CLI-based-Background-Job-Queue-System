import click
import json
import multiprocessing
import signal
import time
import os
from job import Job
from worker import Worker
from storage import enqueue_job, get_job_state_counts, list_jobs, get_job_by_id, update_job_state
from config import set_config, get_all_config

# --- Global Worker Management ---

ACTIVE_WORKERS = [] 
STOP_EVENT = multiprocessing.Event()

def cleanup_workers(signum=None, frame=None):
    """Gracefully stop all running workers (implements graceful shutdown)."""
    if not ACTIVE_WORKERS:
        return
        
    click.echo("\n--- Initiating graceful worker shutdown --- [cite: 56]")
    STOP_EVENT.set() 
    
    for process in ACTIVE_WORKERS:
        process.join(timeout=10) # Wait for worker to finish job
        if process.is_alive():
            process.terminate() 
            click.echo(f"Worker {process.pid} terminated forcefully.")
    
    os._exit(0) 

signal.signal(signal.SIGINT, cleanup_workers) 
signal.signal(signal.SIGTERM, cleanup_workers)

# --- CLI Command Implementation ---

@click.group()
def cli():
    """queuectl: A CLI-based background job queue system. [cite: 10]"""
    pass

@cli.command()
@click.argument('job_data', type=str)
def enqueue(job_data):
    """Add a new job to the queue. [cite: 34]"""
    try:
        data = json.loads(job_data)
        
        if 'id' not in data or 'command' not in data:
            raise ValueError("Job data must contain 'id' and 'command'.")
        
        job = Job(id=data['id'], command=data['command'], 
                  max_retries=data.get('max_retries', 3))
        
        success, message = enqueue_job(job)
        if success:
            click.echo(message)
        else:
            click.echo(f"Error: {message}", err=True)

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format for job data.", err=True)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)

# --- Worker Group ---

@cli.group()
def worker():
    """Manage background worker processes. [cite: 34]"""
    pass

@worker.command()
@click.option('--count', default=1, type=int, help='Number of workers to start.')
def start(count):
    """Start one or more workers. [cite: 34]"""
    if count <= 0:
        click.echo("Error: Worker count must be at least 1.", err=True)
        return

    global ACTIVE_WORKERS, STOP_EVENT
    STOP_EVENT.clear() 
    
    click.echo(f"Starting {count} worker(s)... Press Ctrl+C to stop.")
    
    for i in range(1, count + 1):
        w = Worker(worker_id=i, stop_event=STOP_EVENT)
        p = multiprocessing.Process(target=w.run, name=f"Worker-{i}")
        p.start()
        ACTIVE_WORKERS.append(p)

    while True:
        time.sleep(1)

@worker.command()
def stop():
    """Stop running workers gracefully. [cite: 34]"""
    cleanup_workers() 

# --- Status and List Commands ---

@cli.command()
def status():
    """Show summary of all job states & active workers. [cite: 34]"""
    
    counts = get_job_state_counts()
    click.echo("\n📊 Job State Summary:")
    
    for state in ['pending', 'processing', 'completed', 'failed', 'dead']:
        click.echo(f"- {state.capitalize()}: {counts.get(state, 0)}")

@cli.command(name='list')
@click.option('--state', type=str, help='Filter jobs by state (e.g., pending, failed, dead). [cite: 34]')
def list_jobs_cmd(state):
    """List jobs by state. [cite: 34]"""
    jobs = list_jobs(state_filter=state)
    
    if not jobs:
        click.echo(f"No jobs found in state '{state}'." if state else "No jobs found.")
        return

    click.echo(f"\n📋 Listing {len(jobs)} Job(s) (State: {state if state else 'All'}):")
    
    for job in jobs:
        retry_info = f" | Retrying after: {job.retry_after_time}" if job.retry_after_time else ""
        click.echo(f"[{job.state.upper():<10}] {job.id} | Attempts: {job.attempts}/{job.max_retries} | Command: '{job.command}'{retry_info}")

# --- DLQ Group ---

@cli.group()
def dlq():
    """View or retry DLQ jobs. [cite: 34]"""
    pass

@dlq.command(name='list')
def dlq_list():
    """View jobs in the Dead Letter Queue (DLQ). [cite: 34]"""
    list_jobs_cmd.invoke(list_jobs_cmd.make_context('list', ['--state', 'dead']))

@dlq.command()
@click.argument('job_id')
def retry(job_id):
    """Retry a specific job from the DLQ. [cite: 34]"""
    job = get_job_by_id(job_id)

    if not job:
        click.echo(f"Error: Job ID '{job_id}' not found.")
        return

    if job.state != 'dead':
        click.echo(f"Error: Job '{job_id}' is in state '{job.state}', not 'dead'.")
        return

    # Reset state and attempts
    job.state = 'pending'
    job.attempts = 0
    job.retry_after_time = None
    
    update_job_state(job)
    click.echo(f"Job {job_id} moved from DLQ to 'pending' queue for retry.")

# --- Config Group ---

@cli.group()
def config():
    """Manage configuration (retry, backoff, etc.). [cite: 34]"""
    pass

@config.command()
@click.argument('key', type=str)
@click.argument('value', type=str)
def set(key, value):
    """Set a configuration parameter (e.g., max_retries, retry_base). [cite: 34, 58]"""
    key = key.lower()
    
    if key in ['max_retries', 'retry_base']:
        try:
            int(value)
        except ValueError:
            click.echo(f"Error: Value for '{key}' must be an integer.", err=True)
            return

    set_config(key, value)
    click.echo(f"Configuration set: {key} = {value}")

@config.command(name='list')
def list_config():
    """List all current configuration settings."""
    settings = get_all_config()
    
    click.echo("📝 Current Configuration Settings:")
    if not settings:
        click.echo("- None. System defaults are being used.")
        return
        
    for key, value in settings.items():
        click.echo(f"- {key}: {value}")


if __name__ == '__main__':
    cli()