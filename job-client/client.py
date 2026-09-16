import os
import sys
import time
import json
import click
import requests

DEFAULT_ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://orchestrator:8000").rstrip("/")

@click.group()
def cli():
    """CLI client for submitting and monitoring Transient Spot-Instance MapReduce jobs."""
    pass

@cli.command()
@click.option("--config", "-c", type=click.Path(exists=True), help="Path to job JSON config file")
@click.option("--name", "-n", default="wordcount-job", help="Job name")
@click.option("--map-fn", default="worker.jobs.wordcount.map_fn", help="Dotted module path for mapper")
@click.option("--reduce-fn", default="worker.jobs.wordcount.reduce_fn", help="Dotted module path for reducer")
@click.option("--input", "-i", "input_path", default="/data/input/sample_input.txt", help="Input file path")
@click.option("--output", "-o", "output_path", default="/data/output", help="Output directory path")
@click.option("--mappers", "-m", default=4, type=int, help="Number of mappers")
@click.option("--reducers", "-r", default=4, type=int, help="Number of reducers")
@click.option("--url", default=DEFAULT_ORCHESTRATOR_URL, help="Orchestrator URL")
@click.option("--wait", "-w", is_flag=True, help="Wait for job completion after submission")
def submit(config, name, map_fn, reduce_fn, input_path, output_path, mappers, reducers, url, wait):
    """Submit a MapReduce job to the orchestrator."""
    if config:
        with open(config, "r", encoding="utf-8") as f:
            payload = json.load(f)
    else:
        payload = {
            "name": name,
            "map_fn": map_fn,
            "reduce_fn": reduce_fn,
            "input_path": input_path,
            "output_path": output_path,
            "num_mappers": mappers,
            "num_reducers": reducers
        }

    click.echo(f"Submitting job to {url}/jobs...")
    try:
        resp = requests.post(f"{url}/jobs", json=payload, timeout=10)
        if resp.status_code != 202:
            click.secho(f"Error submitting job ({resp.status_code}): {resp.text}", fg="red")
            sys.exit(1)

        data = resp.json()
        job_id = data["job_id"]
        click.secho(f"Job successfully submitted! Job ID: {job_id}", fg="green", bold=True)
        click.echo(json.dumps(data, indent=2))

        if wait:
            _wait_for_job(job_id, url)

    except Exception as e:
        click.secho(f"Failed to connect to orchestrator: {e}", fg="red")
        sys.exit(1)

@cli.command()
@click.argument("job_id")
@click.option("--url", default=DEFAULT_ORCHESTRATOR_URL, help="Orchestrator URL")
def status(job_id, url):
    """Check the status of a submitted job."""
    try:
        resp = requests.get(f"{url}/jobs/{job_id}", timeout=10)
        if resp.status_code == 404:
            click.secho(f"Job {job_id} not found", fg="red")
            sys.exit(1)
        resp.raise_for_status()
        data = resp.json()
        click.echo(json.dumps(data, indent=2))
    except Exception as e:
        click.secho(f"Failed to fetch job status: {e}", fg="red")
        sys.exit(1)

@cli.command()
@click.argument("job_id")
@click.option("--url", default=DEFAULT_ORCHESTRATOR_URL, help="Orchestrator URL")
def monitor(job_id, url):
    """Continuously monitor a job until it completes or fails."""
    _wait_for_job(job_id, url)

def _wait_for_job(job_id: str, url: str):
    click.echo(f"Monitoring job {job_id}...")
    start_time = time.time()

    while True:
        try:
            resp = requests.get(f"{url}/jobs/{job_id}", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                state = data["state"]
                total = data["tasks_total"]
                done = data["tasks_completed"]
                running = data["tasks_running"]
                queued = data["tasks_queued"]
                failed = data["tasks_failed"]

                elapsed = int(time.time() - start_time)
                click.echo(
                    f"[{elapsed}s] State: {state:<14} | Total: {total} | Completed: {done} | Running: {running} | Queued: {queued} | Failed: {failed}"
                )

                if state == "COMPLETED":
                    click.secho(f"Job {job_id} COMPLETED successfully in {elapsed}s!", fg="green", bold=True)
                    break
                elif state == "FAILED":
                    click.secho(f"Job {job_id} FAILED! Error: {data.get('error_message')}", fg="red", bold=True)
                    sys.exit(1)
            else:
                click.warning(f"Unexpected status check response: {resp.status_code}")
        except Exception as e:
            click.warning(f"Connection error while checking status: {e}")

        time.sleep(2.0)

if __name__ == "__main__":
    cli()
