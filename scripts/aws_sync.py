#!/usr/bin/env python3
"""
AWS Cloud Integration Utility for Spot MapReduce:
1. S3 Storage: Uploads MapReduce output partitions & downloads input datasets.
2. CloudWatch Telemetry: Publishes custom metrics (Shares, Preemptions, Latency) to CloudWatch.
"""

import os
import sys
import argparse
import glob
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("aws_sync")

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    log.error("boto3 is not installed. Run: pip install boto3")
    sys.exit(1)


def upload_output_to_s3(bucket_name: str, job_id: str, output_base: str = "/data/output", region: str = "us-east-1"):
    """Uploads partition files and _SUCCESS marker to S3."""
    job_dir = os.path.join(output_base, job_id)
    if not os.path.isdir(job_dir):
        # Fallback to local path if running outside container
        job_dir = os.path.abspath(os.path.join("data", "output", job_id))

    if not os.path.isdir(job_dir):
        log.error(f"Job output directory not found: {job_dir}")
        return False

    s3 = boto3.client("s3", region_name=region)
    files = glob.glob(os.path.join(job_dir, "*"))
    if not files:
        log.warning(f"No files found in {job_dir}")
        return False

    log.info(f"==> Uploading {len(files)} partition files for {job_id} to s3://{bucket_name}/output/{job_id}/")
    uploaded = 0
    for fpath in files:
        fname = os.path.basename(fpath)
        s3_key = f"output/{job_id}/{fname}"
        try:
            s3.upload_file(fpath, bucket_name, s3_key)
            log.info(f"  ✓ Uploaded {fname} -> s3://{bucket_name}/{s3_key}")
            uploaded += 1
        except Exception as e:
            log.error(f"  ✗ Failed to upload {fname}: {e}")

    log.info(f"[S3 SUCCESS] Successfully uploaded {uploaded}/{len(files)} files to S3 bucket '{bucket_name}'.")
    return uploaded > 0


def upload_input_to_s3(bucket_name: str, input_target: str = "sample_data", region: str = "us-east-1"):
    """Uploads a local dataset file or the sample_data directory to S3 input/ prefix."""
    s3 = boto3.client("s3", region_name=region)
    if not os.path.exists(input_target):
        # try checking relative to project root
        proj_sample = os.path.join(os.path.dirname(__file__), "..", input_target)
        if os.path.exists(proj_sample):
            input_target = proj_sample

    if os.path.isfile(input_target):
        fname = os.path.basename(input_target)
        s3_key = f"input/{fname}"
        s3.upload_file(input_target, bucket_name, s3_key)
        log.info(f"[S3 SUCCESS] Uploaded {fname} -> s3://{bucket_name}/{s3_key}")
    elif os.path.isdir(input_target):
        files = glob.glob(os.path.join(input_target, "*.csv")) + glob.glob(os.path.join(input_target, "*.txt"))
        for fpath in files:
            fname = os.path.basename(fpath)
            s3_key = f"input/{fname}"
            s3.upload_file(fpath, bucket_name, s3_key)
            log.info(f"  ✓ Uploaded {fname} -> s3://{bucket_name}/{s3_key}")
        log.info(f"[S3 SUCCESS] Finished uploading {len(files)} datasets to s3://{bucket_name}/input/")
    else:
        log.error(f"Input target path not found: {input_target}")


def download_dataset_from_s3(bucket_name: str, s3_key: str, dest_path: str = "/data/input", region: str = "us-east-1"):
    """Downloads an input dataset from S3 to local input folder."""
    s3 = boto3.client("s3", region_name=region)
    fname = os.path.basename(s3_key)
    out_file = os.path.join(dest_path, fname)
    os.makedirs(dest_path, exist_ok=True)

    log.info(f"==> Downloading s3://{bucket_name}/{s3_key} to {out_file}...")
    try:
        s3.download_file(bucket_name, s3_key, out_file)
        log.info(f"[S3 SUCCESS] Downloaded {out_file} ({os.path.getsize(out_file):,} bytes)")
        return out_file
    except Exception as e:
        log.error(f"[S3 ERROR] Failed to download {s3_key} from {bucket_name}: {e}")
        return None


def publish_cloudwatch_metrics(job_id: str, shares_processed: int, preemptions: int, duration_sec: float, region: str = "us-east-1"):
    """Publishes custom MapReduce metrics to AWS CloudWatch."""
    cw = boto3.client("cloudwatch", region_name=region)
    namespace = "SpotMapReduce"

    metrics = [
        {"MetricName": "TotalSharesProcessed", "Value": float(shares_processed), "Unit": "Count"},
        {"MetricName": "CleanSpotPreemptions", "Value": float(preemptions), "Unit": "Count"},
        {"MetricName": "JobDurationSeconds", "Value": float(duration_sec), "Unit": "Seconds"},
        {"MetricName": "JobsCompleted", "Value": 1.0, "Unit": "Count"},
    ]

    log.info(f"==> Publishing {len(metrics)} custom metrics to CloudWatch namespace '{namespace}' (Job: {job_id})...")
    try:
        cw.put_metric_data(Namespace=namespace, MetricData=metrics)
        log.info("[CLOUDWATCH SUCCESS] Custom metrics published successfully to AWS CloudWatch!")
        return True
    except Exception as e:
        log.error(f"[CLOUDWATCH ERROR] Failed to publish metrics: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AWS S3 & CloudWatch integration for Spot MapReduce")
    parser.add_argument("--bucket", help="Target Amazon S3 bucket name")
    parser.add_argument("--job-id", help="Job ID to sync (defaults to latest)")
    parser.add_argument("--region", default="us-east-1", help="AWS Region (default: us-east-1)")
    parser.add_argument("--upload-output", action="store_true", help="Upload job partition outputs to S3")
    parser.add_argument("--upload-input", nargs="?", const="sample_data", help="Upload dataset file or directory to s3://<bucket>/input/")
    parser.add_argument("--download-s3-key", help="S3 object key to download to /data/input")
    parser.add_argument("--publish-metrics", action="store_true", help="Push custom metrics to CloudWatch")
    parser.add_argument("--shares", type=int, default=123704766, help="Shares processed count for metrics")
    parser.add_argument("--preemptions", type=int, default=1, help="Preemptions count for metrics")
    parser.add_argument("--duration", type=float, default=24.5, help="Job duration in seconds")

    args = parser.parse_args()

    if args.upload_output:
        if not args.bucket:
            log.error("--bucket is required for S3 upload.")
            sys.exit(1)
        job_id = args.job_id
        if not job_id:
            # find latest
            out_dirs = [d for d in glob.glob("/data/output/job-*") if os.path.isdir(d)]
            if not out_dirs:
                out_dirs = [d for d in glob.glob("data/output/job-*") if os.path.isdir(d)]
            if out_dirs:
                out_dirs.sort(key=os.path.getmtime, reverse=True)
                job_id = os.path.basename(out_dirs[0])
            else:
                log.error("No job output directories found.")
                sys.exit(1)
        upload_output_to_s3(args.bucket, job_id, region=args.region)

    if args.upload_input:
        if not args.bucket:
            log.error("--bucket is required for S3 upload.")
            sys.exit(1)
        upload_input_to_s3(args.bucket, args.upload_input, region=args.region)

    if args.download_s3_key:
        if not args.bucket:
            log.error("--bucket is required for S3 download.")
            sys.exit(1)
        download_dataset_from_s3(args.bucket, args.download_s3_key, region=args.region)

    if args.publish_metrics:
        publish_cloudwatch_metrics(args.job_id or "job-latest", args.shares, args.preemptions, args.duration, region=args.region)


if __name__ == "__main__":
    main()
