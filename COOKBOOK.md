# Transient Spot-Instance MapReduce: Engineering Cookbook
**A Step-by-Step Deployment, Operations, and Reproduction Runbook**

---

## 1. Overview & Architectural Blueprint

This cookbook provides a complete, step-by-step technical guide for setting up, executing, chaos-testing, and deploying the **Transient Spot-Instance MapReduce Framework** locally and on **Amazon Web Services (AWS)**.

### Core System Topology

```
                       ┌──────────────────────────────────────────────┐
                       │          Master Orchestrator (:8000)         │
                       │          FastAPI • SQLite (WAL Mode)         │
                       │    - Task Scheduler & Barrier Coordinator    │
                       │    - Heartbeat Lease Expiry Watchdog         │
                       │    - Docker Autoscaler (Spares & Recovery)   │
                       └──────────────▲────────────────▲──────────────┘
                                      │                │
                    Heartbeat & Lease │                │ Poll Tasks / Lease Claims
                                      │                │
        ┌─────────────────────────────┴────────────────┴─────────────────────────────┐
        │                                                                            │
 ┌──────┴─────────────────────────┐                          ┌───────────────────────┴────────┐
 │   Spot Worker 1..3 (Active)    │                          │   Spot Worker 4..5 (Spares)    │
 │ - Sub-task line reader         │                          │ - Hot standby in idle pool     │
 │ - Partition hasher (% num_red) │                          │ - Instant lease takeover on    │
 │ - Checkpoint engine (Redis)    │                          │   preemption or worker crash   │
 │ - SIGTERM Sentinel (15s drain) │                          │ - Resumes from exact byte ptr  │
 └──────────────┬─────────────────┘                          └────────────────▲───────────────┘
                │                                                             │
                │ HTTP POST /chunks (NDJSON)                                  │ HTTP GET /chunks
                ▼                                                             │
 ┌────────────────────────────────────────────────────────────────────────────┴───────────────┐
 │                            Remote Shuffle Service (RSS :8001)                              │
 │ - Decoupled shuffle partition storage (/data/rss)                                          │
 │ - In-memory slabs + chunk files + partition manifest.json                                  │
 └─────────────────────────────────────────────▲──────────────────────────────────────────────┘
                                               │
                                               │ Atomic CAS Lua Script & Byte Offsets
 ┌─────────────────────────────────────────────┴──────────────────────────────────────────────┐
 │                                   Redis 7.2 Store (:6379)                                  │
 │ - Monotonic fencing tokens (prevents zombie worker writes)                                 │
 │ - Sub-task byte offsets (f.tell() pointers for instant resume)                             │
 └────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Prerequisites & Environment Setup

### 2.1 System Requirements
- **Operating System:** Linux (Ubuntu 22.04/24.04/26.04 LTS), macOS, or Windows with WSL2.
- **Docker Engine:** `docker` >= 24.0 and `docker compose` >= 2.20.
- **Python:** 3.10, 3.11, or 3.12 (standard packages: `requests`, `boto3`).
- **Memory & CPU:** Minimum 4 vCPU, 8 GB RAM (runs comfortably on a standard laptop or EC2 `c7i-flex.large`).

### 2.2 Host Docker Socket Permissions
The Orchestrator interacts with the host Docker daemon via `/var/run/docker.sock` to dynamically spawn spare replacement containers and simulate preemption/crashes. Ensure your user has Docker permissions:

```bash
sudo usermod -aG docker $USER
newgrp docker
docker ps
```

---

## 3. Local Installation & Deployment

### Step 1: Clone the Repository
```bash
git clone https://github.com/Ghostblaster08/Spot_MapReduce.git
cd Spot_MapReduce
```

### Step 2: Verify Configuration Files
Inspect `docker-compose.yml` to confirm all 10 services and network definitions:
- `orchestrator` (:8000)
- `rss` (:8001)
- `redis` (:6379)
- `worker` (5 replicas: 3 active + 2 spares)
- `prometheus` (:9090)
- `grafana` (:3030)

### Step 3: Launch the Stack
Build the container images and start the services in detached mode:
```bash
docker compose up --build -d
```

### Step 4: Verify Container Health
Run:
```bash
docker compose ps
```
Expected output:
```text
NAME                     IMAGE                    STATUS
orchestrator             spot_mapreduce-orchestrator  Up (healthy)
rss                      spot_mapreduce-rss           Up (healthy)
redis                    redis:7.2-alpine             Up (healthy)
spot_mapreduce-worker-1  spot_mapreduce-worker        Up
spot_mapreduce-worker-2  spot_mapreduce-worker        Up
spot_mapreduce-worker-3  spot_mapreduce-worker        Up
spot_mapreduce-worker-4  spot_mapreduce-worker        Up
spot_mapreduce-worker-5  spot_mapreduce-worker        Up
prometheus               prom/prometheus:v2.51.2      Up
grafana                  grafana/grafana:10.4.2       Up
```

---

## 4. Workload: Historical Financial Data Analytics

The framework runs **Application 4: Historical Financial Data Analytics**. Instead of a toy word count, it processes historical market trade records to compute:
- **Total Trading Volume** ($\sum \text{Volume}$)
- **Trade Counts**
- **Volume-Weighted Average Price (VWAP):** $\frac{\sum (\text{Price} \times \text{Volume})}{\sum \text{Volume}}$
- **Arithmetic Average Execution Price**
- **Execution Price Extremes:** $[\text{Price}_{\min}, \text{Price}_{\max}]$

### 4.1 Sample Data Generation
Three real-world market datasets are provided in `sample_data/`:
1. `tech_giants_trades.csv` (AAPL, MSFT, NVDA, GOOGL, AMZN, META, TSLA)
2. `crypto_market_trades.csv` (BTC, ETH, SOL, BNB, DOGE)
3. `sp500_etf_trades.csv` (SPY, QQQ, DIA, IWM, JPM, V, INTC)

To regenerate or scale these datasets:
```bash
python3 scripts/create_sample_csvs.py
```

### 4.2 Code Structure of Map and Reduce
Inspect `worker/jobs/financial_analytics.py`:
- `map_fn(line)`: Parses `timestamp,symbol,price,volume`, filters bad lines/headers, and returns `[(symbol, [price, volume])]`.
- `reduce_fn(key, values)`: Iterates through price/volume pairs for each stock ticker, accumulating volume, dollar volume, and price ranges.

---

## 5. Running Jobs & Custom Dataset Ingestion

### Option A: Via the Interactive Flow Canvas (Recommended)
1. Open **`http://localhost:8000/flow`** in your browser.
2. Click **▶ Run job** in the top action bar to execute the baseline 125,000-record dataset.
3. **Uploading Custom CSVs:**
   - Click the **📁 Upload CSV** button.
   - Drag & drop any `.csv` or `.txt` file into the modal (e.g. `sample_data/crypto_market_trades.csv`).
   - Click **▶ Run on 3 Nodes**.
   - Watch the animated nodes light up: mappers partition by ticker $\rightarrow$ stream intermediate chunks to RSS $\rightarrow$ stage barrier clears $\rightarrow$ reducers compute VWAP and volumes.

### Option B: Via the Classic Telemetry Dashboard
1. Open **`http://localhost:8000/ui`** in your browser.
2. Review live task statuses, worker Docker IDs, Redis checkpoint counters, and RSS partition buffers.
3. Click **📁 Custom CSV / Dataset** to ingest new data.

### Option C: Via CLI / REST API
Submit a job directly via `curl`:
```bash
curl -s -X POST "http://localhost:8000/dashboard/submit-job?dataset=large&num_mappers=3&num_reducers=3"
```
Or use the CLI client container:
```bash
docker compose run --rm job-client submit \
  --name financial-analytics \
  --map-fn worker.jobs.financial_analytics.map_fn \
  --reduce-fn worker.jobs.financial_analytics.reduce_fn \
  --input /data/input/large_input.txt \
  --output /data/output \
  --mappers 3 \
  --reducers 3 \
  --wait
```

---

## 6. Chaos Testing: Simulating Cloud Spot Preemptions & Failures

The project features realistic failure injection tools that issue real Linux kernel signals to active containers.

### Scenario 1: Clean Spot Preemption (Graceful Drain - SIGTERM)
Simulates an AWS EC2 120-second spot termination notice or GCP 30-second ACPI soft off:
```bash
./scripts/simulate_preemption.sh spot_mapreduce-worker-1 15
```
*(Or click **⚡ Preempt** in the Flow UI).*

**What happens:**
1. The container receives `SIGTERM`.
2. The worker's `PreemptionHandler` intercepts the signal.
3. The mapper stops reading new lines, flushes all dirty in-memory partition slabs to RSS (`:8001`), and commits its final byte offset to Redis CAS.
4. The container shuts down cleanly.
5. The Master Orchestrator reassigns the task to an idle standby spare node (e.g., Worker 4).
6. Worker 4 inspects Redis, performs `f.seek(last_checkpoint_offset)`, and continues reading with **zero lost work**.

### Scenario 2: Abrupt Worker Crash (SIGKILL)
Simulates instant hardware failure, kernel panic, or unannounced hypervisor eviction:
```bash
./scripts/crash_worker.sh
```
*(Or click **💥 Crash** in the Flow UI).*

**What happens:**
1. Docker sends `SIGKILL` directly to the active worker process (no drain window).
2. The Orchestrator's `LeaseMonitor` detects missing heartbeats within 5 seconds.
3. The expired task is re-queued with an incremented attempt ID (fencing token).
4. A spare node takes over, seeks to the last committed 10-second micro-checkpoint in Redis, and completes the partition without full task restart.

### Scenario 3: Correlated Spot Storm Wave
Simulates simultaneous eviction of multiple spot workers:
```bash
./scripts/spot_storm.sh
```

---

## 7. Output Verification & Ground Truth Validation

To prove that the distributed MapReduce execution produced 100% mathematically correct results with zero duplicate counts and zero data loss, run:

```bash
./scripts/verify_output.sh
```

**Output:**
```text
==> Verifying output for Job: job-efd909976e26 against job-client/jobs/large_input.txt...
==> [PASS] Output matches ground truth baseline with 0 discrepancies! (15 keys verified)
```

You can inspect the generated partition files directly:
```bash
cat data/output/job-*/part-00000.txt
```
Example output:
```tsv
GOOGL	VOL: 3,880,680  | TRADES: 8,280 | VWAP: $164.03 | AVG: $164.02 | RANGE: [$160.00 - $168.00]
INTC	VOL: 17,095,660 | TRADES: 8,333 | VWAP: $22.77  | AVG: $22.77  | RANGE: [$21.50 - $24.00]
NFLX	VOL: 1,691,065  | TRADES: 8,170 | VWAP: $702.62 | AVG: $702.64 | RANGE: [$690.00 - $715.00]
QQQ	VOL: 19,121,660 | TRADES: 8,399 | VWAP: $491.55 | AVG: $491.55 | RANGE: [$485.00 - $498.00]
```

---

## 8. Monitoring & Observability Runbook

| Dashboard | URL | Key Metrics / Features |
| :--- | :--- | :--- |
| **Flow Topology** | `http://localhost:8000/flow` | Visual DAG state, live byte progress bars, fault alerts |
| **Classic UI** | `http://localhost:8000/ui` | Docker container inventory, Redis checkpoint table |
| **Grafana** | `http://localhost:3030` | Tasks running/queued, clean preemption rate, crash count |
| **Prometheus** | `http://localhost:9090` | Raw time-series PromQL queries & target health |
| **Dozzle** | `http://localhost:9999` | Real-time colorized multi-container log streaming |

### Opening Grafana
1. Navigate to `http://localhost:3030`.
2. Anonymous admin access is pre-configured (no login required).
3. The home page opens directly to **Transient Spot-Instance MapReduce Dashboard**.
4. Set refresh interval to **2s** and time range to **Last 15 minutes**.

---

## 9. AWS Production Deployment (EC2 + S3 + CloudWatch)

To deploy the entire solution to AWS, leverage 3 core services:

```
                  ┌────────────────────────────────────────────────────────┐
                  │                 Amazon Web Services                    │
                  │                                                        │
                  │   ┌────────────────────────────────────────────────┐   │
                  │   │        Amazon EC2 (c7i-flex.large)             │   │
                  │   │   Ubuntu 24.04 / 26.04 Host                    │   │
                  │   │   - Spot MapReduce Docker Compose Stack        │   │
                  │   │   - IAM Role: SpotMapReduce-EC2-Role           │   │
                  │   └───────────────▲────────────────▲───────────────┘   │
                  │                   │                │                   │
                  │     Input / Output│                │ PutMetricData     │
                  │     Sync          │                │ Custom Metrics    │
                  │                   ▼                ▼                   │
                  │   ┌──────────────────────┐   ┌─────────────────────┐   │
                  │   │      Amazon S3       │   │  Amazon CloudWatch  │   │
                  │   │   Bucket:            │   │  Namespace:         │   │
                  │   │   spot-mapreduce-    │   │  SpotMapReduce      │   │
                  │   │   lab-ghostblaster   │   │  (Live AWS Graphs)  │   │
                  │   └──────────────────────┘   └─────────────────────┘   │
                  └────────────────────────────────────────────────────────┘
```

### Step 9.1: Launch the EC2 Host Instance
1. In AWS Console $\rightarrow$ **EC2** $\rightarrow$ **Launch instance**.
2. **AMI:** Ubuntu Server 24.04 LTS (64-bit x86).
3. **Instance Type:** `c7i-flex.large` (2 vCPU, 4 GiB Memory) or `t3.medium`.
4. **Key Pair:** Select your `.pem` key (e.g. `Omarchy.pem`).
5. **Storage:** 30 GiB gp3 (Free tier eligible).

### Step 9.2: Configure Security Group Inbound Rules
Add the following inbound rules to the instance Security Group:

| Type | Protocol | Port Range | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| **SSH** | TCP | `22` | `0.0.0.0/0` (or My IP) | SSH terminal connection |
| **Custom TCP** | TCP | `8000` | `0.0.0.0/0` | Flow Canvas & Orchestrator UI |
| **Custom TCP** | TCP | `3030` | `0.0.0.0/0` | Grafana Live Dashboards |
| **Custom TCP** | TCP | `9999` | `0.0.0.0/0` | Dozzle Real-Time Log Viewer |

### Step 9.3: Create and Attach the IAM Role
1. In AWS Console $\rightarrow$ **IAM** $\rightarrow$ **Roles** $\rightarrow$ **Create role**.
2. **Trusted Entity:** AWS service $\rightarrow$ **Use case:** EC2.
3. **Attach Policies:**
   - `AmazonS3FullAccess`
   - `CloudWatchFullAccess`
4. **Name:** `SpotMapReduce-EC2-Role`.
5. Attach the role to your instance: **EC2 Console** $\rightarrow$ Select Instance $\rightarrow$ **Actions** $\rightarrow$ **Security** $\rightarrow$ **Modify IAM role** $\rightarrow$ Select `SpotMapReduce-EC2-Role`.

### Step 9.4: Create the Amazon S3 Bucket
1. In AWS Console $\rightarrow$ **S3** $\rightarrow$ **Create bucket**.
2. **Bucket name:** `spot-mapreduce-lab-ghostblaster` (or your unique name).
3. **Region:** Same region as EC2 (e.g. `us-east-1`).
4. Keep **Block all public access** enabled $\rightarrow$ Click **Create bucket**.

### Step 9.5: Sync Code & Launch on EC2
From your local machine, connect and sync:
```bash
# SSH into EC2
ssh -i Omarchy.pem ubuntu@<EC2_PUBLIC_IP>

# Clone repository
git clone https://github.com/Ghostblaster08/Spot_MapReduce.git
cd Spot_MapReduce

# Install Docker on Ubuntu (if fresh instance)
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 python3-boto3
sudo usermod -aG docker $USER && newgrp docker

# Start the cluster
docker compose up -d
docker run -d --name dozzle -p 9999:8080 -v /var/run/docker.sock:/var/run/docker.sock --restart unless-stopped amir20/dozzle:latest
```

### Step 9.6: Sync Datasets and Partitions with S3
Use the automated `scripts/aws_sync.py` utility:

```bash
# 1. Upload sample datasets to S3 (input/ prefix)
python3 scripts/aws_sync.py --bucket spot-mapreduce-lab-ghostblaster --upload-input sample_data

# 2. Upload completed job partition files to S3 (output/ prefix)
python3 scripts/aws_sync.py --bucket spot-mapreduce-lab-ghostblaster --upload-output

# 3. Publish custom metrics to AWS CloudWatch
python3 scripts/aws_sync.py --publish-metrics --shares 123704766 --preemptions 1 --duration 24.5
```

### Step 9.7: View Live Graphs in AWS CloudWatch Console
1. Open [AWS CloudWatch Console](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#metricsV2:).
2. Go to **Metrics** $\rightarrow$ **All metrics** $\rightarrow$ **Custom namespaces** $\rightarrow$ **`SpotMapReduce`** $\rightarrow$ **Metric Name**.
3. Check `TotalSharesProcessed`, `CleanSpotPreemptions`, `JobDurationSeconds`, and `JobsCompleted`.
4. Click the **Graphed metrics** tab to see real-time AWS graphs!

---

## 10. Troubleshooting & FAQ

### Q1: What happens if an old worker wakes up after lease expiry?
**Answer:** The old worker is safely rejected via a **fencing token**. In `worker/checkpoint/store.py`, Redis executes an atomic Lua script that compares the caller's attempt token against the stored token. Because the replacement worker committed with an incremented attempt token, Redis throws `STALE_FENCING_TOKEN` and discards the stale write.

### Q2: Why are intermediate shuffle files not written to worker local disks?
**Answer:** Standard Hadoop/Spark workers store shuffle files on ephemeral local disks. When a spot instance is evicted, those local files vanish, forcing the cluster to recompute all upstream map stages. Our decoupled **Remote Shuffle Service (RSS)** stores chunks independently on dedicated storage volumes, insulating completed map work from worker eviction.

### Q3: How do I stop AWS costs when finished?
**Answer:** Stop the EC2 instance from the AWS console (**Instance state** $\rightarrow$ **Stop instance**). While stopped, compute costs drop to **$0.00/hour**, while your disk contents, Docker images, and configurations remain 100% preserved.
