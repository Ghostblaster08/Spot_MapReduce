#!/usr/bin/env python3
"""
Generates synthetic historical market trade data for Spot MapReduce.
Each record represents a market transaction:
timestamp,symbol,price,volume
"""

import os
import random
from datetime import datetime, timedelta

SYMBOLS = {
    'AAPL': (228.0, 235.0, 50, 1500),
    'NVDA': (132.0, 142.0, 100, 3000),
    'MSFT': (415.0, 428.0, 30, 800),
    'GOOGL': (160.0, 168.0, 40, 900),
    'AMZN': (182.0, 189.0, 50, 1200),
    'TSLA': (210.0, 225.0, 50, 2000),
    'META': (570.0, 595.0, 20, 600),
    'AMD': (152.0, 162.0, 50, 1800),
    'NFLX': (690.0, 715.0, 10, 400),
    'JPM': (218.0, 226.0, 40, 1000),
    'V': (275.0, 283.0, 30, 700),
    'DIS': (92.0, 98.0, 50, 1500),
    'INTC': (21.5, 24.0, 100, 4000),
    'SPY': (575.0, 585.0, 100, 5000),
    'QQQ': (485.0, 498.0, 100, 4500)
}

def generate_market_data(num_records: int, output_file: str):
    random.seed(42)  # Deterministic seed for repeatable benchmark
    start_time = datetime(2026, 10, 23, 9, 30, 0)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        # Header line (skipped by mapper)
        f.write("timestamp,symbol,price,volume\n")
        symbols_list = list(SYMBOLS.keys())
        for i in range(num_records):
            sym = random.choice(symbols_list)
            min_p, max_p, min_v, max_v = SYMBOLS[sym]
            price = round(random.uniform(min_p, max_p), 2)
            vol = random.randint(min_v, max_v)
            t = start_time + timedelta(seconds=i * 0.15)
            t_str = t.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{t_str},{sym},{price:.2f},{vol}\n")

    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Generated {output_file}: {num_records:,} records ({size_mb:.2f} MB)")

if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jobs_dir = os.path.join(base, "job-client", "jobs")
    # Small dataset (~2,500 records, ~90 KB)
    generate_market_data(2500, os.path.join(jobs_dir, "sample_input.txt"))
    # Large dataset (~125,000 records, ~4.35 MB)
    generate_market_data(125000, os.path.join(jobs_dir, "large_input.txt"))
