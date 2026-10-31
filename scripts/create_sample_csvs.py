#!/usr/bin/env python3
"""
Generates real-world market-data sample CSV files for Spot MapReduce:
1. tech_giants_trades.csv - Magnificent 7 tech leaders (AAPL, MSFT, NVDA, GOOGL, AMZN, META, TSLA)
2. crypto_market_trades.csv - Major spot cryptocurrencies (BTC, ETH, SOL, BNB, DOGE)
3. sp500_etf_trades.csv - Benchmark ETFs & financials (SPY, QQQ, DIA, IWM, JPM, V, INTC)
"""

import os
import random
from datetime import datetime, timedelta

SAMPLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sample_data"))
JOBS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "job-client", "jobs"))
os.makedirs(SAMPLE_DIR, exist_ok=True)
os.makedirs(JOBS_DIR, exist_ok=True)

random.seed(42)

def generate_market_file(filepath, tickers, n_records, base_date="2026-10-23"):
    start_time = datetime.strptime(f"{base_date} 09:30:00", "%Y-%m-%d %H:%M:%S")
    prices = {t: cfg["base"] for t, cfg in tickers.items()}

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("timestamp,symbol,price,volume\n")
        curr_time = start_time
        for _ in range(n_records):
            curr_time += timedelta(milliseconds=random.randint(50, 4500))
            sym = random.choices(list(tickers.keys()), weights=[cfg["weight"] for cfg in tickers.values()])[0]
            cfg = tickers[sym]

            # Price random walk with volatility
            drift = random.gauss(0, cfg["volatility"])
            prices[sym] = max(cfg["min_price"], min(cfg["max_price"], prices[sym] + drift))
            price_rounded = round(prices[sym], cfg.get("precision", 2))

            # Realistic volume
            vol = random.randint(cfg["min_vol"], cfg["max_vol"])

            ts_str = curr_time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{ts_str},{sym},{price_rounded},{vol}\n")

    size_kb = os.path.getsize(filepath) / 1024
    print(f"[OK] Generated {os.path.basename(filepath)}: {n_records:,} records ({size_kb:.1f} KB)")


def main():
    # 1. Tech Giants (Mag 7)
    tech_tickers = {
        "AAPL":  {"base": 231.50, "volatility": 0.15, "min_price": 228.0, "max_price": 236.0, "min_vol": 50,  "max_vol": 3500, "weight": 20},
        "NVDA":  {"base": 139.80, "volatility": 0.25, "min_price": 133.0, "max_price": 146.0, "min_vol": 100, "max_vol": 5000, "weight": 25},
        "MSFT":  {"base": 424.20, "volatility": 0.20, "min_price": 418.0, "max_price": 432.0, "min_vol": 30,  "max_vol": 2000, "weight": 15},
        "GOOGL": {"base": 165.20, "volatility": 0.12, "min_price": 161.0, "max_price": 170.0, "min_vol": 40,  "max_vol": 2500, "weight": 12},
        "AMZN":  {"base": 186.40, "volatility": 0.18, "min_price": 182.0, "max_price": 192.0, "min_vol": 50,  "max_vol": 3000, "weight": 12},
        "META":  {"base": 584.50, "volatility": 0.40, "min_price": 572.0, "max_price": 598.0, "min_vol": 20,  "max_vol": 1500, "weight": 8},
        "TSLA":  {"base": 218.90, "volatility": 0.35, "min_price": 210.0, "max_price": 228.0, "min_vol": 80,  "max_vol": 4000, "weight": 14},
    }
    p1 = os.path.join(SAMPLE_DIR, "tech_giants_trades.csv")
    generate_market_file(p1, tech_tickers, 6000)

    # 2. Crypto Assets
    crypto_tickers = {
        "BTC":  {"base": 67450.0, "volatility": 25.0, "min_price": 66200.0, "max_price": 68800.0, "min_vol": 1,    "max_vol": 15,    "weight": 30, "precision": 1},
        "ETH":  {"base": 2640.0,  "volatility": 2.5,  "min_price": 2560.0,  "max_price": 2720.0,  "min_vol": 2,    "max_vol": 60,    "weight": 25, "precision": 2},
        "SOL":  {"base": 172.50,  "volatility": 0.4,  "min_price": 165.0,   "max_price": 182.0,   "min_vol": 10,   "max_vol": 500,   "weight": 20, "precision": 2},
        "BNB":  {"base": 592.0,   "volatility": 0.8,  "min_price": 580.0,   "max_price": 605.0,   "min_vol": 5,    "max_vol": 80,    "weight": 15, "precision": 2},
        "DOGE": {"base": 0.1420,  "volatility": 0.001,"min_price": 0.1350,  "max_price": 0.1500,  "min_vol": 1000, "max_vol": 50000,"weight": 10, "precision": 4},
    }
    p2 = os.path.join(SAMPLE_DIR, "crypto_market_trades.csv")
    generate_market_file(p2, crypto_tickers, 6000)

    # 3. S&P 500 ETFs and Blue Chips
    etf_tickers = {
        "SPY":  {"base": 580.80, "volatility": 0.15, "min_price": 576.0, "max_price": 586.0, "min_vol": 200,  "max_vol": 12000, "weight": 25},
        "QQQ":  {"base": 492.50, "volatility": 0.20, "min_price": 486.0, "max_price": 499.0, "min_vol": 150,  "max_vol": 10000, "weight": 20},
        "DIA":  {"base": 431.20, "volatility": 0.12, "min_price": 427.0, "max_price": 435.0, "min_vol": 100,  "max_vol": 5000,  "weight": 15},
        "IWM":  {"base": 221.70, "volatility": 0.18, "min_price": 218.0, "max_price": 226.0, "min_vol": 100,  "max_vol": 6000,  "weight": 10},
        "JPM":  {"base": 222.40, "volatility": 0.15, "min_price": 219.0, "max_price": 226.0, "min_vol": 100,  "max_vol": 4000,  "weight": 10},
        "V":    {"base": 279.10, "volatility": 0.14, "min_price": 275.0, "max_price": 284.0, "min_vol": 80,   "max_vol": 3000,  "weight": 10},
        "INTC": {"base": 22.85,  "volatility": 0.05, "min_price": 21.8,  "max_price": 23.9,   "min_vol": 500,  "max_vol": 20000, "weight": 10},
    }
    p3 = os.path.join(SAMPLE_DIR, "sp500_etf_trades.csv")
    generate_market_file(p3, etf_tickers, 6000)

    # Also copy to job-client/jobs for instant CLI usage
    import shutil
    for fn in ["tech_giants_trades.csv", "crypto_market_trades.csv", "sp500_etf_trades.csv"]:
        shutil.copy(os.path.join(SAMPLE_DIR, fn), os.path.join(JOBS_DIR, fn))
    print("[OK] Copied all 3 sample files to job-client/jobs/")

if __name__ == "__main__":
    main()
