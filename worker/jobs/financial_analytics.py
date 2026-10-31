from typing import List, Tuple, Any, Union

def map_fn(line: str) -> List[Tuple[str, List[Union[float, int]]]]:
    """
    Parses a historical trade/market-data record and emits:
    (symbol, [price, volume])

    Expected record format:
    timestamp,symbol,price,volume
    e.g. "2026-10-24 09:30:00,AAPL,232.50,1500"
    """
    line = line.strip()
    if not line or line.startswith("#") or line.lower().startswith("timestamp") or line.lower().startswith("date"):
        return []

    parts = line.split(",")
    if len(parts) >= 4:
        try:
            symbol = parts[1].strip().upper()
            price = float(parts[2].strip())
            volume = int(float(parts[3].strip()))
            if symbol and volume > 0 and price > 0:
                return [(symbol, [price, volume])]
        except (ValueError, IndexError):
            pass
    return []

def reduce_fn(key: str, values: List[List[Union[float, int]]]) -> str:
    """
    Aggregates historical market statistics for a stock symbol:
    - Total Trading Volume (shares)
    - Total Number of Trades
    - Volume-Weighted Average Price (VWAP) = sum(price * volume) / sum(volume)
    - Simple Average Execution Price
    - High (Max) and Low (Min) Execution Prices
    """
    if not values:
        return "VOL: 0 | TRADES: 0 | VWAP: $0.00"

    total_vol = 0
    trade_count = len(values)
    dollar_vol = 0.0
    min_price = float("inf")
    max_price = float("-inf")
    sum_price = 0.0

    for item in values:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            p = float(item[0])
            v = int(item[1])
            total_vol += v
            dollar_vol += p * v
            sum_price += p
            if p < min_price:
                min_price = p
            if p > max_price:
                max_price = p

    vwap = (dollar_vol / total_vol) if total_vol > 0 else 0.0
    avg_price = (sum_price / trade_count) if trade_count > 0 else 0.0

    return f"VOL: {total_vol:,} | TRADES: {trade_count:,} | VWAP: ${vwap:.2f} | AVG: ${avg_price:.2f} | RANGE: [${min_price:.2f} - ${max_price:.2f}]"
