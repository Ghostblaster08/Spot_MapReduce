from typing import List, Tuple

def map_fn(line: str) -> List[Tuple[str, str]]:
    """Takes 100-byte TeraSort record (10-byte key + 90-byte value)."""
    if len(line) >= 10:
        return [(line[:10], line[10:])]
    return [(line, "")]

def reduce_fn(key: str, values: List[str]) -> str:
    """Pass-through sorted records."""
    return "".join(values)
