import re
from typing import List, Tuple

WORD_RE = re.compile(r"[a-zA-Z0-9]+")

def map_fn(line: str) -> List[Tuple[str, int]]:
    """Tokenizes text line into (word, 1) tuples."""
    words = WORD_RE.findall(line.lower())
    return [(w, 1) for w in words]

def reduce_fn(key: str, values: List[int]) -> int:
    """Sums up occurrence counts for a word."""
    return sum(values)
