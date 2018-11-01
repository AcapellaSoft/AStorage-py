from typing import TypeVar, Dict, Optional

K = TypeVar('K')
V = TypeVar('V')


def remove_none_values(c: Dict[K, Optional[V]]) -> Dict[K, V]:
    return {k: v for k, v in c.items() if v is not None}
