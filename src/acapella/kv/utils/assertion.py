from typing import List


def check_key(key: List[str]):
    if len(key) == 0:
        raise ValueError("key must not be empty")
    for part in key:
        check_key_part(part)


def check_key_part(part: str):
    if len(part) == 0:
        raise ValueError("All key parts must not be empty")


def check_nrw(n: int, r: int, w: int):
    if n <= 0:
        raise ValueError("N parameter must be greater than zero")
    if n > 127:
        raise ValueError("N parameter must be less than 128")
    if r <= 0:
        raise ValueError("R parameter must be greater than zero")
    if w <= 0:
        raise ValueError("W parameter must be greater than zero")
    if r > n:
        raise ValueError("R parameter must be less then or equal to N")
    if w > n:
        raise ValueError("W parameter must be less then or equal to N")
