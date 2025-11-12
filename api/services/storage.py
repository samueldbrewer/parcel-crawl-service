"""Placeholder storage helpers.

In a future milestone, these functions will interface with S3/R2 or Railway volumes.
"""
from pathlib import Path
from typing import BinaryIO

STORAGE_ROOT = Path("storage")
STORAGE_ROOT.mkdir(exist_ok=True)


def save_file(rel_path: str, data: bytes) -> Path:
    path = STORAGE_ROOT / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def load_file(rel_path: str) -> bytes:
    path = STORAGE_ROOT / rel_path
    return path.read_bytes()


def save_stream(rel_path: str, stream: BinaryIO) -> Path:
    path = STORAGE_ROOT / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(stream.read())
    return path
