"""Where generated events go. Phase 2 adds a Kafka/Redpanda sink."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Protocol


class Sink(Protocol):
    def write(self, event: dict[str, Any]) -> None: ...
    def close(self) -> None: ...


class JsonlSink:
    """Writes one JSON object per line to a file, or to stdout when path is '-'."""

    def __init__(self, path: str = "-"):
        if path == "-":
            self._f = sys.stdout
            self._owned = False
        else:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._f = open(path, "w")
            self._owned = True

    def write(self, event: dict[str, Any]) -> None:
        self._f.write(json.dumps(event, separators=(",", ":")) + "\n")

    def close(self) -> None:
        self._f.flush()
        if self._owned:
            self._f.close()
