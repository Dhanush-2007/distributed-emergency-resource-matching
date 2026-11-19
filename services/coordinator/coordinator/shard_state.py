"""Persistent shard state management for the coordinator.

Persists shard metadata (epoch and primaryNodeId) to STATE_DIR/shard_state.json
using atomic file replacement (write temp file + os.replace).
"""

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ShardStateManager:
    """Thread-safe manager for persisting shard epoch and primaryNodeId to disk."""

    def __init__(self, state_dir: Path | str) -> None:
        self._state_dir = Path(state_dir)
        self._file_path = self._state_dir / "shard_state.json"
        self._lock = threading.Lock()
        self._shards: dict[str, dict[str, Any]] = {}
        self.load()

    def load(self) -> None:
        """Load state from disk. Missing file = empty; corrupt file = log error and start empty."""
        with self._lock:
            if not self._file_path.exists():
                self._shards = {}
                return

            try:
                with open(self._file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    shards_data = data.get("shards", {})
                    self._shards = {
                        str(sid): {
                            "epoch": int(sinfo.get("epoch", 1)),
                            "primaryNodeId": str(sinfo.get("primaryNodeId", "")),
                        }
                        for sid, sinfo in shards_data.items()
                        if isinstance(sinfo, dict)
                    }
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Corrupt or unreadable shard state file; starting empty",
                    extra={
                        "event": "shard_state.load_error",
                        "path": str(self._file_path),
                        "error": str(exc),
                    },
                )
                self._shards = {}

    def save(self) -> None:
        """Atomically persist shard state to disk using temp file + os.replace."""
        with self._lock:
            self._state_dir.mkdir(parents=True, exist_ok=True)
            temp_path = (
                self._state_dir
                / f"shard_state.json.tmp.{os.getpid()}.{threading.get_ident()}"
            )
            payload = {"shards": self._shards}
            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, self._file_path)
            except Exception as exc:
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except OSError:
                        pass
                logger.error(
                    "Failed to save shard state atomically",
                    extra={
                        "event": "shard_state.save_error",
                        "path": str(self._file_path),
                        "error": str(exc),
                    },
                )
                raise

    def get_shard(self, shard_id: int | str) -> dict[str, Any] | None:
        with self._lock:
            shard_info = self._shards.get(str(shard_id))
            return dict(shard_info) if shard_info is not None else None

    def get_primary(self, shard_id: int | str) -> str | None:
        with self._lock:
            shard_info = self._shards.get(str(shard_id))
            if shard_info:
                primary = shard_info.get("primaryNodeId")
                return primary if primary else None
            return None

    def get_epoch(self, shard_id: int | str) -> int | None:
        with self._lock:
            shard_info = self._shards.get(str(shard_id))
            if shard_info:
                return shard_info.get("epoch")
            return None

    def set_primary(self, shard_id: int | str, node_id: str, epoch: int) -> None:
        sid = str(shard_id)
        with self._lock:
            self._shards[sid] = {
                "epoch": int(epoch),
                "primaryNodeId": str(node_id),
            }
        self.save()

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {"shards": {k: dict(v) for k, v in self._shards.items()}}
