"""SQLite storage database layer for a storage node."""

import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ems_common.errors import NotFound, VersionConflict


@dataclass(frozen=True)
class Record:
    key: str
    value: dict[str, Any]
    version: int
    deleted: int
    updated_at: str


@dataclass(frozen=True)
class PutResult:
    version: int
    seq: int
    created: bool


@dataclass(frozen=True)
class DeleteResult:
    version: int
    seq: int


class ReplicationGap(Exception):
    def __init__(self, last_seq: int):
        self.last_seq = last_seq
        super().__init__(f"Replication gap: expected seq {last_seq + 1}")

    def __repr__(self) -> str:
        return f"ReplicationGap(last_seq={self.last_seq})"


class StorageDB:
    """Manages SQLite storage node operations for key-value records, oplog, and meta."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        # Ensure parent directory exists if a path is given
        parent = Path(self.db_path).parent
        if parent and str(parent) != ".":
            parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # Autocommit mode, manual BEGIN IMMEDIATE for transactions
        )
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=FULL;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS oplog (
                    seq INTEGER PRIMARY KEY,
                    epoch INTEGER NOT NULL,
                    key TEXT NOT NULL,
                    op TEXT NOT NULL,
                    value TEXT,
                    version INTEGER NOT NULL,
                    ts TEXT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );
            """)
            # Initialize default metadata if absent
            cur.execute("INSERT OR IGNORE INTO meta (k, v) VALUES ('last_applied_seq', '0');")
            cur.execute("INSERT OR IGNORE INTO meta (k, v) VALUES ('epoch', '1');")
            cur.execute(
                "INSERT OR IGNORE INTO meta (k, v) VALUES ('role', ?);",
                (os.getenv("INITIAL_ROLE", "PRIMARY"),),
            )
            cur.execute("INSERT OR IGNORE INTO meta (k, v) VALUES ('last_log_epoch', '0');")
            

    OPLOG_RETAIN: int = 10000  # default retain count

    def _prune_oplog(self, cur: sqlite3.Cursor) -> None:
        """Prune oplog entries older than the retain window."""
        last_seq = int(self._get_meta(cur, "last_applied_seq"))
        retain_seq = last_seq - self.OPLOG_RETAIN
        if retain_seq > 0:
            cur.execute("DELETE FROM oplog WHERE seq <= ?;", (retain_seq,))

    def _get_meta(self, cur: sqlite3.Cursor, key: str) -> str:
        cur.execute("SELECT v FROM meta WHERE k = ?;", (key,))
        row = cur.fetchone()
        return row[0] if row else ""

    def put(
        self,
        key: str,
        value: dict[str, Any],
        if_version: int | None,
        epoch: int,
    ) -> PutResult:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    "SELECT key, value, version, deleted, updated_at FROM kv WHERE key = ?;",
                    (key,),
                )
                existing = cur.fetchone()

                now = datetime.now(UTC).isoformat()
                value_json = json.dumps(value)

                if existing is None:
                    # Key never existed
                    if if_version is not None and if_version != 0:
                        raise VersionConflict(
                            f"Key '{key}' does not exist (expected version {if_version})"
                        )
                    new_version = 1
                    created = True
                else:
                    is_deleted = existing["deleted"] == 1
                    curr_version = existing["version"]

                    if is_deleted:
                        # Existing tombstone
                        if if_version is not None and if_version != 0:
                            raise VersionConflict(
                                f"Key '{key}' is deleted (cannot match expected version {if_version})"
                            )
                        new_version = curr_version + 1
                        created = True
                    else:
                        # Existing live record
                        if if_version == 0:
                            raise VersionConflict(
                                f"Key '{key}' already exists (create-only specified)"
                            )
                        if if_version is not None and if_version != curr_version:
                            raise VersionConflict(
                                f"Version conflict for key '{key}': current {curr_version}, expected {if_version}"
                            )
                        new_version = curr_version + 1
                        created = False

                # Read and increment last_applied_seq
                last_applied_seq = int(self._get_meta(cur, "last_applied_seq"))
                new_seq = last_applied_seq + 1

                # Update or insert into kv
                cur.execute(
                    """
                    INSERT INTO kv (key, value, version, deleted, updated_at)
                    VALUES (?, ?, ?, 0, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        version = excluded.version,
                        deleted = excluded.deleted,
                        updated_at = excluded.updated_at;
                    """,
                    (key, value_json, new_version, now),
                )

                # Append to oplog
                cur.execute(
                    """
                    INSERT INTO oplog (seq, epoch, key, op, value, version, ts)
                    VALUES (?, ?, ?, 'PUT', ?, ?, ?);
                    """,
                    (new_seq, epoch, key, value_json, new_version, now),
                )

                # Update meta last_applied_seq
                cur.execute(
                    "UPDATE meta SET v = ? WHERE k = 'last_applied_seq';",
                    (str(new_seq),),
                )

                cur.execute("COMMIT;")
                return PutResult(version=new_version, seq=new_seq, created=created)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    def get(self, key: str) -> Record | None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT key, value, version, deleted, updated_at FROM kv WHERE key = ?;",
                (key,),
            )
            row = cur.fetchone()
            if row is None or row["deleted"] == 1:
                return None
            return Record(
                key=row["key"],
                value=json.loads(row["value"]),
                version=row["version"],
                deleted=row["deleted"],
                updated_at=row["updated_at"],
            )

    def apply_replicated(
        self,
        epoch: int,
        seq: int,
        op: str,
        key: str,
        value: dict[str, Any] | None,
        version: int,
    ) -> str:
        """Apply a replicated operation coming from the primary.

        Returns "APPLIED" if the operation was applied, "DUPLICATE" if the
        sequence number is less than or equal to the last applied sequence.
        Raises ``ReplicationGap`` if ``seq`` is greater than ``last_seq + 1``.
        """
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                last_seq = int(self._get_meta(cur, "last_applied_seq"))
                if seq > last_seq + 1:
                    raise ReplicationGap(last_seq)
                if seq <= last_seq:
                    # Duplicate or replay; no state change
                    cur.execute("COMMIT;")
                    return "DUPLICATE"

                # seq == last_seq + 1 : apply operation
                now = datetime.now(UTC).isoformat()
                if op.upper() == "DELETE":
                    # For delete, we ignore the provided value and mark tombstone
                    cur.execute(
                        """
                        INSERT INTO kv (key, value, version, deleted, updated_at)
                        VALUES (?, ?, ?, 1, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            version = excluded.version,
                            deleted = 1,
                            updated_at = excluded.updated_at;
                        """,
                        (key, "{}", version, now),
                    )
                    value_json = None
                else:
                    # PUT operation
                    value_json = json.dumps(value) if value is not None else "{}"
                    cur.execute(
                        """
                        INSERT INTO kv (key, value, version, deleted, updated_at)
                        VALUES (?, ?, ?, 0, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value = excluded.value,
                            version = excluded.version,
                            deleted = 0,
                            updated_at = excluded.updated_at;
                        """,
                        (key, value_json, version, now),
                    )

                # Append to oplog with same seq/epoch
                cur.execute(
                    """
                    INSERT INTO oplog (seq, epoch, key, op, value, version, ts)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    (seq, epoch, key, op.upper(), value_json, version, now),
                )

                # Update metadata
                cur.execute(
                    "UPDATE meta SET v = ? WHERE k = 'last_applied_seq';",
                    (str(seq),),
                )
                cur.execute(
                    "UPDATE meta SET v = ? WHERE k = 'last_log_epoch';",
                    (str(epoch),),
                )
                cur.execute("COMMIT;")
                return "APPLIED"
            except Exception:
                cur.execute("ROLLBACK;")
                raise
    def delete(self, key: str, epoch: int) -> DeleteResult:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    "SELECT key, value, version, deleted, updated_at FROM kv WHERE key = ?;",
                    (key,),
                )
                existing = cur.fetchone()
                if existing is None or existing["deleted"] == 1:
                    raise NotFound(f"Key '{key}' not found or already deleted")

                new_version = existing["version"] + 1
                now = datetime.now(UTC).isoformat()

                last_applied_seq = int(self._get_meta(cur, "last_applied_seq"))
                new_seq = last_applied_seq + 1

                # Update tombstone in kv
                cur.execute(
                    """
                    UPDATE kv
                    SET deleted = 1, version = ?, updated_at = ?
                    WHERE key = ?;
                    """,
                    (new_version, now, key),
                )

                # Append to oplog
                cur.execute(
                    """
                    INSERT INTO oplog (seq, epoch, key, op, value, version, ts)
                    VALUES (?, ?, ?, 'DELETE', NULL, ?, ?);
                    """,
                    (new_seq, epoch, key, new_version, now),
                )

                # Update meta last_applied_seq
                cur.execute(
                    "UPDATE meta SET v = ? WHERE k = 'last_applied_seq';",
                    (str(new_seq),),
                )

                cur.execute("COMMIT;")
                return DeleteResult(version=new_version, seq=new_seq)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    def scan(self, prefix: str) -> list[Record]:
        with self._lock:
            cur = self._conn.cursor()
            # Prefix search for live records ordered by key
            # SQLite LIKE wildcard pattern: escape % and _ if needed, or query key >= prefix and key < next_prefix
            cur.execute(
                """
                SELECT key, value, version, deleted, updated_at
                FROM kv
                WHERE key LIKE ? AND deleted = 0
                ORDER BY key ASC;
                """,
                (f"{prefix}%",),
            )
            rows = cur.fetchall()
            return [
                Record(
                    key=row["key"],
                    value=json.loads(row["value"]),
                    version=row["version"],
                    deleted=row["deleted"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]

    def stats(self) -> dict[str, Any]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*) FROM kv WHERE deleted = 0;")
            records_count = cur.fetchone()[0]

            last_seq = int(self._get_meta(cur, "last_applied_seq"))
            epoch = int(self._get_meta(cur, "epoch"))
            role = self._get_meta(cur, "role")

            # Namespace counts: text before the first ":" in each live key.
            cur.execute("SELECT key FROM kv WHERE deleted = 0;")
            ns_request = 0
            ns_resource = 0
            ns_other = 0
            for (key,) in cur.fetchall():
                ns = key.split(":", 1)[0] if ":" in key else ""
                if ns == "request":
                    ns_request += 1
                elif ns == "resource":
                    ns_resource += 1
                else:
                    ns_other += 1

            record_count = {
                "request": ns_request,
                "resource": ns_resource,
                "other": ns_other,
            }

            return {
                "records": records_count,
                "last_seq": last_seq,
                "epoch": epoch,
                "role": role,
                "recordCount": record_count,
                "record_count": record_count,
            }

    def update_meta(
        self,
        role: str | None = None,
        epoch: int | str | None = None,
    ) -> None:
        """Persist role and/or epoch received from the coordinator."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                if role is not None:
                    cur.execute(
                        "UPDATE meta SET v = ? WHERE k = 'role';",
                        (role.upper(),),
                    )
                if epoch is not None:
                    cur.execute(
                        "UPDATE meta SET v = ? WHERE k = 'epoch';",
                        (str(epoch),),
                    )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()
