"""FastAPI application for a storage node."""

import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from ems_common.errors import (
    Gap,
    NotFound,
    NotPrimary,
    NotReplica,
    StaleEpoch,
    StorageUnavailable,
    register_error_handlers,
)
from ems_common.logging import register_logging_middleware, setup_json_logger
from fastapi import Body, FastAPI, Query, Response, status
from pydantic import BaseModel, Field

from storage_node.db import ReplicationGap, StorageDB


class Settings:
    def __init__(self) -> None:
        self.node_id: str = os.getenv("NODE_ID", "s0-a")
        self.shard_id: int = int(os.getenv("SHARD_ID", "0"))
        raw_data_dir = os.getenv("DATA_DIR", "/data")

        # Determine writable data directory
        data_path = Path(raw_data_dir)
        try:
            data_path.mkdir(parents=True, exist_ok=True)
            test_file = data_path / ".perm_check"
            test_file.touch()
            test_file.unlink()
            self.data_dir: Path = data_path
        except (PermissionError, OSError):
            local_fallback = Path("./data") / self.node_id
            local_fallback.mkdir(parents=True, exist_ok=True)
            self.data_dir: Path = local_fallback

        self.port: int = int(os.getenv("PORT", "9000"))
        self.standalone: bool = (
            os.getenv("STANDALONE", "true").lower() in ("true", "1", "yes")
        )
        self.coordinator_url: str = os.getenv(
            "COORDINATOR_URL", "http://coordinator:8000"
        )
        self.advertise_url: str = os.getenv("ADVERTISE_URL", "")
        self.initial_role: str = os.getenv("INITIAL_ROLE", "primary")
        self.peer_url: str = os.getenv("PEER_URL", "").rstrip("/")
        self.replication_timeout: float = float(os.getenv("REPLICATION_TIMEOUT_SEC", "2"))
        self.heartbeat_interval: float = float(
            os.getenv("HEARTBEAT_INTERVAL_SEC", "2")
        )
        self.http_timeout: float = float(os.getenv("HTTP_TIMEOUT_SEC", "3"))

        self.db_path: Path = self.data_dir / f"{self.node_id}.db"


settings = Settings()
logger = setup_json_logger(service_name=f"storage-node-{settings.node_id}")
db = StorageDB(settings.db_path)


class PutDataRequest(BaseModel):
    value: dict[str, Any] = Field(...)
    ifVersion: int | None = None
    epoch: int | None = None


def create_app(
    node_id: str | None = None,
    shard_id: int | None = None,
    data_dir: Path | str | None = None,
    db_instance: StorageDB | None = None,
    standalone: bool | None = None,
    coordinator_url: str | None = None,
    advertise_url: str | None = None,
    initial_role: str | None = None,
    peer_url: str | None = None,
    replication_timeout: float | None = None,
    heartbeat_interval: float | None = None,
    http_timeout: float | None = None,
    http_client: Any = None,
    initial_backoff: float | None = None,
    max_backoff: float | None = None,
) -> FastAPI:
    """Factory to create a configured storage node FastAPI application."""
    nid = node_id or settings.node_id
    sid = shard_id if shard_id is not None else settings.shard_id
    is_standalone = standalone if standalone is not None else settings.standalone

    if db_instance is not None:
        local_db = db_instance
    elif data_dir is not None:
        p = Path(data_dir)
        p.mkdir(parents=True, exist_ok=True)
        local_db = StorageDB(p / f"{nid}.db")
    else:
        local_db = db

    # coordinator_client and replica_monitor
    _coord_client: Any = None
    _replica_monitor: Any = None
    _recovery_manager: Any = None
    _replica_ack_seq: int | None = None
    _write_lock = threading.Lock()

    def _get_replica_ack_seq() -> int | None:
        return _replica_ack_seq

    effective_peer_url = peer_url if peer_url is not None else settings.peer_url
    effective_coord_url = coordinator_url or settings.coordinator_url
    effective_repl_timeout = (
        replication_timeout
        if replication_timeout is not None
        else settings.replication_timeout
    )

    def _on_registered(assigned_role: str, assigned_epoch: int) -> None:
        if assigned_role == "REPLICA" and _recovery_manager is not None:
            _recovery_manager.run_full_recovery()

    def _on_coord_status_change(new_status: str) -> None:
        pass

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[misc]
        nonlocal _coord_client, _replica_monitor, _recovery_manager
        if not is_standalone:
            from storage_node.coordinator_client import CoordinatorClient
            from storage_node.recovery import FollowerRecoveryManager
            from storage_node.replication import ReplicaMonitor

            if effective_peer_url:
                _recovery_manager = FollowerRecoveryManager(
                    node_id=nid,
                    peer_url=effective_peer_url,
                    db=local_db,
                    heartbeat_interval=(
                        heartbeat_interval
                        if heartbeat_interval is not None
                        else settings.heartbeat_interval
                    ),
                    http_client=http_client,
                    on_status_change=lambda s: _coord_client._set_status(s)
                    if _coord_client
                    else None,
                    status_getter=lambda: _coord_client.status
                    if _coord_client
                    else "STARTING",
                )
                _recovery_manager.start()

            kwargs: dict[str, Any] = {
                "node_id": nid,
                "shard_id": sid,
                "advertise_url": (
                    advertise_url
                    if advertise_url is not None
                    else (settings.advertise_url or f"http://{nid}:{settings.port}")
                ),
                "initial_role": initial_role or settings.initial_role,
                "coordinator_url": effective_coord_url,
                "db": local_db,
                "heartbeat_interval": (
                    heartbeat_interval
                    if heartbeat_interval is not None
                    else settings.heartbeat_interval
                ),
                "http_timeout": (
                    http_timeout
                    if http_timeout is not None
                    else settings.http_timeout
                ),
                "http_client": http_client,
                "get_replica_ack_seq": _get_replica_ack_seq,
                "on_registered": _on_registered,
            }
            if initial_backoff is not None:
                kwargs["initial_backoff"] = initial_backoff
            if max_backoff is not None:
                kwargs["max_backoff"] = max_backoff

            _coord_client = CoordinatorClient(**kwargs)
            _coord_client.start()

            if effective_peer_url:
                _replica_monitor = ReplicaMonitor(
                    peer_url=effective_peer_url,
                    heartbeat_interval=(
                        heartbeat_interval
                        if heartbeat_interval is not None
                        else settings.heartbeat_interval
                    ),
                    http_client=http_client,
                )
                _replica_monitor.start()

        yield
        if _recovery_manager is not None:
            _recovery_manager.stop()
        if _replica_monitor is not None:
            _replica_monitor.stop()
        if _coord_client is not None:
            _coord_client.stop()

    application = FastAPI(title=f"Storage Node {nid}", lifespan=lifespan)
    register_logging_middleware(application)
    register_error_handlers(application)

    application.state.get_replica_monitor = lambda: _replica_monitor

    def _node_status() -> str:
        """Return the current node status string."""
        if is_standalone:
            return "READY"
        if _coord_client is None:
            return "STARTING"
        return _coord_client.status

    def _assert_ready() -> None:
        """Raise 503 if the node is not yet registered (non-standalone only)."""
        if not is_standalone and _node_status() not in ("READY", "CATCHING_UP"):
            raise StorageUnavailable(
                "Node is not yet registered with the coordinator"
            )

    @application.get("/health/live")
    def health_live() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health/ready")
    def health_ready() -> dict[str, str]:
        st = _node_status()
        if st != "READY":
            raise StorageUnavailable(f"Node is not READY (status: {st})")
        return {"status": "ok"}

    @application.post("/internal/replicate")
    def replicate_data(payload: dict = Body(...)) -> dict[str, Any]:
        """Endpoint for replicas to receive replication ops from primary.

        Expected payload fields: epoch, seq, op, key, value, version.
        """
        stats = local_db.stats()
        node_role = stats["role"]
        node_epoch = stats["epoch"]

        if node_role != "REPLICA":
            raise NotReplica()

        # Validate epoch and seq
        incoming_epoch = int(payload.get("epoch", 0))
        incoming_seq = int(payload.get("seq", 0))
        op = payload.get("op", "").upper()
        key = payload.get("key")
        value = payload.get("value")
        version = int(payload.get("version", 0))

        if incoming_epoch < node_epoch:
            raise StaleEpoch(details=[{"epoch": node_epoch}])
        if incoming_epoch > node_epoch:
            # adopt newer epoch
            local_db.update_meta(epoch=incoming_epoch)
            node_epoch = incoming_epoch

        try:
            result = local_db.apply_replicated(
                epoch=node_epoch,
                seq=incoming_seq,
                op=op,
                key=key,
                value=value,
                version=version,
            )
        except ReplicationGap as gap_err:
            if _recovery_manager is not None:
                _recovery_manager.trigger_sync()
            raise Gap(details=[{"lastSeq": gap_err.last_seq}])

        # result is "APPLIED" or "DUPLICATE"
        return {"ack": incoming_seq, "lastSeq": local_db.stats()["last_seq"], "result": result}

    @application.get("/internal/sync")
    def sync_data(
        since: int = Query(ge=0),
        epoch: int = Query(...),
        limit: int = Query(default=500, ge=1, le=1000),
    ) -> dict[str, Any]:
        stats = local_db.stats()
        node_role = stats["role"]
        node_epoch = stats["epoch"]
        last_seq = stats["last_seq"]

        if node_role != "PRIMARY":
            raise NotPrimary(f"Node '{nid}' is {node_role}, not PRIMARY")

        # Log matching check:
        # since > my last seq -> snapshotRequired;
        # if since == 0: if log has been pruned and does not start at 1 -> snapshotRequired.
        # if since > 0: entry_epoch_at(since) is None or != epoch -> snapshotRequired.
        snapshot_required = False
        if since > last_seq:
            snapshot_required = True
        elif since == 0:
            if last_seq > 0:
                first_entry = local_db.oplog_since(0, limit=1)
                if not first_entry or first_entry[0]["seq"] != 1:
                    snapshot_required = True
        elif since > 0:
            match_epoch = local_db.entry_epoch_at(since)
            if match_epoch is None or match_epoch != epoch:
                snapshot_required = True

        if snapshot_required:
            return {
                "snapshotRequired": True,
                "fromSeq": since,
                "toSeq": since,
                "entries": [],
                "lastSeq": last_seq,
                "epoch": node_epoch,
            }

        entries = local_db.oplog_since(since=since, limit=limit)
        to_seq = entries[-1]["seq"] if entries else since

        return {
            "snapshotRequired": False,
            "fromSeq": since,
            "toSeq": to_seq,
            "entries": entries,
            "lastSeq": last_seq,
            "epoch": node_epoch,
        }

    @application.get("/internal/snapshot")
    def snapshot_data() -> dict[str, Any]:
        stats = local_db.stats()
        node_role = stats["role"]
        if node_role != "PRIMARY":
            raise NotPrimary(f"Node '{nid}' is {node_role}, not PRIMARY")

        return local_db.export_snapshot()

    @application.get("/health")
    def health() -> dict[str, Any]:
        stats = local_db.stats()
        return {
            "nodeId": nid,
            "shardId": sid,
            "role": stats["role"],
            "status": _node_status(),
            "epoch": stats["epoch"],
            "lastSeq": stats["last_seq"],
            "records": stats["records"],
            "recordCount": stats["recordCount"],
        }

    @application.put("/internal/data/{key:path}")
    def put_data(
        key: str, payload: PutDataRequest, response: Response
    ) -> dict[str, Any]:
        nonlocal _replica_ack_seq
        stats = local_db.stats()
        node_role = stats["role"]
        node_epoch = stats["epoch"]

        if node_role != "PRIMARY":
            raise NotPrimary(f"Node '{nid}' is {node_role}, not PRIMARY")

        _assert_ready()

        if payload.epoch is not None and payload.epoch != node_epoch:
            raise StaleEpoch(
                f"Stale epoch {payload.epoch} provided, current epoch is {node_epoch}"
            )

        with _write_lock:
            res = local_db.put(
                key=key,
                value=payload.value,
                if_version=payload.ifVersion,
                epoch=node_epoch,
            )

            replicated = False
            if not is_standalone:
                from storage_node.replication import replicate_or_degrade

                repl_client = http_client or httpx.Client()
                replicated, ack_seq = replicate_or_degrade(
                    peer_url=effective_peer_url,
                    coordinator_url=effective_coord_url,
                    replica_monitor=_replica_monitor,
                    http_client=repl_client,
                    replication_timeout=effective_repl_timeout,
                    node_id=nid,
                    shard_id=sid,
                    epoch=node_epoch,
                    seq=res.seq,
                    op="PUT",
                    key=key,
                    value=payload.value,
                    version=res.version,
                )
                if ack_seq is not None:
                    _replica_ack_seq = (
                        max(_replica_ack_seq, ack_seq)
                        if _replica_ack_seq is not None
                        else ack_seq
                    )

        if res.created:
            response.status_code = status.HTTP_201_CREATED
        else:
            response.status_code = status.HTTP_200_OK

        logger.info(
            "Stored key",
            extra={
                "event": "kv.put",
                "key": key,
                "version": res.version,
                "seq": res.seq,
                "is_created": res.created,
                "replicated": replicated,
            },
        )

        return {
            "key": key,
            "version": res.version,
            "seq": res.seq,
            "replicated": replicated,
        }

    @application.get("/internal/data/{key:path}")
    def get_data(key: str) -> dict[str, Any]:
        record = local_db.get(key)
        if record is None:
            raise NotFound(f"Key '{key}' not found")

        logger.info(
            "Retrieved key",
            extra={
                "event": "kv.get",
                "key": key,
                "version": record.version,
            },
        )

        return {
            "key": record.key,
            "value": record.value,
            "version": record.version,
        }

    @application.delete("/internal/data/{key:path}")
    def delete_data(
        key: str,
        epoch: int | None = Query(None),
    ) -> dict[str, Any]:
        nonlocal _replica_ack_seq
        stats = local_db.stats()
        node_role = stats["role"]
        node_epoch = stats["epoch"]

        if node_role != "PRIMARY":
            raise NotPrimary(f"Node '{nid}' is {node_role}, not PRIMARY")

        _assert_ready()

        if epoch is not None and epoch != node_epoch:
            raise StaleEpoch(
                f"Stale epoch {epoch} provided, current epoch is {node_epoch}"
            )

        with _write_lock:
            res = local_db.delete(key=key, epoch=node_epoch)

            replicated = False
            if not is_standalone:
                from storage_node.replication import replicate_or_degrade

                repl_client = http_client or httpx.Client()
                replicated, ack_seq = replicate_or_degrade(
                    peer_url=effective_peer_url,
                    coordinator_url=effective_coord_url,
                    replica_monitor=_replica_monitor,
                    http_client=repl_client,
                    replication_timeout=effective_repl_timeout,
                    node_id=nid,
                    shard_id=sid,
                    epoch=node_epoch,
                    seq=res.seq,
                    op="DELETE",
                    key=key,
                    value=None,
                    version=res.version,
                )
                if ack_seq is not None:
                    _replica_ack_seq = (
                        max(_replica_ack_seq, ack_seq)
                        if _replica_ack_seq is not None
                        else ack_seq
                    )

        logger.info(
            "Deleted key",
            extra={
                "event": "kv.delete",
                "key": key,
                "version": res.version,
                "seq": res.seq,
                "replicated": replicated,
            },
        )

        return {
            "key": key,
            "version": res.version,
            "seq": res.seq,
            "replicated": replicated,
        }

    @application.get("/internal/scan")
    def scan_data(prefix: str = Query(default="")) -> dict[str, Any]:
        records = local_db.scan(prefix=prefix)

        logger.info(
            "Scanned records",
            extra={
                "event": "kv.scan",
                "prefix": prefix,
                "count": len(records),
            },
        )

        items = [
            {"key": r.key, "value": r.value, "version": r.version}
            for r in records
        ]
        return {
            "items": items,
            "count": len(items),
        }

    return application


app = create_app()
