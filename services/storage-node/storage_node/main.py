"""FastAPI application for a storage node."""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from ems_common.errors import (
    NotFound,
    NotPrimary,
    StaleEpoch,
    StorageUnavailable,
    NotReplica,
    Gap,
    register_error_handlers,
)
from fastapi import FastAPI, Query, Response, status, Body
from pydantic import BaseModel, Field
from storage_node.db import StorageDB, ReplicationGap




from fastapi import FastAPI, Query, Response, status, Body
from pydantic import BaseModel, Field
from storage_node.db import StorageDB, ReplicationGap

from fastapi import FastAPI, Query, Response, status, Body
from pydantic import BaseModel, Field


    NotFound,
    NotPrimary,
    StaleEpoch,
    StorageUnavailable,
    register_error_handlers,
)
from ems_common.logging import register_logging_middleware, setup_json_logger
from fastapi import FastAPI, Query, Response, status, Body
from pydantic import BaseModel, Field
from fastapi import FastAPI, Query, Response, status, Body
from pydantic import BaseModel, Field
from pydantic import BaseModel, Field

from storage_node.db import StorageDB


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

    # coordinator_client is set only when not standalone
    _coord_client: Any = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[misc]
        nonlocal _coord_client
        if not is_standalone:
            from storage_node.coordinator_client import CoordinatorClient

            kwargs: dict[str, Any] = {
                "node_id": nid,
                "shard_id": sid,
                "advertise_url": (
                    advertise_url
                    if advertise_url is not None
                    else (settings.advertise_url or f"http://{nid}:{settings.port}")
                ),
                "initial_role": initial_role or settings.initial_role,
                "coordinator_url": coordinator_url or settings.coordinator_url,
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
            }
            if initial_backoff is not None:
                kwargs["initial_backoff"] = initial_backoff
            if max_backoff is not None:
                kwargs["max_backoff"] = max_backoff

            _coord_client = CoordinatorClient(**kwargs)
            _coord_client.start()
        yield
        if _coord_client is not None:
            _coord_client.stop()

    application = FastAPI(title=f"Storage Node {nid}", lifespan=lifespan)
    register_logging_middleware(application)
    register_error_handlers(application)

    def _node_status() -> str:
        """Return the current node status string."""
        if is_standalone:
            return "READY"
        if _coord_client is None:
            return "STARTING"
        return _coord_client.status

    def _assert_ready() -> None:
        """Raise 503 if the node is not yet registered (non-standalone only)."""
        if not is_standalone and _node_status() != "READY":
            raise StorageUnavailable(
                "Node is not yet registered with the coordinator"
            )

    @application.get("/health/live")
    def health_live() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health/ready")
    def health_ready() -> dict[str, str]:
        return {"status": "ok"}

        @application.post("/internal/replicate")
    def replicate_data(payload: dict = Body(...)) -> dict[str, Any]:
        """Endpoint for replicas to receive replication ops from primary.

        Expected payload fields: epoch, seq, op, key, value, version.
        """
        _assert_ready()

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
            raise Gap(details=[{"lastSeq": gap_err.last_seq}])

        # result is "APPLIED" or "DUPLICATE"
        return {"ack": incoming_seq, "lastSeq": local_db.stats()["last_seq"], "result": result}

    def replicate_data(payload: dict = Body(...)) -> dict[str, Any]:
        """Endpoint for replicas to receive replication ops from primary.

        Expected payload fields: epoch, seq, op, key, value, version.
        """
        _assert_ready()

        stats = local_db.stats()
        node_role = stats["role"]
        node_epoch = stats["epoch"]

        if node_role != "REPLICA":
            raise NotReplica()

        # Validate epoch
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
        except Gap as gap_err:
            raise Gap(details=[{"lastSeq": gap_err.last_seq}])

        # result is "APPLIED" or "DUPLICATE"
        return {"ack": incoming_seq, "lastSeq": local_db.stats()["last_seq"], "result": result}

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
        _assert_ready()

        stats = local_db.stats()
        node_role = stats["role"]
        node_epoch = stats["epoch"]

        if node_role != "PRIMARY":
            raise NotPrimary(f"Node '{nid}' is {node_role}, not PRIMARY")

        if payload.epoch is not None and payload.epoch != node_epoch:
            raise StaleEpoch(
                f"Stale epoch {payload.epoch} provided, current epoch is {node_epoch}"
            )

        res = local_db.put(
            key=key,
            value=payload.value,
            if_version=payload.ifVersion,
            epoch=node_epoch,
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
            },
        )

        return {
            "key": key,
            "version": res.version,
            "seq": res.seq,
            "replicated": False,
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
        _assert_ready()

        stats = local_db.stats()
        node_role = stats["role"]
        node_epoch = stats["epoch"]

        if node_role != "PRIMARY":
            raise NotPrimary(f"Node '{nid}' is {node_role}, not PRIMARY")

        if epoch is not None and epoch != node_epoch:
            raise StaleEpoch(
                f"Stale epoch {epoch} provided, current epoch is {node_epoch}"
            )

        res = local_db.delete(key=key, epoch=node_epoch)

        logger.info(
            "Deleted key",
            extra={
                "event": "kv.delete",
                "key": key,
                "version": res.version,
                "seq": res.seq,
            },
        )

        return {
            "key": key,
            "version": res.version,
            "seq": res.seq,
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
