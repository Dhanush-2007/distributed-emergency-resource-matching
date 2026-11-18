"""FastAPI application for a storage node."""

import os
from pathlib import Path
from typing import Any

from ems_common.errors import (
    NotFound,
    NotPrimary,
    StaleEpoch,
    register_error_handlers,
)
from ems_common.logging import register_logging_middleware, setup_json_logger
from fastapi import FastAPI, Query, Response, status
from pydantic import BaseModel, Field

from app.db import StorageDB


class Settings:
    def __init__(self) -> None:
        self.node_id: str = os.getenv("NODE_ID", "s0-a")
        self.shard_id: int = int(os.getenv("SHARD_ID", "0"))
        raw_data_dir = os.getenv("DATA_DIR", "/data")

        # Determine writable data directory
        data_path = Path(raw_data_dir)
        try:
            data_path.mkdir(parents=True, exist_ok=True)
            # Test write access
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
) -> FastAPI:
    """Factory to create a configured storage node FastAPI application."""
    nid = node_id or settings.node_id
    sid = shard_id if shard_id is not None else settings.shard_id

    if db_instance is not None:
        local_db = db_instance
    elif data_dir is not None:
        p = Path(data_dir)
        p.mkdir(parents=True, exist_ok=True)
        local_db = StorageDB(p / f"{nid}.db")
    else:
        local_db = db

    application = FastAPI(title=f"Storage Node {nid}")
    register_logging_middleware(application)
    register_error_handlers(application)

    @application.get("/health/live")
    def health_live() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health/ready")
    def health_ready() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health")
    def health() -> dict[str, Any]:
        stats = local_db.stats()
        return {
            "nodeId": nid,
            "shardId": sid,
            "role": stats["role"],
            "status": "READY",
            "epoch": stats["epoch"],
            "lastSeq": stats["last_seq"],
            "records": stats["records"],
        }

    @application.put("/internal/data/{key:path}")
    def put_data(
        key: str, payload: PutDataRequest, response: Response
    ) -> dict[str, Any]:
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
