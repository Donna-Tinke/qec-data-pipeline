"""Supplied connection helpers for the local course platform."""

from __future__ import annotations

from urllib.parse import urlparse

import psycopg
from minio import Minio

from .config import Settings


def minio_client(settings: Settings) -> Minio:
    parsed = urlparse(settings.s3_endpoint)
    endpoint = parsed.netloc or parsed.path
    return Minio(
        endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        secure=parsed.scheme == "https",
    )


def bronze_inventory(settings: Settings) -> list[tuple[str, int]]:
    if settings.lake_backend == "local":
        raw = settings.local_lake_root / "bronze"
        if not raw.exists():
            # The downloadable archive retains its packaging directory name;
            # the course platform seeds that same content as Bronze.
            raw = settings.local_lake_root / "raw"
        return [
            (path.relative_to(settings.local_lake_root).as_posix(), path.stat().st_size)
            for path in sorted(raw.rglob("*"))
            if path.is_file()
        ]
    client = minio_client(settings)
    return [
        (item.object_name, item.size or 0)
        for item in client.list_objects(
            settings.s3_bucket, prefix="bronze/", recursive=True
        )
    ]


def postgres_connection(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(settings.postgres_dsn)


def check_platform(settings: Settings) -> dict[str, str]:
    inventory = bronze_inventory(settings)
    if not inventory:
        raise RuntimeError(
            "No Bronze objects found. From infrastructure/, run `make seed`."
        )
    with postgres_connection(settings) as connection:
        row = connection.execute(
            "SELECT value FROM course_admin.platform_info WHERE key = 'platform'"
        ).fetchone()
    if not row:
        raise RuntimeError("The course platform metadata table is unavailable.")
    return {
        "object_store": f"{len(inventory)} Bronze object(s) available",
        "postgres": str(row[0]),
    }
