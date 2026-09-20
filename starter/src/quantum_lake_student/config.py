"""Runtime configuration shared by the supplied connection helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    lake_backend: str
    local_lake_root: Path
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_environment(cls) -> "Settings":
        settings = cls(
            lake_backend=os.getenv("LAKE_BACKEND", "minio"),
            local_lake_root=Path(os.getenv("LOCAL_LAKE_ROOT", "lake")).resolve(),
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            s3_access_key=os.getenv("S3_ACCESS_KEY", "quantum"),
            s3_secret_key=os.getenv("S3_SECRET_KEY", "quantum-course-only"),
            s3_bucket=os.getenv("S3_BUCKET", "quantum-lake"),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "quantum_lake"),
            postgres_user=os.getenv("POSTGRES_USER", "quantum"),
            postgres_password=os.getenv(
                "POSTGRES_PASSWORD", "quantum-course-only"
            ),
        )
        if settings.lake_backend not in {"minio", "local"}:
            raise ValueError("LAKE_BACKEND must be either 'minio' or 'local'")
        return settings

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

