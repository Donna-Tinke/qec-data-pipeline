"""Read/write Parquet objects in the lake (local folder or MinIO bucket).

Keys are lake-relative, e.g. "silver/qec_syndromes/syndrome_observation.parquet"
or "ml/ml_syndrome_decoder_example.parquet".
"""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import minio_client


def write_parquet(table: pa.Table, key: str, settings: Settings) -> str:
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    data = buffer.getvalue()

    if settings.lake_backend == "local":
        target = settings.local_lake_root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return str(target)

    client = minio_client(settings)
    client.put_object(
        settings.s3_bucket,
        key,
        io.BytesIO(data),
        len(data),
        content_type="application/octet-stream",
    )
    return f"s3://{settings.s3_bucket}/{key}"


def read_parquet(key: str, settings: Settings) -> pa.Table:
    if settings.lake_backend == "local":
        return pq.read_table(settings.local_lake_root / key)
    client = minio_client(settings)
    response = client.get_object(settings.s3_bucket, key)
    try:
        return pq.read_table(io.BytesIO(response.read()))
    finally:
        response.close()
        response.release_conn()
