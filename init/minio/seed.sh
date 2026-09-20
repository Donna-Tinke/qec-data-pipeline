#!/bin/sh
set -eu

alias_name="course"
endpoint="http://minio:9000"
bucket="${S3_BUCKET:-quantum-lake}"

until mc alias set "$alias_name" "$endpoint" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  echo "Waiting for object storage..."
  sleep 1
done

mc mb --ignore-existing "$alias_name/$bucket"
mc version enable "$alias_name/$bucket" >/dev/null
mc anonymous set none "$alias_name/$bucket" >/dev/null

mc mirror --overwrite /seed/raw "$alias_name/$bucket/bronze"
mc mirror --overwrite /seed/metadata "$alias_name/$bucket/metadata/course-release"

echo "Copied the unchanged course inputs to $alias_name/$bucket."
mc ls --recursive "$alias_name/$bucket/bronze"
