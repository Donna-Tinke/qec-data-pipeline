#!/usr/bin/env python3
"""Perform friendly health checks for the local teaching platform."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class HttpCheck:
    name: str
    url: str


def check_http(item: HttpCheck, attempts: int = 12) -> bool:
    """Wait briefly for an HTTP service to survive a cold first start."""

    ok = False
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(item.url, timeout=5) as response:
                ok = response.status < 500
        except (OSError, TimeoutError):
            ok = False
        if ok:
            break
        if attempt + 1 < attempts:
            time.sleep(2)
    print(f"{'OK' if ok else 'FAIL'} {item.name}: {item.url}")
    return ok


def check_port(name: str, host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=5):
            ok = True
    except OSError:
        ok = False
    print(f"{'OK' if ok else 'FAIL'} {name}: {host}:{port}")
    return ok


def check_postgres() -> bool:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "pg_isready",
            "-U",
            os.getenv("POSTGRES_USER", "quantum"),
            "-d",
            os.getenv("POSTGRES_DB", "quantum_lake"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    ok = result.returncode == 0
    print(f"{'OK' if ok else 'FAIL'} PostgreSQL readiness")
    return ok


def main() -> None:
    checks = [
        check_http(
            HttpCheck(
                "MinIO",
                f"http://localhost:{os.getenv('MINIO_API_PORT', '9000')}/minio/health/live",
            )
        ),
        check_http(
            HttpCheck(
                "Adminer",
                f"http://localhost:{os.getenv('ADMINER_PORT', '8080')}/",
            )
        ),
        check_http(
            HttpCheck(
                "JupyterLab",
                f"http://localhost:{os.getenv('JUPYTER_PORT', '8888')}/lab",
            )
        ),
        check_port(
            "PostgreSQL",
            "localhost",
            int(os.getenv("POSTGRES_PORT", "5432")),
        ),
        check_postgres(),
    ]
    if not all(checks):
        print(
            "One or more services are not ready. Run `make ps` and `make logs`.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    print("The course platform is ready.")


if __name__ == "__main__":
    main()
