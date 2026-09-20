"""Command-line entry point for the student workspace."""

from __future__ import annotations

import argparse
import sys

from rich.console import Console
from rich.table import Table

from .config import Settings
from .connections import bronze_inventory, check_platform


console = Console()


def command_check(settings: Settings) -> int:
    result = check_platform(settings)
    for service, message in result.items():
        console.print(f"[green]OK[/green] {service}: {message}")
    return 0


def command_inventory(settings: Settings) -> int:
    table = Table(title="Supplied course data files (kept unchanged)")
    table.add_column("Stored path")
    table.add_column("Bytes", justify="right")
    for key, size in bronze_inventory(settings):
        table.add_row(key, f"{size:,}")
    console.print(table)
    return 0


def command_run(_: Settings) -> int:
    console.print(
        "[yellow]Pipeline stages are intentionally unimplemented.[/yellow]\n"
        "Implement your pipeline modules under src/quantum_lake_student, then "
        "replace this command with your orchestrated Part I runner."
    )
    return 2


def command_train(_: Settings) -> int:
    console.print(
        "[yellow]The AI/ML stage is intentionally unimplemented.[/yellow]\n"
        "Consume the required ML input tables through the supplied helpers and "
        "write model files and the required results/part2 files."
    )
    return 2


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "command",
        choices=("check", "inventory", "run", "train"),
        help="Action to perform",
    )
    return result


def main() -> None:
    arguments = parser().parse_args()
    settings = Settings.from_environment()
    commands = {
        "check": command_check,
        "inventory": command_inventory,
        "run": command_run,
        "train": command_train,
    }
    raise SystemExit(commands[arguments.command](settings))


if __name__ == "__main__":
    main()
