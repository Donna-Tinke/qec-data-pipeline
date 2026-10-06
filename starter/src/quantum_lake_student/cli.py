"""Command-line entry point for the student workspace."""

from __future__ import annotations

import argparse
import sys

from rich.console import Console
from rich.table import Table

from .config import Settings
from .connections import bronze_inventory, check_platform
from .runner import run_pipeline


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


def command_run(settings: Settings) -> int:
    console.print("[bold cyan]Starting Quantum Lake Pipeline (Part I)...[/bold cyan]\n")
    try:
        run_data = run_pipeline(settings)
    except Exception as exc:
        console.print(f"[bold red]Pipeline failed:[/bold red] {exc}")
        return 1

    table = Table(title=f"Part I Pipeline Run Results ({run_data['run_id']})")
    table.add_column("Stage", style="cyan")
    table.add_column("Input Count", justify="right")
    table.add_column("Output Count", justify="right")
    table.add_column("Issue Count", justify="right")
    table.add_column("Status", style="green")

    for name, res in run_data["stages"].items():
        table.add_row(
            name,
            f"{res.input_count:,}",
            f"{res.output_count:,}",
            f"{res.issue_count:,}",
            "[green]SUCCESS[/green]",
        )

    console.print(table)
    console.print(
        f"\n[green]Completed in {run_data['duration_seconds']:.2f}s.[/green] "
        "Generated deliverables written to [bold]results/part1/[/bold]."
    )
    return 0


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
