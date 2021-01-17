import asyncio
import logging
from enum import Enum
from pathlib import Path
from typing import Optional

import typer

from . import utils
from .main import DownloadAPTNotes
from .parsing import OptionalDepedencyMissing

logging.basicConfig(level=logging.INFO, format="%(message)s")

app = typer.Typer()


class Format(str, Enum):
    sqlite = "sqlite"
    pdf = "pdf"
    json = "json"
    csv = "csv"


@app.command()
def main(
    form: Format = typer.Option(..., "--format", "-f", help="Output format"),
    path: Path = typer.Option(
        ..., "--output", "-o", help="Output path of file or directory"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l", help="Number of files to download"
    ),
    parallel: int = typer.Option(
        10, "--parallel", "-p", help="Number of parallell downloads"
    ),
) -> None:
    """Download and (optionally) parse APTNotes quickly and easily"""
    asyncio.run(async_main(form, path, limit, parallel))


@utils.log_duration("DEBUG", "Total time:")
async def async_main(form: Format, path: Path, limit: Optional[int], parallel: int):
    aptnotesdownload = DownloadAPTNotes()
    await aptnotesdownload.add_downloading(parallel, limit)
    try:
        await aptnotesdownload.add_saving(form.value, ensure_path(path))
    except OptionalDepedencyMissing:
        typer.echo(
            "Optional dependency tika is not installed. Only format 'pdf' is available."
        )
        raise typer.Exit()
    aptnotesdownload.start()


def ensure_path(path: Path) -> Path:
    """Ensure that relative and absolute paths are treated accordingly"""
    if path.is_absolute():
        return path
    else:
        return Path.cwd() / path
