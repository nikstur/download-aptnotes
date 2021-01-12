import asyncio
from enum import Enum
from pathlib import Path
from typing import Optional

import typer

from .main import APTNotesDownload

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


async def async_main(form: Format, path: Path, limit: Optional[int], parallel: int):
    aptnotesdownload = APTNotesDownload()
    await aptnotesdownload.add_downloading(parallel, limit)
    await aptnotesdownload.add_saving(form.value, ensure_path(path))
    aptnotesdownload.start()


def ensure_path(path: Path) -> Path:
    """Ensure that relative and absolute paths are treating accordingly"""
    if path.is_absolute():
        return path
    else:
        return Path.cwd() / path
