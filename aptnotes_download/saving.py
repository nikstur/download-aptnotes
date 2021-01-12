import asyncio
import csv
import io
import json
from asyncio import Queue
from pathlib import Path
from sqlite3 import OperationalError
from threading import Condition, Event

import aiofiles
import aiosqlite
import uvloop


def save(
    form: str, queue: Queue, condition: Condition, finish_event: Event, path: Path
) -> None:
    uvloop.install()
    if form == "sqlite":
        asyncio.run(save_to_sqlite(queue, condition, finish_event, path))
    if form == "pdf":
        asyncio.run(save_to_files(queue, condition, finish_event, path))
    if form == "json":
        save_to_json(queue, condition, finish_event, path)
    if form == "csv":
        save_to_csv(queue, condition, finish_event, path)


def save_to_csv(queue: Queue, condition: Condition, finish_event: Event, path: Path):
    fieldnames = (
        "unique_id",
        "filename",
        "title",
        "source",
        "splash_url",
        "sha1",
        "date",
        "file_url",
        "fulltext",
        "creation_date",
        "creator_tool",
        "creator_title",
    )
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    while not finish_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                augmented_aptnote = queue.get_nowait()
            finally:
                queue.task_done()
        writer.writerow(augmented_aptnote)
    with open(path, "wt") as f:
        print(buffer.getvalue(), file=f)


def save_to_json(queue: Queue, condition: Condition, finish_event: Event, path: Path):
    aptnotes = []
    while not finish_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                augmented_aptnote = queue.get_nowait()
            finally:
                queue.task_done()
        aptnotes.append(augmented_aptnote)
    with open(path, "wt") as f:
        json.dump(aptnotes, f, sort_keys=True, indent=2)


async def save_to_files(
    queue: Queue, condition: Condition, finish_event: Event, directory: Path,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    while not finish_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                buffer, aptnote = queue.get_nowait()
            finally:
                queue.task_done()
        await write_file(buffer, directory, aptnote["filename"])
    print(f"No. of downloaded files {len(list(directory.iterdir()))}")


async def write_file(buffer: bytes, directory: Path, filename: str) -> None:
    path = directory / filename
    path = path.with_suffix(".pdf")
    async with aiofiles.open(path, mode="wb") as f:  # type: ignore
        await f.write(buffer)


async def save_to_sqlite(
    queue: Queue, condition: Condition, finish_event: Event, path: Path
) -> None:
    async with aiosqlite.connect(path) as db:
        await db_init(db)

        inserted_values = 0
        while not finish_event.is_set() or not queue.empty():
            with condition:
                while queue.empty():
                    condition.wait()
                try:
                    augmented_aptnote = queue.get_nowait()
                except Exception as e:
                    print(e)
                finally:
                    queue.task_done()
            await insert_values(db, augmented_aptnote)
            await db.commit()
            inserted_values += 1

    print(f"Downloaded and parsed {inserted_values} files.")


async def db_init(db: aiosqlite.Connection) -> None:
    try:
        await db.execute("DROP TABLE aptnotes")
    except OperationalError:
        pass
    await create_table(db)
    await db.commit()


async def create_table(db: aiosqlite.Connection) -> None:
    """Create aptnotes table in database"""
    await db.execute(
        """
        CREATE TABLE aptnotes (
            id integer,
            filename text,
            title text,
            source text,
            splash_url text,
            sha1 text,
            date date,
            file_url text,
            fulltext text,
            creation_date datetime,
            creator_tool text,
            creator_title text
        )
        """
    )


async def insert_values(db: aiosqlite.Connection, parameters: dict) -> None:
    """Insert values of parameters into database"""
    await db.execute(
        """
        INSERT INTO aptnotes VALUES (
            :unique_id,
            :filename,
            :title,
            :source,
            :splash_url,
            :sha1,
            :date,
            :file_url,
            :fulltext,
            :creation_date,
            :creator_tool,
            :creator_title
        )
        """,
        parameters,
    )
