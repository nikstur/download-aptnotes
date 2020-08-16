import asyncio
import sqlite3
from pathlib import Path
from queue import Queue
from sqlite3 import Connection, Cursor, OperationalError
from threading import Condition, Event
from typing import Tuple

import aiofiles


def save_to_files(
    queue: Queue, condition: Condition, finish_event: Event, directory: Path
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    asyncio.run(save_and_write_to_files(queue, condition, finish_event, directory))


async def save_and_write_to_files(
    queue: Queue, condition: Condition, finish_event: Event, directory: Path,
) -> None:
    while not finish_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                buffer, aptnote = queue.get()
            finally:
                queue.task_done()
        await write_file(buffer, directory, aptnote["filename"])
    print(f"No. of downloaded files {len(list(directory.iterdir()))}")


async def write_file(buffer: bytes, directory: Path, filename: str) -> None:
    path = directory / filename
    path = path.with_suffix(".pdf")
    async with aiofiles.open(path, mode="wb") as f:
        await f.write(buffer)


def save_to_db(
    queue: Queue, condition: Condition, finish_event: Event, path: Path
) -> None:
    connection, cursor = setup_db_connection(path)

    inserted_values = 0
    while not finish_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                augmented_aptnote = queue.get()
            except Exception as e:
                print(e)
            finally:
                queue.task_done()
        insert_values(cursor, augmented_aptnote)
        connection.commit()
        inserted_values += 1

    connection.close()
    print(f"Downloaded and parsed {inserted_values} files.")


def setup_db_connection(path: Path) -> Tuple[Connection, Cursor]:
    """Setup DB connection"""
    connection: Connection = sqlite3.connect(path)
    cursor: Cursor = connection.cursor()
    try:
        cursor.execute("DROP TABLE aptnotes")
    except OperationalError:
        pass
    create_table(cursor)
    connection.commit()
    return connection, cursor


def create_table(cursor: Cursor) -> None:
    """Create aptnotes table in database"""
    cursor.execute(
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


def insert_values(cursor: Cursor, parameters: dict) -> None:
    """Insert values of parameters into database"""
    cursor.execute(
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
