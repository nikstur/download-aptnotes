import asyncio
import logging
import sqlite3
from pathlib import Path
from queue import Queue
from sqlite3 import Cursor
from threading import Condition, Event
from typing import Dict

import aiofiles


def save_to_files(
    queue: Queue, condition: Condition, finish_event: Event, base_path: Path
) -> None:
    base_path.mkdir(parents=True, exist_ok=True)
    asyncio.run(save_and_write_to_files(queue, condition, finish_event, base_path))


async def save_and_write_to_files(
    queue: Queue, condition: Condition, finish_event: Event, base_path: Path,
) -> None:

    while not finish_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                buffer, aptnote = queue.get()
                logging.debug("Retrieved item from buffer queue")
            finally:
                queue.task_done()

        filename = aptnote.get("filename")
        path = base_path / filename
        path = path.with_suffix(".pdf")
        await write_file(buffer, path)
    print(f"No. of downloaded files {len(list(base_path.iterdir()))}")


async def write_file(buffer: bytes, path: Path) -> None:
    async with aiofiles.open(path, mode="wb") as f:
        await f.write(buffer)


def save_to_db(
    queue: Queue, condition: Condition, finish_event: Event, path: Path
) -> None:
    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    setup_db(cursor)
    connection.commit()

    inserted_values = 0
    retrieved_items = 0
    while not finish_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                aptnote, document = queue.get()
                logging.debug("Retrieved item from parsed doc queue")
                retrieved_items += 1
            except Exception as e:
                print(e)
            finally:
                queue.task_done()

        insert_values(cursor, "aptnotes", aptnote)
        insert_values(cursor, "documents", document)
        connection.commit()

        logging.debug("Inserted values into db")
        inserted_values += 1

    connection.close()
    logging.info(f"No. of retrieved items from parsed doc queue: {retrieved_items}")
    print(f"Downloaded and parsed {inserted_values} files.")


def setup_db(cursor: Cursor) -> None:
    """When database already exists, drop all tables and recreate them"""
    if check_table(cursor, "aptnotes"):
        cursor.execute("DROP TABLE aptnotes")
    create_table(cursor, "aptnotes")

    if check_table(cursor, "documents"):
        cursor.execute("DROP TABLE documents")
    create_table(cursor, "documents")


def check_table(cursor: Cursor, tablename: str) -> bool:
    cursor.execute(
        "SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name=?",
        (tablename,),
    )
    is_there = cursor.fetchone()[0] == 1
    logging.debug(f"{tablename} present: {is_there}")
    return is_there


def create_table(cursor: Cursor, tablename: str) -> None:
    if tablename == "aptnotes":
        cursor.execute(
            """
            CREATE TABLE aptnotes
            (id integer, filename text, title text, source text, splash_url text, sha1 text, date date, file_url text)
            """
        )
    elif tablename == "documents":
        cursor.execute(
            """
            CREATE TABLE documents
            (id integer, fulltext text, creation_date datetime, creator_tool text, creator_title text)
            """
        )


def insert_values(cursor: Cursor, tablename: str, parameters: Dict) -> None:
    if tablename == "aptnotes":
        cursor.execute(
            "INSERT INTO aptnotes VALUES (:unique_id, :filename, :title, :source, :splash_url, :sha1, :date, :file_url)",
            parameters,
        )
    elif tablename == "documents":
        cursor.execute(
            "INSERT INTO documents VALUES (:unique_id, :fulltext, :creation_date, :creator_tool, :creator_title)",
            parameters,
        )
