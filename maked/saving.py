import asyncio
import logging
import sqlite3
from pathlib import Path
from queue import Queue
from sqlite3 import Cursor
from threading import Condition, Event
from typing import Union, List, Dict

import aiofiles  # type: ignore


def save_to_files(
    queue: Queue, condition: Condition, finished_download_event: Event, base_path: Path
) -> None:
    base_path.mkdir(parents=True, exist_ok=True)
    asyncio.run(
        save_and_write_to_files(queue, condition, finished_download_event, base_path)
    )


async def save_and_write_to_files(
    queue: Queue, condition: Condition, finished_download_event: Event, base_path: Path,
) -> None:
    while not finished_download_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                buffer, aptnote = queue.get()
                logging.debug("Retrieved item from saving queue")
            finally:
                queue.task_done()

        filename = aptnote.get("filename")
        path = base_path / filename
        path = path.with_suffix(".pdf")
        await write_file(buffer, path)


async def write_file(buffer: bytes, path: Path) -> None:
    async with aiofiles.open(path, mode="wb") as f:
        await f.write(buffer)


def save_to_db(
    queue: Queue, condition: Condition, finished_download_event: Event, path: Path
) -> None:
    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    setup_db(cursor, drop=True)
    connection.commit()

    while not finished_download_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                aptnote, document = queue.get()
                logging.debug("Retrieved item from saving queue")
            finally:
                queue.task_done()

        insert_aptnote(cursor, aptnote)
        authors = document["author"]
        del document["author"]
        insert_document(cursor, document)
        insert_authors(cursor, authors, document["unique_id"], aptnote.get("title"))
        connection.commit()
        logging.debug("Inserted values into db")

    connection.close()


def setup_db(cursor: Cursor, drop: bool = False) -> None:
    """Check whether necessary tables are present, create them otherwise.
    
    When drop=True, drop all tables and recreate them.
    """
    is_there_aptnotes = check_table(cursor, "aptnotes")
    is_there_documents = check_table(cursor, "documents")
    is_there_authors = check_table(cursor, "authors")

    if not is_there_aptnotes and not is_there_documents and not is_there_authors:
        logging.debug("Tables not present. Creating them...")
        create_aptnotes_table(cursor)
        create_documents_table(cursor)
        create_authors_tables(cursor)
    else:
        if is_there_aptnotes:
            if drop == True:
                logging.debug("Dropping aptnotes table...")
                cursor.execute("DROP TABLE aptnotes")
                create_aptnotes_table(cursor)
        if is_there_documents:
            if drop == True:
                logging.debug("Dropping documents table...")
                cursor.execute("DROP TABLE documents")
                create_documents_table(cursor)
        if is_there_authors:
            if drop == True:
                logging.debug("Dropping authors table...")
                cursor.execute("DROP TABLE authors")
                create_authors_tables(cursor)


def check_table(cursor: Cursor, tablename: str) -> bool:
    cursor.execute(
        "SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name=?",
        (tablename,),
    )
    is_there = cursor.fetchone()[0] == 1
    if is_there:
        logging.debug(f"{tablename} present.")
    else:
        logging.debug(f"{tablename} not present.")
    return is_there


def create_aptnotes_table(cursor: Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE aptnotes 
        (id integer, filename text, title text, source text, splash_url text, sha1 text, date date, file_url text)
        """
    )


def create_documents_table(cursor: Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE documents
        (id integer, fulltext text, creation_date datetime, creator_tool text, creator_title text)
        """
    )


def create_authors_tables(cursor: Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE authors
        (name text, document_id integer, title text)
        """
    )


def insert_aptnote(cursor: Cursor, parameters: Dict) -> None:
    cursor.execute(
        "INSERT INTO aptnotes VALUES (:unique_id, :filename, :title, :source, :splash_url, :sha1, :date, :file_url)",
        parameters,
    )


def insert_document(cursor: Cursor, parameters: Dict) -> None:
    cursor.execute(
        "INSERT INTO documents VALUES (:unique_id, :fulltext, :creation_date, :creator_tool, :creator_title)",
        parameters,
    )


def insert_authors(
    cursor: Cursor, authors: Union[List, str], document_id: int, title: str
) -> None:
    author_row = {
        "name": None,
        "document_id": document_id,
        "title": title,
    }
    if isinstance(authors, list):
        for author in authors:
            author_row["name"] = author
            insert_author(cursor, author_row)
    elif not authors or isinstance(authors, str):
        insert_author(cursor, author_row)


def insert_author(cursor: Cursor, parameters: Dict) -> None:
    cursor.execute(
        """
        INSERT INTO authors VALUES (:name, :document_id, :title)
        """,
        parameters,
    )
