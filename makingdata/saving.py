import logging
import sqlite3
from queue import Queue
from sqlite3 import Cursor
from threading import Condition, Event


def save(queue: Queue, condition: Condition, finished_event: Event) -> None:
    connection = sqlite3.connect("aptnotes.sqlite")
    cursor = connection.cursor()

    setup_db(cursor, drop=True)
    connection.commit()

    while not finished_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                aptnote, document = queue.get()
                logging.debug("Retrieved item from saving queue")
            finally:
                queue.task_done()

        insert_aptnote(cursor, **aptnote)
        insert_document(cursor, **document)
        connection.commit()
        logging.debug("Inserted values into db")

    connection.close()


def setup_db(cursor: Cursor, drop: bool = False) -> None:
    """Check whether necessary tables are present, create them otherwise.
    
    When drop=True, drop all tables and recreate them.
    """
    is_there_aptnotes = check_table(cursor, "aptnotes")
    is_there_documents = check_table(cursor, "documents")

    if not is_there_aptnotes or not is_there_documents:
        logging.debug("Tables not present. Creating them...")
        create_aptnotes_table(cursor)
        create_documents_table(cursor)
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
        (id integer, fulltext text, author text, creation_date datetime, creator_tool text, creator_title text)
        """
    )


def insert_aptnote(
    cursor, unique_id, filename, title, source, splash_url, sha1, date, file_url,
) -> None:
    cursor.execute(
        "INSERT INTO aptnotes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (unique_id, filename, title, source, splash_url, sha1, date, file_url),
    )


def insert_document(
    cursor, unique_id, fulltext, author, creation_date, creator_tool, creator_title
) -> None:
    parameters = {
        "id": unique_id,
        "fulltext": fulltext,
        "author": author,
        "creation_date": creation_date,
        "creator_tool": creator_tool,
        "creator_title": creator_title,
    }
    try:
        cursor.execute(
            "INSERT INTO documents VALUES (:id, :fulltext, :author, :creation_date, :creator_tool, :creator_title)",
            parameters,
        )
    except sqlite3.InterfaceError as e:
        logging.error("Error in interfaces", exc_info=e)
        logging.error(f"Faulty parameter {parameters}")
