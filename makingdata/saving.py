import sqlite3
from queue import Queue
from threading import Condition, Event


def save(queue: Queue, condition: Condition, finished_event: Event) -> None:
    connection = sqlite3.connect("aptnotes.sqlite")
    cursor = connection.cursor()

    setup_db(cursor)
    connection.commit()

    while not finished_event.is_set() or not queue.empty():
        with condition:
            while queue.empty():
                condition.wait()
            try:
                document = queue.get()
                # print("Retrieved item from saving queue")
            finally:
                queue.task_done()
        insert_values(cursor, **document)
        connection.commit()
        # print("Inserted values into db")

    connection.close()


def setup_db(cursor, drop=False):
    if cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='aptnotes';"
    ):
        print("Aptnotes table present.")
        if drop == True:
            print("Dropping it...")
            cursor.execute("DROP TABLE aptnotes")
            create_aptnotes_table(cursor)
    else:
        create_aptnotes_table(cursor)


def create_aptnotes_table(cursor):
    cursor.execute(
        """
        CREATE TABLE aptnotes 
        (filename text, title text, source text, splash_url text, sha1 text, date date, file_url text, fulltext text)
        """
    )


def insert_values(
    cursor, filename, title, source, splash_url, sha1, date, file_url, fulltext
):
    cursor.execute(
        "INSERT INTO aptnotes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (filename, title, source, splash_url, sha1, date, file_url, fulltext),
    )
