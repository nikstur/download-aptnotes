import concurrent.futures
import logging
from queue import Queue
from threading import Condition, Event
from typing import Dict

from tika import parser


def parse(
    consumer_queue: Queue,
    consumer_condition: Condition,
    producer_queue: Queue,
    producer_condition: Condition,
    finished_download_event: Event,
) -> None:
    while not finished_download_event.is_set() or not consumer_queue.empty():
        with consumer_condition:
            while consumer_queue.empty():
                consumer_condition.wait()
            try:
                buffer, aptnote = consumer_queue.get()
                logging.debug("Retrieved item from parsing queue")
            finally:
                consumer_queue.task_done()
        parse_and_enqueue(buffer, aptnote, producer_queue, producer_condition)


def parse_and_enqueue(
    buffer: bytes, aptnote: Dict, queue: Queue, condition: Condition
) -> None:
    parsed_buffer = parser.from_buffer(buffer)
    logging.debug("Parsed buffer")
    unique_id = aptnote["unique_id"]
    document = assemble_document(unique_id, parsed_buffer)
    with condition:
        queue.put((aptnote, document))
        logging.debug("Enqueued document with parsed buffer")
        condition.notify()


def assemble_document(unique_id: int, parsed_buffer: Dict) -> Dict:
    content = parsed_buffer["content"]
    metadata = parsed_buffer["metadata"]

    document = {
        "unique_id": unique_id,
        "fulltext": content,
        "author": metadata.get("Author"),
        "creation_date": metadata.get("Creation-Date"),
        "creator_tool": metadata.get("pdf:docinfo:creator_tool"),
        "creator_title": metadata.get("pdf:docinfo:title"),
    }
    return document
