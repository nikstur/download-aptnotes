import logging
from queue import Queue
from threading import Condition, Event
from typing import Dict

from tika import parser


def parse(
    input_queue: Queue,
    input_queue_condition: Condition,
    input_finish_event: Event,
    output_queue: Queue,
    output_queue_condition: Condition,
    output_finish_event: Event,
) -> None:
    retrieved_items = 0
    while not input_finish_event.is_set() or not input_queue.empty():
        with input_queue_condition:
            while input_queue.empty():
                input_queue_condition.wait()
            try:
                buffer, aptnote = input_queue.get()
                logging.debug("Retrieved item from buffer queue")
                retrieved_items += 1
            finally:
                input_queue.task_done()
        parse_and_enqueue(buffer, aptnote, output_queue, output_queue_condition)
    output_finish_event.set()
    logging.info(f"No. of retrieved items from buffer queue: {retrieved_items}")


def parse_and_enqueue(
    buffer: bytes, aptnote: Dict, queue: Queue, condition: Condition
) -> None:
    parsed_buffer = parser.from_buffer(buffer)
    logging.debug("Parsed buffer")
    unique_id = aptnote["unique_id"]
    document = assemble_document(unique_id, parsed_buffer)
    with condition:
        queue.put((aptnote, document))
        logging.debug("Enqueued parsed buffer into parsed doc queue")
        condition.notify()


def assemble_document(unique_id: int, parsed_buffer: Dict) -> Dict:
    content = parsed_buffer["content"]
    metadata = parsed_buffer["metadata"]

    document = {
        "unique_id": unique_id,
        "fulltext": content,
        "creation_date": metadata.get("Creation-Date"),
        "creator_tool": metadata.get("pdf:docinfo:creator_tool"),
        "creator_title": metadata.get("pdf:docinfo:title"),
    }
    return document
