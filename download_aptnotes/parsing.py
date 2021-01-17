import logging
from queue import Queue
from threading import Condition, Event

try:
    from tika import parser
except ImportError:
    tika_installed = False
else:
    tika_installed = True
    logging.getLogger("tika.tika").setLevel("CRITICAL")


class OptionalDepedencyMissing(Exception):
    pass


def parse(
    input_queue: Queue,
    input_queue_condition: Condition,
    input_finish_event: Event,
    output_queue: Queue,
    output_queue_condition: Condition,
    output_finish_event: Event,
) -> None:
    if not tika_installed:
        raise OptionalDepedencyMissing("tika")
    while not input_finish_event.is_set() or not input_queue.empty():
        with input_queue_condition:
            while input_queue.empty():
                input_queue_condition.wait()
            try:
                buffer, aptnote = input_queue.get()
            finally:
                input_queue.task_done()
        parse_and_enqueue(buffer, aptnote, parser, output_queue, output_queue_condition)
    output_finish_event.set()


def parse_and_enqueue(
    buffer: bytes, aptnote: dict, parser, queue: Queue, condition: Condition
) -> None:
    parsed_buffer: dict = parser.from_buffer(buffer)
    augmented_aptnote: dict = augment_aptnote(aptnote, parsed_buffer)
    with condition:
        queue.put(augmented_aptnote)
        condition.notify()


def augment_aptnote(aptnote: dict, parsed_buffer: dict) -> dict:
    """Augment single aptnote by adding metadata"""
    content: str = parsed_buffer["content"]
    metadata: dict = parsed_buffer["metadata"]
    aptnote.update(
        {
            "fulltext": content,
            "creation_date": metadata.get("Creation-Date"),
            "creator_tool": metadata.get("pdf:docinfo:creator_tool"),
            "creator_title": metadata.get("pdf:docinfo:title"),
        }
    )
    return aptnote
