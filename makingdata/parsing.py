import concurrent.futures
from queue import Queue
from threading import Condition, Event

from tika import parser


def parse(
    consumer_queue: Queue,
    consumer_condition: Condition,
    producer_queue: Queue,
    producer_condition: Condition,
    finsihed_event: Event,
) -> None:
    while not finsihed_event.is_set() or not consumer_queue.empty():
        with consumer_condition:
            while consumer_queue.empty():
                consumer_condition.wait()
            try:
                buffer, document = consumer_queue.get()
                # print("Retrieved item from parsing queue")
            finally:
                consumer_queue.task_done()
        parse_and_enqueue(buffer, document, producer_queue, producer_condition)


def parse_and_enqueue(buffer, document, queue, condition):
    parsed_buffer = parser.from_buffer(buffer)
    # print("Parsed buffer")
    document["fulltext"] = parsed_buffer["content"]
    with condition:
        queue.put(document)
        # print("Enqueued document with parsed buffer")
        condition.notify()
