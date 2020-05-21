import concurrent.futures
import threading
from queue import Queue

from makingdata import downloading, parsing, saving

if __name__ == "__main__":
    parsing_condition = threading.Condition()
    saving_condition = threading.Condition()

    finished_event = threading.Event()

    parsing_queue = Queue()
    saving_queue = Queue()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.submit(
            downloading.download, parsing_queue, parsing_condition, finished_event
        )
        executor.submit(
            parsing.parse,
            parsing_queue,
            parsing_condition,
            saving_queue,
            saving_condition,
            finished_event,
        )
        executor.submit(saving.save, saving_queue, saving_condition, finished_event)
