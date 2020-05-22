import concurrent.futures
import logging
import threading
import time
from queue import Queue

from makingdata import downloading, parsing, saving

logging.basicConfig(
    filename="makedata.log",
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

if __name__ == "__main__":
    start = time.time()
    parsing_queue = Queue()
    saving_queue = Queue()

    parsing_condition = threading.Condition()
    saving_condition = threading.Condition()

    finished_event = threading.Event()

    t_download = threading.Thread(
        target=downloading.download,
        args=(parsing_queue, parsing_condition, finished_event),
    )

    t_parse = threading.Thread(
        target=parsing.parse,
        args=(
            parsing_queue,
            parsing_condition,
            saving_queue,
            saving_condition,
            finished_event,
        ),
    )

    t_save = threading.Thread(
        target=saving.save, args=(saving_queue, saving_condition, finished_event)
    )

    threads = [t_download, t_parse, t_save]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print(f"Total time: {time.time() - start}s")
