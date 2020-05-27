import logging
import threading
import time
from pathlib import Path
from queue import Queue

import typer

from . import downloading, parsing, saving

logging.basicConfig(
    filename="makedata.log",
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

app = typer.Typer()


@app.command()
def files(directory: Path):
    datamaker = DataMaker()
    datamaker.add_saving_to_files(directory)
    datamaker.start()


@app.command()
def sqlite(path: Path):
    datamaker = DataMaker()
    datamaker.add_saving_to_db(path)
    datamaker.start()


class DataMaker:
    def __init__(self) -> None:
        self.threads = []
        self._add_downloading()

    def _add_downloading(self) -> None:
        self.buffer_queue = Queue()
        self.buffer_queue_condition = threading.Condition()
        self.finished_download_event = threading.Event()

        self.threads.append(
            threading.Thread(
                target=downloading.download,
                args=(
                    self.buffer_queue,
                    self.buffer_queue_condition,
                    self.finished_download_event,
                ),
            )
        )

    def add_saving_to_db(self, path: Path) -> None:
        self._add_parsing()
        self.threads.append(
            threading.Thread(
                target=saving.save_to_db,
                args=(
                    self.parsed_doc_queue,
                    self.parsed_doc_queue_condition,
                    self.finished_download_event,
                    path,
                ),
            )
        )

    def _add_parsing(self) -> None:
        self.parsed_doc_queue = Queue()
        self.parsed_doc_queue_condition = threading.Condition()
        self.threads.append(
            threading.Thread(
                target=parsing.parse,
                args=(
                    self.buffer_queue,
                    self.buffer_queue_condition,
                    self.parsed_doc_queue,
                    self.parsed_doc_queue_condition,
                    self.finished_download_event,
                ),
            )
        )

    def add_saving_to_files(self, base_path: Path) -> None:
        self.threads.append(
            threading.Thread(
                target=saving.save_to_files,
                args=(
                    self.buffer_queue,
                    self.buffer_queue_condition,
                    self.finished_download_event,
                    base_path,
                ),
            )
        )

    def start(self) -> None:
        start = time.time()
        for thread in self.threads:
            thread.start()
        self._join_all()
        print(f"Total time: {time.time() - start}s")

    def _join_all(self):
        for thread in self.threads:
            thread.join()


if __name__ == "__main__":
    app()
