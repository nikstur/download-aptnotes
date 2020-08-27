import logging
import threading
import time
from pathlib import Path
from queue import Queue
from typing import Dict, List, Optional, Tuple

import typer

from . import downloading, parsing, saving

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s",
)


app = typer.Typer()


@app.command()
def main(
    form: str = typer.Argument(..., help="Form of data, can be sqlite or pdfs"),
    path: Path = typer.Option(
        "aptnotes.sqlite", "--path", "-p", help="Path of sqlite file"
    ),
    directory: Path = typer.Option(
        "files/", "--directory", "-d", help="Directory for saving PDFs"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l", help="Limit the number of downloads"
    ),
):
    """Download and (optionally) parse APTNotes quickly and easily """
    datamaker = DataMaker(limit)
    if "sqlite" in form:
        path = ensure_correct_path(path)
        datamaker.add_saving_to_db(path)
    if "pdfs" in form:
        directory = ensure_correct_path(directory)
        datamaker.add_saving_to_files(directory)
    datamaker.start()


def ensure_correct_path(path: Path) -> Path:
    """Ensure that relative and absolute paths are treating accordingly"""
    if path.is_absolute():
        return path
    else:
        return Path.cwd() / path


class DataMaker:
    def __init__(self, limit: Optional[int]) -> None:
        self.threads: List[threading.Thread] = []
        self._add_downloading(limit)

    def _add_downloading(self, limit: Optional[int]) -> None:
        """Add downloading thread"""
        self.buffer_queue: "Queue[Tuple[bytes, Dict]]" = Queue()
        self.buffer_queue_condition = threading.Condition()
        self.finished_download_event = threading.Event()

        downloading_thread = threading.Thread(
            target=downloading.download,
            args=(
                self.buffer_queue,
                self.buffer_queue_condition,
                self.finished_download_event,
                limit,
            ),
        )
        self.threads.append(downloading_thread)

    def add_saving_to_db(self, path: Path) -> None:
        """Add saving thread"""
        self._add_parsing()

        saving_to_db_thread = threading.Thread(
            target=saving.save_to_db,
            args=(
                self.parsed_doc_queue,
                self.parsed_doc_queue_condition,
                self.finished_parsing_event,
                path,
            ),
        )
        self.threads.append(saving_to_db_thread)

    def _add_parsing(self) -> None:
        """Add parsing thread"""
        self.parsed_doc_queue: "Queue[Tuple[Dict, Dict]]" = Queue()
        self.parsed_doc_queue_condition = threading.Condition()
        self.finished_parsing_event = threading.Event()

        parsing_thread = threading.Thread(
            target=parsing.parse,
            args=(
                self.buffer_queue,
                self.buffer_queue_condition,
                self.finished_download_event,
                self.parsed_doc_queue,
                self.parsed_doc_queue_condition,
                self.finished_parsing_event,
            ),
        )
        self.threads.append(parsing_thread)

    def add_saving_to_files(self, base_path: Path) -> None:
        """Add saving to files thread"""
        saving_to_files_thread = threading.Thread(
            target=saving.save_to_files,
            args=(
                self.buffer_queue,
                self.buffer_queue_condition,
                self.finished_download_event,
                base_path,
            ),
        )
        self.threads.append(saving_to_files_thread)

    def start(self) -> None:
        """Start all threads and join them after they have finished"""
        start_time = time.time()
        for thread in self.threads:
            thread.start()
        self._join_all()
        print(f"Total time: {round(time.time() - start_time, 1)}s")

    def _join_all(self):
        for thread in self.threads:
            thread.join()


if __name__ == "__main__":
    app()
