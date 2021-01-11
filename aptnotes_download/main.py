import logging
import threading
import time
from pathlib import Path
from typing import List, Optional

import janus

from . import downloading, parsing, saving

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s",
)


class APTNotesDownload:
    def __init__(self) -> None:
        self.threads: List[threading.Thread] = []

    async def add_downloading(self, semaphore_value: int, limit: Optional[int]) -> None:
        """Add downloading thread"""
        self.buffer_queue: janus.Queue = janus.Queue()
        self.buffer_queue_condition = threading.Condition()
        self.finished_download_event = threading.Event()

        thread = threading.Thread(
            target=downloading.download,
            args=(
                self.buffer_queue.async_q,
                self.buffer_queue_condition,
                self.finished_download_event,
                semaphore_value,
                limit,
            ),
        )
        self.threads.append(thread)

    async def _add_parsing(self) -> None:
        """Add parsing thread"""
        self.parsed_doc_queue: janus.Queue = janus.Queue()
        self.parsed_doc_queue_condition = threading.Condition()
        self.finished_parsing_event = threading.Event()

        thread = threading.Thread(
            target=parsing.parse,
            args=(
                self.buffer_queue.sync_q,
                self.buffer_queue_condition,
                self.finished_download_event,
                self.parsed_doc_queue.sync_q,
                self.parsed_doc_queue_condition,
                self.finished_parsing_event,
            ),
        )
        self.threads.append(thread)

    async def add_saving(self, form: str, path: Path) -> None:
        if form == "sqlite":
            await self._add_saving_to_db(form, path)
        if form == "pdf":
            await self._add_saving_to_files(form, path)

    async def _add_saving_to_db(self, form: str, path: Path) -> None:
        """Add saving thread"""
        await self._add_parsing()

        thread = threading.Thread(
            target=saving.save,
            args=(
                form,
                self.parsed_doc_queue.async_q,
                self.parsed_doc_queue_condition,
                self.finished_parsing_event,
                path,
            ),
        )
        self.threads.append(thread)

    async def _add_saving_to_files(self, form: str, directory: Path) -> None:
        """Add saving to files thread"""
        thread = threading.Thread(
            target=saving.save,
            args=(
                form,
                self.buffer_queue.async_q,
                self.buffer_queue_condition,
                self.finished_download_event,
                directory,
            ),
        )
        self.threads.append(thread)

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
