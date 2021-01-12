import logging
import threading
import time
from pathlib import Path
from typing import Callable, List, Optional

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

        self._add_thread(
            downloading.download,
            self.buffer_queue.async_q,
            self.buffer_queue_condition,
            self.finished_download_event,
            semaphore_value,
            limit,
        )

    async def add_saving(self, form: str, path: Path) -> None:
        if form == "sqlite":
            await self._add_parsed_doc_saving_async(form, path)
        if form == "pdf":
            await self._add_buffer_saving_async(form, path)
        if form in ("json", "csv"):
            await self._add_parsed_doc_saving_sync(form, path)

    async def _add_parsing(self) -> None:
        """Add parsing thread"""
        self.parsed_doc_queue: janus.Queue = janus.Queue()
        self.parsed_doc_queue_condition = threading.Condition()
        self.finished_parsing_event = threading.Event()

        self._add_thread(
            parsing.parse,
            self.buffer_queue.sync_q,
            self.buffer_queue_condition,
            self.finished_download_event,
            self.parsed_doc_queue.sync_q,
            self.parsed_doc_queue_condition,
            self.finished_parsing_event,
        )

    async def _add_parsed_doc_saving_async(self, form: str, path: Path):
        await self._add_parsing()
        self._add_thread(
            saving.save,
            form,
            self.parsed_doc_queue.async_q,
            self.parsed_doc_queue_condition,
            self.finished_parsing_event,
            path,
        )

    async def _add_buffer_saving_async(self, form: str, directory: Path) -> None:
        self._add_thread(
            saving.save,
            form,
            self.buffer_queue.async_q,
            self.buffer_queue_condition,
            self.finished_download_event,
            directory,
        )

    async def _add_parsed_doc_saving_sync(self, form: str, path: Path) -> None:
        await self._add_parsing()
        self._add_thread(
            saving.save,
            form,
            self.parsed_doc_queue.sync_q,
            self.parsed_doc_queue_condition,
            self.finished_parsing_event,
            path,
        )

    def _add_thread(self, func: Callable, *args) -> None:
        thread = threading.Thread(target=func, args=args,)
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
