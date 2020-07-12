import asyncio
import hashlib
import json
import logging
import time
from asyncio import BoundedSemaphore
from queue import Queue
from threading import Condition, Event
from typing import Any, Dict, List

import aiohttp
from aiohttp import ClientSession
from aiohttp.web import HTTPException
from bs4 import BeautifulSoup


def download(
    queue: Queue, condition: Condition, finish_event: Event, limit: int
) -> None:
    asyncio.run(download_and_enqueue(queue, condition, limit))
    finish_event.set()


async def download_and_enqueue(queue: Queue, condition: Condition, limit: int) -> None:
    source_json_url = (
        "https://raw.githubusercontent.com/aptnotes/data/master/APTnotes.json"
    )

    if limit == 0:
        limit_slice = slice(None)
    else:
        limit_slice = slice(limit)

    async with aiohttp.ClientSession() as session:
        start = time.time()

        # Step 1: Get source json
        aptnotes = await get_aptnotes(session, source_json_url)
        step1 = time.time()
        logging.info(f"Time for reformatting aptnotes.json: {step1 - start}s")
        logging.info(f"Length of aptnotes: {len(aptnotes)}")

        # Step 2: Get source json with file urls
        aptnotes_with_file_urls = await get_aptnotes_with_file_urls(
            session, aptnotes[limit_slice]
        )
        step2 = time.time()
        logging.info(f"Time for retreiving file urls: {step2 - step1}s")
        logging.info(
            f"Length of aptnotes_with_file_urls: {len(aptnotes_with_file_urls)}"
        )

        # Step 3: Get files
        await fetch_and_enqueue_multiple(
            aptnotes_with_file_urls, session, condition, queue
        )
        step3 = time.time()
        logging.info(f"Time for retrieving files: {step3 - step2}s.")


# Step 1: Get source json


async def get_aptnotes(session: ClientSession, url: str) -> List[Dict]:
    semaphore = BoundedSemaphore(1)
    data = await fetch(semaphore, session, url, return_type="json")
    aptnotes = rename_aptnotes(data)
    return aptnotes


def rename_aptnotes(aptnotes: List[Dict]) -> List[Dict]:
    renamed_aptnotes = []
    for count, doc in enumerate(aptnotes):
        doc = {
            "unique_id": count,
            "filename": doc["Filename"],
            "title": doc["Title"],
            "source": doc["Source"],
            "splash_url": doc["Link"],
            "sha1": doc["SHA-1"],
            "date": doc["Date"],
        }
        renamed_aptnotes.append(doc)
    return renamed_aptnotes


# Step 2: Get source json with file urls


async def get_aptnotes_with_file_urls(
    session: ClientSession, aptnotes: List[Dict]
) -> List[Dict]:
    semaphore = BoundedSemaphore(50)
    coros = [get_file_url(semaphore, session, aptnote) for aptnote in aptnotes]
    aptnotes_with_file_urls = await asyncio.gather(*coros, return_exceptions=True)
    return aptnotes_with_file_urls


async def get_file_url(
    semaphore: BoundedSemaphore, session: ClientSession, document: Dict
) -> Dict:
    url = document.get("splash_url", "")
    splash_page = await fetch(semaphore, session, url, return_type="text")
    document["file_url"] = find_file_url(splash_page)
    return document


def find_file_url(page: str) -> str:
    """Parse preview page for desired elements to build download URL"""
    soup = BeautifulSoup(page, "lxml")
    scripts = soup.find("body").find_all("script")
    sections = scripts[-1].contents[0].split(";")
    app_api = json.loads(sections[0].split("=")[1])["/app-api/enduserapp/shared-item"]
    file_url = build_file_url(app_api["sharedName"], app_api["itemID"])
    return file_url


def build_file_url(shared_name: str, item_id: str) -> str:
    url = "https://app.box.com/index.php"
    parameters = [
        "?rm=box_download_shared_file",
        f"&shared_name={shared_name}",
        f"&file_id=f_{item_id}",
    ]
    file_url = url + "".join(parameters)
    return file_url


# Step 3: Get files


async def fetch_and_enqueue_multiple(
    aptnotes: List[Dict], session: ClientSession, condition: Condition, queue: Queue
) -> None:
    semaphore = BoundedSemaphore(50)
    coros = [
        fetch_and_enqueue(aptnote, semaphore, session, condition, queue)
        for aptnote in aptnotes
        if not isinstance(aptnote, Exception)
    ]
    await asyncio.gather(*coros)


async def fetch_and_enqueue(
    aptnote: Dict,
    semaphore: BoundedSemaphore,
    session: ClientSession,
    condition: Condition,
    queue: Queue,
) -> None:
    url = aptnote["file_url"]
    buffer = await fetch(semaphore, session, url)

    if buffer:
        if not check_integrity(buffer, aptnote["sha1"]):
            logging.error(
                f"Integrity of file from {url} could not be verified. Retrying once..."
            )
            buffer = await fetch(semaphore, session, url)
            if not check_integrity(buffer, aptnote["sha1"]):
                logging.error(
                    f"Integrity of file from {url} could not be verified on second try. Discarding it...",
                )
                raise ValueError("File integrity could not be verified")

        with condition:
            queue.put((buffer, aptnote))
            condition.notify()


def check_integrity(buffer: bytes, correct_hash: str) -> bool:
    hash_check = hashlib.sha1()
    hash_check.update(buffer)
    result = hash_check.hexdigest() == correct_hash
    return result


async def fetch(
    semaphore: BoundedSemaphore,
    session: ClientSession,
    url: str,
    return_type: str = "bytes",
) -> Any:
    async with semaphore:
        async with session.get(url) as response:
            if response.status == 200:
                if return_type == "bytes":
                    return await response.read()
                elif return_type == "json":
                    return await response.json(content_type=None)
                elif return_type == "text":
                    return await response.text()
            else:
                exception_text = f"Response status: {response.status}"
                raise HTTPException(text=exception_text)
