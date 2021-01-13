import asyncio
import hashlib
import json
import logging
from asyncio import BoundedSemaphore, Queue
from threading import Condition, Event
from typing import Coroutine, List, Literal, Optional, Union, overload

import aiohttp
import uvloop
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from . import utils


def download(
    queue: Queue,
    condition: Condition,
    finish_event: Event,
    semaphore_value: int,
    limit: Optional[int],
) -> None:
    uvloop.install()
    asyncio.run(download_and_enqueue(queue, condition, semaphore_value, limit))
    finish_event.set()


async def download_and_enqueue(
    queue: Queue, condition: Condition, semaphore_value: int, limit: Optional[int]
) -> None:
    """Download source json and all documentsand enqueue all documents from """
    source_json_url = (
        "https://raw.githubusercontent.com/aptnotes/data/master/APTnotes.json"
    )
    async with aiohttp.ClientSession(headers={"Connection": "keep-alive"}) as session:
        semaphore = BoundedSemaphore(semaphore_value)
        source_json = await fetch_source_json(session, source_json_url)
        source_json_with_file_urls = await add_file_urls_source_json(
            session, semaphore, source_json[slice(limit)]
        )
        await fetch_and_enqueue_multiple(
            session, semaphore, source_json_with_file_urls, condition, queue
        )


# Step 1: Get source json


async def fetch_source_json(session: ClientSession, url: str) -> List[dict]:
    semaphore: BoundedSemaphore = BoundedSemaphore(1)
    data: List[dict] = await fetch(semaphore, session, url, return_type="json")
    source_json = rename_source_json_keys(data)
    logging.debug(f"Files available for download: {len(source_json)}")
    return source_json


def rename_source_json_keys(source_json: List[dict]) -> List[dict]:
    """Rename keys in source json and add unique id"""
    renamed_source_json: List[dict] = []
    for count, doc in enumerate(source_json):
        doc = {
            "unique_id": count,
            "filename": doc["Filename"],
            "title": doc["Title"],
            "source": doc["Source"],
            "splash_url": doc["Link"],
            "sha1": doc["SHA-1"],
            "date": doc["Date"],
        }
        renamed_source_json.append(doc)
    return renamed_source_json


# Step 2: Get source json with file urls


@utils.log_duration("DEBUG", "Time for adding file URLs:")
async def add_file_urls_source_json(
    session: ClientSession, semaphore: BoundedSemaphore, aptnotes: List[dict]
) -> List[dict]:
    """Add file urls to aptnotes by scraping pdf splash for download link"""
    coros = [get_file_url(semaphore, session, aptnote) for aptnote in aptnotes]
    aptnotes_with_file_urls = await asyncio.gather(*coros, return_exceptions=True)
    return aptnotes_with_file_urls


async def get_file_url(
    semaphore: BoundedSemaphore, session: ClientSession, document: dict
) -> dict:
    """Fetch splash page and find file url in it"""
    url: str = document.get("splash_url", "")
    splash_page: str = await fetch(semaphore, session, url, return_type="text")
    document["file_url"] = find_file_url(splash_page)
    return document


def find_file_url(page: str) -> str:
    """Parse splash for desired elements to build download URL"""
    soup = BeautifulSoup(page, "lxml")
    scripts = soup.find("body").find_all("script")
    sections: List[str] = scripts[-1].contents[0].split(";")
    app_api = json.loads(sections[0].split("=")[1])["/app-api/enduserapp/shared-item"]
    file_url = build_file_url(app_api["sharedName"], app_api["itemID"])
    return file_url


def build_file_url(shared_name: str, item_id: str) -> str:
    """Build the correct file url from elemnts of the splash page"""
    url = "https://app.box.com/index.php"
    parameters = (
        "?rm=box_download_shared_file"
        f"&shared_name={shared_name}"
        f"&file_id=f_{item_id}"
    )
    file_url = url + parameters
    return file_url


# Step 3: Fetch and enqueue pdf buffer and metadata


@utils.log_duration("DEBUG", "Time to fetch documents:")
async def fetch_and_enqueue_multiple(
    session: ClientSession,
    semaphore: BoundedSemaphore,
    aptnotes: List[dict],
    condition: Condition,
    queue: Queue,
) -> None:
    """Fetch and enqueue multiple pdfs"""
    coros: List[Coroutine] = [
        fetch_and_enqueue(aptnote, semaphore, session, condition, queue)
        for aptnote in aptnotes
        if not isinstance(aptnote, Exception)
    ]
    await asyncio.gather(*coros, return_exceptions=True)


async def fetch_and_enqueue(
    aptnote: dict,
    semaphore: BoundedSemaphore,
    session: ClientSession,
    condition: Condition,
    queue: Queue,
) -> None:
    """Fetch and enqueue a single pdf"""
    url = aptnote["file_url"]
    buffer: bytes = await fetch(semaphore, session, url)

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
            await queue.put((buffer, aptnote))
            condition.notify()


def check_integrity(buffer: bytes, correct_hash: str) -> bool:
    """Check the integrity of buffer with sha1 hash"""
    hash_check = hashlib.sha1()
    hash_check.update(buffer)
    result: bool = hash_check.hexdigest() == correct_hash
    return result


@overload
async def fetch(
    semaphore: BoundedSemaphore,
    session: ClientSession,
    url: str,
    return_type: Literal["bytes"] = "bytes",
) -> bytes:
    ...


@overload
async def fetch(
    semaphore: BoundedSemaphore,
    session: ClientSession,
    url: str,
    return_type: Literal["json"],
) -> List[dict]:
    ...


@overload
async def fetch(
    semaphore: BoundedSemaphore,
    session: ClientSession,
    url: str,
    return_type: Literal["text"],
) -> str:
    ...


async def fetch(
    semaphore: BoundedSemaphore,
    session: ClientSession,
    url: str,
    return_type: Literal["bytes", "json", "text"] = "bytes",
) -> Union[bytes, List[dict], str]:
    """Fetch resource from url"""
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
                raise Exception(
                    f"Unsuccessful request for {url}. Reponse status: {response.status}"
                )
