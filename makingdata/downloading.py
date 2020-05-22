import asyncio
import json
import logging
import time
from queue import Queue
from threading import Condition, Event
from typing import Dict, List

import aiohttp
from aiohttp import ClientSession
from aiohttp.client_exceptions import InvalidURL
from bs4 import BeautifulSoup


def download(queue: Queue, condition: Condition, finished_event: Event) -> None:
    asyncio.run(download_and_enqueue(queue, condition))
    finished_event.set()


async def download_and_enqueue(queue: Queue, condition: Condition) -> None:
    source_json_url = (
        "https://raw.githubusercontent.com/aptnotes/data/master/APTnotes.json"
    )

    async with aiohttp.ClientSession() as session:
        start = time.time()

        # Step 1: Get source json
        aptnotes = await get_aptnotes(session, source_json_url)
        step1 = time.time()
        print(f"Time for reformatting aptnotes.json: {step1 - start}s")

        # Step 2: Get source json with file urls
        aptnotes_with_file_urls = await get_aptnotes_with_file_urls(session, aptnotes)
        step2 = time.time()
        print(f"Time for retreiving file urls: {step2 - step1}s")

        # Step 3: Get files
        await fetch_and_enqueue_multiple(
            session, queue, condition, aptnotes_with_file_urls
        )
        step3 = time.time()
        print(f"Time for retrieving files: {step3 - step2}s")


# Step 1: Get source json


async def get_aptnotes(session: ClientSession, url: str) -> List[Dict]:
    data = await fetch(session, url, return_type="json")
    aptnotes = rename_aptnotes(data)
    return aptnotes


def rename_aptnotes(aptnotes: Dict) -> List[Dict]:
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
    session: ClientSession, aptnotes: Dict
) -> List[Dict]:
    coros = [get_file_url(session, aptnote) for aptnote in aptnotes]
    aptnotes_with_file_urls = await asyncio.gather(*coros, return_exceptions=True)
    return aptnotes_with_file_urls


async def get_file_url(session: ClientSession, document: Dict) -> Dict:
    url = document.get("splash_url")
    splash_page = await fetch(session, url, return_type="text")
    file_url = find_file_url(splash_page)
    document["file_url"] = file_url
    return document


def find_file_url(page: str) -> str:
    """Parse preview page for desired elements to build download URL"""
    soup = BeautifulSoup(page, "lxml")
    scripts = soup.find("body").find_all("script")
    sections = scripts[-1].contents[0].split(";")
    app_api = json.loads(sections[0].split("=")[1])["/app-api/enduserapp/shared-item"]

    # Build download URL
    box_url = "https://app.box.com/index.php"
    box_args = "?rm=box_download_shared_file&shared_name={}&file_id={}"
    file_url = box_url + box_args.format(
        app_api["sharedName"], "f_{}".format(app_api["itemID"])
    )
    return file_url


# Step 3: Get files


async def fetch_and_enqueue_multiple(
    session: ClientSession, queue: Queue, condition: Condition, aptnotes: List[Dict]
) -> None:
    coros = [
        fetch_and_enqueue(session, queue, condition, aptnote)
        for aptnote in aptnotes
        if not isinstance(aptnote, Exception)
    ]
    await asyncio.gather(*coros)


async def fetch_and_enqueue(
    session: ClientSession, queue: Queue, condition: Condition, aptnote: Dict
) -> None:
    url = aptnote["file_url"]
    buffer = await fetch(session, url)
    if not buffer == None:
        with condition:
            queue.put((buffer, aptnote))
            condition.notify()


async def fetch(session: ClientSession, url: str, return_type: str = "bytes") -> None:
    try:
        async with session.get(url) as response:
            if response.status == 200:
                if return_type == "bytes":
                    data = await response.read()
                elif return_type == "json":
                    data = await response.json(content_type=None)
                elif return_type == "text":
                    data = await response.text()
                return data
    except InvalidURL as e:
        logging.error("Error in fetch", exc_info=e)
