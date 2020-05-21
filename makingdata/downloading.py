import asyncio
import json
import time
from asyncio.exceptions import TimeoutError
from queue import Queue
from threading import Condition, Event

import aiohttp
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
        source_json = await get_source_json(session, source_json_url)
        step1 = time.time()
        print(f"Time for reformatting source json: {step1 - start} s")

        # Step 2: Get source json with file urls
        source_json_with_file_urls = await get_source_json_with_file_urls(
            session, source_json
        )
        step2 = time.time()
        print(f"Time for retreiving file urls 2: {step2 - step1} s")

        # Step 3: Get files
        await fetch_and_enqueue_multiple(
            session, queue, condition, source_json_with_file_urls
        )
        step3 = time.time()
        print(f"Time for retrieving files: {step3 - step2} s")


# Step 1: Get source json


async def get_source_json(session, url):
    data = await fetch(session, url, return_type="json")
    source_json = rename_source_json(data)
    return source_json


def rename_source_json(source_json):
    renamed_source_json = []
    for doc in source_json:
        doc = {
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


async def get_source_json_with_file_urls(session, source_json):
    coros = [get_file_url(session, document) for document in source_json]
    results = await asyncio.gather(*coros)
    return results


async def get_file_url(session, document):
    url = document.get("splash_url")
    data = await fetch(session, url, return_type="text")
    try:
        real_url = find_file_url(data)
    except TypeError as e:
        print("Error in get_file_url: ", e)
    else:
        document["file_url"] = real_url
    return document


def find_file_url(page):
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


async def fetch_and_enqueue_multiple(session, queue, condition, source_json):
    coros = [
        fetch_and_enqueue(session, queue, condition, document)
        for document in source_json
    ]
    await asyncio.gather(*coros)


async def fetch_and_enqueue(session, queue, condition, document):
    url = document.get("file_url")
    buffer = await fetch(session, url)
    if not buffer == None:
        with condition:
            queue.put((buffer, document))
            condition.notify()


async def fetch(session, url, return_type="bytes"):
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
    except (InvalidURL, TypeError, TimeoutError,) as e:
        print("Error in fetch: ", e)
