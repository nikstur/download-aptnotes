# APTNotes Download

Download and (optionally) parse [APTNotes](https://github.com/aptnotes/data) quickly and easily.

## Quick Start

You need to have [Apache Tika](https://tika.apache.org/) installed.

Install `aptnotes-download` in a virtualenv. From the root of the cloned directory, call:

````bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
````

To download the documents as PDFs in a directory called  `files/`:

````bash
python -m aptnotes_download.main pdfs
````

To download the documents, parse them, and then store them in a SQLite file called `aptnotes.sqlite`:

````bash
python -m aptnotes_download.main sqlite
````
