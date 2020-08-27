# APTNotes Download

Download and (optionally) parse APTNotes quickly and easily.

## Quick Start

You need to have [Apache Tika](https://tika.apache.org/) installed.

To make the data use the `aptnotes_download` command line tool. To download the documents as PDFs:

````bash
aptnotes_download.main pdfs
````

To download the documents, parse them, and then store them in a SQLite DB called `aptnotes.sqlite`:

````bash
aptnotes_download.main sqlite
````
