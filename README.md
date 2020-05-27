# Aptnotes Institutions

Analysing the Sentiment towards Governmental Institutions in aptnotes

## Quick Start

This project uses [poetry](https://github.com/python-poetry/poetry) to manage dependencies. Install the tool and then install the dependencies with `poetry install --no-root`.

Additionally, you need to have [Apache Tika](https://tika.apache.org/) installed. Tika is the best PDF parser available.

To make the data use the `maked` command line tool. To download the PDFs to the directory `data/`:

````bash
poetry run python -m maked.main files data
````

To download the documents, parse them, and then store them in a SQLite called `aptnotes.sqlite` database this command is used:

````bash
poetry run python -m maked.main sqlite aptnotes.sqlite
````
