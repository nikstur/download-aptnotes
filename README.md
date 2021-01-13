# Download APTNotes

Download and (optionally) parse [APTNotes](https://github.com/aptnotes/data) quickly and easily

## Installation

```bash
pip install download-aptnotes
```

To enable parsing the downloaded PDFs you need to install the extra `tika`. This
will try to install the Apache Tika Server which depends on Java 7+. Make sure
that you have an adequate version of Java installed before you try to install it
Without this extra, the only output format available is `pdf`.

```bash
pip install download-aptnotes[tika]
```

## Usage

```txt
Usage: download-aptnotes [OPTIONS]

  Download and (optionally) parse APTNotes quickly and easily

Options:
  -f, --format [pdf|sqlite|json|csv]
                                  Output format  [required]
  -o, --output PATH               Output path of file or directory  [required]
  -l, --limit INTEGER             Number of files to download
  -p, --parallel INTEGER          Number of parallell downloads  [default: 10]
  --install-completion            Install completion for the current shell.
  --show-completion               Show completion for the current shell, to
                                  copy it or customize the installation.

  --help                          Show this message and exit.
```

Download all documents, parse them and store them in an SQLite database:

```bash
download-aptnotes -f sqlite -o aptnotes.sqlite
```

Download the first 10 documents in the source list, parse them and store
them in an SQLite database:

```bash
download-aptnotes -f sqlite -o aptnotes.sqlite -l 10
```

Download all documents and store them as individual files in a directory:

```bash
download-aptnotes -f pdf -o aptnotes/
```

## Contributing

Dependencies:

- Java 7+
- Poetry

Clone this repository and install all dependencies:

````bash
git clone https://github.com/nikstur/download-aptnotes.git
cd download-aptnotes
poetry install
````
