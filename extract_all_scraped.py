import configparser
from pathlib import Path

import polars as pl

from lib.database_util import DatabaseConnection

_ROOT_DIR = Path(".").absolute()
RUNTIME_CONFIG_FILENAME = _ROOT_DIR / "runtime.ini"

OUTPUT_FOLDER = _ROOT_DIR.parent / "liscu"


def run():
    config = configparser.ConfigParser()
    config.read(RUNTIME_CONFIG_FILENAME)

    db_conn = DatabaseConnection(
        host=config["database"]["host"],
        user=config["database"]["user"],
        password=config["database"]["password"],
        dbname=config["database"]["dbname"],
    )

    characters = db_conn.read_character_info()
    books = db_conn.read_book_info()

    characters_df = pl.DataFrame(characters)
    books_df = pl.DataFrame(books)

    OUTPUT_FOLDER.mkdir(exist_ok=True, parents=True)
    characters_df.write_ndjson(OUTPUT_FOLDER / "characters.jsonl")
    books_df.write_ndjson(OUTPUT_FOLDER / "books.jsonl")


if __name__ == "__main__":
    run()
