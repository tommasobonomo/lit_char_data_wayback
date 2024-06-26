from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import psycopg2

BookKey = Tuple[str, str]
CharKey = Tuple[str, str, str]


@dataclass
class BookInfo(object):
    book_title: str
    book_author: str
    source: str
    summary: str

    @property
    def book_key(self) -> BookKey:
        return (self.book_title, self.source)


@dataclass
class CharacterInfo(object):
    character_name: str
    book_title: str
    source: str
    description: str

    @property
    def book_key(self) -> BookKey:
        return (self.book_title, self.source)

    @property
    def char_key(self) -> CharKey:
        return (self.book_title, self.source, self.character_name)


@dataclass
class CharacterInfoWithMaskedDescription(object):
    character_name: str
    book_title: str
    source: str
    description: str
    masked_description: str

    @property
    def book_key(self) -> BookKey:
        return (self.book_title, self.source)

    @property
    def char_key(self) -> CharKey:
        return (self.book_title, self.source, self.character_name)

    @classmethod
    def generate_from_char_info(
        cls,
        char_info: CharacterInfo,
        masked_description: str,
    ) -> CharacterInfoWithMaskedDescription:
        return cls(
            character_name=char_info.character_name,
            book_title=char_info.book_title,
            source=char_info.source,
            description=char_info.description,
            masked_description=masked_description,
        )


@dataclass
class DatabaseConnection(object):
    host: str  # database host name
    user: str  # database user name
    password: str  # user password
    dbname: str  # database name

    def _connect(self):
        self.conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
        )
        self.cur = self.conn.cursor()

    def _close(self):
        self.cur.close()
        self.conn.close()

    def read_book_info(self) -> List[BookInfo]:
        self._connect()
        query = (
            "SELECT book_title, author, source, summary_text FROM literatures "
            "WHERE summary_text IS NOT NULL and summary_text <> '';"
        )

        self.cur.execute(query)
        books: List[BookInfo] = [BookInfo(*row) for row in self.cur.fetchall()]
        self._close()
        return books

    def read_character_info(self) -> List[CharacterInfo]:
        self._connect()
        query = (
            "SELECT character_name, book_title, source, description_text "
            "FROM characters "
            "WHERE description_text IS NOT NULL AND description_text <> '' "
            "AND character_name <> 'Major' "
            "AND character_name <> 'Minor' "
            "AND character_name <> 'Major Characters' "
            "AND character_name <> 'Minor Characters';"
        )

        self.cur.execute(query)
        characters: List[CharacterInfo] = [
            CharacterInfo(*row) for row in self.cur.fetchall()
        ]
        self._close()
        return characters
