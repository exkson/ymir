# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import sqlite3

from itemadapter import ItemAdapter


class YmirPipeline:
    def process_item(self, item, spider):
        return item


class SQLiteDatabasePipeline:
    def __init__(self):
        self.conn = sqlite3.connect("emails.db")
        self.cursor = self.conn.cursor()

    def open_spider(self, spider):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email VARCHAR(255),
                channel VARCHAR(255),
                query VARCHAR(255),
                UNIQUE(email, channel, query),
                CHECK(email <> '')
            );
            """
        )
        self.cursor.execute("CREATE INDEX IF NOT EXISTS email_index ON emails (email);")

    def process_item(self, item, spider):
        if not (
            self.cursor.execute(
                "SELECT email FROM emails WHERE email = ?", (item["email"],)
            ).fetchone()
        ):
            self.cursor.execute(
                "INSERT INTO emails (email, channel, query) VALUES (?, ?, ?)",
                (item["email"], item["channel"], item["query"]),
            )
            self.conn.commit()
        return item

    def close_spider(self, spider):
        self.conn.close()
