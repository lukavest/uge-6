import logging
import psycopg2
from etl.config import settings

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self) -> None:
        self.conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
        self.cur = self.conn.cursor()

    def execute(self, query: str, args: tuple | None = None) -> None:
        try:
            self.cur.execute(query, args)
        except Exception:
            self.conn.rollback()
            logger.exception("SQL execute failed")
            raise

    def executemany(self, query: str, rows: list[tuple]) -> None:
        try:
            self.cur.executemany(query, rows)
        except Exception:
            self.conn.rollback()
            logger.exception("SQL executemany failed")
            raise

    def fetch_all(self, query: str, args: tuple | None = None) -> list[tuple]:
        self.execute(query, args)
        return self.cur.fetchall()

    def fetch_one(self, query: str, args: tuple | None = None) -> tuple | None:
        self.execute(query, args)
        return self.cur.fetchone()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.cur.close()
        self.conn.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()