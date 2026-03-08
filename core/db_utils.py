import sqlite3
from contextlib import contextmanager


@contextmanager
def connect_db(path: str):
    """
    Safe SQLite connection context.

    Ensures:
    - row_factory = sqlite3.Row
    - commit on success
    - rollback on error
    - connection always closes
    """
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
