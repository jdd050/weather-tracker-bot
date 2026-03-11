import sqlite3
import logging
from dataclasses import dataclass
from typing import List

# Child logger named after the module
logger = logging.getLogger(__name__)

@dataclass
class SQLiteDataTypes:
    INT = "INTEGER"
    TEXT = "TEXT"
    REAL = "REAL"
    BLOB = "BLOB"
    PRIMARY_KEY = "PRIMARY KEY"
    AUTOINCREMENT = "AUTOINCREMENT"
    NOT_NULL = "NOT NULL"
    UNIQUE = "UNIQUE"

class DbColumn:
    def __init__(self, name: str, data_types: List[str]):
        self.name = name
        self.data_type_string = " ".join(data_types)

    def __str__(self):
        return f"{self.name} {self.data_type_string}"

class DbTable:
    def __init__(self, name: str, columns: List[DbColumn]):
        self.name = name
        self.columns = columns

    def get_create_sql(self) -> str:
        column_defs = ", ".join(str(col) for col in self.columns)
        return f"CREATE TABLE IF NOT EXISTS {self.name} ({column_defs})"

class __DatabaseWrapper:
    def __init__(self, db_file):
        self.db_file = db_file
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_file)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            # exc_info=True tells logger to capture the full traceback automatically
            logger.error(f"Database transaction failed: {exc_val}", exc_info=True)
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()

class DatabaseApi:
    def __init__(self, db_file: str, table: 'DbTable'):
        self.db_file = db_file
        self.table = table
        self._initialize_db()
    
    def _initialize_db(self):
        logger.info(f"Initializing database table: {self.table.name}")
        with __DatabaseWrapper(self.db_file) as conn:
            conn.execute(self.table.get_create_sql())

    def _build_where(self, filters: dict):
        if not filters:
            return "", ()
        keys = [f"{k} = ?" for k in filters.keys()]
        return f" WHERE {' AND '.join(keys)}", tuple(filters.values())

    def insert(self, **data):
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        sql = f"INSERT INTO {self.table.name} ({cols}) VALUES ({placeholders})"
        with __DatabaseWrapper(self.db_file) as conn:
            conn.execute(sql, tuple(data.values()))

    def bulk_insert(self, data_list: list[dict]):
        if not data_list: return
        cols = ", ".join(data_list[0].keys())
        placeholders = ", ".join(["?"] * len(data_list[0]))
        sql = f"INSERT INTO {self.table.name} ({cols}) VALUES ({placeholders})"
        values = [tuple(d.values()) for d in data_list]
        with __DatabaseWrapper(self.db_file) as conn:
            conn.executemany(sql, values)

    def select(self, **filters):
        where_sql, params = self._build_where(filters)
        sql = f"SELECT * FROM {self.table.name}{where_sql}"
        with __DatabaseWrapper(self.db_file) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    def update(self, filters: dict, **updates):
        set_sql = ", ".join([f"{k} = ?" for k in updates.keys()])
        where_sql, where_params = self._build_where(filters)
        sql = f"UPDATE {self.table.name} SET {set_sql}{where_sql}"
        with __DatabaseWrapper(self.db_file) as conn:
            conn.execute(sql, tuple(updates.values()) + where_params)

    def delete(self, **filters):
        where_sql, params = self._build_where(filters)
        sql = f"DELETE FROM {self.table.name}{where_sql}"
        with __DatabaseWrapper(self.db_file) as conn:
            conn.execute(sql, params)