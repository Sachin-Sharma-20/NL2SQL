import os
from typing import List
from dotenv import load_dotenv
load_dotenv()

from utils.db_utils import get_db_connection

def _format_column(col_name: str, data_type: str, is_nullable: str, col_default):
    # simple textual format for schema prompt
    nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
    default = f"DEFAULT {col_default}" if col_default is not None else ""
    return f"  {col_name} {data_type} {nullable} {default}".strip()

def get_full_db_schema(raw: bool = False) -> str:
    """
    Returns a textual representation of the current database schema suitable for
    inclusion in the NL -> SQL prompt. This intentionally creates simple
    CREATE TABLE-like blocks for each table in the current database.
    """
    conn = get_db_connection()
    if conn is None:
        raise Exception("DB connection failed for schema extraction")

    cur = conn.cursor()
    # get tables
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]

    blocks: List[str] = []
    for t in tables:
        cur.execute("""
            SELECT column_name, column_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = %s
            ORDER BY ordinal_position
        """, (t,))
        cols = cur.fetchall()
        col_lines = []
        for col_name, col_type, is_nullable, col_default in cols:
            col_lines.append(_format_column(col_name, col_type, is_nullable, col_default))
        block = f"CREATE TABLE {t} (\n" + ",\n".join(col_lines) + "\n);"
        blocks.append(block)

    cur.close()
    conn.close()

    return "\n\n".join(blocks)
