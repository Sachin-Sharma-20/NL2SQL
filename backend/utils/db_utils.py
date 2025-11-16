import os
import re
from dotenv import load_dotenv
load_dotenv()

# mysql connector
import mysql.connector
from typing import Tuple, Dict, Set

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASS", ""),
            database=os.getenv("DB_NAME", ""),
            charset="utf8mb4",
            use_unicode=True,
        )
        return conn
    except Exception as e:
        print("DB CONNECTION ERROR:", e)
        return None


# ---------------------------------------------------------------------------
# Execute SQL (preview)
# returns (rows, pretty_sql) or ({"error": "..."} , sql)
# ---------------------------------------------------------------------------
def execute_sql_query(sql: str):
    conn = get_db_connection()
    if conn is None:
        return {"error": "DB connection failed"}, sql

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows, sql
    except Exception as e:
        return {"error": str(e)}, sql
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


# ---------------------------------------------------------------------------
# Load real schema metadata from INFORMATION_SCHEMA
# returns: (set_of_tables, dict table->set(columns))
# ---------------------------------------------------------------------------
def load_schema_metadata() -> Tuple[Set[str], Dict[str, Set[str]]]:
    conn = get_db_connection()
    if conn is None:
        raise Exception("Unable to connect to DB for schema metadata")

    cursor = conn.cursor()
    # tables in current database
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
    """)
    tables = {row[0].lower() for row in cursor.fetchall()}

    # columns
    cursor.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
    """)
    columns = {}
    for table_name, column_name in cursor.fetchall():
        t = table_name.lower()
        c = column_name.lower()
        columns.setdefault(t, set()).add(c)

    cursor.close()
    conn.close()
    return tables, columns


# ---------------------------------------------------------------------------
# SQL VALIDATOR (uses live metadata)
# - blocks destructive statements
# - checks referenced tables exist
# - checks referenced columns exist (alias.column or table.column)
# ---------------------------------------------------------------------------
def validate_sql(sql: str):
    sql_low = sql.lower()

    # 1) Block destructive SQL
    forbidden = [
        "delete", "insert", "update", "drop", "alter",
        "truncate", "create", "replace", "merge"
    ]
    for f in forbidden:
        # match as standalone word to reduce false positives
        if re.search(rf"\b{f}\b", sql_low):
            raise Exception("Unsafe SQL detected (destructive command).")

    # 2) load real schema metadata
    valid_tables, valid_columns = load_schema_metadata()

    # 3) table validation (from / join)
    table_pattern = r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)|\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)"
    found = re.findall(table_pattern, sql_low)
    tables = [t[0] or t[1] for t in found]
    for t in tables:
        if t.lower() not in valid_tables:
            raise Exception(f"Unknown table '{t}'")

    # 4) column validation: find alias.column or table.column
    col_pattern = r"([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)"
    cols = re.findall(col_pattern, sql_low)
    for alias, col in cols:
        col = col.lower()
        found = False
        # if column exists in any table in DB, accept it
        for table_cols in valid_columns.values():
            if col in table_cols:
                found = True
                break
        if not found:
            raise Exception(f"Unknown column '{col}'")

    return True
