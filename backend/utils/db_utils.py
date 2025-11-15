import os
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path
import mysql.connector
from mysql.connector import Error as MySQLError

env_path = Path(__file__).resolve().parent.parent / ".env"

load_dotenv()

# MySQL Configuration from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DATABASE_NAME = os.getenv("DB_NAME")


# Optional convenience DATABASE_URL (for pandas)
# e.g. mysql+mysqlconnector://user:pass@host:port/database
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """MySQL connection: 30 sec handshake, 10 min query execution allowed."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASS,
            database=DATABASE_NAME,

            autocommit=True,

            # 30 seconds handshake timeout (optimal)
            connection_timeout=30,

            # Use MySQL C-extension (stable, avoids Errno 115)
            use_pure=False
        )

        cursor = conn.cursor()

        # ========================================================
        # EXECUTION TIMEOUT SETTINGS (10 MINUTES)
        # ========================================================

        # Allow SELECT queries to run at least 10 minutes (600k ms)
        cursor.execute("SET SESSION MAX_EXECUTION_TIME=600000;")

        # Allow MySQL to WAIT up to 10 minutes while sending results
        cursor.execute("SET SESSION NET_READ_TIMEOUT=600;")

        # Allow backend to TAKE up to 10 minutes while reading results
        cursor.execute("SET SESSION NET_WRITE_TIMEOUT=600;")

        # Keep connection alive (just safety)
        cursor.execute("SET SESSION wait_timeout=28800;")
        cursor.execute("SET SESSION interactive_timeout=28800;")

        cursor.close()

        return conn

    except MySQLError as e:
        print(f"MySQL connection error: {e}")
        return None



def extract_db_schema() -> str:
    """
    Dynamically extracts the schema using MySQL's INFORMATION_SCHEMA.
    Returns CREATE TABLE-like text for LLM consumption.
    """
    conn = get_db_connection()
    if conn is None:
        return "ERROR: Could not connect to the database to extract schema."

    schema_info = []
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{DATABASE_NAME}'
        ORDER BY TABLE_NAME, ORDINAL_POSITION;
        """
        cursor.execute(query)
        columns = cursor.fetchall()

        current_table = None
        table_schema = []
        for table_name, col_name, col_type, col_key in columns:
            if table_name != current_table:
                if table_schema:
                    schema_info.append(f"CREATE TABLE {current_table} (\n" + ",\n".join(table_schema) + "\n);\n")
                current_table = table_name
                table_schema = []

            definition = f"    {col_name} {col_type}"
            if col_key == 'PRI':
                definition += " PRIMARY KEY"
            elif col_key == 'MUL':
                definition += " (INDEX)"
            table_schema.append(definition)

        if table_schema:
            schema_info.append(f"CREATE TABLE {current_table} (\n" + ",\n".join(table_schema) + "\n);\n")

    except MySQLError as e:
        print(f"Error extracting schema: {e}")
        return f"ERROR: Failed to extract database schema. Details: {e}"
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass

    return "-- MySQL Dialect Schema --\n\n" + "\n".join(schema_info)


def execute_sql_query(sql_query: str):
    """
    Executes a given SQL query.
    If DATABASE_URL is set, pandas.read_sql_query is used (safer).
    Otherwise falls back to mysql.connector and returns results as list[dict].
    Returns (results, prettified_markdown) or ({"error": "..."} , None) on failure.
    """
    db_url = DATABASE_URL
    if not db_url:
        # Use mysql.connector fallback
        conn = get_db_connection()
        if conn is None:
            return {"error": "DB connection failed"}, None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql_query)
            rows = cursor.fetchmany(10000)  # protect default read size for preview
            # convert to dataframe if pandas available
            try:
                df = pd.DataFrame(rows)
                if not df.empty:
                    prettified = df.to_markdown(index=False)
                    results = df.to_dict('records')
                else:
                    prettified = ""
                    results = []
            except Exception:
                results = rows
                prettified = None
            return results, prettified
        except Exception as e:
            return {"error": f"SQL execution/DB error: {e}"}, None
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # If DATABASE_URL provided, use pandas
    try:
        df = pd.read_sql_query(sql_query, db_url)
        results = df.to_dict('records')
        prettified = df.to_markdown(index=False)
        return results, prettified
    except Exception as e:
        return {"error": f"SQL execution/DB error: {e}"}, None
