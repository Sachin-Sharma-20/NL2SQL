# backend/utils/db_utils.py

import os
from dotenv import load_dotenv
import pandas as pd
import mysql.connector
from mysql.connector import Error as MySQLError

load_dotenv()

# MySQL Configuration from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DATABASE_NAME = os.getenv("DB_NAME")

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    print()
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DATABASE_NAME
        )
        return conn
    except MySQLError as e:
        print(f"MySQL connection error: {e}")
        return None

def extract_db_schema() -> str:
    """
    Dynamically extracts the schema using MySQL's INFORMATION_SCHEMA.
    """
    conn = get_db_connection()
    if conn is None:
        return "ERROR: Could not connect to the database to extract schema."

    schema_info = []
    try:
        cursor = conn.cursor()
        
        # Query to get CREATE TABLE statements for all non-system tables
        query = f"""
        SELECT 
            TABLE_NAME, 
            COLUMN_NAME, 
            COLUMN_TYPE, 
            COLUMN_KEY
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_SCHEMA = '{DATABASE_NAME}'
        ORDER BY 
            TABLE_NAME, ORDINAL_POSITION;
        """
        cursor.execute(query)
        columns = cursor.fetchall()
        
        # Format the result into CREATE TABLE statements for the LLM
        current_table = None
        table_schema = []
        
        for table_name, col_name, col_type, col_key in columns:
            if table_name != current_table:
                if table_schema:
                    # Finalize the previous table's CREATE statement
                    schema_info.append(f"CREATE TABLE {current_table} (\n" + ",\n".join(table_schema) + "\n);\n")
                
                current_table = table_name
                table_schema = []
            
            definition = f"    {col_name} {col_type}"
            if col_key == 'PRI':
                definition += " PRIMARY KEY"
            elif col_key == 'MUL': # Assuming foreign keys are indexed
                definition += " (INDEX)"
                
            table_schema.append(definition)

        # Add the last table's schema
        if table_schema:
            schema_info.append(f"CREATE TABLE {current_table} (\n" + ",\n".join(table_schema) + "\n);\n")
            
    except MySQLError as e:
        print(f"Error extracting schema: {e}")
        return f"ERROR: Failed to extract database schema. Details: {e}"
    finally:
        if conn and conn.is_connected():
            conn.close()

    # The LLM needs to know the dialect!
    return "-- MySQL Dialect Schema --\n\n" + "\n".join(schema_info)


# backend/utils/db_utils.py (REVISED)

# ... (Imports and DB Configuration remain the same) ...

# NOTE: The get_db_connection and extract_db_schema functions can remain, 
# but we will bypass them in execute_sql_query for stability.

def execute_sql_query(sql_query: str):
    """
    Executes a given SQL query using a connection URL for stability with Pandas.
    """
    
    # Construct the MySQL connection URL
    # Format: mysql+mysqlconnector://user:pass@host:port/database
    db_url = os.getenv("DATABASE_URL")

    # Note: If you face issues with the raw mysql.connector driver, 
    # you might need to install and use 'PyMySQL' for the URL:
    # pip install PyMySQL
    # db_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DATABASE_NAME}"


    conn = None # Initialize connection to None for the final block
    try:
        # 1. Execute Query using the URL
        # Pandas manages opening and closing the connection for this single query,
        # which is more robust than passing a raw DBAPI object.
        df = pd.read_sql_query(sql_query, db_url) 
        
        # 2. Get Results
        results = df.to_dict('records')
        prettified_result = df.to_markdown(index=False)
        
        return results, prettified_result
        
    except Exception as e:
        # Catch any connection, execution, or Pandas error
        return {"error": f"SQL execution/DB error: {e}"}, None
    finally:
        # We don't need to manually close the connection here if using the URL,
        # as Pandas handles it internally.
        pass
