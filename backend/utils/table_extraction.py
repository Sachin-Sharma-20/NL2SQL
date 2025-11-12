# backend/utils/table_extraction.py

from .db_utils import extract_db_schema

def get_full_db_schema() -> str:
    """
    Retrieves the entire schema dynamically from the database.
    
    This function currently serves as the gateway to the dynamic schema.
    If the project requires true "relevant" table extraction, the logic 
    (e.g., using LLM or embedding search on table names/comments) 
    will be placed here to filter the schema returned by extract_db_schema().
    """
    return extract_db_schema()