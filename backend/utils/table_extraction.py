from .db_utils import extract_db_schema

def get_full_db_schema() -> str:
    """
    Retrieves the entire schema dynamically from the database.
    This wrapper can be extended later to filter relevant tables (using embeddings or LLM).
    """
    return extract_db_schema()

