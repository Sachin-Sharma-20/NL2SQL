# backend/prompts/prompt_builder.py

def build_nl2sql_prompt(db_schema: str, question: str, history: list[str]) -> str:
    """
    Constructs the final prompt for the Gemini model, explicitly specifying MySQL.
    """
    chat_history = "\n".join(history)
    
    # 🌟 Added "MySQL" to the system instructions and added CRITICAL INSTRUCTION
    prompt = f"""
    You are an expert SQL generator. Your task is to convert a natural language question into a syntactically correct and executable **MySQL** SQL query for the 'tpch' database.
    
    # CRITICAL INSTRUCTION
    1. **STRICTLY** use only the table names and column names exactly as they are defined in the schema below (e.g., use CUSTOMER, not Customers).
    2. Ensure the query uses **MySQL syntax** (e.g., use backticks `table_name` if necessary, though not strictly required for standard names).
    
    # DATABASE SCHEMA
    The database schema is provided below. Analyze it carefully to construct your query.
    {db_schema}
    
    # CHAT HISTORY
    This is the conversation history. Use it to answer follow-up questions.
    {chat_history if history else 'No prior conversation history.'}
    
    # INSTRUCTIONS
    1. The resulting SQL must be runnable on the given schema.
    2. **DO NOT** include any explanations, comments, markdown delimiters (```sql), or extra text.
    3. **ONLY** output the raw SQL query.
    4. Always use **LIMIT 10** for any SELECT query unless an aggregation is requested or the question explicitly asks for all results.
    
    # QUESTION
    Convert the following natural language question into a single SQL query:
    "{question}"
    
    SQL Query:
    """
    return prompt