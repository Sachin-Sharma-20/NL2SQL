import os
import json
import time
from typing import Dict, Any, List
from dotenv import load_dotenv
import requests

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Assuming db_utils.py is available and contains the following functions:
# - extract_db_schema(): Fetches schema as a string.
# - execute_sql_query(sql): Executes SQL and returns (results_data, prettified_result).
try:
    # Changed from relative import (.utils) to non-relative import (utils)
    # This assumes 'utils' is either directly visible or added to the path when running Uvicorn.
    from utils.db_utils import extract_db_schema, execute_sql_query 
except ImportError:
    # Fallback for environments where the direct import fails, 
    # use the relative import structure but warn the user.
    print("Warning: Direct import 'from utils.db_utils' failed. Falling back to relative import which may fail in Uvicorn.")
    from .utils.db_utils import extract_db_schema, execute_sql_query

# --- Configuration & Initialization ---

load_dotenv()
app = FastAPI(title="Gemini NL2SQL Backend")

# In-memory history for session management: {session_id: [User Q1, Generated SQL 1, ...]}
chat_history_db: Dict[str, List[str]] = {}

# LLM API configuration (using Google Search grounding is optional for summarization)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not GEMINI_API_KEY:
    print("Warning: GEMINI_API_KEY or GOOGLE_API_KEY not set.")
    
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"
API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
API_URL = f"{API_BASE_URL}/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"


# --- Pydantic Model for Request Body ---

class NLQuery(BaseModel):
    """Defines the expected input structure from the frontend."""
    session_id: str
    question: str


# --- Core LLM Utility Functions (Self-Contained) ---

def llm_api_call(payload: Dict[str, Any], max_retries: int = 3) -> str:
    """Handles API call with exponential backoff and returns raw text."""
    headers = {'Content-Type': 'application/json'}
    
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=headers, json=payload, timeout=60)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            
            result = response.json()
            text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '').strip()
            
            if text:
                return text
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"LLM API attempt {attempt+1} failed ({e}). Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise Exception(f"LLM API call failed after {max_retries} attempts: {e}")
                
    raise Exception("LLM API call failed without detailed error.")

def clean_sql_output(text: str) -> str:
    """Extracts raw SQL from LLM output, removing delimiters and explanations."""
    # Remove markdown formatting if present
    text = text.strip()
    if text.startswith('```sql'):
        text = text[6:]
    if text.endswith('```'):
        text = text[:-3]
    return text.strip()

# --- Prompt Builders (Self-Contained) ---

def build_nl2sql_prompt(db_schema: str, question: str, history: List[str]) -> Dict[str, Any]:
    """Constructs the payload for SQL generation."""
    chat_history = "\n".join(history)
    
    prompt = f"""
    You are an expert SQL generator. Convert the natural language question into a syntactically correct and executable **MySQL** SQL query for the 'tpch' database.
    
    # CRITICAL INSTRUCTION
    1. **STRICTLY** use only the table names and column names exactly as they are defined in the schema below.
    2. **ONLY** output the raw SQL query. Do not include any explanations, markdown delimiters (```sql), or comments.
    
    # DATABASE SCHEMA
    {db_schema}
    
    # CHAT HISTORY
    {chat_history if history else 'No prior conversation history.'}
    
    # QUESTION
    Convert the following natural language question into a single SQL query:
    "{question}"
    
    SQL Query:
    """
    return {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": "You are a MySQL query generator. Output only the raw SQL."}]},
    }

def build_summary_prompt(question: str, raw_sql: str, results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Constructs the payload for conversational summary generation."""
    
    # Use only the first 10 rows for summarization to save tokens and avoid JSON overload
    concise_results = json.dumps(results[:10], indent=2)

    prompt = f"""
    You are a friendly, expert data analyst. Your task is to provide a concise, human-like, conversational summary of the query results below, specifically addressing the user's original question.
    
    - Do NOT mention the SQL query itself.
    - Do NOT use technical terms like 'row count', 'data set', or 'query criteria'.
    - Use specific numbers from the results if appropriate.
    - Start with a conversational phrase (e.g., "Certainly!", "That's a great question.").
    
    # USER QUESTION
    "{question}"
    
    # SQL EXECUTED
    {raw_sql}
    
    # RESULT DATA (JSON)
    {concise_results}
    
    Conversational Summary:
    """
    return {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": "You are a friendly data analyst. Provide a conversational, non-robotic summary of the provided data in a single paragraph."}]},
    }


# --- FastAPI Endpoint ---

@app.post("/query")
async def generate_and_execute_sql(query: NLQuery):
    """
    Main endpoint to handle the NL2SQL process and two-step LLM summarization.
    """
    session_id = query.session_id
    question = query.question
    
    # 1. Get History and Schema
    current_history = chat_history_db.get(session_id, [])
    db_schema = extract_db_schema()

    if "ERROR" in db_schema:
        raise HTTPException(status_code=500, detail=f"Database schema extraction failed: {db_schema}")
    
    try:
        # --- LLM Call 1: Generate SQL ---
        sql_payload = build_nl2sql_prompt(db_schema, question, current_history)
        raw_sql_output = llm_api_call(sql_payload)
        raw_sql = clean_sql_output(raw_sql_output)
        
    except Exception as e:
        # LLM failed to generate SQL
        raise HTTPException(status_code=500, detail=f"LLM failed to generate SQL: {e}")

    # 2. Execute SQL Query
    results_data, prettified_result = execute_sql_query(raw_sql)
    
    if results_data and "error" in results_data:
        # SQL execution failed (e.g., syntax error, table not found)
        # We still save the generated SQL to history for debugging
        current_history.append(f"User Question: {question}")
        current_history.append(f"Generated SQL: {raw_sql}")
        chat_history_db[session_id] = current_history
        
        raise HTTPException(status_code=500, detail=results_data["error"])
        
    # --- LLM Call 2: Generate Conversational Summary ---
    conversational_summary = "I executed the query successfully, but could not generate a conversational summary."
    
    if results_data: # Only summarize if results are meaningful (not empty or error)
        try:
            summary_payload = build_summary_prompt(question, raw_sql, results_data)
            summary_output = llm_api_call(summary_payload)
            
            # Simple cleaning for summary text
            conversational_summary = summary_output.strip()

        except Exception as e:
            print(f"Warning: LLM failed to generate summary. Falling back to default response. Error: {e}")
            # If summarization fails, we fall back and proceed, but log the error
            conversational_summary = "I retrieved the data for you, but encountered a slight issue generating the natural language summary."
    
    
    # 3. History Update (Only after successful execution)
    current_history.append(f"User Question: {question}")
    current_history.append(f"Generated SQL: {raw_sql}")
    chat_history_db[session_id] = current_history
    
    # 4. Return Final Response
    return JSONResponse(content={
        "raw_sql": raw_sql,
        "prettified_result": prettified_result, # The Markdown table
        "results_data": results_data,          # The raw list of dicts
        "conversational_summary": conversational_summary # The human-like answer
    })
