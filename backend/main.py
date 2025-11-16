
import os
import time
import csv
import shutil
import asyncio
import logging
from decimal import Decimal
from datetime import date, datetime
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import requests

# local modules
from prompts.prompt_builder import build_nl2sql_prompt
from utils.table_extraction import get_full_db_schema
from utils.db_utils import get_db_connection, execute_sql_query,validate_sql

load_dotenv()

app = FastAPI(title="NL2SQL Engine")

origins = os.getenv("FRONTEND_ALLOWED_ORIGINS", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origins] if origins != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

MAX_PREVIEW_ROWS = int(os.getenv("PREVIEW_ROWS", "50"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))

# Cleanup on startup
@app.on_event("startup")
def clean_startup():
    try:
        shutil.rmtree(DATA_DIR)
    except Exception:
        pass
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.info("Cleaned data directory on startup.")

# Normalize DB values
def normalize_value(v):
    if isinstance(v, Decimal):
        try:
            return float(v)
        except Exception:
            return str(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", errors="ignore")
        except Exception:
            return str(v)
    return v

def normalize_rows(rows: List[Dict[str, Any]]):
    return [{k: normalize_value(v) for k, v in r.items()} for r in rows]

# Add LIMIT for preview
def enforce_preview_limit(sql: str):
    if "limit" in sql.lower():
        return sql
    return f"{sql.rstrip().rstrip(';')} LIMIT {MAX_PREVIEW_ROWS}"

# Stream large CSV
def stream_full_csv(sql: str):
    conn = get_db_connection()
    if conn is None:
        raise Exception("DB connection failed.")

    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)

    timestamp = int(time.time())
    filename = f"results_{timestamp}.csv"
    path = os.path.join(DATA_DIR, filename)

    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = None
            first_chunk = cursor.fetchmany(MAX_PREVIEW_ROWS)
            preview = normalize_rows(first_chunk)

            if preview:
                fieldnames = list(preview[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(preview)
            else:
                desc = cursor.description
                if desc:
                    fieldnames = [d[0] for d in desc]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

            while True:
                chunk = cursor.fetchmany(CHUNK_SIZE)
                if not chunk:
                    break
                normalized = normalize_rows(chunk)
                if writer is None and normalized:
                    fieldnames = list(normalized[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                if normalized:
                    writer.writerows(normalized)

    finally:
        cursor.close()
        conn.close()

    return preview, filename

# session history
chat_history: Dict[str, List[str]] = {}

# Request body model
class NLQuery(BaseModel):
    session_id: str
    question: str

# LLM wrapper
def call_llm_generate(prompt: str, timeout: int = 60) -> str:
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise Exception("LLM API key missing.")

    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-preview-09-2025")
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    r = requests.post(api_url, json=payload, timeout=timeout)
    r.raise_for_status()

    cand = r.json().get("candidates", [{}])[0]
    text = cand.get("content", {}).get("parts", [{}])[0].get("text", "")
    return text.strip()

# -------------------------------------------------------------------
# MAIN NL2SQL ENDPOINT
# -------------------------------------------------------------------
@app.post("/query")
async def query_endpoint(q: NLQuery, request: Request):

    session_id = q.session_id.strip()
    if not session_id or session_id in ["null", "undefined"]:
        raise HTTPException(status_code=400, detail="Invalid session_id provided")

    if session_id not in chat_history:
        chat_history[session_id] = []   # create stable history

    question = q.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Empty question")

    # load schema
    try:
        schema_text = get_full_db_schema()
        schema_obj = get_full_db_schema(raw=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema load failed: {e}")
    
    # build LLM prompt
    history_list = chat_history.get(session_id, [])
    prompt = build_nl2sql_prompt(schema_text, question, history_list)

    # generate SQL
    try:
        raw_sql = await asyncio.to_thread(call_llm_generate, prompt)
        raw_sql = raw_sql.replace("```sql", "").replace("```", "").strip()
        if not raw_sql:
            raise Exception("LLM returned empty SQL.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM error: {e}")

    # VALIDATION
    try:
        validate_sql(raw_sql)
    except Exception as e:
        print("VALIDATION FAILED:", e)
        raise HTTPException(status_code=400, detail=f"SQL validation failed: {e}")

    # preview execution
    preview_sql = enforce_preview_limit(raw_sql)
    try:
        preview_res, pretty = await asyncio.to_thread(execute_sql_query, preview_sql)
        if isinstance(preview_res, dict) and preview_res.get("error"):
            raise Exception(preview_res.get("error"))
        preview_rows = normalize_rows(preview_res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview query failed: {e}")

    # full CSV
    try:
        preview_from_full, csv_filename = await asyncio.to_thread(stream_full_csv, raw_sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV export failed: {e}")

    # conversational summary
    conversational_summary = "Executed query; preview shown."
    try:
        import json
        sample = json.dumps(preview_rows[:10], default=str)
        sum_prompt = f"Summarize briefly using ONLY these preview rows:\n{sample}\n\nQuestion:\n{question}"
        conv_output = await asyncio.to_thread(call_llm_generate, sum_prompt, 30)
        if conv_output.strip():
            conversational_summary = conv_output.strip()
    except Exception:
        pass

    # update session history
    history_list.append(f"User: {question}")
    history_list.append(f"SQL: {raw_sql}")
    chat_history[session_id] = history_list

    # download link
    host = os.getenv("FASTAPI_PUBLIC_HOST", "127.0.0.1")
    port = os.getenv("FASTAPI_PUBLIC_PORT", "8000")
    download_url = f"http://{host}:{port}/download/{csv_filename}"

    return JSONResponse({
        "raw_sql": raw_sql,
        "preview_rows": preview_rows,
        "csv_filename": csv_filename,
        "csv_download_url": download_url,
        "conversational_summary": conversational_summary
    })


# -------------------------------------------------------------------
# CSV DOWNLOAD
# -------------------------------------------------------------------
@app.get("/download/{filename}")
def download_csv(filename: str):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename, media_type="text/csv")
