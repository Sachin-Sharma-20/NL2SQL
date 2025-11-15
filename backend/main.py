# backend/main.py

import os
import json
import time
import csv
import shutil
import asyncio
import logging
from decimal import Decimal
from datetime import date, datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import requests

# local modules
from prompts.prompt_builder import build_nl2sql_prompt
from utils.table_extraction import get_full_db_schema
from utils.db_utils import get_db_connection, execute_sql_query

load_dotenv()

app = FastAPI(title="NL2SQL Engine")

# data dir
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# tuning
MAX_PREVIEW_ROWS = int(os.getenv("PREVIEW_ROWS", "50"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))

# startup cleanup so old CSVs are removed even after crashes
@app.on_event("startup")
def clean_startup():
    try:
        shutil.rmtree(DATA_DIR)
    except Exception:
        pass
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.info("[STARTUP] Cleaned data directory.")


# normalization helpers
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


def normalize_rows(rows):
    return [{k: normalize_value(v) for k, v in r.items()} for r in rows]


# preview SQL enforcement; limit only for preview
def enforce_preview_limit(sql: str):
    if "limit" in sql.lower():
        return sql
    return f"{sql.rstrip().rstrip(';')} LIMIT {MAX_PREVIEW_ROWS}"


# stream full SQL -> CSV without loading all rows into memory
def stream_full_csv(sql: str):
    conn = get_db_connection()
    if conn is None:
        raise Exception("DB connection failed (stream_full_csv).")

    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)

    timestamp = int(time.time())
    filename = f"results_{timestamp}.csv"
    path = os.path.join(DATA_DIR, filename)

    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = None

            # preview chunk (first N rows)
            first_chunk = cursor.fetchmany(MAX_PREVIEW_ROWS)
            preview = normalize_rows(first_chunk)

            if preview:
                fieldnames = list(preview[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(preview)
            else:
                # if no preview rows, try to create header from cursor.description
                desc = cursor.description
                if desc:
                    fieldnames = [d[0] for d in desc]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

            # stream remaining rows in chunks
            while True:
                chunk = cursor.fetchmany(CHUNK_SIZE)
                if not chunk:
                    break
                normalized = normalize_rows(chunk)
                if writer is None:
                    fieldnames = list(normalized[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                writer.writerows(normalized)

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return preview, filename


# request model
class NLQuery(BaseModel):
    session_id: str
    question: str


chat_history_db = {}


@app.post("/query")
async def query_endpoint(q: NLQuery):
    session_id = q.session_id
    question = q.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Empty question")

    # 1) get schema for prompt
    schema_text = get_full_db_schema()

    # 2) build prompt
    history = chat_history_db.get(session_id, [])
    prompt = build_nl2sql_prompt(schema_text, question, history)

    # 3) call LLM to generate SQL
    API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not API_KEY:
        raise HTTPException(status_code=500, detail="LLM API key not configured")
    MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-preview-09-2025")
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent?key={API_KEY}"

    try:
        r = requests.post(api_url, json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=60)
        r.raise_for_status()
        candidate = r.json().get("candidates", [{}])[0]
        raw_sql = candidate.get("content", {}).get("parts", [{}])[0].get("text", "").strip()
        raw_sql = raw_sql.replace("```sql", "").replace("```", "").strip()
        if not raw_sql:
            raise Exception("LLM returned empty SQL.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM call failed: {e}")

    # 4) preview (limited) — use execute_sql_query for preview (your utils)
    preview_sql = enforce_preview_limit(raw_sql)
    try:
        preview_res, pretty = await asyncio.to_thread(execute_sql_query, preview_sql)
        if isinstance(preview_res, dict) and preview_res.get("error"):
            raise Exception(preview_res.get("error"))
        preview_rows = normalize_rows(preview_res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview execution failed: {e}")

    # 5) full CSV export (no limit) — stream in thread
    try:
        preview_from_full, csv_filename = await asyncio.to_thread(stream_full_csv, raw_sql)
        # preview_from_full contains first N rows from full export, but we already have preview_rows above
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Full CSV export failed: {e}")

    # 6) summary using preview (safe)
    conversational_summary = "Executed query; preview shown."
    try:
        sample = json.dumps(preview_rows[:10], default=str)
        summary_prompt = f"Summarize in 2 short sentences using ONLY the preview values:\n{sample}\nQuestion:\n{question}"
        r2 = requests.post(api_url, json={"contents": [{"parts": [{"text": summary_prompt}]}]}, timeout=30)
        r2.raise_for_status()
        conv = r2.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
        if conv:
            conversational_summary = conv
    except Exception:
        conversational_summary = "Executed query; preview shown."

    # 7) update history
    history.append(f"User Question: {question}")
    history.append(f"Generated SQL: {raw_sql}")
    chat_history_db[session_id] = history

    # 8) build download URL
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


@app.get("/download/{filename}")
def download_csv(filename: str):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename, media_type="text/csv")

