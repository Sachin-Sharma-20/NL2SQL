import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()
BACKEND_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000/query")

# --- Streamlit Page Config ---
st.set_page_config(page_title="NL2SQL", layout="wide")

# --- Custom CSS Styling ---
st.markdown("""
    <style>
    /* Main layout */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1100px;
    }

    /* Title */
    h1 {
        color: #00B4D8;
        text-align: center;
        font-size: 2.4rem;
        margin-bottom: 1rem;
    }

    /* Subtitle */
    p, .stMarkdown {
        color: #d1d5db;
        font-size: 1rem;
    }

    /* Buttons */
    div.stButton > button {
        width: 100%;
        border-radius: 10px;
        height: 3rem;
        font-weight: 600;
        background: linear-gradient(90deg, #0077b6, #00b4d8);
        color: white;
        border: none;
        box-shadow: 0 0 10px rgba(0, 180, 216, 0.5);
    }
    div.stButton > button:hover {
        background: linear-gradient(90deg, #00b4d8, #48cae4);
        color: #fff;
    }

    /* SQL code block */
    pre {
        background-color: #1E1E1E !important;
        border-radius: 10px;
        padding: 1rem !important;
        font-size: 0.95rem;
        line-height: 1.6;
        color: #80ed99 !important;
    }

    /* Section headers */
    .section-header {
        font-size: 1.4rem;
        font-weight: 700;
        color: #90e0ef;
        margin-top: 2rem;
        margin-bottom: 0.8rem;
    }

    /* Dataframe styling */
    div[data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #2b2b2b;
    }
    </style>
""", unsafe_allow_html=True)

# --- Header ---
st.title("Natural Language → SQL Query")

st.markdown("""
Turn your plain English questions into live SQL queries.
Ask about your database naturally — We will do the translation and execution.
""")

# --- User Input ---
query_input = st.text_area(
    "Ask your question:",
    height=100,
    placeholder="e.g. Show total revenue for each region in 2023"
)

# --- Run Query Button ---
if st.button("Run Query"):
    if not query_input.strip():
        st.warning("Please enter a question first.")
    else:
        with st.spinner("Thinking... Generating SQL and fetching data..."):
            try:
                response = requests.post(
                    BACKEND_URL,
                    json={"question": query_input, "session_id": "default"},
                    timeout=90
                )
                if response.status_code == 200:
                    data = response.json()
                    # Query Result Section
                    result = data.get("results_data", [])
                    
                    if data.get("conversational_summary"):
                        st.markdown('<div class="section-header">Summary</div>', unsafe_allow_html=True)
                        st.success(data["conversational_summary"])

                    if isinstance(result, list) and len(result) > 0:
                        df = pd.DataFrame(result)
                        st.markdown('<div class="section-header">Query Result</div>', unsafe_allow_html=True)
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info("No results found for your query.")
                    
                    # SQL Section
                    st.markdown('<div class="section-header">Generated SQL</div>', unsafe_allow_html=True)
                    st.code(data.get("raw_sql", "No SQL generated"), language="sql")

                    # Conversational Summary

                else:
                    st.error(f"Server error ({response.status_code}): {response.text}")

            except requests.exceptions.RequestException as e:
                st.error(f"Connection failed: {e}")
