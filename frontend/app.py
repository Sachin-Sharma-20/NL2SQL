import streamlit as st
import requests
import pandas as pd
import os
import re
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000/query")

st.set_page_config(page_title="Natural Query Engine", layout="wide")

# ---------------------- CSS ----------------------
st.markdown("""
<style>

header[data-testid="stHeader"] {display: none;}
footer {display: none;}

.custom-header {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 58px;
    background-color: #181818;
    border-bottom: 1px solid #2e2e2e;
    display: flex;
    justify-content: center;
    align-items: center;
    z-index: 9999;
}

.custom-header-title {
    font-size: 1.4rem;
    font-weight: 650;
    color: #f3f3f3;
}

/* push content below header */
.block-container {
    padding-top: 100px !important;
}

/* center content container */
.center-container {
    display: flex;
    flex-direction: column;
    align-items: center;
}

.label {
    font-size: 1rem;
    font-weight: 600;
    color: white;
    margin-bottom: 4px;
    text-align: center;
}

.stTextArea textarea {
    background: #1e1e1e !important;
    border: 1px solid #333 !important;
    border-radius: 6px !important;
    color: white !important;
    font-size: 0.95rem !important;
}

.stButton>button {
    background: #2c7efb !important;
    color: white !important;
    padding: 7px 18px !important;
    border-radius: 6px !important;
    font-size: 0.95rem;
    margin-top: 8px;
}

.result-box {
    background: #1a1a1a;
    border: 1px solid #2d2d2d;
    padding: 14px;
    border-radius: 6px;
    color: #e5e5e5;
    line-height: 1.45;
    margin-top: 8px;
    font-size: 0.95rem;
}

.sql-box {
    background:#111111;
    border:1px solid #2c2c2c;
    padding:12px;
    border-radius:6px;
    font-family:monospace;
    font-size:0.9rem;
    white-space: pre-wrap;
    color:#dcdcdc;
}

.section-label {
    font-size:1rem;
    font-weight:600;
    color:white;
    margin-top:25px;
    margin-bottom:6px;
}

</style>

<div class="custom-header">
    <div class="custom-header-title">Natural Query Engine</div>
</div>

""", unsafe_allow_html=True)


# ---------------------- STATE ----------------------
if "last" not in st.session_state:
    st.session_state.last = None


# ---------------------- INPUT AREA ----------------------
st.markdown('<div class="center-container">', unsafe_allow_html=True)
st.markdown('<div class="label">Ask a question</div>', unsafe_allow_html=True)

query = st.text_area(
    "Query Input",
    height=80,
    placeholder="e.g., Show top 20 customers by revenue",
    label_visibility="collapsed"
)

if st.button("Run Query"):
    if not query.strip():
        st.warning("Enter a valid question.")
    else:
        with st.spinner("Processing query..."):
            try:
                resp = requests.post(
                    BACKEND_URL,
                    json={"question": query, "session_id": "default"},
                    timeout=None  # allow long-running queries
                )
                st.session_state.last = resp.json()

            except Exception as e:
                st.error(f"Backend error: {e}")

st.markdown('</div>', unsafe_allow_html=True)  # close center container


# ---------------------- RESULTS ----------------------
data = st.session_state.last

if data:

    # Summary
    st.markdown('<div class="section-label">Summary</div>', unsafe_allow_html=True)
    st.markdown(f"<div class='result-box'>{data.get('conversational_summary','')}</div>",
                unsafe_allow_html=True)

    # Preview table
    st.markdown('<div class="section-label">Preview (first rows)</div>', unsafe_allow_html=True)
    preview = data.get("preview_rows", [])
    if preview:
        st.dataframe(pd.DataFrame(preview), use_container_width=True, height=260)
    else:
        st.info("No preview available.")

    # CSV download
    st.markdown('<div class="section-label">Full CSV</div>', unsafe_allow_html=True)
    url = data.get("csv_download_url", "")
    fname = data.get("csv_filename", "")
    if url:
        try:
            file_bytes = requests.get(url).content
            st.download_button(
                "⬇ Download CSV",
                file_bytes,
                mime="text/csv",
                file_name=fname
            )
        except:
            st.error("Failed to download CSV.")
    else:
        st.info("No CSV generated.")

    # SQL shown
    st.markdown('<div class="section-label">Generated SQL</div>', unsafe_allow_html=True)
    st.markdown(f"<div class='sql-box'>{data.get('raw_sql','')}</div>",
                unsafe_allow_html=True)
