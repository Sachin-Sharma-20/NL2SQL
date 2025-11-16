import streamlit as st
import requests
import pandas as pd
import os
import uuid
from dotenv import load_dotenv

load_dotenv()
BACKEND_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000/query")

st.set_page_config(page_title="Natural Query Engine", layout="wide")

# =====================================================================
# STATE INITIALIZATION
# =====================================================================
def init_session_state():
    """Initialize all session state variables"""
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if "result" not in st.session_state:
        st.session_state.result = None
    if "history" not in st.session_state:
        st.session_state.history = []
    if "current_csv_bytes" not in st.session_state:
        st.session_state.current_csv_bytes = None
    if "current_csv_filename" not in st.session_state:
        st.session_state.current_csv_filename = "results.csv"
    if "run_query_flag" not in st.session_state:
        st.session_state.run_query_flag = False
    if "query_text" not in st.session_state:
        st.session_state.query_text = ""

init_session_state()

# =====================================================================
# API FUNCTIONS
# =====================================================================
def fetch_csv_from_url(csv_url):
    """Download CSV file from URL"""
    try:
        csv_resp = requests.get(csv_url, timeout=30)
        if csv_resp.status_code == 200:
            return csv_resp.content
    except Exception as e:
        print(f"CSV fetch error: {e}")
    return None

def execute_backend_query(query_text, session_id):
    """Execute query against backend API"""
    resp = requests.post(
        BACKEND_URL,
        json={"question": query_text, "session_id": session_id},
        timeout=90
    )
    resp.raise_for_status()
    return resp.json()

def process_query_execution():
    """Process query if flag is set"""
    if not st.session_state.run_query_flag:
        return
    
    st.session_state.run_query_flag = False
    query_to_execute = st.session_state.query_text
    
    # Show spinner while processing
    with st.spinner("Executing query... Generating SQL and fetching results..."):
        try:
            print(f"\n{'='*60}")
            print(f"EXECUTING QUERY: {query_to_execute}")
            print(f"SESSION ID: {st.session_state.session_id}")
            
            # Execute query
            result = execute_backend_query(query_to_execute, st.session_state.session_id)
            
            print(f"BACKEND RESPONSE:")
            print(f"  - SQL: {result.get('raw_sql', 'N/A')[:100]}...")
            print(f"  - Preview rows: {len(result.get('preview_rows', []))}")
            print(f"  - Summary: {result.get('conversational_summary', 'N/A')}")
            
            # Fetch CSV if available
            csv_bytes = None
            csv_filename = result.get("csv_filename", "results.csv")
            csv_url = result.get("csv_download_url")
            if csv_url:
                print(f"FETCHING CSV from: {csv_url}")
                csv_bytes = fetch_csv_from_url(csv_url)
                print(f"CSV downloaded: {len(csv_bytes) if csv_bytes else 0} bytes")
            
            # Update session state
            st.session_state.result = result
            st.session_state.current_csv_bytes = csv_bytes
            st.session_state.current_csv_filename = csv_filename
            st.session_state.history.append({
                "question": query_to_execute,
                "summary": result.get("conversational_summary", ""),
                "sql": result.get("raw_sql", "")
            })
            
            print(f"STATE UPDATED - Result stored: {st.session_state.result is not None}")
            print(f"{'='*60}\n")
            
        except requests.exceptions.Timeout:
            print("ERROR: Request timed out")
            st.error("Request timed out. Try a simpler query.")
        except Exception as e:
            print(f"ERROR: {e}")
            st.error(f"Error: {e}")

# Execute query at the top before rendering
process_query_execution()

# =====================================================================
# STYLING
# =====================================================================
def apply_custom_styles():
    """Apply custom CSS styles"""
    st.markdown("""
    <style>
    :root{
        --bg:#0f1115;
        --surface:#17191f;
        --surface-light:#1d1f25;
        --text:#e5e5e5;
        --muted:#b8b8b8;
        --border:#282b32;
        --primary:#3b82f6;
        --primary-hover:#2563eb;
        --accent:#f97316;
        --accent-hover:#ea580c;
    }
    body, html { 
        background: var(--bg); 
    }
    header[data-testid="stHeader"], footer { 
        display:none; 
    }
    h1 { 
        font-size: 42px !important; 
        font-weight: 800; 
        text-align:center;
        margin-bottom: 10px !important;
    }
    h2, h3 {
        margin-top: 30px !important;
        margin-bottom: 20px !important;
    }
    .stTextArea textarea {
        background: var(--surface-light) !important;
        color: var(--text) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px;
        padding: 12px;
        font-size: 17px;
        min-height: 110px !important;
    }
    div[data-testid="stButton"] button {
        background: var(--primary) !important;
        border-radius: 8px !important;
        color: white !important;
        padding: 12px 24px !important;
        width: 100%;
    }
    div[data-testid="stButton"] button:hover {
        background: var(--primary-hover) !important;
    }
    /* Style only example query buttons (in the example section) */
    div[data-testid="column"] div[data-testid="stButton"] button {
        height: 80px;
        display: flex;
        align-items: center;
        justify-content: center;
        text-align: center;
        padding: 16px 24px !important;
    }
    /* Override for Run Query button - orange with white text */
    button[kind="primary"][key="run_query_btn"],
    button[data-testid="baseButton-primary"] {
        height: auto !important;
        min-height: 42px !important;
        background: var(--accent) !important;
        color: white !important;
        font-weight: 600 !important;
        font-size: 15px !important;
        border: none !important;
    }
    button[kind="primary"][key="run_query_btn"]:hover,
    button[data-testid="baseButton-primary"]:hover {
        background: var(--accent-hover) !important;
        color: white !important;
    }
    .info-box {
        background: var(--surface);
        border: 1px solid var(--border);
        padding: 20px;
        border-radius: 10px;
        margin: 20px 0;
    }
    /* Make columns equal height */
    div[data-testid="column"] {
        display: flex;
        flex-direction: column;
    }
    div[data-testid="column"] > div {
        flex: 1;
    }
    /* Spacing adjustments */
    .block-container {
        padding-top: 3rem !important;
        padding-bottom: 3rem !important;
        max-width: 1400px !important;
    }
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        background-color: var(--surface-light);
        border-radius: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

apply_custom_styles()

# =====================================================================
# HEADER SECTION
# =====================================================================
def render_header():
    """Render page header and info"""
    st.markdown("<h1>Natural Query Engine</h1>", unsafe_allow_html=True)
    st.markdown(
        "<p style='text-align:center;color:#b8b8b8;'>TPCH dataset — supports follow-up queries.</p>",
        unsafe_allow_html=True
    )
    st.markdown("""
    <div class="info-box">
    This tool uses the <strong>TPCH analytical dataset</strong> with tables:
    <strong>customer, orders, lineitem, supplier, partsupp, part, nation, region</strong>.<br>
    Ask questions in plain English — the engine automatically converts them to SQL.
    </div>
    """, unsafe_allow_html=True)

render_header()

# =====================================================================
# EXAMPLE QUERIES SECTION
# =====================================================================
def render_example_queries():
    """Render example query buttons"""
    st.subheader("Example Queries")
    
    examples = [
        "Show customers with nation and region names.",
        "Show total revenue grouped by order date.",
        "Show the top 100 most expensive orders.",
    ]
    
    cols = st.columns(3)
    for i, ex in enumerate(examples):
        with cols[i]:
            if st.button(ex, key=f"ex_{i}", use_container_width=True):
                st.session_state.query_text = ex
                st.session_state.run_query_flag = True
                st.rerun()

render_example_queries()

# =====================================================================
# QUERY INPUT SECTION
# =====================================================================
def render_query_input():
    """Render query input area"""
    query = st.text_area(
        "Query Input",
        value=st.session_state.query_text,
        height=110,
        placeholder="Example: Show total revenue for each region",
        key="query_input",
        label_visibility="visible"
    )
    
    # Custom CSS just for this button
    st.markdown("""
        <style>
        div.stButton > button[kind="primary"] {
            background-color: #f97316 !important;
            color: white !important;
            border: none !important;
            font-weight: 600 !important;
        }
        div.stButton > button[kind="primary"]:hover {
            background-color: #ea580c !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Button in a narrow column to make it smaller
    col1, col2, col3 = st.columns([0.8, 4.2, 1])
    
    with col1:
        if st.button("Run Query", type="primary", key="run_query_btn", use_container_width=True):
            if not query.strip():
                st.error("Please enter a query!")
            else:
                st.session_state.query_text = query
                st.session_state.run_query_flag = True
                # Don't rerun here - let it execute in the same flow

# Execute query AFTER input is rendered, so button click can set the flag
render_query_input()
st.markdown("---")

# Now execute if flag was set
process_query_execution()

# =====================================================================
# RESULTS SECTION
# =====================================================================
def render_summary_tab(data):
    """Render summary tab content"""
    summary = data.get('conversational_summary', 'No summary')
    st.write(summary)

def render_preview_tab(data):
    """Render preview table tab content"""
    preview = data.get("preview_rows", [])
    if preview:
        df = pd.DataFrame(preview)
        st.dataframe(df, use_container_width=True, height=400)
        st.caption(f"Showing {len(preview)} rows")
        
        if st.session_state.current_csv_bytes:
            st.download_button(
                "📥 Download Full CSV",
                st.session_state.current_csv_bytes,
                file_name=st.session_state.current_csv_filename,
                mime="text/csv"
            )
    else:
        st.info("No data returned.")

def render_sql_tab(data):
    """Render SQL tab content"""
    st.code(data.get("raw_sql", "-- No SQL --"), language="sql")

def render_history_tab():
    """Render history tab content"""
    if st.session_state.history:
        for idx, prev in enumerate(st.session_state.history):
            is_current = (idx == len(st.session_state.history) - 1)
            with st.expander(
                f"Query {idx + 1}: {prev['question'][:60]}...",
                expanded=is_current
            ):
                st.markdown(f"**Q:** {prev['question']}")
                st.markdown(f"**A:** {prev['summary']}")
                if prev.get('sql'):
                    st.code(prev['sql'], language="sql")
    else:
        st.info("No history yet.")

def render_results():
    """Render results section"""
    if st.session_state.result is None:
        return
    
    data = st.session_state.result
    st.subheader("📊 Query Response")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "💬 Summary",
        "📋 Preview Table",
        "🔍 Generated SQL",
        "📜 History"
    ])
    
    with tab1:
        render_summary_tab(data)
    
    with tab2:
        render_preview_tab(data)
    
    with tab3:
        render_sql_tab(data)
    
    with tab4:
        render_history_tab()

render_results()

# =====================================================================
# FOOTER
# =====================================================================
def render_footer():
    """Render footer section"""
    st.divider()
    st.caption(f"🔑 Session: `{st.session_state.session_id}`")
    st.caption("💡 This session persists for follow-up queries.")

render_footer()