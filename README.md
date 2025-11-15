
## Project Summary

NL2SQL allows users to input natural language questions such as:

> “Show top 10 customers by revenue.”

The system automatically generates SQL, runs it on the database, and returns the results.

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/Sachin-Sharma-20/NL2SQL.git
cd NL2SQL
```

### 2. Create and Activate a Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r backend/requirements.txt
pip install -r frontend/requirements.txt
```

---

## Database Setup (TPC-H)

### Step 1: Generate the Dataset

Navigate to the **TPC-H generator** directory and create data:

```bash
cd database/tpch-dbgen
./dbgen -s 1
```

This will generate `.tbl` data files such as `customer.tbl`, `orders.tbl`, etc.
The `-s 1` flag sets the data scale to 1GB.

### Step 2: Move Generated Tables

Move all `.tbl` files into the `database/db` directory:

```bash
mv *.tbl ../db/
```

### Step 3: Load Data into MySQL

Inside the `database/db` folder, run:

```bash
bash full_load_into_mysql.sh
```

This script will:

* Create a MySQL database named `tpch`
* Create all required tables
* Load `.tbl` data files into MySQL

`.tbl` files are ignored in Git and will not be pushed to the repository.

---

## Environment Setup

Create a `.env` file in the **backend** directory with the following variables:

```
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASS=password
DB_NAME=tpch
DATABASE_URL=mysql+pymysql://root:password@localhost:3306/tpch
GEMINI_API_KEY=
```

Replace values according to your setup.

---

## Running the Application

### Backend (FastAPI)

```bash
cd backend
uvicorn main:app --reload
```

Access it at:
[http://127.0.0.1:8000](http://127.0.0.1:8000)

### Frontend (Streamlit or Python App)

```bash
cd frontend
streamlit run app.py
```

---

## Project Structure

```
NLP2SQL/
├── backend/
│   ├── main.py
│   ├── prompts/
│   │   └── prompt_builder.py
│   ├── utils/
│   │   ├── db_utils.py
│   │   └── table_extraction.py
│   ├── .env
│   └── requirements.txt
│
├── database/
│   ├── db/
│   │   ├── full_load_into_mysql.sh
│   │   ├── .gitignore
│   │   └── *.tbl (ignored)
│   └── tpch-dbgen/
│
├── frontend/
│   ├── app.py
│   └── requirements.txt
│
├── .gitignore
└── README.md
```

---

## Tech Stack

* **FastAPI** — backend REST framework
* **MySQL** — relational database
* **PyMySQL / SQLAlchemy** — database connector and ORM
* **Python Dotenv** — environment configuration
* **Google Gemini API** — converts natural language to SQL
* **Streamlit / Python UI** — frontend query interface

---

## Author

**Sachin Sharma**
GitHub: [Sachin-Sharma-20](https://github.com/Sachin-Sharma-20)

---
