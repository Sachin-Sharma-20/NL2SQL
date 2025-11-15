#!/bin/bash
# ==============================
# Load TPC-H .tbl data into MySQL (FINAL FIXED)
# ==============================

DBNAME="tpch"
USER="root"

# prompt password securely
read -s -p "Enter MySQL password for $USER: " PASS
echo

MYSQL="mysql -u $USER -p$PASS --local-infile=1"

echo "🔹 Creating database '$DBNAME'..."
$MYSQL -e "DROP DATABASE IF EXISTS $DBNAME; CREATE DATABASE $DBNAME; USE $DBNAME;"

echo "🔹 Creating lowercase schema (to match .tbl files)..."
cat <<EOF | $MYSQL $DBNAME
CREATE TABLE region (
    r_regionkey INT PRIMARY KEY,
    r_name CHAR(25),
    r_comment VARCHAR(152)
);

CREATE TABLE nation (
    n_nationkey INT PRIMARY KEY,
    n_name CHAR(25),
    n_regionkey INT,
    n_comment VARCHAR(152),
    FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)
);

CREATE TABLE part (
    p_partkey INT PRIMARY KEY,
    p_name VARCHAR(55),
    p_mfgr CHAR(25),
    p_brand CHAR(10),
    p_type VARCHAR(25),
    p_size INT,
    p_container CHAR(10),
    p_retailprice DECIMAL(12,2),
    p_comment VARCHAR(23)
);

CREATE TABLE supplier (
    s_suppkey INT PRIMARY KEY,
    s_name CHAR(25),
    s_address VARCHAR(40),
    s_nationkey INT,
    s_phone CHAR(15),
    s_acctbal DECIMAL(12,2),
    s_comment VARCHAR(101),
    FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
);

CREATE TABLE partsupp (
    ps_partkey INT,
    ps_suppkey INT,
    ps_availqty INT,
    ps_supplycost DECIMAL(12,2),
    ps_comment VARCHAR(199),
    PRIMARY KEY (ps_partkey, ps_suppkey),
    FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
    FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
);

CREATE TABLE customer (
    c_custkey INT PRIMARY KEY,
    c_name VARCHAR(25),
    c_address VARCHAR(40),
    c_nationkey INT,
    c_phone CHAR(15),
    c_acctbal DECIMAL(12,2),
    c_mktsegment CHAR(10),
    c_comment VARCHAR(117),
    FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
);

CREATE TABLE orders (
    o_orderkey INT PRIMARY KEY,
    o_custkey INT,
    o_orderstatus CHAR(1),
    o_totalprice DECIMAL(12,2),
    o_orderdate DATE,
    o_orderpriority CHAR(15),
    o_clerk CHAR(15),
    o_shippriority INT,
    o_comment VARCHAR(79),
    FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
);

CREATE TABLE lineitem (
    l_orderkey INT,
    l_partkey INT,
    l_suppkey INT,
    l_linenumber INT,
    l_quantity DECIMAL(12,2),
    l_extendedprice DECIMAL(12,2),
    l_discount DECIMAL(12,2),
    l_tax DECIMAL(12,2),
    l_returnflag CHAR(1),
    l_linestatus CHAR(1),
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct CHAR(25),
    l_shipmode CHAR(10),
    l_comment VARCHAR(44),
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),
    FOREIGN KEY (l_partkey) REFERENCES part(p_partkey),
    FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey)
);
EOF

echo "🔹 Loading .tbl data files into MySQL..."
TABLES=(region nation part supplier partsupp customer orders lineitem)

for t in "${TABLES[@]}"; do
    FILE="$(pwd)/${t}.tbl"
    if [ -f "$FILE" ]; then
        echo "➡️  Loading $t.tbl..."
        $MYSQL $DBNAME -e "
            SET foreign_key_checks=0;
            LOAD DATA LOCAL INFILE '${FILE}'
            INTO TABLE ${t}
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\n'
            IGNORE 0 LINES;
            SET foreign_key_checks=1;
        "
    else
        echo "⚠️  File $FILE not found — skipping."
    fi
done

echo "✅ All data loaded successfully into database '$DBNAME'!"
