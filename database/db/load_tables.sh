#!/bin/bash
# ==========================================
# Load existing TPC-H .tbl files into MySQL
# (Assumes tables are already created)
# ==========================================

DBNAME="tpch"
USER="root"
PASS="sachin123"  # <-- change this to your MySQL password
MYSQL="mysql -u $USER -p$PASS --local-infile=1"

TABLES=(REGION NATION PART SUPPLIER PARTSUPP CUSTOMER ORDERS LINEITEM)


echo "🔹 Starting TPC-H data load into database '$DBNAME'..."
for t in "${TABLES[@]}"; do
    FILE="$(pwd)/${t}.tbl"
    if [ -f "$FILE" ]; then
        echo "➡️  Loading $t.tbl..."
        $MYSQL $DBNAME -e "
            SET foreign_key_checks = 0;
            LOAD DATA LOCAL INFILE '${FILE}'
            INTO TABLE ${t}
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\n'
            IGNORE 0 LINES;
            SET foreign_key_checks = 1;
        "
    else
        echo "⚠️  File ${FILE} not found — skipping."
    fi
done

echo "✅ All .tbl files loaded into '$DBNAME'!"
