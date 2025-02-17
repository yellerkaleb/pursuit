import pandas as pd
import sqlite3

# Database file
DB_FILE = "pursuit.db"

# List of CSV files
csv_files = {
    "tblContacts": "contacts.csv",
    "tblEntities": "places.csv",
    "tblTechstacks": "techstacks.csv",
    "tblCRMa": "crma.csv",
    "tblCRMb": "crmb.csv",
}

# Establish SQLite Connection
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Create Tables
cursor.executescript("""
DROP TABLE IF EXISTS tblContacts;            
CREATE TABLE "tblContacts" (
    "contact_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "place_id" TEXT,
    "first_name" TEXT,
    "last_name" TEXT,
    "emails" TEXT,
    "phone" TEXT,
    "url" TEXT,
    "title" TEXT,
    "department" TEXT,
    "created_at" timestamp
);
DROP TABLE IF EXISTS tblEntities;
CREATE TABLE "tblEntities" (
    "place_id"	TEXT PRIMARY KEY,
    "state_abbr"	TEXT,
    "lat" NUMERIC,
    "long" NUMERIC,
    "pop_estimate_2022"	INTEGER,
    "place_fips"	TEXT,
    "sum_lev" INTEGER,
    "url"	TEXT,
    "lsadc"	TEXT,
    "display_name"	TEXT,
    "parent_id"	TEXT,
    "address"	TEXT,
    "created_at" timestamp
);
                     
DROP TABLE IF EXISTS tblTechstacks;
CREATE TABLE "tblTechstacks" (
    "techstack_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "name"	TEXT,
    "place_id"	TEXT,
    "type"	TEXT,
    "created_at" timestamp
);

DROP TABLE IF EXISTS tblCRMa;
CREATE TABLE "tblCRMa" (
    "sfdc_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "place_id"	TEXT,
    "created_at" timestamp
);

DROP TABLE IF EXISTS tblCRMb;
CREATE TABLE "tblCRMb" (
    "hubspot_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "place_id"	TEXT,
    "created_at" timestamp
);
                     
""")

# Function to load CSV dynamically
def load_csv_to_sqlite(table_name, csv_file):
    df = pd.read_csv(csv_file)  # Read CSV
    df.drop_duplicates(inplace=True)  # Remove duplicates

    for col in df.columns:
        if df[col].dtype == "float64":  # Convert float columns to string to avoid FutureWarning
            df[col] = df[col].astype(str).replace("nan", "")

    df.fillna("", inplace=True)  # Ensure all missing values are handled properly

    # Store in SQLite
    df.to_sql(table_name, conn, if_exists="replace", index=False)

# Process each CSV
for table, file in csv_files.items():
    load_csv_to_sqlite(table, file)

# Commit & Close
conn.commit()

#alter datat type and create materialized view
cursor.executescript("""

/*SQLlite doesnt support directly altering data type so doing following*/
                     
ALTER TABLE tblEntities ADD COLUMN pop_temp INT;
UPDATE tblEntities SET pop_temp = CAST(pop_estimate_2022 AS SIGNED);
ALTER TABLE tblEntities DROP COLUMN pop_estimate_2022;
ALTER TABLE tblEntities ADD COLUMN pop_estimate_2022 INT;
UPDATE tblEntities SET pop_estimate_2022 = CAST(pop_temp AS SIGNED);
ALTER TABLE tblEntities DROP COLUMN pop_temp;

DROP TABLE IF EXISTS mvEntConCRM;

CREATE TABLE mvEntConCRM (
    entitiescrm_id INT AUTO_INCREMENT PRIMARY KEY,
    place_id TEXT,
    customer_id INT,
    crm_fid INT,
    first_name TEXT,
    last_name TEXT,
    emails TEXT, 
    title TEXT,
    department TEXT,
    pop_estimate_2022 INT,
    display_name TEXT,
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO mvEntConCRM  
SELECT 
	ROW_NUMBER() OVER(),
	te.place_id,tcrm.customer_id, tcrm.crm_fid,
	tc.first_name,tc.last_name,tc.emails,tc.title,tc.department,
	te.pop_estimate_2022,te.display_name, te.address,tc.created_at
FROM 
	tblContacts tc, 
	tblEntities te, 
	(SELECT 1 customer_id, ta.sfdc_id crm_fid, ta.place_id
	FROM tblCRMa ta
	UNION 
	SELECT 2 customer_id, tb.hubspot_id crm_fid, tb.place_id
	FROM tblCRMb tb) tCRM 
WHERE 
	te.place_id=tc.place_id AND 
	te.place_id=tcrm.place_id;
""")

conn.close()

print("SQLite ETL Pipeline Completed.")
