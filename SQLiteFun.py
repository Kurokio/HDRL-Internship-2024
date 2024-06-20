import sqlite3

# add table to existing db
def create_tables():
    sql_statements = (
        """CREATE TABLE IF NOT EXISTS MetadataEntries (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL, 
                author TEXT,
                authorRole TEXT,
                publisher TEXT,
                publicationYr TEXT,
                dataset TEXT,
                license TEXT,
                URL TEXT,
                prodKey TEXT,
                description TEXT,
                PI TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS MetadataSources (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL UNIQUE, 
                author_source TEXT,
                publisher_source TEXT,
                publication_yr_source TEXT,
                dataset_source TEXT,
                license_source TEXT,
                datalink_source TEXT,
                description_source TEXT,
                PI_source TEXT
        );"""
        """CREATE TABLE IF NOT EXISTS TestResults (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL UNIQUE, 
                FAIR_Score INTEGER,
                FAIR_ScoreDate TEXT,
                MostRecent TEXT
        );"""
        """CREATE TABLE IF NOT EXISTS Records (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL UNIQUE, 
                SPASE_Version INTEGER,
                LastModified TEXT,
                SPASE_URL TEXT
        );""")
    # create a database connection
    try:
        with sqlite3.connect('SPASE_Data.db') as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.executescript(statement)

            conn.commit()
    except sqlite3.Error as e:
        print(e)

# add entries/rows to table
def add_Metadata(conn, entry):
    sql = '''INSERT INTO MetadataEntries(SPASE_id,author,authorRole,publisher,publicationYr,dataset,
            license,URL,prodKey,description,PI)
            VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

def add_Sources(conn, entry):
    sql = '''INSERT INTO MetadataSources(SPASE_id,author_source,publisher_source,
            publication_yr_source,dataset_source,license_source,datalink_source,description_source,PI_source)
            VALUES(?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

def add_TestResults(conn, entry):
    sql = '''INSERT INTO TestResults(SPASE_id,FAIR_Score,FAIR_ScoreDate,MostRecent)
            VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

def add_Records(conn, entry):
    sql = '''INSERT INTO Records(SPASE_id, SPASE_Version,LastModified,SPASE_URL)
            VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

# executes given SQLite statement
def execution(stmt):
    try:
        with sqlite3.connect("SPASE_Data.db") as conn:
            cur = conn.cursor()
            #print(stmt)
            cur.execute(stmt)
            rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    return [row[0] for row in rows]

# How many records published by SDAC in X year?
def SDAC_records(yr):
    try:
        with sqlite3.connect("SPASE_Data.db") as conn:
            cur = conn.cursor()
            cur.execute("""SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                            WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                            "%Solar Data Analysis Center") AND publicationYr=?""", (yr,))
            rows = cur.fetchall()
            for row in rows:
                print("There are " + str(row[0]) + " records published by SDAC in the year " + yr)
    except sqlite3.Error as e:
        print(e)