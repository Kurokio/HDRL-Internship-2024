""" Function List:
create_tables: Creates the MetadataEntries, MetadataSources, TestResults, and Records tables
add_Metadata: Inserts values into the MetadataEntries table and returns its row number
add_Sources: Inserts values into the MetadataSources table and returns its row number
add_TestResults: Inserts values into the TestResults table and returns its row number
add_Records: Inserts values into the Records table and returns its row number
execution: Executes a given SQLite statement and returns the result in a list
"""

import sqlite3

# add table to existing db
# Test Results holds input of a 1 or 0 depending on if record meets tested criteria

def create_tables():
    """Connects to the given SQLite database, creates a cursor object, and calls the executescript method 
    with the sql_statements argument. These changes are then committed which creates the MetadataEntries, 
    MetadataSources, TestResults, and Records tables in the specified database. This also displays error 
    messages if any arise."""
    
    sql_statements = (
        """CREATE TABLE IF NOT EXISTS MetadataEntries (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL, 
                author TEXT,
                authorRole TEXT,
                publisher TEXT,
                publicationYr TEXT,
                datasetName TEXT,
                license TEXT,
                URL TEXT,
                prodKey TEXT,
                description TEXT,
                PID TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS MetadataSources (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL UNIQUE,
                author_source TEXT,
                publisher_source TEXT,
                publication_yr_source TEXT,
                datasetName_source TEXT,
                license_source TEXT,
                datalink_source TEXT,
                description_source TEXT,
                PID_source TEXT
        );"""
        """CREATE TABLE IF NOT EXISTS TestResults (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL UNIQUE, 
                FAIR_Score INTEGER,
                FAIR_ScoreDate TEXT,
                MostRecent TEXT,
                has_author INTEGER,
                has_pub INTEGER,
                has_pubYr INTEGER,
                has_datasetName INTEGER,
                has_license INTEGER,
                has_url INTEGER,
                has_NASAurl INTEGER,
                has_PID INTEGER,
                has_desc INTEGER,
                has_citation INTEGER,
                has_compliance INTEGER,
                Errors TEXT
        );"""
        """CREATE TABLE IF NOT EXISTS Records (
                rowNum INTEGER PRIMARY KEY, 
                SPASE_id TEXT NOT NULL UNIQUE, 
                SPASE_Version INTEGER,
                LastModified TEXT,
                SPASE_URL TEXT
        );""")
    # create a database connection, executes statement, and commits the changes to db
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
    """Initializes a SQLite INSERT statement with ? serving as placeholders to be replaced by the values 
    specified in the entry parameter. Connects to the given SQLite database, creates a cursor object, 
    and calls the execute method with the sql and entry arguments. These changes are then committed which 
    inserts the values provided into the fields specified such as SPASE_id, author, etc. in the MetadataEntries 
    table. It finally returns the row number inserted into the table.
    
    :param conn: A string of the SQLite database that holds the table to insert into.
    :type conn: String
    :param entry: A tuple of the values being inserted into the table.
    :type entry: tuple
    :return: The row number of the data that was inserted.
    :rtype: int
    """
    
    sql = '''INSERT INTO MetadataEntries(SPASE_id,author,authorRole,publisher,publicationYr,datasetName,
            license,URL,prodKey,description,PID)
            VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

def add_Sources(conn, entry):
    """Initializes a SQLite INSERT statement with ? serving as placeholders to be replaced by the values 
    specified in the entry parameter. Connects to the given SQLite database, creates a cursor object, 
    and calls the execute method with the sql and entry arguments. These changes are then committed which 
    inserts the values provided into the fields specified such as SPASE_id, author_source, etc. in the 
    MetadataSources table. It finally returns the row number inserted into the table.
    
    :param conn: A string of the SQLite database that holds the table to insert into.
    :type conn: String
    :param entry: A tuple of the values being inserted into the table.
    :type entry: tuple
    :return: The row number of the data that was inserted.
    :rtype: int
    """
    
    sql = '''INSERT INTO MetadataSources(SPASE_id,author_source,publisher_source,
            publication_yr_source,datasetName_source,license_source,datalink_source,description_source,PID_source)
            VALUES(?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

def add_TestResults(conn, entry):
    """Initializes a SQLite INSERT statement with ? serving as placeholders to be replaced by the values 
    specified in the entry parameter. Connects to the given SQLite database, creates a cursor object, 
    and calls the execute method with the sql and entry arguments. These changes are then committed which 
    inserts the values provided into the fields specified such as SPASE_id, FAIR_Score, etc. in the 
    TestResults table. It finally returns the row number inserted into the table.
    
    :param conn: A string of the SQLite database that holds the table to insert into.
    :type conn: String
    :param entry: A tuple of the values being inserted into the table.
    :type entry: tuple
    :return: The row number of the data that was inserted.
    :rtype: int
    """
    
    sql = '''INSERT INTO TestResults(SPASE_id,FAIR_Score,FAIR_ScoreDate,MostRecent,has_author,
                has_pub,has_pubYr,has_datasetName,has_license,has_url,has_NASAurl,has_PID,has_desc,
                has_citation,has_compliance, Errors)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

def add_Records(conn, entry):
    """Initializes a SQLite INSERT statement with ? serving as placeholders to be replaced by the values 
    specified in the entry parameter. Connects to the given SQLite database, creates a cursor object, 
    and calls the execute method with the sql and entry arguments. These changes are then committed which 
    inserts the values provided into the fields specified such as SPASE_id, SPASE_Version, etc. in the 
    Records table. It finally returns the row number inserted into the table.
    
    :param conn: A string of the SQLite database that holds the table to insert into.
    :type conn: String
    :param entry: A tuple of the values being inserted into the table.
    :type entry: tuple
    :return: The row number of the data that was inserted.
    :rtype: int
    """
    
    sql = '''INSERT INTO Records(SPASE_id, SPASE_Version,LastModified,SPASE_URL)
            VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid

# executes given SQLite SELECT statement
def execution(stmt):
    """Connects to the given SQLite database, creates a cursor object, and calls the execute method 
    with the stmt argument. Calls the fetchall method to get all rows returned by the statement that 
    was executed. This also displays error messages if any arise. Lastly, it returns the results of 
    the SQLite statement in a list
    
    :param stmt: A string of the SQLite statement to be executed.
    :type stmt: String
    :return: The list of the results from the SQLite statement
    :rtype: list
    """
    # create a database connection, executes SELECT statement, and returns all the results
    try:
        with sqlite3.connect("SPASE_Data.db") as conn:
            cur = conn.cursor()
            cur.execute(stmt)
            rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    return [row[0] for row in rows]

# executes given SQLite statement
def executionALL(stmt):
    """Connects to the given SQLite database, creates a cursor object, and calls the execute method 
    with the stmt argument. Calls the fetchall method to get all rows returned by the statement that 
    was executed. This also displays error messages if any arise. Lastly, it returns the results of 
    the SQLite statement in a list
    
    :param stmt: A string of the SQLite statement to be executed.
    :type stmt: String
    """
    # create a database connection and execute statement
    try:
        with sqlite3.connect("SPASE_Data.db") as conn:
            cur = conn.cursor()
            cur.execute(stmt)
            conn.commit()
    except sqlite3.Error as e:
        print(e)

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
        