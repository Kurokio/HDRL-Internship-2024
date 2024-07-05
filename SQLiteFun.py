""" Function List:
create_sqlite_database: Creates a new SQLite database 
create_tables: Creates the MetadataEntries, MetadataSources, TestResults, and Records tables
add_Metadata: Inserts values into the MetadataEntries table and returns its row number
add_Sources: Inserts values into the MetadataSources table and returns its row number
add_TestResults: Inserts values into the TestResults table and returns its row number
add_Records: Inserts values into the Records table and returns its row number
execution: Executes a given SQLite SELECT statement and returns the result in a list
executionALL: Executes a given SQLite statement
TestUpdate: Updates the given column's values in TestResults to a 1 for the records given
databaseInfo: Prints out all table names and column names
FAIRScorer: Calculates FAIR Scores for all records and updates the FAIR_Score, FAIR_ScoreDate, and MostRecent columns for each record in TestResults.
"""

import sqlite3

def create_sqlite_database(filename):
    """Creates a database connection to a new SQLite database with the name provided. This also tells
    you the SQLite version as well as any errors that may occur.
    
    :param filename: String that contains the desired name of the database
    :type filename: String
    """
    conn = None
    try:
        conn = sqlite3.connect(filename)
        print(sqlite3.sqlite_version)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

# add table to existing db
# Test Results hOLDs input of a 1 or 0 depending on if record meets tested criteria

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
                SPASE_id TEXT NOT NULL, 
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
    """Initializes a SQLite INSERT statement with ? serving as placehOLDers to be replaced by the values 
    specified in the entry parameter. Connects to the given SQLite database, creates a cursor object, 
    and calls the execute method with the sql and entry arguments. These changes are then committed which 
    inserts the values provided into the fields specified such as SPASE_id, author, etc. in the MetadataEntries 
    table. It finally returns the row number inserted into the table.
    
    :param conn: A string of the SQLite database that hOLDs the table to insert into.
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
    """Initializes a SQLite INSERT statement with ? serving as placehOLDers to be replaced by the values 
    specified in the entry parameter. Connects to the given SQLite database, creates a cursor object, 
    and calls the execute method with the sql and entry arguments. These changes are then committed which 
    inserts the values provided into the fields specified such as SPASE_id, author_source, etc. in the 
    MetadataSources table. It finally returns the row number inserted into the table.
    
    :param conn: A string of the SQLite database that hOLDs the table to insert into.
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
    """Initializes a SQLite INSERT statement with ? serving as placehOLDers to be replaced by the values 
    specified in the entry parameter. Connects to the given SQLite database, creates a cursor object, 
    and calls the execute method with the sql and entry arguments. These changes are then committed which 
    inserts the values provided into the fields specified such as SPASE_id, FAIR_Score, etc. in the 
    TestResults table. It finally returns the row number inserted into the table.
    
    :param conn: A string of the SQLite database that hOLDs the table to insert into.
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
def execution(stmt, number):
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
    if number == 1:
        return [row[0] for row in rows]
    else:
        return rows
    
# executes given SQLite statement
def executionALL(stmt):
    """Connects to the given SQLite database, creates a cursor object, and calls the execute method 
    with the stmt argument. This also displays error messages if any arise.
    
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
        
# updates the TestResults column provided for all links that fulfill a certain test so they have a 1/"True"
def TestUpdate(records, column):
    """Iterates through the parameter records to set the value for each record in the column provided as 1.
    For each record, a SQLite UPDATE statement is made which is then passed to the executionALL function 
    to be executed. The rowNum of the record updated is also collected by calling the execution function.
    
    :param records: A list of the links that pass a specific analysis test.
    :type records: list
    :param column: A string of the TestResults column to be updated.
    :type column: string
    """
    for record in records:
        #print(record + " is the current record")
        Stmt = f""" UPDATE TestResults
                            SET '{column}' = 1
                            WHERE SPASE_id = '{record}' """
        Record_id = execution(f""" SELECT rowNum FROM TestResults WHERE SPASE_id = '{record}';""", 1)
        executionALL(Stmt)
        #print(f"Updated a TestResults entry with the row number {Record_id}")
        #print("===========================================================================")
        
def databaseInfo():
    # print all table names and the names of their columns
    conn = sqlite3.connect('SPASE_Data.db')
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for name in res.fetchall():
        print("The table " + name[0] + " has columns:")
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(f'select * from {name[0]}')
        row = cursor.fetchone()
        names = row.keys()
        print(names)
        print()
        
# updates the TestResults column FAIRScore for all links to have their updated FAIR score
def FAIRScorer(records, first):
    """Iterates through the has_x column names of the TestResults table to calculate the FAIR Score of all the records in
    the parameter. For each record, its score is printed. FAIR Score is calculated according to the algorithm described in the 
    notebook. Once the FAIR score is calculated, the FAIR_Score, MostRecent, and FAIR_ScoreDate columns are updated for that 
    record. If it is the first time updating the FAIR Score after default values assigned, functionality that replaces these 
    default values before adding new rows for each subsequent FAIR Score update is provided by the 'first' parameter. If first 
    time, drop the trigger if needed and pass True. Otherwise, pass False.
    
    :param records: A list of all the links in table.
    :type records: list
    :param first: A boolean indicating if this is the first time populating the TestResults table after assigning its default 
                    values.
    :type first: Boolean
    """
    conn = sqlite3.connect('SPASE_Data.db')
    # collect all column names
    cols = execution("SELECT name FROM PRAGMA_TABLE_INFO('TestResults')", 1)
    # get only the has_x columns
    cols = cols[5:16]
    for record in records:
        score = 0
        #print(record + " is the current record")
        
        # calculate FAIR score value
        i = 0
        for col in cols:
            # has_url not counted towards FAIR score
            if col == "has_url":
                #print("has_url skipped!")
                continue
            else:
                val = execution("SELECT " + col + f" FROM TestResults WHERE SPASE_id = '{record}' ", 1)
                val = val[0]
                if (1 == val):
                    #print(f"{col} had value 1, adding 1 to FAIR score")
                    score += 1
                else:
                    #print(f"{col} had value 0, checking next column")
                    continue
            i += 1
        print(record + " has score of " + str(score) + ", updating this in the table")
        
        # if not first time running after default values assigned by main function
        if not first:
            # create the trigger that makes a new row for the out of date entry after updating
            Stmt = """ CREATE TRIGGER IF NOT EXISTS FAIR_Update
                        AFTER UPDATE ON TestResults
                        WHEN ((OLD.SPASE_id = NEW.SPASE_id)
                            AND (OLD.FAIR_Score != NEW.FAIR_Score))
                        BEGIN
                            INSERT INTO TestResults (
                                SPASE_id, 
                                FAIR_Score,
                                FAIR_ScoreDate,
                                MostRecent,
                                has_author,
                                has_pub,
                                has_pubYr,
                                has_datasetName,
                                has_license,
                                has_url,
                                has_NASAurl,
                                has_PID,
                                has_desc,
                                has_citation,
                                has_compliance,
                                Errors)
                        VALUES (
                                OLD.SPASE_id, 
                                OLD.FAIR_Score,
                                OLD.FAIR_ScoreDate,
                                'F',
                                OLD.has_author,
                                OLD.has_pub,
                                OLD.has_pubYr,
                                OLD.has_datasetName,
                                OLD.has_license,
                                OLD.has_url,
                                OLD.has_NASAurl,
                                OLD.has_PID,
                                OLD.has_desc,
                                OLD.has_citation,
                                OLD.has_compliance,
                                OLD.Errors);
                        END;"""
            executionALL(Stmt)

            # updating the columns in the table with new score and date
            Stmt = f""" UPDATE TestResults
                                SET (FAIR_Score, FAIR_ScoreDate, MostRecent) = ({score},datetime('now'),'T')
                                WHERE SPASE_id = '{record}' """
            executionALL(Stmt)
        # if first time running after default values assigned in main function
        else:
            # updating the columns in the table with new score and date
            Stmt = f""" UPDATE TestResults
                                SET (FAIR_Score, FAIR_ScoreDate, MostRecent) = ({score},datetime('now'),'T')
                                WHERE SPASE_id = '{record}' """
            executionALL(Stmt)