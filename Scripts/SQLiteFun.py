""" Function List:
create_sqlite_database: Creates a new SQLite database or
                        returns a connection to an existing
                        one
create_tables: Creates the MetadataEntries, MetadataSources,
                TestResults, and Records tables
add_TestResults: Inserts values into the TestResults table
                    and returns its row number
execution: Executes a given SQLite SELECT statement and
            returns the result in a list
executionALL: Executes a given SQLite statement
TestUpdate: Updates the given column's values in TestResults
            to a 1 for the records given
databaseInfo: Prints out all table names and column names
FAIRScorer: Calculates FAIR Scores for all records and updates
            the FAIR_Score, FAIR_ScoreDate, and MostRecent columns for
            each record in TestResults.
"""

import sqlite3


def create_sqlite_database(filename):
    """
    Creates a database connection to a new or existing SQLite database with
    the name provided. If the database already exists, it returns
    a connection object. This also tells you any errors that may occur.

    :param filename: String that contains the desired name of the database
    :type filename: String
    :return: Connection object
    """

    conn = None
    try:
        conn = sqlite3.connect(filename)
        return conn
    except sqlite3.Error as e:
        print(e)


# creates tables in existing db
def create_tables(conn):
    """
    Connects to the given SQLite database, creates a cursor object,
    and calls the executescript method with the sql_statements argument.
    These changes are then committed which creates the MetadataEntries,
    MetadataSources, TestResults, and Records tables in the specified
    database. This also displays error messages if any arise.

    :param conn: A connection to the desired database
    :type conn: Connection object
    :return: None
    """

    # Code T goes in this assignment statement
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
                PID TEXT,
                UNIQUE(SPASE_id, URL, prodKey)
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
                SPASE_Version TEXT,
                LastModified TEXT,
                SPASE_URL TEXT
        );""")
    # create a database connection, executes statement,
    #    and commits the changes to db
    try:
        cursor = conn.cursor()
        for statement in sql_statements:
            cursor.executescript(statement)

        conn.commit()
    except sqlite3.Error as e:
        print(e)


def add_TestResults(conn, entry):
    """
    Initializes a SQLite INSERT statement with ? serving as placeholders
    to be replaced by the values specified in the entry parameter.
    Connects to the given SQLite database, creates a cursor object, and
    calls the execute method with the sql and entry arguments. These
    changes are then committed which inserts the values provided into the
    fields specified such as SPASE_id, FAIR_Score, etc. in the TestResults
    table. It finally returns the row number inserted into the table.

    :param conn: A string of the SQLite database that holds the table
                    to insert into.
    :type conn: String
    :param entry: A tuple of the values being inserted into the table.
    :type entry: tuple
    :return: The row number of the data that was inserted.
    :rtype: int
    """

    # https://stackoverflow.com/questions/1609637/how-to-insert-multiple-rows-in-sqlite
    # https://www.sqlitetutorial.net/sqlite-generated-columns/
    # create an alternate function that fills in by columns instead of rows when 
    #    number of rows is bigger than number of columns
    sql = '''INSERT INTO TestResults(SPASE_id,FAIR_Score,FAIR_ScoreDate,
                MostRecent,has_author,has_pub,has_pubYr,has_datasetName,
                has_license,has_url,has_NASAurl,has_PID,has_desc,
                has_citation,has_compliance, Errors)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, entry)
    conn.commit()
    return cur.lastrowid


# executes given SQLite SELECT statement
def execution(stmt, conn, number="single"):
    """
    Connects to the given SQLite database, creates a cursor object,
    and calls the execute method with the stmt argument. The number
    argument has default value of single, which will format the return
    correctly when selecting only one item. Otherwise pass 'multiple'
    as the argument when selecting more than one item. Calls the
    fetchall method to get all rows returned by the statement that was
    executed. This also displays error messages if any arise. Lastly,
    it returns the values of the matching items from the SQLite SELECT
    statement in a list.

    :param stmt: A string of the SQLite statement to be executed.
    :type stmt: String
    :param conn: A connection to the desired database
    :type conn: Connection object
    :param number: An string that formats the return based on how many
                    items are being selected.
    :type number: String
    :return: The list of the results from the SQLite statement
    :rtype: list
    """
    # create a database connection, executes SELECT statement,
    #    and returns all the results
    try:
        cur = conn.cursor()
        cur.execute(stmt)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    if number == "single":
        return [row[0] for row in rows]
    elif number == "multiple":
        return rows


# executes given SQLite statement
def executionALL(stmt, conn):
    """
    Connects to the given SQLite database, creates a cursor object,
    and calls the execute method with the stmt argument. This also
    displays error messages if any arise.

    :param stmt: A string of the SQLite statement to be executed.
    :type stmt: String
    :param conn: A connection to the desired database
    :type conn: Connection object
    :return: None
    """
    # create a database connection and execute statement
    try:
        cur = conn.cursor()
        cur.execute(stmt)
        conn.commit()
    except sqlite3.Error as e:
        print(e)


# updates the TestResults column provided for all links that fulfill
#     a certain test so they have a 1/"True"
def TestUpdate(records, column, conn):
    """
    Iterates through the parameter records to set the value for each
    record in the column provided as 1. For each record, a SQLite
    UPDATE statement is made which is then passed to the executionALL
    function to be executed. The rowNum of the record updated is also
    collected by calling the execution function.

    :param records: A list of the links that pass a specific analysis test.
    :type records: list
    :param column: A string of the TestResults column to be updated.
    :type column: string
    :return: None
    """
    for record in records:
        # print(record + " is the current record")
        Stmt = f""" UPDATE TestResults
                            SET '{column}' = 1
                            WHERE SPASE_id = '{record}' """
        Record_id = execution(f""" SELECT rowNum FROM TestResults
                                    WHERE SPASE_id = '{record}';""", conn)
        executionALL(Stmt, conn)
        # print(f"Updated a TestResults entry with the row number {Record_id}")
        # print("=================================================
        # ==========================")


def databaseInfo(conn, print_flag=True):
    """
    Prints all table names and all the names of their associated columns

    :param conn: A connection to the desired database
    :type conn: Connection object
    :return: dictionary with table names as keys and column names as values
    """
    # print and store all table names and the names of their columns
    name_dict = {}
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for name in res.fetchall():
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(f'select * from {name[0]}')
        row = cursor.fetchone()
        names = row.keys()
        name_dict[name[0]] = names
        if print_flag:
            print("The table " + name[0] + " has columns:")
            print(names)
            print()

    return name_dict


# updates the TestResults column FAIRScore for all links to have
#     their updated FAIR score
def FAIRScorer(conn):
    """
    Iterates through the has_x column names of the TestResults table
    to calculate the FAIR Score of all the records in the parameter.
    FAIR Score is calculated according to the algorithm described in
    the notebook. Once the FAIR score is calculated, the FAIR_Score,
    MostRecent, and FAIR_ScoreDate columns are updated for that record.

    :param conn: A connection to the desired database
    :type conn: Connection object
    :return: None
    """
    from Scripts import View

    # retrieve record names using View
    records = View(conn, desired=['all'], print_flag=False)
    print(f"Analyzing {len(records['all'])} records found")

    # collect all column names
    cols = execution("SELECT name FROM PRAGMA_TABLE_INFO('TestResults')", conn)
    # get only the has_x columns
    FAIR_Date = cols[3]
    cols = cols[5:16]
    for record in records['all']:
        score = 0
        # print(type(record))
        # print(record + " is the current record")

        # calculate FAIR score value
        i = 0
        for col in cols:
            # has_url not counted towards FAIR score
            if col == "has_url":
                # print("has_url skipped!")
                continue
            else:
                val = execution("SELECT " + col + f""" FROM TestResults
                                    WHERE SPASE_id = '{record}' """, conn)
                val = val[0]
                if (1 == val):
                    # print(f"{col} had value 1, adding 1 to FAIR score")
                    score += 1
                else:
                    # print(f"{col} had value 0, checking next column")
                    continue
            i += 1
        # print(record + " has score of " + str(score) +
        # ", updating this in the table")

        # get value of FAIR_ScoreDate
        DateVal = execution("SELECT " + FAIR_Date + f""" FROM TestResults
                                WHERE SPASE_id = '{record}' """, conn)
        DateVal = DateVal[0]

        # if FAIR_ScoreDate nonempty = not first time record was passed
        #     after default values assigned by main function
        if DateVal:
            # create the trigger that makes a new row for the out of date
            #     entry after updating
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
            executionALL(Stmt, conn)

            # updating the columns in the table with new score and date
            # save FAIR Score before UPDATE
            Stmt = f""" SELECT FAIR_Score FROM TestResults
                            WHERE (SPASE_id = '{record}'
                            AND MostRecent = 'T') """
            OldScore = execution(Stmt, conn)
            OldScore = OldScore[0]
            # do update
            Stmt = f""" UPDATE TestResults
                                SET (FAIR_Score, FAIR_ScoreDate, MostRecent) =
                                ({score},datetime('now'),'T')
                                WHERE SPASE_id = '{record}' """
            executionALL(Stmt, conn)
            # if new FAIR Score < Old, then printout message along w
            #     ResourceID and FAIR Scores
            if score < OldScore:
                print(f"""Record {record} has decreased in FAIR Score \
                            from {OldScore} to {score}.""")

        # if FAIR_ScoreDate is empty = first time running after
        #     default values assigned in main function
        # replace default value row with fully populated row
        elif not DateVal:
            # drop trigger that makes new rows
            executionALL("DROP TRIGGER IF EXISTS FAIR_Update", conn)
            # updating the columns in the table with new score and date
            Stmt = f""" UPDATE TestResults
                                SET (FAIR_Score, FAIR_ScoreDate, MostRecent) =
                                ({score},datetime('now'),'T')
                                WHERE SPASE_id = '{record}' """
            executionALL(Stmt, conn)
    print("FAIR scores calculated. Use the View function to see the results.")

    return None