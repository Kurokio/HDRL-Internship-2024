""" Function List:
Create: Scrapes all desired metadata present in the given
            directory and creates and populates the
            MetadataEntries, MetadataSources, Records,
            and TestResults tables.
View: Prints the count of and returns the SPASE_id's of
        the records desired.
"""


def Create(folder, conn, printFlag=False):
    """
    Scrapes all records that are found in the directory given
    for the desired metadata. Creates the MetadataEntries,
    MetadataSources, Records, and TestResults tables and populates
    them using the data scraped for each record. Populates the
    TestResult table with default values to be overwritten by
    the call to the FAIRScorer function in the notebook.

    :param folder: The absolute file path of the SPASE record/directory
                        containing the record(s) the user wants scraped.
    :type folder: String
    :param conn: A connection to the desired database
    :type conn: Connection object
    :param printFlag: A boolean determining if the user wants to print
                        more details of what the function is doing.
    :type printFlag: Boolean
    :return: None
    """
    # import functions from .py files and from built-in packages
    import pprint
    import sqlite3
    from .SPASE_Scraper_Script import SPASE_Scraper
    from .PathGrabber import getPaths
    from .SQLiteFun import (create_tables, add_Metadata, add_Sources,
                            add_Records, execution, executionALL,
                            add_TestResults, TestUpdate,
                            create_sqlite_database)
    from .DatalinkSep import AccessRightsSep
    from .RecordGrabber import Links

    # list that holds paths returned by PathGrabber
    SPASE_paths = []

    # get user input and extract all SPASE records
    # print("Enter root folder you want to search")
    # folder = input()
    print("You entered " + folder)
    SPASE_paths = getPaths(folder, SPASE_paths)
    if printFlag:
        print("The number of records is " + str(len(SPASE_paths)))
        print("The SPASE records found are:")
        print(SPASE_paths)
        print("""======================================================\
              ================================================""")

    # list that holds SPASE records already checked
    searched = []

    # iterate through all SPASE records returned by PathGrabber
    for record in SPASE_paths:
        # scrape metadata for each record
        if record not in searched:
            (ResourceID, ResourceIDField, author, authorField, authorRole,
             pub, pubField, pubDate, pubDateField, datasetName,
             datasetNameField, desc, descField, PID, PIDField,
             AccessRights, licenseField, datalinkField,
             version, ReleaseDate) = SPASE_Scraper(record)

            # list that holds required fields
            # required = [ResourceID, description, author, authorRole, url]

            # add record to searched
            searched.append(record)

            # grab only year from the date
            pubYear = pubDate[0:4]

            # concatenate author and authorRole into single strings
            # add Code E here
            author = ", ".join(author)
            authorRole = ", ".join(authorRole)

            if printFlag:
                print("""The ResourceID is " + ResourceID + " which was\
                        obtained from """ + ResourceIDField)
                print("The author(s) are " + author + " who are"
                      + authorRole + "which was obtained from " + authorField)
                # add Code F here
                print("The publication year is " + pubYear + """ which was \
                        obtained from """ + pubDateField)
                print("The publisher is " + pub + """ which was
                        obtained from """ + pubField)
                print("The dataset name is " + datasetName +
                      "which was obtained from " + datasetNameField)
                print("The description is " + desc + """ which was
                        obtained from """ + descField)
                print("The persistent identifier is " + PID + """ which was
                        obtained from """ + PIDField)
                print("""The URLs with their associated product keys \
                        obtained from """ + datalinkField + """ and their
                        license(s) obtained from """ + licenseField + " are: ")
                pprint.pprint(AccessRights)

            # separate license, url, and product keys from AccessRights
            #    to store in db
            license, url, prodKey = AccessRightsSep(AccessRights, printFlag)

            # add tables to existing database
            create_tables(conn)

            # insert metadata entries into table
            i = 0
            try:
                # with sqlite3.connect('SPASE_Data.db') as conn:
                # add or update entry to MetadataEntries
                for urls in url:
                    # Add Code G, Code H, and Code I in this statement
                    UpdateStmt = f''' INSERT INTO MetadataEntries
                                        (SPASE_id,author,authorRole,publisher,publicationYr,datasetName,
                                        license,URL,prodKey,description,PID)
                                    VALUES ("{ResourceID}","{author}",
                                    "{authorRole}","{pub}","{pubYear}","{datasetName}",
                                    "{license}","{url[i]}","{prodKey[i]}",
                                    "description found","{PID}")
                                    ON CONFLICT (SPASE_id, URL, prodKey) DO
                                    UPDATE
                                    SET
                                        author = excluded.author,
                                        authorRole = excluded.authorRole,
                                        publisher = excluded.publisher,
                                        publicationYr = excluded.publicationYr,
                                        datasetName = excluded.datasetName,
                                        license = excluded.license,
                                        description = excluded.description,
                                        PID = excluded.PID; '''
                    if printFlag:
                        print(f"'{url[i]}' was assigned to URL")
                        print(f"'{prodKey[i]}' was assigned to prodKey")
                    executionALL(UpdateStmt, conn)
                    i += 1
                # add or update Records entry
                before, sep, after = ResourceID.partition('NASA')
                compURL = """https://github.com/hpde/NASA/blob/master
                            """ + after + ".xml"
                UpdateStmt = f''' INSERT INTO Records
                                        (SPASE_id, SPASE_Version,
                                            LastModified,SPASE_URL)
                                    VALUES ("{ResourceID}","{version}","{ReleaseDate}",
                                            "{compURL}")
                                    ON CONFLICT (SPASE_id) DO
                                    UPDATE
                                    SET
                                        SPASE_version = excluded.SPASE_version,
                                        LastModified = excluded.LastModified,
                                        SPASE_URL = excluded.SPASE_URL; '''
                executionALL(UpdateStmt, conn)                    
                # add or update Source record
                # Code U, Code V, and Code W here
                UpdateStmt = f''' INSERT INTO MetadataSources
                                        (SPASE_id,author_source,publisher_source,
                                        publication_yr_source,datasetName_source,license_source,
                                        datalink_source,description_source,PID_source)
                                    VALUES ("{ResourceID}","{authorField}","{pubField}",
                                            "{pubDateField}","{datasetNameField}","{licenseField}",
                                            "{datalinkField}","{descField}","{PIDField}")
                                    ON CONFLICT (SPASE_id) DO
                                    UPDATE
                                    SET
                                        author_source = excluded.author_source,
                                        publisher_source = excluded.publisher_source,
                                        publication_yr_source = excluded.publication_yr_source,
                                        datasetName_source = excluded.datasetName_source,
                                        license_source = excluded.license_source,
                                        datalink_source = excluded.datalink_source,
                                        description_source = excluded.description_source,
                                        PID_source = excluded.PID_source; '''
                executionALL(UpdateStmt, conn)

            except sqlite3.Error as e:
                print(e)

            if printFlag:
                print("======================================================================================================")

        else:
            continue
            
    # collects SPASE_id's of records that answer analysis questions
    testObj = Links()
    # Add Code N here
    (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
     PIDRecords, descriptionRecords, citationRecords, complianceRecords) = testObj.allRecords(conn)
    #testObj.SDAC_Records()
    #testObj.SPDF_Records()
    TestResultRecords = execution("SELECT DISTINCT(SPASE_id) FROM TestResults", conn)

    # create the table with 0 as default for all, passing all records to the first insert call
    try:
        #with sqlite3.connect('SPASE_Data.db') as conn:
        k = 0
        for record in records:
            # if it is not a new SPASE Record
            if record in TestResultRecords:
                continue
            # if it is a new SPASE record
            else:
                # Add Code J to this assignment statement
                Test = (record,0,"","",0,0,0,0,0,0,0,0,0,0,0,"")
                Record_id = add_TestResults(conn, Test)
                if printFlag:
                    print(f'Created a TestResults entry with the row number {Record_id}')
                elif k == 0:
                    print("Creating TestResults entries")
            k += 1

    except sqlite3.Error as e:
                print(e)

    # iterate thru lists one by one and update column for each record if in the list (if record in author, has_author = 1)
    # UPDATE stmt for each test
    # Add Code O here
    TestUpdate(authorRecords, "has_author", conn)    
    TestUpdate(pubRecords, "has_pub", conn)
    TestUpdate(pubYrRecords, "has_pubYr", conn)
    TestUpdate(datasetNameRecords, "has_datasetName", conn)
    TestUpdate(licenseRecords, "has_license", conn)
    TestUpdate(urlRecords, "has_url", conn)
    TestUpdate(NASAurlRecords, "has_NASAurl", conn)
    TestUpdate(PIDRecords, "has_PID", conn)
    TestUpdate(descriptionRecords, "has_desc", conn)
    TestUpdate(citationRecords, "has_citation", conn)
    TestUpdate(complianceRecords, "has_compliance", conn)
    
    # Add Code Q here
def View(conn, All = True, desired = ["all", "author", "pub", "pubYr", "datasetName", "license",
                    "url","NASAurl", "PID", "description", "citation", "compliance"]):
    """
    Prints the number of records that meet each test criteria provided as well as return those SPASE_id's
    to the caller in the form of a dictionary. The keys are the Strings passed as parameters and the
    values are the list of SPASE_id's that fulfill that test.
    
    :param conn: A connection to the desired database
    :type conn: Connection object
    :param All: A boolean determining if the records returned will be from the set containing
                all records present in the database or only those with NASA URLs.
    :type All: Boolean
    :param desired: A list of Strings which determine the kind of records whose counts are printed and whose
                        SPASE_id's are assigned to the dictionary returned. The default value is all kinds.
    :param type: list
    :return: A dictionary containing lists of all records that fulfill certain test criteria as values.
    :rtype: dictionary
    
    Method Calls:
    
    records = View(conn):
    - prints the number of (distinct) records that are present in the MetdataEntries table
    - prints number of records that have authors
    - prints number of records that have publishers
    - prints number of records that have publication years
    - prints number of records that have datasetNames
    - prints number of records that have licenses
    - prints number of records that have URLs
    - prints number of records that have NASA URLs
    - prints number of records that have persistent identifiers
    - prints number of records that have descriptions
    - prints number of records that have citation info
    - prints number of records that meet compliance standards. 
    - returns a dictionary that contains all the SPASE_id's of these records, separated on their
        types by keys of the same name
    - assign this dictionary to records
    
    records = View(conn, desired = ['all']):
    - prints the number of distinct SPASE records in the MetadataEntries table
    - returns the SPASE_id's of these records in a dictionary labeled by key of the same name ("all")
    - assigns the returned dictionary to records. 
    
    records = View(conn, All = False, desired = ['pub', 'PID']):
    - prints the number of records that have publishers and have NASA URLs
    - prints the number of records that have persistent identifiers and NASA URLs.
    - returns the SPASE_id's of these records in a dictionary labeled by keys of the same name ("pub" and "PID")
    - assigns the returned dictionary to records. 
    """

    from .RecordGrabber import Links
    # Add Code R here
    desiredRecords = {"all": [], "author": [], "pub": [], "pubYr": [], "datasetName": [], "license": [], "url": [],
                      "NASAurl": [], "PID": [], "description": [], "citation": [], "compliance": []}
    testObj = Links()
    # returns records with each metadata field with no restrictions
    if All:
        # Add Code P here
        (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
         PIDRecords, descriptionRecords, citationRecords, complianceRecords) = testObj.allRecords(conn)
    # returns records with each metadata field that have NASA URLs
    else:
        (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
         PIDRecords, descriptionRecords, citationRecords, complianceRecords) = testObj.NASA_URL_Records(conn)
        
    #testObj.SDAC_Records()
    #testObj.SPDF_Records()
    
    # print counts of SPASE records that answer analysis questions
    for record in desired:
        if record == "all":
            if All:
                print("There are " + str(len(records)) + " records total.")
            else:
                print("There are " + str(len(records)) + " records with NASA URLs.")
            desiredRecords["all"] = records
        elif record == "author":
            print("There are " + str(len(authorRecords)) + " records with an author.")
            desiredRecords["author"] = authorRecords
        elif record == "pub":
            print("There are " + str(len(pubRecords)) + " records with a publisher.")
            desiredRecords["pub"] = pubRecords
        elif record == "pubYr":
            print("There are " + str(len(pubYrRecords)) + " records with a publication year.")
            desiredRecords["pubYr"] = pubYrRecords
        elif record == "datasetName":
            print("There are " + str(len(datasetNameRecords)) + " records with a dataset.")
            desiredRecords["datasetName"] = datasetNameRecords
        elif record == "license":
            print("There are " + str(len(licenseRecords)) + " records with a license.")
            desiredRecords["license"] = licenseRecords
        elif record == "url":
            print("There are " + str(len(urlRecords)) + " records with a URL.")
            desiredRecords["url"] = urlRecords
        elif record == "NASAurl":
            print("There are " + str(len(NASAurlRecords)) + " records with a NASA URL.")
            desiredRecords["NASAurl"] = NASAurlRecords
        elif record == "PID":
            print("There are " + str(len(PIDRecords)) + " records with a persistent identifier.")
            desiredRecords["PID"] = PIDRecords
        elif record == "description":
            print("There are " + str(len(descriptionRecords)) + " records with a description.")
            desiredRecords["description"] = descriptionRecords
        elif record == "citation":
            print("There are " + str(len(citationRecords)) + " records with citation info.")
            desiredRecords["citation"] = citationRecords
        elif record == "compliance":
            print("There are " + str(len(complianceRecords)) + " records that meet DCAT-US3 compliance.")
            desiredRecords["compliance"] = complianceRecords
        # add Code S here

    # return SPASE_id's of records that pass the test specified by caller
    return desiredRecords