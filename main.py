def Create(printFlag = False):
    """
    Scrapes all records that are found in the directory given for the desired metadata. Creates the MetadataEntries, 
    MetadataSources, Records, and TestResults tables and populates them using the data scraped for each record. Populates the\
    TestResult table with default values to be overwritten by the call to the FAIRScorer function in the notebook.
    
    :param printFlag: A boolean determining if the user wants to print more details of what the function is doing.
    :type conn: Boolean
    :param update: A boolean determining if updating the MetadataEntries table or not
    :type entry: Boolean
    """
    # import functions from .py files and from built-in packages
    import pprint, sqlite3
    from SPASE_Scraper_Script import SPASE_Scraper
    from PathGrabber import getPaths
    from SQLiteFun import (create_tables, add_Metadata, add_Sources, add_Records, execution, executionALL, create_tables,
                          add_TestResults, TestUpdate)
    from DatalinkSep import AccessRightsSep
    from QueryPrinter import Links
    
    # list that holds paths returned by PathGrabber
    SPASE_paths = []

    # get user input and extract all SPASE records
    print("Enter root folder you want to search")
    folder = input()
    print("You entered " + folder)
    SPASE_paths = getPaths(folder, SPASE_paths)
    if printFlag:
        print("The number of records is "+ str(len(SPASE_paths)))
        print("The SPASE records found are:")
        print(SPASE_paths)
        print("======================================================================================================")

    # list that holds SPASE records already checked
    searched = []
    
    # loop counter
    j = 0

    # iterate through all SPASE records returned by PathGrabber
    for record in SPASE_paths:
        # scrape metadata for each record
        if record not in searched:
            (RID, RIDField, author, authorField, authorRole, pub, pubField, pubDate, pubDateField, datasetName, 
             datasetNameField, desc, descField, PID, PIDField, AccessRights, licenseField, datalinkField, 
             version, ReleaseDate) = SPASE_Scraper(record)

            # list that holds required fields
            # required = [RID, description, author, authorRole, url]

            # add record to searched
            searched.append(record)
            # grab only year from the date
            pubYear = pubDate[0:4]
            # concatenate author and authorRole into single strings
            author = ", ".join(author)
            authorRole = ", ".join(authorRole)

            if printFlag:
                print("The ResourceID is " + RID + " which was obtained from " + RIDField)
                print("The author(s) are " + author + " who are " + authorRole + " which was obtained from " + authorField)
                print("The publication year is " + pubYear + " which was obtained from " + pubDateField)
                print("The publisher is " + pub + " which was obtained from " + pubField)
                print("The dataset name is " + datasetName + " which was obtained from " + datasetNameField)
                print("The description is " + desc + " which was obtained from " + descField)
                print("The persistent identifier is " + PID + " which was obtained from " + PIDField)
                print("The URLs with their associated product keys obtained from " + datalinkField + """ and their 
                      license(s) obtained from """ + licenseField + " are: ")
                pprint.pprint(AccessRights)

            # separate license, url, and product keys from AccessRights to store in db
            license, url, prodKey = AccessRightsSep(AccessRights, printFlag)

            # add tables to existing database
            create_tables()

            # insert metadata entries into table
            i = 0        
            try:
                with sqlite3.connect('SPASE_Data.db') as conn:                
                    # add or update entry to MetadataEntries
                    for urls in url:
                        '''# add a new or update an existing SPASE record in MetadataEntries
                        row = execution(f""" SELECT rowNum FROM MetadataEntries2
                            WHERE SPASE_id = '{RID}' AND URL = '{url[i]}' """, 1)
                        # print if adding new entry
                        if not row:
                            if printFlag:
                                print(f"Created a Metadata entry with the row number '{row[0]}'")
                            elif j == 0:
                                print("Creating Metadata entries")
                        # print that updating an existing entry
                        else:
                            if printFlag:
                                print(f"Updated a MetadataEntries record with the row number '{row[0]}' ")
                            elif j == 0:
                                print("Updating Metadata entries")'''
                        
                        UpdateStmt = f""" INSERT INTO MetadataEntries
                                            (SPASE_id,author,authorRole,publisher,publicationYr,datasetName,
                                            license,URL,prodKey,description,PID)
                                        VALUES ('{RID}','{author}','{authorRole}','{pub}','{pubYear}',
                                        '{datasetName}','{license}','{url[i]}','{prodKey[i]}','{desc}','{PID}')
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
                                            PID = excluded.PID; """
                        if printFlag:
                            print(f"'{url[i]}' was assigned to URL")
                            print(f"'{prodKey[i]}' was assigned to prodKey")
                        executionALL(UpdateStmt)
                        i += 1
                        
                        '''# add a new SPASE Record to MetadataEntries
                        if not update:
                            Metadata = (RID,author,authorRole,pub,pubYear,datasetName,license,url[i],prodKey[i],desc,PID)
                            Record_id = add_Metadata(conn, Metadata)
                            if printFlag:
                                print(f'Created a Metadata entry with the row number {Record_id}')
                            elif j == 0:
                                print("Creating Metadata entries")
                            i += 1
                        # update an existing SPASE record in MetadataEntries
                        elif update:
                            row = execution(f""" SELECT rowNum FROM MetadataEntries
                            WHERE SPASE_id = '{RID}' AND URL = '{url[i]}' """, 1)
                            UpdateStmt = f""" UPDATE MetadataEntries
                            SET (SPASE_id,author,authorRole,publisher,publicationYr,datasetName,
                                license,URL,prodKey,description,PID) = 
                                ('{RID}','{author}','{authorRole}','{pub}','{pubYear}','{datasetName}','{license}',
                                '{url[i]}','{prodKey[i]}','{desc}','{PID}')
                            WHERE rowNum = '{row[0]}' """
                            # see if any new records were added
                            try:
                                executionALL(UpdateStmt)
                                if printFlag:
                                    print(f"Updated a MetadataEntries record with the row number '{row[0]}' ")
                                elif j == 0:
                                    print("Updating Metadata entries")
                            # if there are new records added, use the add_Metadata function
                            except IndexError:
                                Metadata = (RID,author,authorRole,pub,pubYear,datasetName,license,url[i],prodKey[i],desc,PID)
                                Record_id = add_Metadata(conn, Metadata)
                                if printFlag:
                                    print(f'Created a Metadata entry with the row number {Record_id}')
                            finally:
                                i += 1'''
                    # add a new Source record
                    Sources = (RID,authorField,pubField,pubDateField,datasetNameField,licenseField,
                               datalinkField,descField,PIDField)
                    Record_id = add_Sources(conn, Sources)
                    if printFlag:
                        print(f'Created a Sources entry with row number {Record_id}')
                    elif j == 0:
                        print("Creating Sources entries")
                    # add a new Records entry
                    before, sep, after = RID.partition('NASA')
                    compURL =  "https://github.com/hpde/NASA/blob/master" + after + ".xml"
                    entry = (RID,version,ReleaseDate,compURL)
                    Record_id = add_Records(conn, entry)
                    if printFlag:
                        print(f'Created a Records entry with the row number {Record_id}')
                    elif j == 0:
                        print("Creating Records entries")

            except sqlite3.Error as e:
                print(e)

            if printFlag:
                print("======================================================================================================")

        else:
            continue
        j += 1
            
    # collects SPASE_id's of records that answer analysis questions
    testObj = Links()
    (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
     PIDRecords, descriptionRecords, citationRecords, complianceRecords) = testObj.allRecords()
    #testObj.SDAC_Records()
    #testObj.SPDF_Records()
    TestResultRecords = execution("SELECT DISTINCT(SPASE_id) FROM TestResults", 1)

    # create the table with 0 as default for all, passing all records to the first insert call
    try:
        with sqlite3.connect('SPASE_Data.db') as conn:
            k = 0
            for record in records:
                # if it is not a new SPASE Record
                if record in TestResultRecords:
                    continue
                # if it is a new SPASE record
                else:
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
    if len(records) != len(TestResultRecords):
        TestUpdate(authorRecords, "has_author")    
        TestUpdate(pubRecords, "has_pub")
        TestUpdate(pubYrRecords, "has_pubYr")
        TestUpdate(datasetNameRecords, "has_datasetName")
        TestUpdate(licenseRecords, "has_license")
        TestUpdate(urlRecords, "has_url")
        TestUpdate(NASAurlRecords, "has_NASAurl")
        TestUpdate(PIDRecords, "has_PID")
        TestUpdate(descriptionRecords, "has_desc")
        TestUpdate(citationRecords, "has_citation")
        TestUpdate(complianceRecords, "has_compliance")
    
def View():
    """
    Creates Counts and Links objects to print the number of records that meet each test criteria as well as return those links \
    to the caller in the form of a tuple.
    
    :return: A tuple containing lists of all records that fulfill certain test criteria.
    :rtype: tuple
    """
    from QueryPrinter import Counts, Links
    
    # print counts of SPASE records that answer analysis questions
    Obj = Counts()
    Obj.allRecords()
    #Obj.SDAC_Records()
    #Obj.SPDF_Records()
    
    testObj = Links()
    (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
     PIDRecords, descriptionRecords, citationRecords, complianceRecords) = testObj.allRecords()
    #testObj.SDAC_Records()
    #testObj.SPDF_Records()

    # return SPASE_id's of records that answer analysis questions
    return (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
     PIDRecords, descriptionRecords, citationRecords, complianceRecords)