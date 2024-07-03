def Create(printFlag, update):
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
                        # add a new SPASE Record to MetadataEntries
                        if not update:
                            Metadata = (RID,author,authorRole,pub,pubYear,datasetName,license,url[i],prodKey[i],desc,PID)
                            Record_id = add_Metadata(conn, Metadata)
                            print(f'Created a Metadata entry with the row number {Record_id}')
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
                                print(f"Updated a MetadataEntries record with the row number '{row[0]}' ")
                            # if there are new records added, use the add_Metadata function
                            except IndexError:
                                Metadata = (RID,author,authorRole,pub,pubYear,datasetName,license,url[i],prodKey[i],desc,PID)
                                Record_id = add_Metadata(conn, Metadata)
                                print(f'Created a Metadata entry with the row number {Record_id}')
                            finally:
                                i += 1
                    # add a new Source record
                    Sources = (RID,authorField,pubField,pubDateField,datasetNameField,licenseField,
                               datalinkField,descField,PIDField)
                    Record_id = add_Sources(conn, Sources)
                    print(f'Created a Sources entry with row number {Record_id}')
                    # add a new Records entry
                    before, sep, after = RID.partition('NASA')
                    compURL =  "https://github.com/hpde/NASA/blob/master" + after + ".xml"
                    entry = (RID,version,ReleaseDate,compURL)
                    Record_id = add_Records(conn, entry)
                    print(f'Created a Records entry with the row number {Record_id}')

            except sqlite3.Error as e:
                print(e)

            print("======================================================================================================")

        else:
            continue
            
    # collects SPASE_id's of records that answer analysis questions
    testObj = Links()
    (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
     PIDRecords, descriptionRecords, citationRecords, complianceRecords) = testObj.allRecords()
    #testObj.SDAC_Records()
    #testObj.SPDF_Records()
    TestResultRecords = execution("SELECT SPASE_id FROM TestResults", 1)

    # create the table with 0 as default for all, passing all records to the first insert call
    try:
        with sqlite3.connect('SPASE_Data.db') as conn:
            for record in records:
                # if it is not a new SPASE Record
                if record in TestResultRecords:
                    continue
                # if it is a new SPASE record
                else:
                    Test = (record,0,"","",0,0,0,0,0,0,0,0,0,0,0,"")
                    Record_id = add_TestResults(conn, Test)
                    print(f'Created a TestResults entry with the row number {Record_id}')

    except sqlite3.Error as e:
                print(e)

    # iterate thru lists one by one and update column for each record if in the list (if record in author, has_author = 1)
    # UPDATE stmt for each test
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
    from QueryPrinter import Counts, Links
    import sqlite3
    
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