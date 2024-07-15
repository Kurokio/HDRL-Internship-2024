""" Function List:
Create: Scrapes all desired metadata present in the given directory and creates and populates the 
        MetadataEntries, MetadataSources, Records, and TestResults tables.
View: Prints the count of and returns the SPASE_id's of the records desired.
"""

def Create(folder, printFlag = False):
    """
    Scrapes all records that are found in the directory given for the desired metadata. Creates the MetadataEntries, 
    MetadataSources, Records, and TestResults tables and populates them using the data scraped for each record. Populates the\
    TestResult table with default values to be overwritten by the call to the FAIRScorer function in the notebook.
    
    :param folder: The absolute file path of the SPASE record/directory containing the record(s) the user wants scraped.
    :type folder: String
    :param printFlag: A boolean determining if the user wants to print more details of what the function is doing.
    :type printFlag: Boolean
    :return: None
    """
    # import functions from .py files and from built-in packages
    import pprint, sqlite3
    from SPASE_Scraper_Script import SPASE_Scraper
    from PathGrabber import getPaths
    from SQLiteFun import (create_tables, add_Metadata, add_Sources, add_Records, execution, executionALL, create_tables,
                          add_TestResults, TestUpdate)
    from DatalinkSep import AccessRightsSep
    from RecordGrabber import Links
    
    # list that holds paths returned by PathGrabber
    SPASE_paths = []

    # get user input and extract all SPASE records
    #print("Enter root folder you want to search")
    #folder = input()
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
            (ResourceID, ResourceIDField, author, authorField, authorRole, pub, pubField, pubDate, pubDateField, datasetName, 
             datasetNameField, desc, descField, PID, PIDField, AccessRights, licenseField, datalinkField, 
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
                print("The ResourceID is " + ResourceID + " which was obtained from " + ResourceIDField)
                print("The author(s) are " + author + " who are " + authorRole + " which was obtained from " + authorField)
                # add Code F here
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
                        # Add Code G, Code H, and Code I in this statement
                        UpdateStmt = f''' INSERT INTO MetadataEntries
                                            (SPASE_id,author,authorRole,publisher,publicationYr,datasetName,
                                            license,URL,prodKey,description,PID)
                                        VALUES ("{ResourceID}","{author}","{authorRole}","{pub}","{pubYear}",
                                        "{datasetName}","{license}","{url[i]}","{prodKey[i]}","description found","{PID}")
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
                        executionALL(UpdateStmt)
                        i += 1
                    # add or update Records entry
                    before, sep, after = ResourceID.partition('NASA')
                    compURL =  "https://github.com/hpde/NASA/blob/master" + after + ".xml"
                    UpdateStmt = f''' INSERT INTO Records
                                            (SPASE_id, SPASE_Version,LastModified,SPASE_URL)
                                        VALUES ("{ResourceID}","{version}","{ReleaseDate}","{compURL}")
                                        ON CONFLICT (SPASE_id) DO
                                        UPDATE
                                        SET
                                            SPASE_version = excluded.SPASE_version,
                                            LastModified = excluded.LastModified,
                                            SPASE_URL = excluded.SPASE_URL; '''
                    executionALL(UpdateStmt)                    
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
                    executionALL(UpdateStmt)

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
     PIDRecords, descriptionRecords, citationRecords, complianceRecords) = testObj.allRecords()
    #testObj.SDAC_Records()
    #testObj.SPDF_Records()
    TestResultRecords = execution("SELECT DISTINCT(SPASE_id) FROM TestResults")

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
    
    # Add Code Q here
def View(desired = ["all", "author", "pub", "pubYr", "datasetName", "license",
                    "url","NASAurl", "PID", "description", "citation", "compliance"]):
    """
    Prints the number of records that meet each test criteria provided as well as return those SPASE_id's \
    to the caller in the form of a dictionary. The keys are the Strings passed as parameters and the \
    values are the list of SPASE_id's that fulfill that test.
    
    :param desired: A list of Strings which determine the kind of records whose counts are printed and whose \
                        SPASE_id's are assigned to the dictionary returned. The default value is all kinds.
    :param type: list
    
    As an example, calling the View function with no parameters, such as records = View(), will use the \
    default value for desired, which is a list of all the kinds of records. This means it will print the \
    number of (distinct) records that are present in the MetdataEntries table, have authors, have publishers, \
    have publication years, have datasetNames, have licenses, have URLs, have NASA URLs, have persistent \
    identifiers, have descriptions, have citation info, and meet compliance standards. It will also return a \
    dictionary that contains all the SPASE_id's of these records, separated on their types by keys of the \
    same name, and assign this dictionary to records. This means that the SPASE_id's are accessible by calling \
    records["x"], where x is the type of records desired such as "author", "url", etc.
    
    For another example, calling "records = View(desired = ['all'])" prints the number of distinct SPASE records in the \
    MetadataEntries table as well as returns and assigns the returned dictionary to records. This now makes the \
    list of SPASE_id's found in the table accessible by calling records["all"].
    
    One can also pass multiple parameters as seen by calling "records = View(desired = ['pub', 'PID'])". This \
    prints the number of records that have publishers and the number of records that have persistent identifiers. \
    Once again, the SPASE_id's of those records and then accessible by calling records["pub"] and records["PID"].
    
    :return: A dictionary containing lists of all records that fulfill certain test criteria as values.
    :rtype: dictionary
    """

    from RecordGrabber import Links
    # Add Code R here
    desiredRecords = {"all": [], "author": [], "pub": [], "pubYr": [], "datasetName": [], "license": [], "url": [],
                      "NASAurl": [], "PID": [], "description": [], "citation": [], "compliance": []}
    
    testObj = Links()
    # Add Code P here
    (records, authorRecords, pubRecords, pubYrRecords, datasetNameRecords, licenseRecords, urlRecords, NASAurlRecords, 
     PIDRecords, descriptionRecords, citationRecords, complianceRecords, citeWOPIDRecords, AL1Records, AL2Records, AL3Records,
     ALLRecords) = testObj.allRecords()
    #testObj.SDAC_Records()
    #testObj.SPDF_Records()
    
    # print counts of SPASE records that answer analysis questions
    for record in desired:
        if record == "all":
            print("There are " + str(len(records)) + " records total.")
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