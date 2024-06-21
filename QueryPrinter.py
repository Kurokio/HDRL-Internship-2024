""" Function List:
CountRemover(stmt, nested): Converts a COUNT SQLite statement into a SELECT SQLite statement
FormatPrint(self, statement, criteria): Prints result of SQLite statement with context in an
                                        easily understandable format
allRecords: Executes FormatPrint function for all records in table
SDAC_Records: Executes FormatPrint function for records that are published by SDAC
SPDF_Records: Executes FormatPrint function for records that are published by SPDF
"""

"""If additional tests were requested, simply create more string variables which contain the
SQLite queries to be executed and pass them to the correct FormatPrint method depending on
whether you want the links or counts returned. Make sure the query is structured correctly
(if you want links do not include COUNT, if you want the number of records matching a certain
criteria then do include the COUNT). Remember you can always use the CountRemover function to
convert any COUNT query into a generic SELECT query that returns the links."""

import sqlite3
from SQLiteFun import execution

def CountRemover(stmt, nested):
    """Takes SQLite COUNT query to change into a generic SELECT query instead. Also takes an argument to determine
    whether or not to add an extra closing parentheses depending on if the query is nested as a subquery. Pass 1
    to nested if query is a subquery, and pass 0 to it if the query is not a subquery.
    
    :param stmt: A string of the SQLite COUNT query to be changed.
    :type stmt: String
    :param nested: A number signifying whether or not the query is a subquery.
    :type nested: int
    :return: A string that no longer has the COUNT function.
    :rtype: String
    """
    
    # if nested = 0 (stmt not a nested query), just remove the COUNT
    before, sep, after = stmt.partition("COUNT(")
    b4, sep, after = after.partition(")")
    cleanStmt = before + b4 + after
    # add ending parentheses if stmt will be in a nested query (nested=1)
    if nested:
        before, sep, after = cleanStmt.partition(";")
        cleanStmt = before + ")" + sep + after
    return cleanStmt

class Counts(object):
    """Creates the SQLite statements that answer the analysis questions from the Intern Doc. Publisher specific
    statements, such as for SPDF and SDAC, are created by joining two queries using SQLite's INTERSECT. This class
    also houses several functions which then print the results from these queries in a proper format. The printing
    is split into 3 categories: all records, SDAC records, and SPDF records. The results are the COUNTS of
    the records that match the criteria given by the SQLite statements. This is the parent class of Links."""
    
    # print method for counts that prints the number of records matching criteria
    @classmethod
    def FormatPrint(self, stmt, criteria):
        """Prints the results for a given statement in their correct format. In this case, the
        results are counts, so it prints the count of the records that match the statement given.
        Following this, the criteria argument is appended, making a sentence which gives further 
        context to the user of which analysis question is being answered.

        :param stmt: A string of the SQLite statement that is executed
        :type stmt: String
        :param criteria: A string explaining what the resulting number is for.
        :type criteria: String
        """
        rows = execution(stmt)
        print("There are " + str(rows[0]) + " records " + criteria)
    
    # query stmts that answer analysis questions
    totalStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries;"""
    authorStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE author NOT LIKE "" ;"""
    pubStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE publisher NOT LIKE "" ;"""
    pubYrStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE publicationYr NOT LIKE "" ;"""
    datasetStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE dataset NOT LIKE "" ;"""
    licenseStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE license LIKE "%cc0%" 
                        OR license LIKE "%Creative Commons Zero v1.0 Universal%" ;"""
    urlStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE url NOT LIKE "" ;"""
    NASAurlStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE url NOT LIKE "" AND url!="No NASA Links";"""

    PIStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE PI NOT LIKE "" ;"""
    descStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries WHERE description NOT LIKE "" ;"""
    has_citation = """author NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND publicationYr NOT LIKE ""
                    AND publisher NOT LIKE "" """
    citationStmt = f"""SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE {has_citation};"""
    has_compliance = """ description NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND PI NOT LIKE "" """
    complianceStmt = f"""SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE {has_compliance};"""

    # edit once get data link checker to include working data links as a check
    
    # at least one field
    AL1Stmt = f"""SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE {has_citation}
                    OR {has_compliance}
                    OR PI NOT LIKE ""
                    OR license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%";"""

    # at least 2 fields
    AL2Stmt = f"""SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE ({has_citation}
                    AND
                        {has_compliance}) 
                    OR
                        ({has_citation}
                    AND
                        PI NOT LIKE "")
                    OR
                        ({has_citation}
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ({has_compliance}
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ({has_compliance}
                    AND 
                        PI NOT LIKE "")
                    OR  
                        (PI NOT LIKE ""
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%");"""

    # at least 3 fields
    AL3Stmt = f"""SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE ({has_citation}
                    AND
                        {has_compliance}
                    AND
                        PI NOT LIKE "")
                    OR
                        ({has_citation}
                    AND
                        PI NOT LIKE ""
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ({has_citation}
                    AND
                        {has_compliance}
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ({has_compliance}
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%"
                    AND 
                        PI NOT LIKE "");"""

    # not actually all but all at this moment
    allStmt = f"""SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE {has_citation}
                    AND {has_compliance}
                    AND PI NOT LIKE ""
                    AND license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%";"""
    
    # all records with specified publisher
    SDACStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                        WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                                "%Solar Data Analysis Center")"""
    SPDFStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                        WHERE (publisher LIKE "%SPDF" OR publisher LIKE 
                                "%Space Physics Data Facility")"""
    
    # intersect stmts match previous queries with results that only have SPDF or SDAC as publisher
    SDACIntersect = """SELECT COUNT(DISTINCT SPASE_id) FROM (
                        SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                                "%Solar Data Analysis Center")
                        INTERSECT """
    SPDFIntersect = """SELECT COUNT(DISTINCT SPASE_id) FROM (
                        SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SPDF" OR publisher LIKE 
                                "%Space Physics Data Facility")
                        INTERSECT """

    SDACauthor = SDACIntersect + CountRemover(authorStmt,1)
    SPDFauthor = SPDFIntersect + CountRemover(authorStmt,1)
    SDACpubStmt = SDACIntersect + CountRemover(pubStmt,1)
    SPDFpubStmt = SPDFIntersect + CountRemover(pubStmt,1)
    SDACpubYrStmt = SDACIntersect + CountRemover(pubYrStmt,1)
    SPDFpubYrStmt = SPDFIntersect + CountRemover(pubYrStmt,1)
    SDACdatasetStmt = SDACIntersect + CountRemover(datasetStmt,1)
    SPDFdatasetStmt = SPDFIntersect + CountRemover(datasetStmt,1)
    SDAClicenseStmt = SDACIntersect + CountRemover(licenseStmt,1)
    SPDFlicenseStmt = SPDFIntersect + CountRemover(licenseStmt,1)
    SDACurlStmt = SDACIntersect + CountRemover(urlStmt,1)
    SPDFurlStmt = SPDFIntersect + CountRemover(urlStmt,1)
    SDACNASAurlStmt = SDACIntersect + CountRemover(NASAurlStmt,1)
    SPDFNASAurlStmt = SPDFIntersect + CountRemover(NASAurlStmt,1)
    SDACPIStmt = SDACIntersect + CountRemover(PIStmt,1)
    SPDFPIStmt = SPDFIntersect + CountRemover(PIStmt,1)
    SDACdescStmt = SDACIntersect + CountRemover(descStmt,1)
    SPDFdescStmt = SPDFIntersect + CountRemover(descStmt,1)
    SDACcitationStmt = SDACIntersect + CountRemover(citationStmt,1)
    SPDFcitationStmt = SPDFIntersect + CountRemover(citationStmt,1)
    SDACcomplianceStmt = SDACIntersect + CountRemover(complianceStmt,1)
    SPDFcomplianceStmt = SPDFIntersect + CountRemover(complianceStmt,1)

    SDAC_AL1 = SDACIntersect + CountRemover(AL1Stmt,1)
    SPDF_AL1 = SPDFIntersect + CountRemover(AL1Stmt,1)
    SDAC_AL2 = SDACIntersect + CountRemover(AL2Stmt,1)
    SPDF_AL2 = SPDFIntersect + CountRemover(AL2Stmt,1)
    SDAC_AL3 = SDACIntersect + CountRemover(AL3Stmt,1)
    SPDF_AL3 = SPDFIntersect + CountRemover(AL3Stmt,1)

    def allRecords(self):
        """Executes the FormatPrint function for all SQLite statements that do not have a
        specified publisher."""
        
        # prints counts for all records
        self.FormatPrint(self.totalStmt, "total")
        self.FormatPrint(self.authorStmt, "with at least one author")
        self.FormatPrint(self.pubStmt, "with a publisher")
        self.FormatPrint(self.pubYrStmt, "with a publication year")
        self.FormatPrint(self.datasetStmt, "with a dataset")
        self.FormatPrint(self.licenseStmt, "with a license")
        self.FormatPrint(self.urlStmt, "with a url")
        self.FormatPrint(self.NASAurlStmt, "with a NASA url")
        self.FormatPrint(self.PIStmt, "with a persistent identifier")
        self.FormatPrint(self.descStmt, "with a description")
        self.FormatPrint(self.citationStmt, "with citation info")
        self.FormatPrint(self.complianceStmt, "that meet DCAT-US3 compliance")
        self.FormatPrint(self.AL1Stmt, "that have at least one desired field")
        self.FormatPrint(self.AL2Stmt, "that have at least two desired fields")
        self.FormatPrint(self.AL3Stmt, "that have at least three desired fields")
        self.FormatPrint(self.allStmt, "that have all desired fields")

    def SDAC_Records(self):
        """Executes the FormatPrint function for all SQLite statements with the
        specified publisher, SDAC."""
        
        # prints counts for SDAC records only    
        self.FormatPrint(self.SDACStmt, "published by SDAC")
        self.FormatPrint(self.SDACauthor, "with at least one author published by SDAC")
        self.FormatPrint(self.SDACpubStmt, "with a publisher published by SDAC")
        self.FormatPrint(self.SDACpubYrStmt, "with a publication year published by SDAC")
        self.FormatPrint(self.SDACdatasetStmt, "with a dataset published by SDAC")
        self.FormatPrint(self.SDAClicenseStmt, "with a license published by SDAC")
        self.FormatPrint(self.SDACurlStmt, "with a url published by SDAC")
        self.FormatPrint(self.SDACNASAurlStmt, "with a NASA url published by SDAC")
        self.FormatPrint(self.SDACPIStmt, "with a persistent identifier published by SDAC")
        self.FormatPrint(self.SDACdescStmt, "with a description published by SDAC")
        self.FormatPrint(self.SDACcitationStmt, "with citation info published by SDAC")
        self.FormatPrint(self.SDACcomplianceStmt, "that meet DCAT-US3 compliance published by SDAC")
        self.FormatPrint(self.SDAC_AL1, "with at least one desired field published by SDAC")
        self.FormatPrint(self.SDAC_AL2, "with at least two desired fields published by SDAC")
        self.FormatPrint(self.SDAC_AL3, "with at least three desired fields published by SDAC")

    def SPDF_Records(self):
        """Executes the FormatPrint function for all SQLite statements with the
        specified publisher, SPDF."""
        
        # prints counts of SPDF records only
        self.FormatPrint(self.SPDFStmt, "published by SPDF")
        self.FormatPrint(self.SPDFauthor, "with at least one author published by SPDF")
        self.FormatPrint(self.SPDFpubStmt, "with a publisher published by SPDF")
        self.FormatPrint(self.SPDFpubYrStmt, "with a publication year published by SPDF")
        self.FormatPrint(self.SPDFdatasetStmt, "with a dataset published by SPDF")
        self.FormatPrint(self.SPDFlicenseStmt, "with a license published by SPDF")
        self.FormatPrint(self.SPDFurlStmt, "with a url published by SPDF")
        self.FormatPrint(self.SPDFNASAurlStmt, "with a NASA url published by SPDF")
        self.FormatPrint(self.SPDFPIStmt, "with a persistent identifier published by SPDF")
        self.FormatPrint(self.SPDFdescStmt, "with a description published by SPDF")
        self.FormatPrint(self.SPDFcitationStmt, "with citation info published by SPDF")
        self.FormatPrint(self.SPDFcomplianceStmt, "that meet DCAT-US3 compliance published by SPDF")
        self.FormatPrint(self.SPDF_AL1, "with at least one desired field published by SPDF")
        self.FormatPrint(self.SPDF_AL2, "with at least two desired fields published by SPDF")
        self.FormatPrint(self.SPDF_AL3, "with at least three desired fields published by SPDF")
            
class Links(Counts):
    """A subclass of the Counts class. Creates the SQLite statements that answer the analysis questions 
    from the Intern Doc. These statements are created by using the local CountRemover function on the 
    previously defined statements in the parent class, Counts. Publisher specific statements, such as 
    for SPDF and SDAC, are created by joining two queries using SQLite's INTERSECT. This class also houses 
    several functions which then print the results from these queries in a proper format. The printing is 
    split into 3 categories: all records, SDAC records, and SPDF records. The results are the SPASE_id's of
    the records that match the criteria given by the SQLite statements."""
    
    # print method for links that prints the first 10 links matching criteria given
    @classmethod
    def FormatPrint(self, stmt, criteria):
        """Prints the results for a given statement in their correct format. The first thing printed 
        is a sentence using the criteria argument, which gives context to the user of which analysis 
        question is being answered. In this case, the results are SPASE_id's, so it prints the 
        SPASE_id's of the records that match the statement given.

        :param stmt: A string of the SQLite statement that is executed
        :type stmt: String
        :param criteria: A string explaining what the resulting number is for.
        :type criteria: String
        """
        rows = execution(stmt)
        print("The records " + criteria + " are:")
        print(rows[:10])
    
    # can pass any stmt to CountRemover to return SPASE_id's of records instead of counts of records
    # overriding attributes from parent class to now return Spase_id's matching desired criteria
    SDACStmt = CountRemover(Counts.SDACStmt,0)
    SPDFStmt = CountRemover(Counts.SPDFStmt,0)
    totalStmt = CountRemover(Counts.totalStmt,0)
    SDACauthor = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.authorStmt,1)
    SPDFauthor = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.authorStmt,1)
    SDACpubStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.pubStmt,1)
    SPDFpubStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.pubStmt,1)
    SDACpubYrStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.pubYrStmt,1)
    SDACpubYrStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.pubYrStmt,1)
    SPDFpubYrStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.pubYrStmt,1)
    SDACdatasetStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.datasetStmt,1)
    SPDFdatasetStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.datasetStmt,1)
    SDAClicenseStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.licenseStmt,1)
    SPDFlicenseStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.licenseStmt,1)
    SDACurlStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.urlStmt,1)
    SPDFurlStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.urlStmt,1)
    SDACNASAurlStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.NASAurlStmt,1)
    SPDFNASAurlStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.NASAurlStmt,1)
    SDACPIStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.PIStmt,1)
    SPDFPIStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.PIStmt,1)
    SDACdescStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.descStmt,1)
    SPDFdescStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.descStmt,1)
    SDACcitationStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.citationStmt,1)
    SPDFcitationStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.citationStmt,1)
    SDACcomplianceStmt = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.complianceStmt,1)
    SPDFcomplianceStmt = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.complianceStmt,1)
    SDAC_AL1 = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.AL1Stmt,1)
    SPDF_AL1 = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.AL1Stmt,1)
    SDAC_AL2 = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.AL2Stmt,1)
    SPDF_AL2 = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.AL2Stmt,1)
    SDAC_AL3 = CountRemover(Counts.SDACIntersect,0) + CountRemover(Counts.AL3Stmt,1)
    SPDF_AL3 = CountRemover(Counts.SPDFIntersect,0) + CountRemover(Counts.AL3Stmt,1)
    authorStmt = CountRemover(Counts.authorStmt,0)
    pubStmt = CountRemover(Counts.pubStmt,0)
    pubYrStmt = CountRemover(Counts.pubYrStmt,0)
    datasetStmt = CountRemover(Counts.datasetStmt,0)
    licenseStmt = CountRemover(Counts.licenseStmt,0)
    urlStmt = CountRemover(Counts.urlStmt,0)
    NASAurlStmt = CountRemover(Counts.NASAurlStmt,0)
    PIStmt = CountRemover(Counts.PIStmt,0)
    descStmt = CountRemover(Counts.descStmt,0)
    citationStmt = CountRemover(Counts.citationStmt,0)
    complianceStmt = CountRemover(Counts.complianceStmt,0)
    AL1Stmt = CountRemover(Counts.AL1Stmt,0)
    AL2Stmt = CountRemover(Counts.AL2Stmt,0)
    AL3Stmt = CountRemover(Counts.AL3Stmt,0)
    allStmt = CountRemover(Counts.allStmt,0)
    
    
    def allRecords(self):
        """Executes the FormatPrint function for all SQLite statements that do not have a
        specified publisher."""

        # prints links for all records
        self.FormatPrint(self.totalStmt, "in the database")
        self.FormatPrint(self.authorStmt, "with at least one author")
        self.FormatPrint(self.pubStmt, "with a publisher")
        self.FormatPrint(self.pubYrStmt, "with a publication year")
        self.FormatPrint(self.datasetStmt, "with a dataset")
        self.FormatPrint(self.licenseStmt, "with a license")
        self.FormatPrint(self.urlStmt, "with a url")
        self.FormatPrint(self.NASAurlStmt, "with a NASA url")
        self.FormatPrint(self.PIStmt, "with a persistent identifier")
        self.FormatPrint(self.descStmt, "with a description")
        self.FormatPrint(self.citationStmt, "with citation info")
        self.FormatPrint(self.complianceStmt, "that meet DCAT-US3 compliance")
        self.FormatPrint(self.AL1Stmt, "that have at least one desired field")
        self.FormatPrint(self.AL2Stmt, "that have at least two desired fields")
        self.FormatPrint(self.AL3Stmt, "that have at least three desired fields")
        self.FormatPrint(self.allStmt, "that have all desired fields")

    def SDAC_Records(self):
        """Executes the FormatPrint function for all SQLite statements with the
        specified publisher, SDAC."""
        
        # prints links for SDAC records only    
        self.FormatPrint(self.SDACStmt, "published by SDAC")
        self.FormatPrint(self.SDACauthor, "with at least one author published by SDAC")
        self.FormatPrint(self.SDACpubStmt, "with a publisher published by SDAC")
        self.FormatPrint(self.SDACpubYrStmt, "with a publication year published by SDAC")
        self.FormatPrint(self.SDACdatasetStmt, "with a dataset published by SDAC")
        self.FormatPrint(self.SDAClicenseStmt, "with a license published by SDAC")
        self.FormatPrint(self.SDACurlStmt, "with a url published by SDAC")
        self.FormatPrint(self.SDACNASAurlStmt, "with a NASA url published by SDAC")
        self.FormatPrint(self.SDACPIStmt, "with a persistent identifier published by SDAC")
        self.FormatPrint(self.SDACdescStmt, "with a description published by SDAC")
        self.FormatPrint(self.SDACcitationStmt, "with citation info published by SDAC")
        self.FormatPrint(self.SDACcomplianceStmt, "that meet DCAT-US3 compliance published by SDAC")
        self.FormatPrint(self.SDAC_AL1, "with at least one desired field published by SDAC")
        self.FormatPrint(self.SDAC_AL2, "with at least two desired fields published by SDAC")
        self.FormatPrint(self.SDAC_AL3, "with at least three desired fields published by SDAC")

    def SPDF_Records(self):
        """Executes the FormatPrint function for all SQLite statements with the
        specified publisher, SPDF."""
        
        # prints links of SPDF records only
        self.FormatPrint(self.SPDFStmt, "published by SPDF")
        self.FormatPrint(self.SPDFauthor, "with at least one author published by SPDF")
        self.FormatPrint(self.SPDFpubStmt, "with a publisher published by SPDF")
        self.FormatPrint(self.SPDFpubYrStmt, "with a publication year published by SPDF")
        self.FormatPrint(self.SPDFdatasetStmt, "with a dataset published by SPDF")
        self.FormatPrint(self.SPDFlicenseStmt, "with a license published by SPDF")
        self.FormatPrint(self.SPDFurlStmt, "with a url published by SPDF")
        self.FormatPrint(self.SPDFNASAurlStmt, "with a NASA url published by SPDF")
        self.FormatPrint(self.SPDFPIStmt, "with a persistent identifier published by SPDF")
        self.FormatPrint(self.SPDFdescStmt, "with a description published by SPDF")
        self.FormatPrint(self.SPDFcitationStmt, "with citation info published by SPDF")
        self.FormatPrint(self.SPDFcomplianceStmt, "that meet DCAT-US3 compliance published by SPDF")
        self.FormatPrint(self.SPDF_AL1, "with at least one desired field published by SPDF")
        self.FormatPrint(self.SPDF_AL2, "with at least two desired fields published by SPDF")
        self.FormatPrint(self.SPDF_AL3, "with at least three desired fields published by SPDF")