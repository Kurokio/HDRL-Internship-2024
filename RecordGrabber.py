""" Function List:
allRecords: Executes SQLite SELECT statements for all records in table
SDAC_Records: Executes SQLite SELECT statements for records that are published by SDAC
SPDF_Records: Executes SQLite SELECT statements for records that are published by SPDF
"""

"""If additional tests were requested, simply create more string variables which contain the
SQLite queries to be executed and create a new return statement."""

import sqlite3
from SQLiteFun import execution
                
class Links():
    """Creates the SQLite statements that answer the analysis questions 
    from the Intern Doc. Publisher specific statements, such as 
    for SPDF and SDAC, are created by joining two queries using SQLite's INTERSECT. This class also houses 
    several functions which then returns the results from these queries. The printing is 
    split into 3 categories: all records, SDAC records, and SPDF records. The results are the SPASE_id's of
    the records that match the criteria given by the SQLite statements."""
    
    # query stmts that answer analysis questions
    totalStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries;"""
    authorStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE author NOT LIKE "" ;"""
    pubStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE publisher NOT LIKE "" ;"""
    pubYrStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE publicationYr NOT LIKE "" ;"""
    datasetNameStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE datasetName NOT LIKE "" ;"""
    licenseStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE license LIKE "%cc0%" 
                        OR license LIKE "%Creative Commons Zero v1.0 Universal%" ;"""
    urlStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE url NOT LIKE "" ;"""
    NASAurlStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE url NOT LIKE "" AND url!="No NASA Links";"""

    PIDStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE PID NOT LIKE "" ;"""
    descStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE description NOT LIKE "" ;"""
    # Code K can go here
    has_citation = """author NOT LIKE ""
                    AND datasetName NOT LIKE ""
                    AND publicationYr NOT LIKE ""
                    AND publisher NOT LIKE "" """
    citationStmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_citation};"""
    citationWOPIDStmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_citation} AND PID LIKE "" ;"""
    has_compliance = """ description NOT LIKE ""
                    AND datasetName NOT LIKE ""
                    AND PID NOT LIKE "" """
    complianceStmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_compliance};"""

    # edit once get data link checker to include working data links as a check

    # at least one field
    AL1Stmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_citation}
                    OR {has_compliance}
                    OR PID NOT LIKE ""
                    OR license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%";"""

    # at least 2 fields
    AL2Stmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE ({has_citation}
                    AND
                        {has_compliance}) 
                    OR
                        ({has_citation}
                    AND
                        PID NOT LIKE "")
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
                        PID NOT LIKE "")
                    OR  
                        (PID NOT LIKE ""
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%");"""

    # at least 3 fields
    AL3Stmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE ({has_citation}
                    AND
                        {has_compliance}
                    AND
                        PID NOT LIKE "")
                    OR
                        ({has_citation}
                    AND
                        PID NOT LIKE ""
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
                        PID NOT LIKE "");"""

    # not actually all but all at this moment
    allStmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_citation}
                    AND {has_compliance}
                    AND PID NOT LIKE ""
                    AND license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%";"""

    # all records with specified publisher
    SDACStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                                "%Solar Data Analysis Center)"""
    SPDFStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SPDF" OR publisher LIKE 
                                "%Space Physics Data Facility")"""

    # intersect stmts match previous queries with results that only have SPDF or SDAC as publisher
    SDACIntersect = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                                "%Solar Data Analysis Center")
                        INTERSECT """
    SPDFIntersect = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SPDF" OR publisher LIKE 
                                "%Space Physics Data Facility")
                        INTERSECT """

    SDACauthor = SDACIntersect + authorStmt
    SPDFauthor = SPDFIntersect + authorStmt
    SDACpubStmt = SDACIntersect + pubStmt
    SPDFpubStmt = SPDFIntersect + pubStmt
    SDACpubYrStmt = SDACIntersect + pubYrStmt
    SPDFpubYrStmt = SPDFIntersect + pubYrStmt
    SDACdatasetNameStmt = SDACIntersect + datasetNameStmt
    SPDFdatasetNameStmt = SPDFIntersect + datasetNameStmt
    SDAClicenseStmt = SDACIntersect + licenseStmt
    SPDFlicenseStmt = SPDFIntersect + licenseStmt
    SDACurlStmt = SDACIntersect + urlStmt
    SPDFurlStmt = SPDFIntersect + urlStmt
    SDACNASAurlStmt = SDACIntersect + NASAurlStmt
    SPDFNASAurlStmt = SPDFIntersect + NASAurlStmt
    SDACPIDStmt = SDACIntersect + PIDStmt
    SPDFPIDStmt = SPDFIntersect + PIDStmt
    SDACdescStmt = SDACIntersect + descStmt
    SPDFdescStmt = SPDFIntersect + descStmt
    SDACcitationStmt = SDACIntersect + citationStmt
    SPDFcitationStmt = SPDFIntersect + citationStmt
    SDACcitationWOPIDStmt = SDACIntersect + citationWOPIDStmt
    SPDFcitationWOPIDStmt = SPDFIntersect + citationWOPIDStmt
    SDACcomplianceStmt = SDACIntersect + complianceStmt
    SPDFcomplianceStmt = SPDFIntersect + complianceStmt

    SDAC_AL1 = SDACIntersect + AL1Stmt
    SPDF_AL1 = SPDFIntersect + AL1Stmt
    SDAC_AL2 = SDACIntersect + AL2Stmt
    SPDF_AL2 = SPDFIntersect + AL2Stmt
    SDAC_AL3 = SDACIntersect + AL3Stmt
    SPDF_AL3 = SPDFIntersect + AL3Stmt
    
    def allRecords(self):
        """Executes all SQLite SELECT statements that do not have a specified publisher and returns the lists.
        
        :return: The list of all SPASE records that match their specific criteria.
        :rtype: tuple"""

        # prints links for all records
        # Add Code L somewehere here
        links = execution(self.totalStmt, 'multiple')
        authors = execution(self.authorStmt, 'multiple')
        publishers = execution(self.pubStmt, 'multiple')
        pubYrs = execution(self.pubYrStmt, 'multiple')
        datasetNames = execution(self.datasetNameStmt, 'multiple')
        licenses = execution(self.licenseStmt, 'multiple')
        urls = execution(self.urlStmt, 'multiple')
        NASAurls = execution(self.NASAurlStmt, 'multiple')
        PIDs = execution(self.PIDStmt, 'multiple')
        descriptions = execution(self.descStmt, 'multiple')
        citations = execution(self.citationStmt, 'multiple')
        #citeWOPIDs = execution(self.citationWOPIDStmt, 'multiple')
        compliances = execution(self.complianceStmt, 'multiple')
        '''AL1 = execution(self.AL1Stmt, 'multiple')
        AL2 = execution(self.AL2Stmt, 'multiple')
        AL3 = execution(self.AL3Stmt, 'multiple')
        ALL = execution(self.allStmt, 'multiple')'''
        # Add Code M somewhere in this return
        return (links, authors, publishers, pubYrs, datasetNames, licenses, urls, NASAurls, PIDs,
                descriptions, citations, compliances) # citeWOPIDs, AL1, AL2, AL3, ALL)

    def SDAC_Records(self):
        """Executes all SQLite SELECT statements with the specified publisher, SDAC."""
        
        # prints links for SDAC records only    
        execution(self.SDACStmt, "published by SDAC")
        execution(self.SDACauthor, "with at least one author published by SDAC")
        execution(self.SDACpubStmt, "with a publisher published by SDAC")
        execution(self.SDACpubYrStmt, "with a publication year published by SDAC")
        execution(self.SDACdatasetNameStmt, "with a dataset name published by SDAC")
        execution(self.SDAClicenseStmt, "with a license published by SDAC")
        execution(self.SDACurlStmt, "with a url published by SDAC")
        execution(self.SDACNASAurlStmt, "with a NASA url published by SDAC")
        execution(self.SDACPIDStmt, "with a persistent identifier published by SDAC")
        execution(self.SDACdescStmt, "with a description published by SDAC")
        execution(self.SDACcitationStmt, "with citation info published by SDAC")
        execution(self.SDACcitationWOPIDStmt, "with citation info but no PID published by SDAC")
        execution(self.SDACcomplianceStmt, "that meet DCAT-US3 compliance published by SDAC")
        execution(self.SDAC_AL1, "with at least one desired field published by SDAC")
        execution(self.SDAC_AL2, "with at least two desired fields published by SDAC")
        execution(self.SDAC_AL3, "with at least three desired fields published by SDAC")

    def SPDF_Records(self):
        """Executes all SQLite SELECT statements with the specified publisher, SPDF."""
        
        # prints links of SPDF records only
        execution(self.SPDFStmt, "published by SPDF")
        execution(self.SPDFauthor, "with at least one author published by SPDF")
        execution(self.SPDFpubStmt, "with a publisher published by SPDF")
        execution(self.SPDFpubYrStmt, "with a publication year published by SPDF")
        execution(self.SPDFdatasetNameStmt, "with a dataset name published by SPDF")
        execution(self.SPDFlicenseStmt, "with a license published by SPDF")
        execution(self.SPDFurlStmt, "with a url published by SPDF")
        execution(self.SPDFNASAurlStmt, "with a NASA url published by SPDF")
        execution(self.SPDFPIDStmt, "with a persistent identifier published by SPDF")
        execution(self.SPDFdescStmt, "with a description published by SPDF")
        execution(self.SPDFcitationStmt, "with citation info published by SPDF")
        execution(self.SPDFcitationWOPIDStmt, "with citation info but no PID published by SPDF")
        execution(self.SPDFcomplianceStmt, "that meet DCAT-US3 compliance published by SPDF")
        execution(self.SPDF_AL1, "with at least one desired field published by SPDF")
        execution(self.SPDF_AL2, "with at least two desired fields published by SPDF")
        execution(self.SPDF_AL3, "with at least three desired fields published by SPDF")