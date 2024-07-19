""" Function List:
allRecords: Executes SQLite SELECT statements for all records in table
SDAC_Records: Executes SQLite SELECT statements for records that are published by SDAC
SPDF_Records: Executes SQLite SELECT statements for records that are published by SPDF
NASA_URL_Records: Executes SQLite SELECT statements for records in table that have NASA URLs
"""

"""If additional tests were requested, simply create more string variables which contain the
SQLite queries to be executed and create a new return statement."""

import sqlite3
from .SQLiteFun import execution
                
class Links():
    """Creates the SQLite statements that answer the analysis questions 
    from the Intern Doc. Publisher specific statements, such as 
    for SPDF and SDAC, are created by joining two queries using SQLite's INTERSECT. This class also houses 
    several functions which then returns the results from these queries. The printing is 
    split into 3 categories: all records, SDAC records, and SPDF records. The results are the SPASE_id's of
    the records that match the criteria given by the SQLite statements.
    
    :return: a Links object
    """
    
    # query stmts that answer analysis questions
    totalStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries;"""
    authorStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE author NOT LIKE "" ;"""
    pubStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE publisher NOT LIKE "" ;"""
    pubYrStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE publicationYr NOT LIKE "" ;"""
    datasetNameStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE datasetName NOT LIKE "" ;"""
    licenseStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE license LIKE "%cc0%" 
                        OR license LIKE "%Creative Commons Zero v1.0 Universal%" ;"""
    urlStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE url NOT LIKE "" ;"""
    NASAurlStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE (url NOT LIKE "" AND url!="No NASA Links");"""

    PID_Stmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE PID NOT LIKE "" ;"""
    descStmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries WHERE description NOT LIKE "" ;"""
    # Code K can go here
    has_citation = """author NOT LIKE ""
                    AND datasetName NOT LIKE ""
                    AND publicationYr NOT LIKE ""
                    AND publisher NOT LIKE "" """
    citationStmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_citation};"""
    citationWoPID_Stmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_citation} AND PID LIKE "" ;"""
    has_compliance = """ description NOT LIKE ""
                    AND datasetName NOT LIKE ""
                    AND PID NOT LIKE "" """
    complianceStmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_compliance};"""

    # edit once get data link checker to include working data links as a check

    # at least one field
    AL1_Stmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
                    WHERE {has_citation}
                    OR {has_compliance}
                    OR PID NOT LIKE ""
                    OR license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%";"""

    # at least 2 fields
    AL2_Stmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
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
    AL3_Stmt = f"""SELECT DISTINCT SPASE_id FROM MetadataEntries 
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
    SDAC_Stmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                                "%Solar Data Analysis Center")"""
    SPDF_Stmt = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SPDF" OR publisher LIKE 
                                "%Space Physics Data Facility")"""

    # intersect stmts match previous queries with results that only have SPDF or SDAC as publisher
    SDAC_Intersect = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                                "%Solar Data Analysis Center")
                        INTERSECT """
    SPDF_Intersect = """SELECT DISTINCT SPASE_id FROM MetadataEntries 
                        WHERE (publisher LIKE "%SPDF" OR publisher LIKE 
                                "%Space Physics Data Facility")
                        INTERSECT """

    SDACauthor = SDAC_Intersect + authorStmt
    SPDFauthor = SPDF_Intersect + authorStmt
    SDACpubStmt = SDAC_Intersect + pubStmt
    SPDFpubStmt = SPDF_Intersect + pubStmt
    SDACpubYrStmt = SDAC_Intersect + pubYrStmt
    SPDFpubYrStmt = SPDF_Intersect + pubYrStmt
    SDACdatasetNameStmt = SDAC_Intersect + datasetNameStmt
    SPDFdatasetNameStmt = SPDF_Intersect + datasetNameStmt
    SDAClicenseStmt = SDAC_Intersect + licenseStmt
    SPDFlicenseStmt = SPDF_Intersect + licenseStmt
    SDACurlStmt = SDAC_Intersect + urlStmt
    SPDFurlStmt = SPDF_Intersect + urlStmt
    SDAC_NASAurlStmt = SDAC_Intersect + NASAurlStmt
    SPDF_NASAurlStmt = SPDF_Intersect + NASAurlStmt
    SDAC_PID_Stmt = SDAC_Intersect + PID_Stmt
    SPDF_PID_Stmt = SPDF_Intersect + PID_Stmt
    SDACdescStmt = SDAC_Intersect + descStmt
    SPDFdescStmt = SPDF_Intersect + descStmt
    SDACcitationStmt = SDAC_Intersect + citationStmt
    SPDFcitationStmt = SPDF_Intersect + citationStmt
    SDACcitationWoPID_Stmt = SDAC_Intersect + citationWoPID_Stmt
    SPDFcitationWoPID_Stmt = SPDF_Intersect + citationWoPID_Stmt
    SDACcomplianceStmt = SDAC_Intersect + complianceStmt
    SPDFcomplianceStmt = SPDF_Intersect + complianceStmt

    SDAC_AL1 = SDAC_Intersect + AL1_Stmt
    SPDF_AL1 = SPDF_Intersect + AL1_Stmt
    SDAC_AL2 = SDAC_Intersect + AL2_Stmt
    SPDF_AL2 = SPDF_Intersect + AL2_Stmt
    SDAC_AL3 = SDAC_Intersect + AL3_Stmt
    SPDF_AL3 = SPDF_Intersect + AL3_Stmt
    
    # statements to retrieve records with each metadata field that also have NASA URLs
    NASAurlIntersect = NASAurlStmt.replace(";", '')
    NASAurlIntersect += ' INTERSECT '
    
    NASAauthorStmt = NASAurlIntersect + authorStmt
    NASApubStmt = NASAurlIntersect + pubStmt
    NASApubYrStmt = NASAurlIntersect + pubYrStmt
    NASAdatasetNameStmt = NASAurlIntersect + datasetNameStmt
    NASAlicenseStmt = NASAurlIntersect + licenseStmt
    NASA_PID_Stmt = NASAurlIntersect + PID_Stmt
    NASAdescStmt = NASAurlIntersect + descStmt
    NASAcitationStmt = NASAurlIntersect + citationStmt
    NASAcomplianceStmt = NASAurlIntersect + complianceStmt
    
    def allRecords(self, conn):
        """Executes all SQLite SELECT statements that do not have a specified publisher and returns the lists.
        
        :param conn: A connection to the desired database
        :type conn: Connection object
        :return: The list of all SPASE records that match their specific criteria.
        :rtype: tuple"""

        # prints links for all records
        # Add Code L somewehere here
        links = execution(self.totalStmt, conn)
        authors = execution(self.authorStmt, conn)
        publishers = execution(self.pubStmt, conn)
        pubYrs = execution(self.pubYrStmt, conn)
        datasetNames = execution(self.datasetNameStmt, conn)
        licenses = execution(self.licenseStmt, conn)
        urls = execution(self.urlStmt, conn)
        NASAurls = execution(self.NASAurlStmt, conn)
        PIDs = execution(self.PID_Stmt, conn)
        descriptions = execution(self.descStmt, conn)
        citations = execution(self.citationStmt, conn)
        #citeWOPIDs = execution(self.citationWoPID_Stmt)
        compliances = execution(self.complianceStmt, conn)
        '''AL1 = execution(self.AL1_Stmt)
        AL2 = execution(self.AL2_Stmt)
        AL3 = execution(self.AL3_Stmt)
        ALL = execution(self.allStmt)'''
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
        execution(self.SDACPID_Stmt, "with a persistent identifier published by SDAC")
        execution(self.SDACdescStmt, "with a description published by SDAC")
        execution(self.SDACcitationStmt, "with citation info published by SDAC")
        execution(self.SDACcitationWoPID_Stmt, "with citation info but no PID published by SDAC")
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
        execution(self.SPDFPID_Stmt, "with a persistent identifier published by SPDF")
        execution(self.SPDFdescStmt, "with a description published by SPDF")
        execution(self.SPDFcitationStmt, "with citation info published by SPDF")
        execution(self.SPDFcitationWoPID_Stmt, "with citation info but no PID published by SPDF")
        execution(self.SPDFcomplianceStmt, "that meet DCAT-US3 compliance published by SPDF")
        execution(self.SPDF_AL1, "with at least one desired field published by SPDF")
        execution(self.SPDF_AL2, "with at least two desired fields published by SPDF")
        execution(self.SPDF_AL3, "with at least three desired fields published by SPDF")
        
    def NASA_URL_Records(self, conn):
        """Executes all SQLite SELECT statements that have NASA URLs and returns the lists.
        
        :param conn: A connection to the desired database
        :type conn: Connection object
        :return: The list of all SPASE records that match their specific criteria.
        :rtype: tuple"""

        # prints links for all records w NASA URLs
        links = execution(self.NASAurlStmt, conn)
        authors = execution(self.NASAauthorStmt, conn)
        publishers = execution(self.NASApubStmt, conn)
        pubYrs = execution(self.NASApubYrStmt, conn)
        datasetNames = execution(self.NASAdatasetNameStmt, conn)
        licenses = execution(self.NASAlicenseStmt, conn)
        urls = links
        NASAurls = urls
        PIDs = execution(self.NASA_PID_Stmt, conn)
        descriptions = execution(self.NASAdescStmt, conn)
        citations = execution(self.NASAcitationStmt, conn)
        compliances = execution(self.NASAcomplianceStmt, conn)
        return (links, authors, publishers, pubYrs, datasetNames, licenses, urls, NASAurls, PIDs,
                descriptions, citations, compliances)