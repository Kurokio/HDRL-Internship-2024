import sqlite3
from SQLiteFun import execution

def CountRemover(stmt, nested):
        # if nested = 0 (stmt not a nested query)
        before, sep, after = stmt.partition("COUNT(")
        b4, sep, after = after.partition(")")
        cleanStmt = before + b4 + after
        # add ending parentheses if stmt will be in a nested query (nested=1)
        if nested:
            before, sep, after = cleanStmt.partition(";")
            cleanStmt = before + ")" + sep + after
        return cleanStmt

class Counts(object):
    @classmethod
    def FormatPrint(stmt, criteria):
        rows = execution(stmt)
        print("There are " + str(rows[0]) + "records " + criteria)
    
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
    citationStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE author NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND publicationYr NOT LIKE ""
                    AND publisher NOT LIKE "";"""
    complianceStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE description NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND PI NOT LIKE "";"""

    # edit once get data link checker to include working data links as a check
    AL1Stmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE (author NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND publicationYr NOT LIKE ""
                    AND publisher NOT LIKE "")
                    OR (description NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND PI NOT LIKE "")
                    OR PI NOT LIKE ""
                    OR license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%";"""

    AL2Stmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE ((author NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND publicationYr NOT LIKE ""
                        AND publisher NOT LIKE "")
                    AND
                        (description NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND PI NOT LIKE "")) 
                    OR
                        ((author NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND publicationYr NOT LIKE ""
                        AND publisher NOT LIKE "")
                    AND
                        PI NOT LIKE "")
                    OR
                        ((author NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND publicationYr NOT LIKE ""
                        AND publisher NOT LIKE "")
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ((description NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND PI NOT LIKE "")
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ((description NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND PI NOT LIKE "")
                    AND 
                        PI NOT LIKE "")
                    OR  
                        (PI NOT LIKE ""
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%");"""

    AL3Stmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE ((author NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND publicationYr NOT LIKE ""
                        AND publisher NOT LIKE "")
                    AND
                        (description NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND PI NOT LIKE "")
                    AND
                        PI NOT LIKE "")
                    OR
                        ((author NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND publicationYr NOT LIKE ""
                        AND publisher NOT LIKE "")
                    AND
                        PI NOT LIKE ""
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ((author NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND publicationYr NOT LIKE ""
                        AND publisher NOT LIKE "")
                    AND
                        (description NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND PI NOT LIKE "")
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%")
                    OR
                        ((description NOT LIKE ""
                        AND dataset NOT LIKE ""
                        AND PI NOT LIKE "")
                    AND
                        license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%"
                    AND 
                        PI NOT LIKE "");"""

    # not actually all but all at this moment
    allStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                    WHERE (author NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND publicationYr NOT LIKE ""
                    AND publisher NOT LIKE "")
                    AND (description NOT LIKE ""
                    AND dataset NOT LIKE ""
                    AND PI NOT LIKE "")
                    AND PI NOT LIKE ""
                    AND license LIKE "%cc0%" OR license LIKE "%Creative Commons Zero v1.0 Universal%";"""
    # test for above queries with specified publisher
    SDACStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                        WHERE (publisher LIKE "%SDAC" OR publisher LIKE 
                                "%Solar Data Analysis Center")"""
    SPDFStmt = """SELECT COUNT(DISTINCT SPASE_id) FROM MetadataEntries 
                        WHERE (publisher LIKE "%SPDF" OR publisher LIKE 
                                "%Space Physics Data Facility")"""
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
        # prints counts for all records
        FormatPrint(self.totalStmt, "total")

        rows = execution(self.authorStmt)
        print("There are " + str(rows[0]) + " records with at least one author")

        rows = execution(self.pubStmt)
        print("There are " + str(rows[0]) + " records with a publisher")

        rows = execution(self.pubYrStmt)
        print("There are " + str(rows[0]) + " records with a publication year")

        rows = execution(self.datasetStmt)
        print("There are " + str(rows[0]) + " records with a dataset")

        rows = execution(self.licenseStmt)
        print("There are " + str(rows[0]) + " records with a license")

        rows = execution(self.urlStmt)
        print("There are " + str(rows[0]) + " records with a url")

        rows = execution(self.NASAurlStmt)
        print("There are " + str(rows[0]) + " records with a NASA url")

        rows = execution(self.PIStmt)
        print("There are " + str(rows[0]) + " records with a persistent identifier")

        rows = execution(self.descStmt)
        print("There are " + str(rows[0]) + " records with a description")

        rows = execution(self.citationStmt)
        print("There are " + str(rows[0]) + " records with citation info")

        rows = execution(self.complianceStmt)
        print("There are " + str(rows[0]) + " records that meet DCAT-US3 compliance")

        rows = execution(self.AL1Stmt)
        print("There are " + str(rows[0]) + " records that have at least one desired field")

        rows = execution(self.AL2Stmt)
        print("There are " + str(rows[0]) + " records that have at least two desired fields")

        rows = execution(self.AL3Stmt)
        print("There are " + str(rows[0]) + " records that have at least three desired fields")

        rows = execution(self.allStmt)
        print("There are " + str(rows[0]) + " records that have all desired fields")

    def SDAC_Records(self):
        # prints counts for SDAC records only    
        rows = execution(self.SDACStmt)
        print("There are " + str(rows[0]) + " records published by SDAC")

        rows = execution(self.SDACauthor)
            #print(row) in case need links
        print("There are " + str(rows[0]) + " records with at least one author published by SDAC")

        rows = execution(self.SDACpubStmt)
        print("There are " + str(rows[0]) + " records with a publisher published by SDAC")

        rows = execution(self.SDACpubYrStmt)
        print("There are " + str(rows[0]) + " records with a publication year published by SDAC")
        
        rows = execution(self.SDACdatasetStmt)
        print("There are " + str(rows[0]) + " records with a dataset published by SDAC")

        rows = execution(self.SDAClicenseStmt)
        print("There are " + str(rows[0]) + " records with a license published by SDAC")

        rows = execution(self.SDACurlStmt)
        print("There are " + str(rows[0]) + " records with a url published by SDAC")

        rows = execution(self.SDACNASAurlStmt)
        print("There are " + str(rows[0]) + " records with a NASA url published by SDAC")

        rows = execution(self.SDACPIStmt)
        print("There are " + str(rows[0]) + " records with a persistent identifier published by SDAC")

        rows = execution(self.SDACdescStmt)
        print("There are " + str(rows[0]) + " records with a description published by SDAC")

        rows = execution(self.SDACcitationStmt)
        print("There are " + str(rows[0]) + " records with citation info published by SDAC")

        rows = execution(self.SDACcomplianceStmt)
        print("There are " + str(rows[0]) + " records that meet DCAT-US3 compliance published by SDAC")

        rows = execution(self.SDAC_AL1)
        print("There are " + str(rows[0]) + " records with at least one desired field published by SDAC")

        rows = execution(self.SDAC_AL2)
        print("There are " + str(rows[0]) + " records with at least two desired fields published by SDAC")

        rows = execution(self.SDAC_AL3)
        print("There are " + str(rows[0]) + " records with at least three desired fields published by SDAC")

    def SPDF_Records(self):
        # prints counts of SPDF records only
        rows = execution(self.SPDFStmt)
        print("There are " + str(rows[0]) + " records published by SPDF")

        rows = execution(self.SPDFauthor)
        print("There are " + str(rows[0]) + " records with at least one author published by SPDF")

        rows = execution(self.SPDFpubStmt)
        print("There are " + str(rows[0]) + " records with a publisher published by SPDF")

        rows = execution(self.SPDFpubYrStmt)
        print("There are " + str(rows[0]) + " records with a publication year published by SPDF")

        rows = execution(self.SPDFdatasetStmt)
        print("There are " + str(rows[0]) + " records with a dataset published by SPDF")

        rows = execution(self.SPDFlicenseStmt)
        print("There are " + str(rows[0]) + " records with a license published by SPDF")

        rows = execution(self.SPDFurlStmt)
        print("There are " + str(rows[0]) + " records with a url published by SPDF")

        rows = execution(self.SPDFNASAurlStmt)
        print("There are " + str(rows[0]) + " records with a NASA url published by SPDF")

        rows = execution(self.SPDFPIStmt)
        print("There are " + str(rows[0]) + " records with a persistent identifier published by SPDF")

        rows = execution(self.SPDFdescStmt)
        print("There are " + str(rows[0]) + " records with a description published by SPDF")

        rows = execution(self.SPDFcitationStmt)
        print("There are " + str(rows[0]) + " records with citation info published by SPDF")

        rows = execution(self.SPDFcomplianceStmt)
        print("There are " + str(rows[0]) + " records that meet DCAT-US3 compliance published by SPDF")

        rows = execution(self.SPDF_AL1)
        print("There are " + str(rows[0]) + " records with at least one desired field published by SPDF")

        rows = execution(self.SPDF_AL2)
        print("There are " + str(rows[0]) + " records with at least two desired fields published by SPDF")

        rows = execution(self.SPDF_AL3)
        print("There are " + str(rows[0]) + " records with at least three desired fields published by SPDF")
            
class Links(Counts):
    def FormatPrint(stmt, criteria):
        rows = execution(stmt)
        print("The records " + criteria + " are:")
        print(rows[:10])
    
    # can pass any stmt to CountRemover to return records instead of counts of records
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
        # prints links for all records
        rows = execution(self.totalStmt)
        print("Total records are : ")
        print(rows[:10])
        
        rows = execution(self.authorStmt)
        print("Total records with at least one author are : ")
        print(rows[:10])

        rows = execution(self.pubStmt)
        print("Total records with a publisher are : ")
        print(rows[:10])

        rows = execution(self.pubYrStmt)
        print("Total records with a publication year are : ")
        print(rows[:10])

        rows = execution(self.datasetStmt)
        print("Total records with a dataset are : ")
        print(rows[:10])

        rows = execution(self.licenseStmt)
        print("Total records with a license are : ")
        print(rows[:10])

        rows = execution(self.urlStmt)
        print("Total records with a url are : ")
        print(rows[:10])

        rows = execution(self.NASAurlStmt)
        print("Total records with a NASA url are : ")
        print(rows[:10])

        rows = execution(self.PIStmt)
        print("Total records with a persistent identifier are : ")
        print(rows[:10])

        rows = execution(self.descStmt)
        print("Total records with a description are : ")
        print(rows[:10])

        rows = execution(self.citationStmt)
        print("Total records with enough metadata for a citation are : ")
        print(rows[:10])

        rows = execution(self.complianceStmt)
        print("Total records the meet the DCAT-US3 compliance are : ")
        print(rows[:10])

        rows = execution(self.AL1Stmt)
        print("Total records that have at least one desired field are : ")
        print(rows[:10])

        rows = execution(self.AL2Stmt)
        print("Total records that have at least two desired fields are : ")
        print(rows[:10])
        
        rows = execution(self.AL3Stmt)
        print("Total records that have at least three desired fields are : ")
        print(rows[:10])

        rows = execution(self.allStmt)
        print("Total records that have all desired fields are : ")
        print(rows[:10])

    def SDAC_Records(self):
        # prints links for SDAC records only    
        rows = execution(self.SDACStmt)
        print("Total records that are published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACauthor)
        print("Total records with at least one author published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACpubStmt)
        print("Total records with a publisher published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACpubYrStmt)
        print("Total records with a publication year published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACdatasetStmt)
        print("Total records with a dataset published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDAClicenseStmt)
        print("Total records with a license published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACurlStmt)
        print("Total records with a url published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACNASAurlStmt)
        print("Total records with a NASA url published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACPIStmt)
        print("Total records with a persistent identifier published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACdescStmt)
        print("Total records with a description published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACcitationStmt)
        print("Total records that have enough data for a citation and are published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDACcomplianceStmt)
        print("Total records that meet DCAT-US3 compliance published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDAC_AL1)
        print("Total records that have at least one desired field published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDAC_AL2)
        print("Total records that have at least two desired fields published by SDAC are : ")
        print(rows[:10])

        rows = execution(self.SDAC_AL3)
        print("Total records that have at least three desired fields published by SDAC are : ")
        print(rows[:10])

    def SPDF_Records(self):
        # prints links of SPDF records only
        rows = execution(self.SPDFStmt)
        print("Total records that are published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFauthor)
        print("Total records with at least one author published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFpubStmt)
        print("Total records with a publisher published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFpubYrStmt)
        print("Total records with a publication year published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFdatasetStmt)
        print("Total records with a dataset published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFlicenseStmt)
        print("Total records with a license published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFurlStmt)
        print("Total records with a url published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFNASAurlStmt)
        print("Total records with a NASA url published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFPIStmt)
        print("Total records with a persistent identifier published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFdescStmt)
        print("Total records with a description published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFcitationStmt)
        print("Total records that have enough data for a citation and are published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDFcomplianceStmt)
        print("Total records that meet DCAT-US3 compliance published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDF_AL1)
        print("Total records that have at least one desired field published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDF_AL2)
        print("Total records that have at least two desired fields published by SPDF are : ")
        print(rows[:10])

        rows = execution(self.SPDF_AL3)
        print("Total records that have at least three desired fields published by SPDF are : ")
        print(rows[:10])