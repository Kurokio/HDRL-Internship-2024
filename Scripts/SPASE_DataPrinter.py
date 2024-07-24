from .SPASE_Scraper_Script import SPASE_Scraper
from .DatalinkSep import AccessRightsSep


# prints metadata for fields specified in the record provided
def MetadataPrinter(record, fields=["ResourceID", "author", "pub",
                                    "pubYr", "datasetName", "description",
                                    "PID", "license", "url", "prodKey",
                                    "version", "ReleaseDate"]):
    """
    Takes a SPASE id and fields desired and prints the values for
    these fields found by the scraping function. The default
    argument for fields is everything returned by the
    SPASE_Scraper_Script. This function is especially helpful in
    acquiring the description for a given SPASE record, as the
    actual value for this field is not stored in the database.

    :param record: SPASE_id of the SPASE record the user wants data for
    :type record: String
    :param fields: A list of all the metadata fields' data the user
                    wishes to be printed
    :type fields: list
    :return: None
    """

    # make recordPath from SPASE_id provided
    before, sep, after = record.partition('/')
    recordPath = '/home/jovyan' + after + '.xml'

    # retrieve metadata
    (ResourceID, ResourceIDField, author, authorField, authorRole,
     pub, pubField, pubDate, pubDateField, datasetName, datasetNameField,
     desc, descField, PID, PIDField, AccessRights, licenseField,
     datalinkField, version, ReleaseDate) = SPASE_Scraper(recordPath)

    # grab only year from the date
    pubYear = pubDate[0:4]
    # concatenate author and authorRole into single strings
    author = ", ".join(author)
    authorRole = ", ".join(authorRole)

    # separate license, url, and product keys from AccessRights
    license, url, prodKey = AccessRightsSep(AccessRights, False)

    # print values of metadata specified in fields list provided
    # give each print out a label showing what is being printed
    for field in fields:
        if field == "ResourceID":
            print("The ResourceID is " + ResourceID +
                  " which was obtained from " + ResourceIDField)
        if field == "author":
            print("The author(s) are " + author + " who are " +
                  authorRole + " which was obtained from " + authorField)
        if field == "pubYr":
            print("The publication year is " + pubYear +
                  " which was obtained from " + pubDateField)
        if field == "pub":
            print("The publisher is " + pub +
                  " which was obtained from " + pubField)
        if field == "datasetName":
            print("The dataset name is " + datasetName +
                  " which was obtained from " + datasetNameField)
        if field == "description":
            print("The description is " + desc +
                  " which was obtained from " + descField)
        if field == "PID":
            print("The persistent identifier is " + PID +
                  " which was obtained from " + PIDField)
        if field == "license":
            print("The license is " + license +
                  " which was obtained from " + licenseField)
        if field == "url":
            print("The URL(s) are " + url +
                  " which was obtained from " + datalinkField)
        if field == "prodKey":
            print("The product key(s) are " + prodKey +
                  " which was obtained from " + datalinkField)
        if field == "version":
            print("The version is " + version)
        if field == "ReleaseDate":
            print("The ReleaseDate is " + ReleaseDate)
