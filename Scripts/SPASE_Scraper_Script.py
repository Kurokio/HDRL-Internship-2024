"""If additional fields were to be extracted, such as ORCID id,
author institution, etc., all one needs to do is navigate to
the for loop that iterates through the children of the desired
child of the root (NumericalData/DisplayData -> ResourceHeader -> Contact)
for example, and then add an elif statement to check the child.tags
for the desired field. Once that tag is found, store its text into a
variable to be returned by the function. If there are multiple authors,
make the return variables lists and append the role values to the
roles list in the same order you append the author names to their list.
Further edits would need to be made to the main.py file, create_tables
and add_TableName methods in SQLiteFun, and the SQLite query
statements in the QueryPrinter file to address the new return."""

import xml.etree.ElementTree as ET
import os
from datetime import datetime


def SPASE_Scraper(path):
    """Takes path of a .xml SPASE record file and returns a
    tuple of values of varying types which hold all desired
    metadata and the fields they came from. This will collect
    the desired metadata following the priority rules determined
    by SPASE record experts. If any desired metadata is not
    found, the default value assigned is an empty string.

    :param path: A string of the absolute/relative path of the
                    SPASE record to be scraped.
    :type path: String
    :return: A tuple containing the metadata desired and where
                they were obtained.
    :rtype: tuple
    """

    # establish path of XML file
    # print("Scraping " + path)
    if os.path.isfile(path) and path.endswith(".xml"):
        file_size_bytes = os.path.getsize(path)
        file_size = file_size_bytes/(1024*1024*1024)
        if file_size >= 0.5:
            print(f"File size is: {file_size:.2f} GB")
        # root[1] = NumericalData or DisplayData
        # root = Spase
        tree = ET.parse(path)
        root = tree.getroot()
    else:
        print(path + " is not a file or not an xml file")

    # collect version number
    version = root[0].text

    # iterate thru NumericalData/DisplayData to obtain ResourceID
    #    and locate ResourceHeader
    for child in root[1]:
        if child.tag.endswith("ResourceID"):
            # collect ResourceID
            ResourceID = child.text
            # use partition to get just the NumericalData/DisplayData text
            before, sep, after = root[1].tag.partition("}")
            parent, sep, after = after.partition("'")
            # record field where ResourceID was collected
            ResourceIDField = (parent + "/ResourceID")
        elif child.tag.endswith("ResourceHeader"):
            targetChild = child

    # obtain Author, Publication Date, Publisher, Persistent Identifier,
    #    Description, ReleaseDate, and Dataset Name

    # define vars
    # Code A can go here
    access = ""
    author = []
    authorField = ""
    authorRole = []
    pubDate = ""
    pubDateField = ""
    pub = ""
    pubField = ""
    datasetName = ""
    datasetNameField = ""
    desc = ""
    descField = ""
    PID = ""
    PIDField = ""
    licenseField = ""
    datalinkField = ""
    ReleaseDates = []
    ReleaseDate = ""
    PI_Child = None
    priority = False

    # holds role values that are not considered for author var
    UnapprovedAuthors = ["MetadataContact", "ArchiveSpecialist",
                         "HostContact", "Publisher", "User"]

    # iterate thru ResourceHeader
    for child in targetChild:
        # find backup Dataset Name
        if child.tag.endswith("ResourceName"):
            targetChild = child
            datasetName = child.text
            # record field where dataset was collected
            datasetNameField = (parent + "/ResourceHeader/ResourceName")
        # find ReleaseDate
        elif child.tag.endswith("ReleaseDate"):
            date, sep, time = child.text.partition("T")
            if "Z" in child.text:
                time = time.replace("Z", "")
            if "." in child.text:
                time, sep, after = time.partition(".")
            dt_string = date + " " + time
            dt_obj = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
            ReleaseDates.append(dt_obj)
        elif child.tag.endswith("RevisionHistory"):
            RHChild = child
            for child in RHChild:
                REChild = child
                for child in REChild:
                    if child.tag.endswith("ReleaseDate"):
                        date, sep, time = child.text.partition("T")
                        if "Z" in child.text:
                            time = time.replace("Z", "")
                        if "." in child.text:
                            time, sep, after = time.partition(".")
                        dt_string = date + " " + time
                        dt_obj = datetime.strptime(dt_string,
                                                   "%Y-%m-%d %H:%M:%S")
                        ReleaseDates.append(dt_obj)
        # find Description
        elif child.tag.endswith("Description"):
            desc = child.text
            descField = (parent + "/ResourceHeader/Description")
        # find Persistent Identifier
        elif child.tag.endswith("DOI"):
            PID = child.text
            PIDField = (parent + "/DOI")
        # find Publication Info
        elif child.tag.endswith("PublicationInfo"):
            PI_Child = child
        # find Contact
        elif child.tag.endswith("Contact"):
            C_Child = child
            # iterate thru Contact to find PersonID and Role
            for child in C_Child:
                # find PersonID
                if child.tag.endswith("PersonID"):
                    # store PersonID
                    PersonID = child.text

                # Code B can go here

                # find Role
                elif child.tag.endswith("Role"):
                    # backup author
                    if ("PrincipalInvestigator" or "PI") in child.text:
                        # if a lesser priority author found
                        #     first, overwrite author lists
                        if not priority and author:
                            author = [PersonID]
                            authorRole = [child.text]
                            # Code C here
                        else:
                            author.append(PersonID)
                            authorRole.append(child.text)
                            # Code D here
                        # record field where author was collected
                        authorField = (parent +
                                       "/ResourceHeader/Contact/PersonID")
                        # mark that highest priority backup author was found
                        priority = True
                    # backup publisher
                    elif child.text == "Publisher":
                        pub = child.text
                        # record field where publisher was collected
                        pubField = (parent +
                                    "/ResourceHeader/Contact/PersonID")
                    # backup author
                    elif child.text not in UnapprovedAuthors:
                        # checks if higher priority author (PI)
                        #    was added first
                        if not priority:
                            author.append(PersonID)
                            authorRole.append(child.text)
                            # Code D here
                            # record field where author was collected
                            authorField = (parent +
                                           "/ResourceHeader/Contact/PersonID")

    # access Publication Info
    if PI_Child is not None:
        for child in PI_Child:
            # collect preferred author
            if child.tag.endswith("Authors"):
                author = [child.text]
                # record field where author was collected
                authorField = (parent + "/PublicationInfo/Authors")
                authorRole = []
            elif child.tag.endswith("PublicationDate"):
                pubDate = child.text
                pubDateField = (parent + "/PublicationInfo/PublicationDate")
            # collect preferred publisher
            elif child.tag.endswith("PublishedBy"):
                pub = child.text
                # record field where pub was collected
                pubField = (parent + "/PublicationInfo/PublishedBy")
            # collect preferred dataset
            elif child.tag.endswith("Title"):
                dataset = child.text
                # record field where dataset was collected
                datasetField = (parent + "/PublicationInfo/Title")

    # obtain data links and license

    # dictionaries labled by the Access Rights which will
    #     store all URLs and their Product Keys if given
    AccessRights = {}
    AccessRights["Open"] = {}
    AccessRights["PartRest"] = {}
    AccessRights["Rest"] = {}
    AccessRights["None"] = {}

    # iterate thru children to locate Access Information
    for child in root[1]:
        if child.tag.endswith("AccessInformation"):
            targetChild = child
            # iterate thru children to locate AccessURL,
            #    AccessRights, and RepositoryID
            for child in targetChild:
                if child.tag.endswith("AccessRights"):
                    access = child.text
                    licenseField = (parent +
                                    "/AccessInformation/AccessRights")
                elif child.tag.endswith("AccessURL"):
                    targetChild = child
                    # iterate thru children to locate URL
                    for child in targetChild:
                        if child.tag.endswith("URL"):
                            url = child.text
                            datalinkField = (parent +
                                             """/AccessInformation/
                                             AccessURL/URL""")
                            # provide "NULL" value in case no keys are found
                            if access == "Open":
                                AccessRights["Open"][url] = []
                            elif access == "PartiallyRestricted":
                                AccessRights["PartRest"][url] = []
                            elif access == "Restricted":
                                AccessRights["Rest"][url] = []
                            else:
                                AccessRights["None"][url] = []
                        # check if URL has a product key
                        elif child.tag.endswith("ProductKey"):
                            prodKey = child.text
                            if access == "Open":
                                # if only one prodKey exists
                                if AccessRights["Open"][url] == []:
                                    AccessRights["Open"][url] = [prodKey]
                                # if multiple prodKeys exist
                                else:
                                    AccessRights["Open"][url] += [prodKey]
                            elif access == "PartiallyRestricted":
                                if (AccessRights["PartRest"][url] == []):
                                    AccessRights["PartRest"][url] = [prodKey]
                                else:
                                    AccessRights["PartRest"][url] += [prodKey]
                            elif access == "Restricted":
                                if AccessRights["Rest"][url] == []:
                                    AccessRights["Rest"][url] = [prodKey]
                                else:
                                    AccessRights["Rest"][url] += [prodKey]
                            else:
                                if AccessRights["None"][url] == []:
                                    AccessRights["None"][url] = [prodKey]
                                else:
                                    AccessRights["None"][url] += [prodKey]
                # find backup Publisher if needed
                elif pub == "":
                    if child.tag.endswith("RepositoryID"):
                        # use partition to split text by Repository/
                        #    and assign only the text after it to pub
                        (before, sep, after) = child.text.partition("Repo" +
                                                                    "sitory/")
                        pub = after
                        # record field where publisher was collected
                        pubField = (parent + "/AccessInformation/RepositoryID")
                # continue to check for additional AccessURLs
                continue
        # continue to check for additional Access Informations
        continue

    # find latest date
    ReleaseDate = str(ReleaseDates[0])
    if len(ReleaseDates) > 1:
        for i in range(1, len(ReleaseDates)):
            try:
                (ReleaseDates[i] < ReleaseDates[i+1])
            except IndexError:
                if ReleaseDates[i] > ReleaseDates[0]:
                    ReleaseDate = str(ReleaseDates[i])
            else:
                continue

    # return stmt
    return (ResourceID, ResourceIDField, author, authorField, authorRole,
            pub, pubField, pubDate, pubDateField, datasetName,
            datasetNameField, desc, descField, PID, PIDField,
            AccessRights, licenseField, datalinkField, version, ReleaseDate)
