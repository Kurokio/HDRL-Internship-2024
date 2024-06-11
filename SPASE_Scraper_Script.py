def SPASE_Scraper(path):
    """Takes path of a .xml SPASE record file and returns a tuple of values of varying types which hold all 
    desired metadata. This will collect the desired metadata following the priority rules determined by SPASE 
    record experts. If any desired metadata is not found, the default value assigned is NULL.
    
    :param path: A string of the absolute/relative path of the SPASE record to be scraped.
    :type path: String
    :return: A tuple containing the metadata desired.
    :rtype: tuple
    """
    
    import xml.etree.ElementTree as ET
    import os
    
    # establish path of XML file
    print("You entered " + path)
    if os.path.isfile(path) and path.endswith(".xml"):
        file_size_bytes = os.path.getsize(path)
        file_size = file_size_bytes/(1024*1024*1024)
        print(f"File size is: {file_size:.2f} GB")
        # root[1] = NumericalData
        # root = Spase
        tree = ET.parse(path)
        root = tree.getroot()
    else:
        print(path + " is not a file or not an xml file")
        
    
    # iterate thru NumericalData/DisplayData to obtain ResourceID and locate ResourceHeader
    for child in root[1]:
        if child.tag.endswith("ResourceID"):
            RID = child.text
        elif child.tag.endswith("ResourceHeader"):
            targetChild = child

    # obtain Author, Publication Date, Publisher, Persistent Identifier, and Dataset Name

    # define vars
    author="" 
    pubDate=""
    pub = ""
    dataset = ""
    PI = ""
    # holds role values that are not considered for author var
    UnapprovedAuthors = ["MetadataContact", "ArchiveSpecialist", "HostContact", "Publisher", "User"]

    # iterate thru ResourceHeader
    for child in targetChild:
        # find backup Dataset Name
        if child.tag.endswith("ResourceName"):
            targetChild = child
            dataset = child.text
        # find Persistent Identifier
        elif child.tag.endswith("DOI"):
            PI = child.text
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
                    # store PID
                    PID = child.text
                # find Role
                elif child.tag.endswith("Role"):
                    # backup author
                    if child.text == ("PrincipalInvestigator" or "PI"):
                        author = PID
                    # backup publisher
                    elif child.text == "Publisher":
                        pub = child.text
                    # backup author
                    elif child.text not in UnapprovedAuthors:
                        author = PID

    # access Publication Info
    for child in PI_Child:
        if child.tag.endswith("Authors"):
            author = child.text
        elif child.tag.endswith("PublicationDate"):
            pubDate = child.text
        elif child.tag.endswith("PublishedBy"):
            pub = child.text
        elif child.tag.endswith("Title"):
            dataset = child.text
    
    
    # obtain data links and license

    # dictionaries labled by the Access Rights which will store all URLs and their Product Keys if given
    AccessRights = {}
    AccessRights["Open"] = {}
    AccessRights["PartiallyRestricted"] = {}
    AccessRights["Restricted"] = {}

    # iterate thru children to locate Access Information
    for child in root[1]:
        if child.tag.endswith("AccessInformation"):
            targetChild = child
            # iterate thru children to locate AccessURL, AccessRights, and RepositoryID
            for child in targetChild:
                if child.tag.endswith("AccessRights"):
                    access = child.text
                elif child.tag.endswith("AccessURL"):
                    targetChild = child
                    # iterate thru children to locate URL
                    for child in targetChild:
                        if child.tag.endswith("URL"):
                            # check if url is one for consideration
                            if ("nasa.gov" or "virtualsolar.org") in child.text:
                                url = child.text
                                # provide "NULL" value if no keys are found
                                if access == "Open":
                                    AccessRights["Open"][url] = []
                                elif access == "PartiallyRestricted":
                                    AccessRights["PartiallyRestricted"][url] = []
                                else:
                                    AccessRights["Restricted"][url] = []
                            else:
                                break
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
                                if AccessRights["PartiallyRestricted"][url] == []:
                                    AccessRights["PartiallyRestricted"][url] = prodKey
                                else:
                                    AccessRights["PartiallyRestricted"][url] += [prodKey]
                            else:
                                if AccessRights["Restricted"][url] == []:
                                    AccessRights["Restricted"][url] = prodKey
                                else:
                                    AccessRights["Restricted"][url] += [prodKey]
                # find backup Publisher if needed
                elif pub == "":
                    if child.tag.endswith("RepositoryID"):
                        # use partition to split text by Repository/ and assign only the text after it to pub 
                        before, sep, after = child.text.partition("Repository/")
                        pub = after
                # continue to check for additional AccessURLs            
                continue
        # continue to check for additional Access Informations
        continue
        
    #pubYear = pubDate[0:4]
    
    # return stmt
    return RID, author, pub, pubDate, dataset, PI, AccessRights