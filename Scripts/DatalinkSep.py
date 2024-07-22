def AccessRightsSep(AccessRights, printFlag):
    """
    Iterates through the licenses in the dictionary given
    as a parameter. For each license, it further iterates
    through the keys which are the URLs found by the
    scraper. Each NASA URL is assigned to the new list to
    be returned, and if it has a product key, this is
    assigned to another list to be returned.

    :param AccessRights: A dictionary to be split by license,
                            url(s), and product key(s).
    :type records: dictionary
    :param printFlag: A boolean determining if extra context is printed.
    :type printFlag: boolean
    :return license: A string of the license associated with the
                        url and product keys.
    :type license: String
    :return url: A list of the considered URLs found by the scraper.
                    If no NASA URLs are found, the string "No NASA Links"
                    is assigned as the only value to the list. If a
                    NASA URL is found, the non-NASA URLs are replaced
                    in the list with empty strings.
    :type url: list
    :return prodKey: A list of the product keys found by the scraper.
                        If no NASA URLs are found, an empty string is
                        assigned as the only value to the list. If a NASA URL
                        is found, the script then finds those NASA URLs
                        which have product keys. Those that have one have
                        the keys assigned to the list. Those that do not
                        have an empty string assigned to the list.
    :type prodKey: list
    """

    url = []
    prodKey = []
    license = ""
    desired = False

    for k, v in AccessRights.items():
        # check if no URLs returned for any access
        if not v:
            continue
        else:
            license = k
            # check if any urls are for consideration
            for link in v.keys():
                if ("nasa.gov" or "virtualsolar.org") in link:
                    desired = True
            for key, val in v.items():
                # allow desired URLs to be added to database
                if ("nasa.gov" or "virtualsolar.org") in key:
                    url.append(str(key))
                    if str(val) == "[]":
                        prodKey.append("")
                    else:
                        # fix format of product key for easier
                        #     handling in datalink checker
                        pKey = str(val).replace("[\'", "")
                        pKey = pKey.replace("\']", "")
                        prodKey.append(pKey)
                else:
                    # if no desired URLs present
                    if not desired:
                        # lines.append(RID + ", " + str(key))
                        url = ["No NASA Links"]
                        prodKey = [""]

            if printFlag:
                print(k + " was assigned to license")
                print(str(url) + " was assigned to url")
                print(str(prodKey) + " was assigned to prodKey")

    return license, url, prodKey
