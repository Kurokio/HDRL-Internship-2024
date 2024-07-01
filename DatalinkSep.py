# add docstring

def AccessRightsSep(AccessRights, printFlag):
    url = []
    prodKey= []
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
                        # fix format of product key for easier handling in datalink checker
                        pKey = str(val).replace("[\'","")
                        pKey = pKey.replace("\']","")
                        prodKey.append(pKey)
                else:
                    # if no desired URLs present
                    if not desired:
                        url = ["No NASA Links"]
                        prodKey = [""]
                        
            if printFlag:
                print(k + " was assigned to license")
                print(str(url) + " was assigned to url")
                print(str(prodKey) + " was assigned to prodKey")
    return license, url, prodKey