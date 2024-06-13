import os

# list that holds abs paths of SPASE records to scrape
paths = []

def getPaths(entry):
    if os.path.exists(entry):
        for root, dirs, files in os.walk(entry):
            if files:
                for file in files:
                    paths.append(root + "/" + file)
    else:
        print(entry + " does not exist")
    return paths