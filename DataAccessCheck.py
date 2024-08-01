from datetime import datetime, timedelta
from Scripts.SQLiteFun import execution, executionALL
from hapiclient.util import HAPIError
from hapiclient import hapi
from time import sleep
import multiprocessing

# check and see if data can be retrieved from HAPI server
def HAPIChecker(server, dataset, start, stop, parameters, return_dict):
    # control loops
    dataFound = False
    broken = False
    
    try:
        data, meta = hapi(server, dataset, parameters, start, stop)

    # if error(s) arise
    except HAPIError as err:
        print("HAPIError caught: " + err) 
        broken = True

    # if no error arises
    else:
        # search for data
        while not dataFound:
            if len(data) == 0:
                pass
            else:
                if str(data[0][0]) != "":
                    dataFound = True
                    #print("Example data looks like " + str(data[0][0]))
    finally:
        return_dict["dataFound"] = dataFound
        return_dict["attempts"] += 1
        return_dict["broken"] = broken
        return None

# checks all HAPI links in db to see if they provide data
def DataChecker(prodKeys, conn):
    print("The datasets are " + str(prodKeys))
    lines = []

    # iterate thru prodKeys to assign as dataset
    for prodKey in prodKeys:
        # check if multiple prodKeys for same URL (mult keys in one string)
        if ", " in prodKey:
            print("This HAPI URL has multiple product keys.")
            index = prodKeys.index(prodKey)
            prodKey = prodKey.replace("\'", "")
            # keep separating them until each prodKey is in own string
            # while ", " in prodKey:
            before, sep, after = prodKey.partition(", ")
            prodKeys[index] = before # remove this line if need to check all keys
            #prodKeys[index] = after
            #prodKeys.insert(index, before)
            prodKey = prodKeys[index]
        dataset = str(prodKey)

        # creating multiprocessing manager and dictionary to hold returns from subprocess
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        # count of attempts to get data
        return_dict["attempts"] = 0
        # hold control loop vars
        return_dict["dataFound"] = False
        return_dict["broken"] = False
        tooLong = False

        # list that holds all parameters for a given server
        paramNames =  []
        # initialize HAPI server
        server = 'https://cdaweb.gsfc.nasa.gov/hapi'

        # retrieve all parameters and the start date from the server
        sleep(5.0)
        try:
            meta = hapi(server,dataset)
        # catches errors like 'unknown dataset id'
        except HAPIError as e:
            print("Caught HAPIError for dataset " + dataset)
            print(" ", e)
            # record error message saying the dataset failed at info check stage
            errMessage = "HAPI info check failed"
            HAPIErrorStmt = f""" UPDATE TestResults
                                    SET Errors = '{errMessage}'
                                    FROM (SELECT SPASE_id, prodKey FROM TestResults
                                            INNER JOIN MetadataEntries USING (SPASE_id))
                                    WHERE prodKey = '{dataset}' """
            Record_id = execution(f""" SELECT rowNum 
                                    FROM (SELECT TestResults.rowNum, SPASE_id, prodKey FROM TestResults 
                                        INNER JOIN MetadataEntries USING (SPASE_id))
                                    WHERE prodKey = '{dataset}';""", conn)
            executionALL(HAPIErrorStmt, conn)
            print(f"Sent error message to a TestResults entry with the row number {Record_id}")
            continue
        else:
            # get parameters
            for k, v in meta.items():
                if k == "parameters":
                    for params in v:
                        for key, value in params.items():
                            if key == "name":
                                paramNames.append(value)
                # get start date
                elif k == "startDate":
                    start = v

            #print(paramNames)

            # dictionary that holds datetime obj for each interval
            intervals = {}
            intervals["1s"] = ""
            intervals["10s"] = ""
            intervals["1min"] = ""
            intervals["10min"] = ""
            intervals["1hr"] = ""
            intervals["1d"] = ""
            intervals["3d"] = ""
            intervals["1w"] = ""
            intervals["1mon"] = ""

            # create incremental intervals to test for data check
            date, sep, time = start.partition("T")
            time = time.replace("Z", "")
            dt_string = date + " " + time
            dt_obj = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
            # 1 second
            intervals["1s"] = dt_obj + timedelta(seconds=1)
            # 10 seconds
            intervals["10s"] = dt_obj + timedelta(seconds=10)
            # 1 minute
            intervals["1min"] = dt_obj + timedelta(minutes=1)
            # 10 minutes
            intervals["10min"] = dt_obj + timedelta(minutes=10)
            # hour
            intervals["1hr"] = dt_obj + timedelta(hours=1)
            # day
            intervals["1d"] = dt_obj + timedelta(days=1)
            # 3 days
            intervals["3d"] = dt_obj + timedelta(days=3)
            # week
            intervals["1w"] = dt_obj + timedelta(weeks=1)
            # month
            intervals["1mon"] = dt_obj + timedelta(weeks=4,days=2)

            # to iterate thru all parameters in a server, use the for loop below to enclose the code below
            # for parameters in paramNames[1:]:
            # only check Time parameter
            parameter = paramNames[0]
            # have data check occur in increasingly larger start->stop intervals until data is returned
            for k, v in intervals.items():
                # while data is not found and the data link is not broken
                if (not return_dict["dataFound"]) and (not return_dict["broken"]):
                    # assign interval to stop time
                    stop = str(v)
                    stop = stop.replace(" ", "T") + "Z"
                    print("Checking parameter " + str(parameter) + " in HAPI record with id " + dataset +
                          " at the interval of " + str(k))
                    # start HAPIChecker as a subprocess
                    p = multiprocessing.Process(target=HAPIChecker, args=(server, dataset, start, stop,
                                                      parameter, return_dict))
                    sleep(5.0)
                    p.start()
                    p.join(10)
                    # if still running after 10 seconds -> end it
                    if p.is_alive():
                        print(f"Data retrieval took more than 10 seconds for dataset." +
                              " Skipping this dataset and trying the next.")
                        p.terminate()
                        #return_dict["attempts"] += 1
                        # add to list holding datasets that take too long
                        lines.append(dataset)
                        tooLong = True
                        break
                    if return_dict["dataFound"]:
                        print("Data was successfully accessed")
            # if all intervals fail or link is broken -> no data
            if (not return_dict["dataFound"]) and (not tooLong):
            # inputs error message into TestResults table
                print("No data was found")
                errMessage = "HAPI info check passed after 1 attempt. HAPI data check"
                errMessage += f" failed after {return_dict['attempts']} attempt(s)."
                HAPIErrorStmt = f""" UPDATE TestResults
                                        SET Errors = '{errMessage}'
                                        FROM (SELECT SPASE_id, prodKey FROM TestResults
                                                INNER JOIN MetadataEntries USING (SPASE_id))
                                        WHERE prodKey = '{dataset}' """
                Record_id = execution(f""" SELECT rowNum 
                                        FROM (SELECT TestResults.rowNum, SPASE_id, prodKey FROM TestResults 
                                            INNER JOIN MetadataEntries USING (SPASE_id))
                                        WHERE prodKey = '{dataset}';""", conn)
                executionALL(HAPIErrorStmt, conn)
                print(f"Sent error message to a TestResults entry with the row number {Record_id}")
    return lines
