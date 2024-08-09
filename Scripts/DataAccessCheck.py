# STILL TESTING

from datetime import datetime, timedelta
from .SQLiteFun import execution, executionALL
from hapiclient.util import HAPIError
from hapiclient import hapi
from time import sleep
import multiprocessing

# check and see if data can be retrieved from HAPI server


def HAPIChecker(server, dataset, start, stop, parameters, return_dict):
    """
    Calls the hapi function from hapi-client to see if data can be
    downloaded from a dataset according to the specifics given by the
    other parameters. Returns a dictionary which holds values signifying
    if the dataset successfully retrieved data, if the dataset is broken,
    and a number which keeps track of how many times this you have requested
    data for this dataset.

    :param server: The server argument to hapi function
    :type server: String
    :param dataset: The dataset argument to hapi function
    :type dataset: String
    :param start: The start argument to hapi function
    :type start: datetime object
    :param stop: The stop argument to hapi function
    :type stop: datetime object
    :param parameters: The parameters argument to hapi function
    :type parameters: String
    :param return_dict: The dictionary to be returned
    :type return_dict: dictionary
    :return: a dictionary holding data retrieval results and information
    :return type: dictionary
    """
    # control loops
    dataFound = False
    broken = False

    # data request attempt
    try:
        data, meta = hapi(server, dataset, parameters, start, stop)

    # if error(s) arise
    except HAPIError as err:
        print("HAPIError caught: " + err)
        broken = True

    # if no error arises
    else:
        # if data is successfully retrieved
        if len(data) != 0:
            if str(data[0][0]) != "":
                dataFound = True
                # print("Example data looks like " + str(data[0][0]))
    finally:
        return_dict["dataFound"] = dataFound
        return_dict["attempts"] += 1
        return_dict["broken"] = broken
        return None

# checks all HAPI links in db to see if they provide data


def DataChecker(prodKeys, conn):
    """
    Iterates through all datasets/product keys given and
    checks them for data accessibility. Only checks one
    dataset if multiple are found for one SPASE record.
    Also calls the hapi function from hapi-client to see
    if dataset provided is even a recognized dataset.
    Checks are done using incremental stop times until
    data is found or the link is recorded as a failure.
    Results are sent to the dataAccess and Errors columns
    in the TestResults table in the database.

    :param prodKeys: A list of product keys to serve as
                        datasets for the hapi function call
    :type prodKeys: List
    :param conn: A connection object to the sqlite database
    :type conn: Connection object
    :return: a list of the datasets which time out for some
                intervals and fail to retrieve data
    :return type: List
    """
    print("The datasets are " + str(prodKeys))

    # list that holds datasets in need of more testing
    lines = []

    multiKeys = False

    # iterate thru prodKeys to assign as dataset
    for prodKey in prodKeys:
        # check if multiple prodKeys for same URL (mult keys in one string)
        # if want to implement checks for all prodKeys, use the comments
        if ", " in prodKey:
            print("This HAPI URL has multiple product keys.")
            multiKeys = True
            index = prodKeys.index(prodKey)
            prodKey = prodKey.replace("\'", "")
            # keep separating them until each prodKey is in own string
            # while ", " in prodKey:
            before, sep, after = prodKey.partition(", ")
            prodKeys[index] = before  # remove line if need to check all keys
            # prodKeys[index] = after
            # prodKeys.insert(index, before)
            prodKey = prodKeys[index]
        dataset = str(prodKey)

        # creating multiprocessing manager and
        #   dictionary to hold returns from subprocess
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        # count of attempts to get data
        return_dict["attempts"] = 0
        # hold control loop vars
        return_dict["dataFound"] = False
        return_dict["broken"] = False
        # keeps track of if ANY intervals timed out
        tooLong = False

        # list that holds all parameters for a given server
        paramNames = []
        # initialize HAPI server
        server = 'https://cdaweb.gsfc.nasa.gov/hapi'

        # get row number of record updated for visual confirmation later
        if multiKeys:
            Record_id = execution(f""" SELECT rowNum
                                    FROM (SELECT TestResults.rowNum, SPASE_id,
                                        prodKey, url FROM TestResults
                                        INNER JOIN MetadataEntries
                                            USING (SPASE_id))
                                    WHERE prodKey LIKE '{dataset},%' AND
                                        url LIKE '%/hapi';""", conn)
        else:
            Record_id = execution(f""" SELECT rowNum
                                FROM (SELECT TestResults.rowNum, SPASE_id,
                                    prodKey, url FROM TestResults
                                    INNER JOIN MetadataEntries
                                        USING (SPASE_id))
                                WHERE prodKey = '{dataset}' AND
                                    url LIKE '%/hapi';""", conn)

        # get SPASE_id that matches the dataset
        if multiKeys:
            prodKeyStmt = f""" SELECT SPASE_id FROM MetadataEntries
                                WHERE prodKey LIKE '{dataset},%' """
        else:
            prodKeyStmt = f""" SELECT SPASE_id FROM MetadataEntries
                                WHERE prodKey = '{dataset}' """
        SPASE_ID = execution(prodKeyStmt, conn)
        try:
            SPASE_ID = SPASE_ID[0]
        # means that this dataset was not the only one for its record
        except IndexError:
            print("IndexError occured at this dataset.")
            prodKeyStmt = f""" SELECT SPASE_id FROM MetadataEntries
                                WHERE prodKey LIKE '{dataset}%' """
            SPASE_ID = execution(prodKeyStmt, conn)
            SPASE_ID = SPASE_ID[0]
            Record_id = execution(f""" SELECT rowNum
                                    FROM (SELECT TestResults.rowNum, SPASE_id,
                                        prodKey, url FROM TestResults
                                        INNER JOIN MetadataEntries
                                            USING (SPASE_id))
                                    WHERE prodKey LIKE '{dataset}_,%' AND
                                        url LIKE '%/hapi';""", conn)

        # retrieve all parameters and the start date from the server
        sleep(5.0)
        try:
            meta = hapi(server, dataset)
        # catches errors like 'unknown dataset id' and
        #   one that arises when server is overwhelmed
        except HAPIError as e:
            print("Caught HAPIError for dataset " + dataset)
            print(" ", e)
            errMessage = "HAPI info check failed"
            # update Errors and dataAccess value for that record in TestResults
            HAPIErrorStmt = f""" UPDATE TestResults
                                    SET Errors = '{errMessage}'
                                    WHERE SPASE_id = '{SPASE_ID}' """
            executionALL(HAPIErrorStmt, conn)
            HAPIStmt = f""" UPDATE TestResults
                                    SET dataAccess = 'Failed'
                                    WHERE SPASE_id = '{SPASE_ID}' """
            executionALL(HAPIStmt, conn)
            print("Sent error message to a TestResults entry with the " +
                  f"row number {Record_id}")
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

            # print(paramNames)

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
            intervals["1mon"] = dt_obj + timedelta(weeks=4, days=2)

            # to iterate thru all parameters in a server,
            #   use the for loop below to enclose the code below
            # for parameters in paramNames[1:]:
            # only check Time parameter
            parameter = paramNames[0]
            # have data check occur in increasingly larger start->stop
            #   intervals until data is returned
            for k, v in intervals.items():
                # keeps track of if that interval timed out
                tempTooLong = False
                # while data is not found and the data link is not broken
                if ((not return_dict["dataFound"]) and
                   (not return_dict["broken"])):
                    # assign interval to stop time
                    stop = str(v)
                    stop = stop.replace(" ", "T") + "Z"
                    print("Checking parameter " + str(parameter) +
                          " in HAPI record with id " + dataset +
                          " at the interval of " + str(k))
                    # start HAPIChecker as a subprocess
                    p = multiprocessing.Process(target=HAPIChecker,
                                                args=(server, dataset, start,
                                                      stop, parameter,
                                                      return_dict))
                    sleep(5.0)
                    p.start()
                    p.join(10)
                    # if still running after 10 seconds -> end it
                    if p.is_alive():
                        # if not last interval
                        if k != "1mon":
                            print("Data retrieval took more than 10 seconds" +
                                  " for interval. Trying the next.")
                        else:
                            print("Data retrieval took more than 10 seconds" +
                                  " for interval.")
                        p.terminate()
                        return_dict["attempts"] += 1
                        tooLong = True
                        tempTooLong = True
                    elif return_dict["dataFound"]:
                        # if data was returned but some earlier intervals
                        #   timed out
                        if tooLong:
                            amtTimedOut = (9 - return_dict['attempts'])
                            errMessage = "Passed data check after"
                            errMessage += f"{amtTimedOut} intervals timed out."
                            # update Errors and dataAccess value for that
                            #   record in TestResults
                            HAPIErrorStmt = f""" UPDATE TestResults
                                                    SET Errors = '{errMessage}'
                                                    WHERE SPASE_id =
                                                    '{SPASE_ID}' """
                            executionALL(HAPIErrorStmt, conn)
                            HAPIStmt = f""" UPDATE TestResults
                                                SET dataAccess = 'Passed'
                                                WHERE SPASE_id =
                                                '{SPASE_ID}' """
                            executionALL(HAPIStmt, conn)
                            print("Data was successfully accessed after some" +
                                  " intervals timed out")
                            print("Sent success message to a TestResults" +
                                  f"entry with the row number {Record_id}")
                        else:
                            # update dataAccess value for that record
                            #   in TestResults
                            HAPIStmt = f""" UPDATE TestResults
                                                SET dataAccess = 'Passed'
                                                WHERE SPASE_id =
                                                '{SPASE_ID}' """
                            executionALL(HAPIStmt, conn)
                            print("Data was successfully accessed")
            # if no data is returned but some intervals timed out
            if (not return_dict["dataFound"]) and tooLong:
                # update dataAccess and Errors value for that record
                #   in TestResults
                HAPIStmt = f""" UPDATE TestResults
                                    SET dataAccess = 'Failed'
                                    WHERE SPASE_id = '{SPASE_ID}' """
                executionALL(HAPIStmt, conn)
                errMessage = f"Failed data check but some intervals timed out."
                HAPIErrorStmt = f""" UPDATE TestResults
                                        SET Errors = '{errMessage}'
                                        WHERE SPASE_id = '{SPASE_ID}' """
                executionALL(HAPIErrorStmt, conn)
                print("No data was found but some intervals timed out")
                print("Sent failure message to a TestResults entry with the" +
                      f"row number {Record_id}")
                # add to list holding datasets that take too long
                lines.append(dataset)
            # if all intervals fail or link is broken -> no data
            elif (not return_dict["dataFound"]) and (not tooLong):
                print("No data was found.")
                # update Errors and dataAccess value for that record
                #   in TestResults
                errMessage = "HAPI data check failed after "
                errMessage += f"{return_dict['attempts']} attempt(s)."
                HAPIErrorStmt = f""" UPDATE TestResults
                                        SET Errors = '{errMessage}'
                                        WHERE SPASE_id = '{SPASE_ID}' """
                executionALL(HAPIErrorStmt, conn)
                HAPIStmt = f""" UPDATE TestResults
                                    SET dataAccess = 'Failed'
                                    WHERE SPASE_id = '{SPASE_ID}' """
                executionALL(HAPIStmt, conn)
                print("Sent error message to a TestResults entry with the" +
                      f"row number {Record_id}")
    return lines
