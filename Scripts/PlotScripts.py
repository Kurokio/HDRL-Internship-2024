import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from datetime import date
from .SQLiteFun import execution

def FAIR_Chart(conn, All = True):
    """
    Executes a SQLite SELECT statement to collect all FAIR Scores and displays a bar chart showing the 
    number of records for each FAIR Score. This does so by using NumPy arrays within a MatPlotLib function.
    The default value for All is True, which uses data from all the records in the database.
    Passing False to the All parameter performs the same, but only for the records with NASA URLs.
    
    :param conn: A connection to the desired database
    :type conn: Connection object
    :param All: A boolean determining whether or not to use all records in the database or only those with NASA URLs.
    :type All: Boolean
    :return: None
    """
    
    # array that holds all FAIR Scores
    scores = []
    # get FAIR scores from TestResults
    # want all records in db
    if All:
        stmt = "SELECT FAIR_Score FROM TestResults WHERE MostRecent = 'T'"
    # want only those records with NASA URLs in db
    else:
        stmt = "SELECT FAIR_Score FROM TestResults WHERE (MostRecent = 'T' AND has_NASAurl = 1)"
    rows = execution(stmt, conn, "multiple")
    for row in rows:
        scores.append(row[0])

    # assemble into Numpy array
    np_scores = np.array(scores)
    
    # get mean FAIR Score
    AvgScore = round(np.mean(np_scores), 2)
    
    # get total number of records
    total = len(scores)
    
    # get the date
    createdDate = date.today()
    
    # get unique values and their occurences
    labels, counts = np.unique(np_scores, return_counts=True)

    # create chart
    plt.rc('font', size = 15)
    fig, ax = plt.subplots()
    chart = ax.bar(labels, counts, align='center')
    ax.set_xlabel("FAIR Score")
    ax.set_ylabel("Number of Records")
    ax.xaxis.set_ticks(np.arange(0, 11))
    ax.set_xlim(0.1,11)
    start, end = ax.get_ylim()
    ax.yaxis.set_ticks(np.arange(start, end, 400))
    ax.set_ylim(0.1, (end+300))
    ax.bar_label(chart)
    plt.annotate(f"Average FAIR Score: {AvgScore}", xy=(.05,.9), xycoords='axes fraction')    
    if All:
        plt.title(f"{createdDate}, Total Records: {total}\nRecords in the NASA SPASE GitHub", fontsize = 14)
    else:
        plt.title(f"{createdDate}, Total Records: {total}\nRecords in the NASA SPASE GitHub with NASA URLs", fontsize = 14)
    fig.tight_layout()

def MetadataBarChart(conn, records, percent = False, All = True):
    """
    Takes a connection object as parameter as well as a dictionary containing the SPASE 
    records with each kind of metadata. It iterates through the values of the dictionary 
    to get the counts of these records. It then uses this to display a bar chart showing the 
    number of records that have each kind of metadata field. This does so by using NumPy 
    arrays within a MatPlotLib function. The default value for the percent parameter is False,
    which means the chart displayed will show the data labels as integers. If True, the data
    labels are displayed as percentages. The default value for All is True, which means that
    it uses all records in the database as data for the chart. Passing False to the All parameter 
    performs the same, but only the records with NASA URLs are used for the chart.
    
    :param conn: A connection to the desired database
    :type conn: Connection object
    :param records: A dictionary with the keys being the metadata fields and the values being the records that have
                    an entry for that field. 
    :type records: dictionary
    :param percent: A boolean determining if the data labels in the chart will be displayed as percentages or integers.
    :type percent: Boolean
    :param All: A boolean determining whether or not to use all records in the database or only those with NASA URLs.
    :type All: Boolean
    :return: None
    """
    
    # list to hold counts of records passing each analysis test
    counts = []
    
    # iterate thru dictionary values to get counts
    # adjust license title
    types = ['Author', 'Publisher', 'Publication Year', 'Dataset Name', 
             'CC0 License', 'URL', 'NASA URL', 'Persistent Identifier', 'Description']
    lists = records.values()
    for kind in lists:
        counts.append(len(kind))
    # skip all type
    counts = counts[1:]
        
    # assemble into Numpy array
    np_types = np.array(types)
    print(np_types)
    np_counts = np.array(counts[:9])
    print(np_counts)
    
    # get total number of records
    # when all records in db
    if All:
        total = len(records["all"])
    # when just records with NASA URLs in db
    else:
        total = len(records["NASAurl"])
    
    # calculate percentages
    if percent:
        percentages = []
        for count in counts:
            per = (count/total)*100
            if All:
                percentages.append(round(per))
            else:
                percentages.append(round(per, 1))
        np_percentages = np.array(percentages[:9])
    
    # create color distinctions between bars that are part of citation and none
    colors = ['tab:orange', 'tab:orange', 'tab:orange', 'tab:orange', 'tab:blue', 'tab:blue', 
              'tab:blue', 'tab:blue', 'tab:blue']
    # make bars needed for compliance have dashes
    # 4, 8, 9 need dashed
    patterns = ['','','','/','','','','/','/']
    
    # get the date
    createdDate = date.today()
    
    # create chart
    fig, ax = plt.subplots()
    chart = ax.bar(np_types, np_counts, align='center', color = colors, hatch = patterns)
    plt.xticks(range(len(np_types)), np_types, rotation = 45, ha = 'right')
    ax.set_xlabel("Metadata Fields")
    ax.set_ylabel("Number of Records")
    start, end = ax.get_ylim()
    ax.yaxis.set_ticks(np.arange(start, end, 1000))
    if All:
        ax.set_ylim(0.1, (end+800))
    else:
        ax.set_ylim(0.1, (end+600))
    if percent:
        i = 0
        for bar in chart:
            width = bar.get_width()
            height = bar.get_height()
            x, y = bar.get_xy()
            plt.text(x + width/2, y + height*1.01, str(np_percentages[i]) + '%', ha = 'center')
            i += 1
        plt.annotate(f"All citation fields: {percentages[9]}%", xy=(.05,.95), xycoords='axes fraction')
        plt.annotate(f"DCAT3-US Compliance: {percentages[10]}%", xy=(.05,.9), xycoords='axes fraction')
    else:
        ax.bar_label(chart)
        plt.annotate(f"All citation fields: {counts[9]}", xy=(.05,.95), xycoords='axes fraction')
        plt.annotate(f"DCAT3-US Compliance: {counts[10]}", xy=(.05,.9), xycoords='axes fraction')
    plt.suptitle(f"  {createdDate}, Total Records: {total}")
    if All:
        plt.title("Records in the NASA SPASE GitHub")
    else:
        plt.title("Records in the NASA SPASE GitHub with NASA URLs")