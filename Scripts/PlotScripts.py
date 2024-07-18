import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from datetime import date
from .SQLiteFun import execution

def FAIR_Chart(conn):
    """
    Executes a SQLite SELECT statement to collect all FAIR Scores and displays a bar chart showing the 
    number of records for each FAIR Score. This does so by using a NumPy array within a MatPlotLib function.
    
    :param conn: A connection to the desired database
    :type conn: Connection object
    :return: None
    """

    # create histogram of FAIR scores
    
    # array that holds all FAIR Scores
    scores = []
    # get all FAIR scores from TestResults
    stmt = "SELECT FAIR_Score FROM TestResults WHERE MostRecent = 'T'"
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
    ax.set_ylim(0.1, 1900)
    ax.bar_label(chart)
    plt.annotate(f"Average FAIR score: {AvgScore}", xy=(.05,.9), xycoords='axes fraction')
    ax.set_title(f"{createdDate}, Total records: {total}")
    fig.tight_layout()

def MetadataBarChart(conn, records, percent = False):
    # takes connection obj and dictionary containing records of all types
    
    counts = []
    
    # iterate thru key value pairs to get labels and counts
    types = ['Author', 'Publisher', 'Publication Year', 'Dataset Name', 
             'CC0 License', 'URL', 'NASA URL', 'Persistent Identifier', 'Description']
    lists = records.values()
    for kind in lists:
        counts.append(len(kind))
    # skip all type and adjust license title
    counts = counts[1:]
        
    # assemble into Numpy array
    np_types = np.array(types)
    print(np_types)
    np_counts = np.array(counts[:9])
    print(np_counts)
    
    # get total number of records
    total = len(records["all"])
    
    # calculate percentages
    if percent:
        percentages = []
        for count in counts:
            per = (count/total)*100
            percentages.append(round(per))
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
    #plt.rc('font', size = 15)
    fig, ax = plt.subplots()
    chart = ax.bar(np_types, np_counts, align='center', color = colors, hatch = patterns)
    plt.xticks(range(len(np_types)), np_types, rotation = 45, ha = 'right')
    ax.set_xlabel("Metadata Fields")
    ax.set_ylabel("Number of Records")
    start, end = ax.get_ylim()
    ax.yaxis.set_ticks(np.arange(start, end, 1000))
    ax.set_ylim(0.1, 3800)
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
    ax.suptitle(f"{createdDate}, Total Records: {total}")
    ax.set_title("DisplayData and NumericalData Records in the NASA SPASE GitHub")
    #fig.tight_layout()