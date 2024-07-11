def FAIR_Chart():
    """
    Executes a SQLite SELECT statement to collect all FAIR Scores and displays a bar chart showing the 
    number of records for each FAIR Score. This does so by using a NumPy array within a MatPlotLib function.
    """
    
    import numpy as np
    import matplotlib
    import matplotlib.pyplot as plt
    from SQLiteFun import execution

    # create histogram of FAIR scores
    
    # array that holds all FAIR Scores
    scores = []
    # get all FAIR scores from TestResults
    stmt = "SELECT FAIR_Score FROM TestResults WHERE MostRecent = 'T'"
    rows = execution(stmt,"multiple")
    for row in rows:
        scores.append(row[0])

    # assemble into Numpy array
    np_scores = np.array(scores)
    
    # get mean FAIR Score
    AvgScore = round(np.mean(np_scores))
    
    # get total number of records
    total = len(scores)
    
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
    plt.annotate(f"Total records: {total}", xy=(.1,.9), xycoords='axes fraction')
    plt.annotate(f"Average FAIR score: {AvgScore}", xy=(.1,.8), xycoords='axes fraction')
    fig.tight_layout()