# PySpark_analysis
Analyzing NYC Parking Violations data and NBA shot logs using PySpark             
# ENGR 516 — Assignment 2
### My code report can be found on report.pdf file.       


## NY Parking Violations

The NYC Department of Finance collects data on every parking ticket issued in NYC. This data
is made publicly available to aid in ticket resolution and to guide policymakers.
You can find the data from the Link of NYC Parking Data.


The above figure shows several records, where each row represents a parking ticket and the
columns are the details of the tickets.
First, please follow this instruction (GitHub Link) to install the spark cluster.
Then, by analyzing the data, you need to answer the following questions:

- When are tickets most likely to be issued? (15 pts)
- What are the most common years and types of cars to be ticketed? (15 pts)
- Where are tickets most commonly issued? (15 pts)
- Which color of the vehicle is most likely to get a ticket? (15 pts)
Based on a K-Means algorithm, please try to answer the following question:
- Given a Black vehicle parking illegally at 34510, 10030, 34050 (street codes). What is
the probability that it will get an ticket? (very rough prediction). (20 pts)
Note that the biggest challenge when using K-Means is to decide on the number of clusters.
Having more clusters creates some small classes with very few records, while having less clus-
ters leads to classes that are too general.

## NBA Shot Logs

This is the DATA on shots taken during the 2014-2015 season, who took the shot, where on
the floor was the shot taken from, who was the nearest defender, how far away was the nearest
defender, time on the shot clock, etc. The column titles are generally self-explanatory.
The below figure shows several records, where each row represents a shot and the columns are
the details of the shot, e.g., game ID, who’s defender, what’s the distance between them.


Please analyze the data and answer the following questions:

- For each pair of the players (A, B), we define thefear soreof A when facing B is the hit
    rate, such that B is closet defender when A is shooting. Based on thefear sore, for each
    player, please find out who is his ”most unwanted defender”. (10 pts)
- For each player, we define thecomfortable zoneof shooting is a matrix of,
    {SHOTDIST, CLOSEDEFDIST, SHOTCLOCK}
    Please develop a Spark-based algorithm to classify each player’s records into 4 comfort-
    able zones. Considering the hit rate, which zone is the best for James Harden, Chris Paul,
    Stephen Curry, and Lebron James. (10 pts)





