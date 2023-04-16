# PySpark_analysis
Analyzing NYC Parking Violations data and NBA shot logs using PySpark             
# ENGR 516 — Assignment 2
### My code report can be found on report.pdf file.       

In ENGR 5950, we have learned the following topics:

1. Set up a 3-node cluster with Hadoop Distributed File System and run examples.
2. On top of HDFS, set up the cluster with MapReduce programming framework.
3. Run examples of MapReduce programs.
4. Scheduling with YARN.
However, you should have observed that developing an integrative program, which involves
multiple Maps and Reduces, with MapReduce programming framework is definitely not a trivial
task. You have to use a loop in the shell program to start the iteration and utilize an indicator to
stop the loop (Figure 1).

```
Figure 1: MapReduce based iterative programming
```
This challenge is caused by the fact that Hadoop is designed to utilize the storage space in the
cluster. Each MapReduce program requires to output the data into the disk. This leads to a large
amount of HDFS reads/writes, which significantly limits the performance.


## Spark Programming

The spark system implements the Resilient Distributed Dataset (RDD) to maximize the memory
space in the cluster. With RDD, most of the operation is done in the memory (Fig. 2).

```
Figure 2: Hadoop v.s. Spark
```
In this project, you are going to design you own Spark programs to analyze the data. To develop
a Spark program, you just need to transform the previous RDD into a new one for the next
iteration. You can use any spark related library package in this project.

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

## Submission

You are expected to upload a zip or tar file by the deadline to Canvas. The zip file should include
your codes, report, and README.

## Useful Links

1. Analysis of NYC Parking Tickets.
2. Preliminary Data Visualization.
3. Exploring 42.3M NYC Parking Tickets.
4. NY Parking Violations Issued.
5. Insights From Raw NBA Shot Log Data.
6. Investigating the hot hand phenomenon in the NBA (CODE).
7. Parallelizing K-Means-Based Clustering on Spark.
8. NBA 16-17 regular season shot log.
9. The Fear Factor.
10. The Best And Worst Defenders.
11. NBA Classification.
12. Stephen Curry’s Decision Tree.
13. Points per Match (ATL vs WAS only).
14. Spark Kmeans.



