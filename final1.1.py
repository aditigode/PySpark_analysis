"""SimpleApp.py"""

from pyspark import SparkContext

from pyspark.sql import SparkSession


NYCdata = "hdfs://127.0.0.1:9000/NYCdata/input/NYdata.csv"  # Should be some file on your system
path = "hdfs://127.0.0.1:9000/NYCdata/output"

spark = SparkSession.builder.appName("DataFrame").getOrCreate()


df = spark.read.format('csv').options(header='true', inferschema='true').load(NYCdata)
df = df.select(df['Summons Number'],df['Issue Date'],df['Vehicle Year'],df['Vehicle Body Type'],df['Violation Location'],df['Vehicle Color'])
df = df.na.drop()
df.createOrReplaceTempView("NYdata")

#Question 1
Q1 = spark.sql("SELECT COUNT(`Summons Number`) AS counts, `Issue Date` FROM NYdata GROUP BY `Issue Date` ORDER BY counts DESC LIMIT 1")
Q1.show()

#Question 2
#Q2 = spark.sql("SELECT `Vehicle Year`,`Vehicle Body Type`, MAX(number_of_tickets) as max_violations FROM (select count(`Summons Number`) as number_of_tickets,`Vehicle Year`,`Vehicle Body Type` FROM NYdata GROUP BY `Vehicle Body Type`,`Vehicle Year`)")
Q2 = spark.sql("SELECT count(`Summons Number`) as number_of_tickets,`Vehicle Year`,`Vehicle Body Type` FROM NYdata GROUP BY `Vehicle Body Type`,`Vehicle Year` ORDER BY number_of_tickets DESC LIMIT 20")
Q2.show()

#Question 3
#Q3 = spark.sql("SELECT `Violation Location`, MAX(number_of_violations) as max_violations FROM (select count(`Summons Number`) as number_of_violations,`Violation Location` FROM NYdata GROUP BY `Violation Location`)")
Q3 = spark.sql("SELECT count(`Summons Number`) as number_of_violations,`Violation Location` FROM NYdata GROUP BY `Violation Location` ORDER BY number_of_violations DESC LIMIT 20")
Q3.show()

#Question 4
#Q4=spark.sql("SELECT `Vehicle Color`, MAX(number_of_tickets)as max_tickets FROM (select count(`Summons Number`) as number_of_tickets,`Vehicle Color` FROM NYdata GROUP BY `Vehicle Color`)")
Q4=spark.sql("SELECT count(`Summons Number`) as number_of_tickets,`Vehicle Color` FROM NYdata GROUP BY `Vehicle Color` ORDER BY number_of_tickets DESC LIMIT 20")
Q4.show()


#result.write.save(path,format="csv",header=True)
