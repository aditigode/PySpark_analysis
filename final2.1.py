#!/usr/bin/env python


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

NBAdata = "hdfs://127.0.0.1:9000/NBAdata/input/NBAdata.csv"  # Should be some file on your system
path = "hdfs://127.0.0.1:9000/NBAdata/output/q2.1"

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df = spark.read.format('csv').options(header='true', inferschema='true').load(NBAdata)

df = df.na.drop()

df.createOrReplaceTempView("NBAdata")
total_result = spark.sql("select count(`SHOT_RESULT`) as total_shots, `player_name` as player,`CLOSEST_DEFENDER` as defender FROM NBAdata GROUP BY `player_name`,`CLOSEST_DEFENDER`")
missed_result = spark.sql("select count(`SHOT_RESULT`) as missed_shots, `player_name`,`CLOSEST_DEFENDER` FROM NBAdata WHERE `SHOT_RESULT`='missed' GROUP BY `player_name`,`CLOSEST_DEFENDER`")

hitDB = total_result.join(missed_result,(total_result.player==missed_result.player_name) & (total_result.defender==missed_result.CLOSEST_DEFENDER))

#hitDB.show()
hitDB.createOrReplaceTempView('hitDB')

hit_result = spark.sql("select `missed_shots`/`total_shots` as hit_rate, `player`,`defender` FROM hitDB")
hit_result.show()
hit_result.createOrReplaceTempView('hit_result')
hit_final = spark.sql("select `hit_rate`,`player`,`defender` FROM hit_result WHERE `hit_rate` in (select max(`hit_rate`) FROM hit_result GROUP BY `player`)")
hit_final.show()
hit_final.write.save(path,format="csv",header=True)

