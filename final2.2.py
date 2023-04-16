#!/usr/bin/env python


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

NBAdata = "hdfs://127.0.0.1:9000/NBAdata/input/NBAdata.csv"  # Should be some file on your system
path = "hdfs://127.0.0.1:9000/NYCdata/output"

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df = spark.read.format('csv').options(header='true', inferschema='true').load(NBAdata)

df = df.na.drop()

shot_df = df.select(df['SHOT_DIST'], df['CLOSE_DEF_DIST'], df['SHOT_CLOCK'])

cols = ['SHOT_DIST', 'CLOSE_DEF_DIST','SHOT_CLOCK']
VAssembler =  VectorAssembler(inputCols=cols, outputCol="features")
df_kmeans = VAssembler.transform(shot_df)

import pandas

# Trains a k-means model.
kmeans = KMeans().setK(4).setSeed(1)
model = kmeans.fit(df_kmeans)
# Make predictions
predictions = model.transform(df_kmeans)
pandasDF = predictions.toPandas()
pandasAllDF = df.toPandas()

df_concat = pandas.concat([pandasDF,pandasAllDF], axis=1)

df_concat_clean = df_concat.loc[:,~df_concat.T.duplicated(keep='first')]

sparkDF=spark.createDataFrame(df_concat_clean)

sparkDF.createOrReplaceTempView("NBAdata")

result_player1_made = spark.sql("select count(`SHOT_RESULT`) as made_shots,`player_name`,`prediction` as prediction_col FROM NBAdata WHERE `SHOT_RESULT`='made' GROUP BY `Player_name`,`prediction` HAVING `player_name`='james harden' ORDER BY made_shots DESC limit 1")
max_pred1 = result_player1_made.select(['prediction_col']).toPandas()
value = max_pred1.iloc[0,0]
result_player1_total = spark.sql(f"""select count(`SHOT_RESULT`) as total_shots,`player_name`as player,`prediction` as cluster FROM NBAdata WHERE `prediction`={value} GROUP BY `player_name`,`prediction` HAVING `player_name`='james harden'""")
result_player1_made.show()
result_player1_total.show()
player1 = result_player1_made.join(result_player1_total,(result_player1_made.player_name==result_player1_total.player))
player1.show()
player1.createOrReplaceTempView('player1')
player1_result = spark.sql("select `player_name`,`prediction_col` as comfort_zone_cluster,`made_shots`/`total_shots` as hit_rate FROM player1")
player1_result.show()
#player1_result.write.save(path,format="csv",header=True)

#player 2
result_player2_made = spark.sql("select count(`SHOT_RESULT`) as made_shots,`player_name`,`prediction` as prediction_col FROM NBAdata WHERE `SHOT_RESULT`='made' GROUP BY `Player_name`,`prediction` HAVING `player_name`='chris paul' ORDER BY made_shots DESC limit 1")
max_pred2 = result_player2_made.select(['prediction_col']).toPandas()
value = max_pred2.iloc[0,0]
result_player2_total = spark.sql(f"""select count(`SHOT_RESULT`) as total_shots,`player_name`as player,`prediction` as cluster FROM NBAdata WHERE `prediction`={value} GROUP BY `player_name`,`prediction` HAVING `player_name`='chris paul'""")
player2 = result_player2_made.join(result_player2_total,(result_player2_made.player_name==result_player2_total.player))
player2.show()
player2.createOrReplaceTempView('player2')
player2_result = spark.sql("select `player_name`,`prediction_col` as comfort_zone_cluster,`made_shots`/`total_shots` as hit_rate FROM player2")
player2_result.show()
#player2_result.write.save(path,format="csv",header=True)


#player 3
result_player3_made = spark.sql("select count(`SHOT_RESULT`) as made_shots,`player_name`,`prediction` as prediction_col FROM NBAdata WHERE `SHOT_RESULT`='made' GROUP BY `Player_name`,`prediction` HAVING `player_name`='stephen curry' ORDER BY made_shots DESC limit 1")
max_pred3 = result_player3_made.select(['prediction_col']).toPandas()
value = max_pred3.iloc[0,0]
result_player3_total = spark.sql(f"""select count(`SHOT_RESULT`) as total_shots,`player_name`as player,`prediction` as cluster FROM NBAdata WHERE `prediction`={value} GROUP BY `player_name`,`prediction` HAVING `player_name`='stephen curry'""")
result_player3_made.show()
result_player3_total.show()
player3 = result_player3_made.join(result_player3_total,(result_player3_made.player_name==result_player3_total.player))
player3.show()
player3.createOrReplaceTempView('player3')
player3_result = spark.sql("select `player_name`,`prediction_col` as comfort_zone_cluster,`made_shots`/`total_shots` as hit_rate FROM player3")
#layer3_result.write.save(path,format="csv",header=True)
player3_result.show()

#player 4
result_player4_made = spark.sql("select count(`SHOT_RESULT`) as made_shots,`player_name`,`prediction` as prediction_col FROM NBAdata WHERE `SHOT_RESULT`='made' GROUP BY `Player_name`,`prediction` HAVING `player_name`='lebron james' ORDER BY made_shots DESC limit 1")
max_pred4 = result_player1_made.select(['prediction_col']).toPandas()
value = max_pred4.iloc[0,0]
result_player4_total = spark.sql(f"""select count(`SHOT_RESULT`) as total_shots,`player_name`as player,`prediction` as cluster FROM NBAdata WHERE `prediction`={value} GROUP BY `player_name`,`prediction` HAVING `player_name`='lebron james'""")
result_player4_made.show()
result_player4_total.show()
player4 = result_player4_made.join(result_player4_total,(result_player4_made.player_name==result_player4_total.player))
player4.show()
player4.createOrReplaceTempView('player4')
player4_result = spark.sql("select `player_name`,`prediction_col` as comfort_zone_cluster,`made_shots`/`total_shots` as hit_rate FROM player4")
#player4_result.write.save(path,format="csv",header=True)
player4_result.show()




