"""SimpleApp.py"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import pandas as pd

NYCdata = "hdfs://127.0.0.1:9000/NYCdata/input/NYdata.csv"  # Should be some file on your system
path = "hdfs://127.0.0.1:9000/NYCdata/output"


spark = SparkSession.builder.appName("DataFrame").getOrCreate()


df = spark.read.format('csv').options(header='true', inferschema='true').load(NYCdata)


black = ['BK', 'BLACK','BLK','Black','BLBL','BL/','BK/','BLCK','BKBK','BLAK','BLAC','BKL','BK.','BCK','BLC','B K','BKACK']
black_df = df.filter(df['Vehicle Color'].isin(black))
black_df = black_df.select(black_df['Street Code1'], black_df['Street Code2'], black_df['Street Code3'])
clean_black_df = black_df.na.drop()
cols = ['Street Code1', 'Street Code2','Street Code3']
VAssembler =  VectorAssembler(inputCols=cols, outputCol="features")
df_kmeans = VAssembler.transform(clean_black_df)
silhouette_score = []
# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

for i in range(2,10):

    # Trains a k-means model.
    kmeans = KMeans().setK(i).setSeed(1)
    model = kmeans.fit(df_kmeans)

    # Make predictions
    predictions = model.transform(df_kmeans)

    silhouette_score.append([evaluator.evaluate(predictions), i])

print("silhouette scores and their respective number of clusters:\n",silhouette_score)
k=999
best_score = 0
for score, cluster in silhouette_score:
    if score > best_score:
        best_score = score
        k = cluster

 # Trains a k-means model.
kmeans = KMeans().setK(k).setSeed(1)
model = kmeans.fit(df_kmeans)

# Make predictions
predictions = model.transform(df_kmeans)
testcode = [[34510, 10030, 34050]]
#testcode_DF = testcode.toDF(cols)
pandas_df =pd.DataFrame(data=testcode,columns=cols)
testcode_DF = spark.createDataFrame(pandas_df)

df_kmeans_test = VAssembler.transform(testcode_DF)
test_prediction = model.transform(df_kmeans_test)
Pandas_test_pred = test_prediction.select(['prediction']).toPandas()
value = Pandas_test_pred.iloc[0,0]

predictions.createOrReplaceTempView("clean_black_db")
result1 = spark.sql(f"""select count(`features`) as black_car_count,`prediction` as predicted_cluster FROM clean_black_db GROUP BY `prediction` HAVING `prediction`={value}""")
result1.show()

result2 = spark.sql("select count(`features`) as total_black_cars FROM clean_black_db")
result2.show()


black_car_count = result1.select(['black_car_count']).toPandas()
value1 = black_car_count.iloc[0,0]

predicted_cluster = result1.select(['predicted_cluster']).toPandas()
value2 = predicted_cluster.iloc[0,0]

total_black_cars = result2.select(['total_black_cars']).toPandas()
value3 = total_black_cars.iloc[0,0]

value4 = value1/value3
values = [[value1, value2, value3,value4]]
cols = ['black car count in cluster','predicted cluster','total black cars','probability']
pandas_DF=pd.DataFrame(data=values,columns=cols)
final_probab = spark.createDataFrame(pandas_DF)
#final_probab = values.toDF(['black car count in cluster','predicted cluster','total black cars','probability'])
final_probab.show()


#result.write.save(path,format="csv",header=True)
