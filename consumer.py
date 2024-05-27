import json
from kafka import KafkaConsumer
from pyspark.ml.recommendation import ALS, ALSModel
from  pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
# from pyspark.ml.pipeline import ALS
KAFKA_TOPIC_NAME = "movielens"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

spark = SparkSession.builder.appName('test1111').master('local[3]').config('spark.driver.memory', '2g').config('spark.executor.memory', '2g').config('spark.driver.extraJavaOptions', '-Xss512m').config('spark.executor.extraJavaOptions', '-Xss512m').config('spark.driver.cores', '2').config('spark.executor.pyspark.memory', '2g').getOrCreate()  # type: ignore
rating_data = spark.read.csv('data/ratings.csv', inferSchema=True, header=True)
model = ALSModel.load('./model/light_data')
training, test = rating_data.randomSplit([0.9, 0.1])

consume_object = KafkaConsumer(
    KAFKA_TOPIC_NAME, 
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
    auto_offset_reset='earliest', 
    enable_auto_commit=True, 
    group_id='grp1', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
# als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
# model = als.fit(training)
# predictions = model.transform(test)

# evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
# rmse = evaluator.evaluate(predictions)
# print("Root-mean-square error = " + str(rmse))


for message in consume_object:
    # print(message.value['userId']) 
    user = test.filter(test["userId"] == message.value['userId']).select(["userId", "movieId"])
    # user1.show()

    recommend = model.transform(user)
    recommend.orderBy("prediction", ascending=False).show()
    recommend.toPandas().to_csv(str(message.value['userId']) + ".csv", index=False)
