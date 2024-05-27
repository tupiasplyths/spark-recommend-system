import json
# import csv
import time
import logging
from kafka import KafkaConsumer
import findspark
findspark.init()
# from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, explode
# from pyspark.context import SparkContext
from  pyspark.sql import SparkSession

KAFKA_TOPIC_NAME = "movielens"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
model_path = 'hdfs://master5:9000/user/dis/output-4'
output_path = './output/'
consume_object = KafkaConsumer(
    KAFKA_TOPIC_NAME, 
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
    auto_offset_reset='earliest', 
    enable_auto_commit=True, 
    group_id='grp1', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
# sc = SparkContext.getOrCreate(SparkConf().setMaster("spark://master5:7077"))
# sqlContext = SQLContext(sc)

spark = SparkSession.builder.master('spark://master5:7077') \
    .config('spark.driver.memory', '2g') \
    .config('spark.executor.memory', '2g') \
    .config('spark.driver.extraJavaOptions', '-Xss512m') \
    .config('spark.executor.extraJavaOptions', '-Xss512m') \
    .config('spark.driver.cores', '2') \
    .config('spark.executor.pyspark.memory', '2g') \
    .config('spark.local.dir', '/tmp/spark_temp') \
    .getOrCreate()

# spark = SparkSession.builder.master("spark://master5:7077").getOrCreate() # type: ignore
# spark = SparkSession.builder.appName('kafka_consumer').master('spark://master5:7077').config('spark.driver.memory', '1g').config('spark.executor.memory', '1g').config('spark.driver.extraJavaOptions', '-Xss512m').config('spark.executor.extraJavaOptions', '-Xss512m').config('spark.driver.cores', '2').config('spark.executor.pyspark.memory', '1g').getOrCreate() #type: ignore

logging.basicConfig(filename='time_benchmark.log', level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# alsn_model = ALSModel.read().load(model_path)
movies = spark.read.parquet('hdfs://master5:9000/user/dis/movielens/movies.parquet')
recommendation = spark.read.parquet(model_path + "recom_als")

def get_recommendations(user_id):
    recs = recommendation.filter(col("userId") == user_id).select("recommendations")
    recs = recs.select(explode(col("recommendations")).alias("rec")).select("rec.movieId", "rec.rating")
    item_list = recs.orderBy(col("rating").desc()).select("movieId").rdd.flatMap(lambda x: x).collect()
    return item_list

print('SYSTEM: ready to go')
logging.info(f'new run')
i = 0
total_time = 0
for message in consume_object:
    # print(message.value) 
    start_time = time.time()
    userID = message.value['userId']
    result = get_recommendations(userID)
    # with open(output_path + str(userID) + '.txt', 'w', encoding='utf-8') as csvfile:
        # csvwriter = csv.writer(csvfile)

        # for movieId in result:
        #     movieName = movies.filter(col('movieId') == movieId).select('title').first()[0]
        #     csvwriter.writerow([movieName])
    with open(output_path + str(userID) + '.txt', 'w', encoding='utf-8') as txtfile:
        for movieId in result:
            txtfile.write(f"{movieId}\n")
    end_time = time.time()
    duration = end_time - start_time
    total_time += duration
    logging.info(f"time taken for {userID}: {duration:.5f}s")
    i+=1
    if (i == 100):
        logging.info(f"Total time taken for {i:d} request is {total_time:.4f}s")
        logging.info(f"Average time per request is {total_time/(i):.4f}s")
    # print('time taken for {userID}: {duration:.5f}s')
    # recommend = model.transform(user)
    # recommend.orderBy("prediction", ascending=False).show()
    # recommend.toPandas().to_csv(message.value['userId'] + ".csv", index=False)