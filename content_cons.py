import findspark
findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from kafka import KafkaConsumer

import time
import pandas as pd
import json
import numpy as np
import logging

logging.basicConfig(filename='time_benchmark_content.log', level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


KAFKA_TOPIC_NAME = "movielens_content"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
model_path = 'hdfs://master5:9000/user/dis/output-3'
output_path = './output_content/'

consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME, 
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
    auto_offset_reset='earliest', 
    enable_auto_commit=True, 
    group_id='grp1', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
spark = SparkSession.builder \
    .appName("Kafka-ContentBased") \
    .master("spark://master5:7077") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

# # MARK: Data prep
df_movies = spark.read.parquet('hdfs://master5:9000/user/dis/movielens/movies.parquet')
df_ratings = spark.read.parquet('hdfs://master5:9000/user/dis/movielens/ratings.parquet')

df_movies.createOrReplaceTempView("movies")
df_ratings.createOrReplaceTempView("ratings")
tags_by_movie_trf_df = spark.read.parquet('hdfs://master5:9000/user/dis/output-11movieId_vecs.parquet')
all_movieId_vecs = tags_by_movie_trf_df.select('movie_id', 'word_vec').rdd.map(lambda x: (x[0], x[1])).collect()


def getSimilarMovies(m_id, sim_mos_limit=5):
    # start_time = time.time()
    schema = StructType([
        StructField("movie_id", IntegerType(), True)
        ,StructField("score", IntegerType(), True)
        ,StructField("input_movie_id", StringType(), True)
    ])

    similar_movies_df = spark.createDataFrame([], schema)
    # print(m_id)
    m_id = int(m_id)
    input_vec = tags_by_movie_trf_df.select('word_vec')\
                .filter(tags_by_movie_trf_df['movie_id'] == m_id)\
                .collect()[0][0]
    similar_movie_rdd = sc.parallelize((i[0], float(CosineSim(input_vec, i[1]))) for i in all_movieId_vecs)
    similar_movie_df = spark.createDataFrame(similar_movie_rdd) \
            .withColumnRenamed('_1', 'movie_id') \
            .withColumnRenamed('_2', 'score') \
            .orderBy("score", ascending = False)

    similar_movie_df = similar_movie_df.filter(col("movie_id") != m_id).limit(sim_mos_limit)
    similar_movie_df = similar_movie_df.withColumn('input_movie_id', lit(m_id))

    similar_movies_df = similar_movies_df \
                                .union(similar_movie_df)

    return similar_movies_df

def CosineSim(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    if norm_vec1 == 0 or norm_vec2 == 0:
        return 0

    return dot_product / (norm_vec1 * norm_vec2)

def getMovieDetails(in_mos):

    a = in_mos.alias("a")
    b = df_movies.alias("b")

    return a.join(b, col("a.movie_id") == col("b.movieId"), 'inner') \
             .select([col('a.'+xx) for xx in a.columns] + [col('b.title'),col('b.genres')])

def getContentRecoms(u_id, sim_mos_limit=5):

    # select movies having rating >= 3
    query = """
    SELECT distinct movieId as movie_id FROM ratings
    where rating >= 3.0
    and userId = "{}"
    """.format(u_id)

    usr_rev_mos = sqlContext.sql(query)

    # from these get sample of 5 movies
    usr_rev_mos = usr_rev_mos.sample(False, 0.5).limit(5)
    usr_rev_mos_det = getMovieDetails(usr_rev_mos)
    mos_list = [i.movie_id for i in usr_rev_mos.collect()]

    sim_mos_dfs = []
    for i in mos_list:
        sim_mos_df = getSimilarMovies(i, sim_mos_limit)
        sim_mos_dfs.append(sim_mos_df)

    # Change to a DataFrame  
    sim_mos_df = sim_mos_dfs[0]
    for i in range(1, len(sim_mos_dfs)):
        sim_mos_df = sim_mos_df.union(sim_mos_dfs[i])

    a = sim_mos_df.alias("a")
    b = usr_rev_mos.alias("b")
    c = a.join(b, col("a.movie_id") == col("b.movie_id"), 'left_outer') \
        .where(col("b.movie_id").isNull()) \
        .select([col('a.movie_id'),col('a.score')]).orderBy("a.score", ascending = False)

    x = c.limit(sim_mos_limit)
    return getMovieDetails(x)

hadoop_output_path = 'hdfs://master5:9000/user/dis/'
print('server ready')
i = 0
total_time = 0
for message in consumer:
    
    userId = message.value['userId']
    # print(userId)
    start_time = time.time()
    result = getContentRecoms(userId)
    # getContentRecoms(userId).write.format('com.databricks.spark.csv').mode('overwrite').save(output_path + str(userId))
    # result.show()
    result.toPandas().to_csv(output_path + str(userId) + '.csv', index=False)
    # result.write.csv(output_path + str(userId), mode='overwrite')
    end_time = time.time()
    duration = end_time - start_time
    total_time += duration

    logging.info(f"time taken for {userId}: {duration:.5f}s")
    i+=1
    if (i == 50):
        logging.info(f"Total time taken for {i:d} request is {total_time:.4f}s")
        logging.info(f"Average time per request is {total_time/(i):.4f}s")
        i = 0