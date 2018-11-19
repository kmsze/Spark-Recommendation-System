from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("spark://ks1:7077", "StreamProcessing3")
sc.addPyFile('/home/kmsze/Spark-Recommendation-System/db_connector.py')
sc.setLogLevel("ERROR")

from db_connector import DBConnector

TRIGGER_INTERVAL = 1  # in seconds
TOPIC_NAME = 'spark_streaming'
KAFKA_PORT = 'ks1:2181'
db = DBConnector('streaming_db')

def fit_model(df):
    als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(df)
    return model


def to_numbers(pair):
    # convert the input from kafka to a tuple of numbers
    data = pair[1].split(',')
    if len(data) != 3:
        return None
    try:
        userId = int(data[0])
        movieId = int(data[1])
        rating = float(data[2])
    except ValueError:
        return None
    return (userId, movieId, rating)


# the main processing function
def process(rdd):
    # check if empty
    if len(rdd.take(1)) == 0:
        # print("empty rdd")
        pass
    else:
        print("started processing")
        incoming_data = rdd.toDF(["userId","movieId","rating"])
        # print('*************1')
        existing_data = db.fetch_raw_data_from_user_ratings_table()
        # print('*************2')
        existing_data = existing_data.drop('_id')
        # print('*************3')
        # use select to make sure the columns are in the same order before union
        incoming_data = incoming_data.select("userId","movieId","rating")
        # print('*************4')
        existing_data = existing_data.select("userId","movieId","rating")
        # merge the two dataframes
        # print('*************5')
        combined_data = existing_data.union(incoming_data)
        # combined_data.rdd.repartition(8)
        # print("number of partitions: %s" % combined_data.rdd.getNumPartitions())
        # print("incoming data:")
        # incoming_data.show(100, truncate=False)
        # print("existing_data count: %s" % existing_data.count())
        # existing_data.show(200, truncate=False)
        # print("combined_data count: %s" % combined_data.count())
        # combined_data.show(200, truncate=False)
        print("training model on combined data...")
        model = fit_model(combined_data)
        predictions = model.recommendForAllUsers(5)
        # might need further processing of userRecs
        db.add_new_user_rating(incoming_data)
        print("stored incoming data, storing results...")
        db.store_prediction_results(predictions)
        # predictions.show(200, truncate=False)
        print("stored results")

# streaming part, entry point of the program
ssc = StreamingContext(sc, TRIGGER_INTERVAL)
kafkaStream = KafkaUtils.createStream(ssc, KAFKA_PORT, 'spark-streaming', {TOPIC_NAME: 1})
ratings = kafkaStream.map(to_numbers)
ratings.foreachRDD(process)

ssc.start()  # Start the computation
print "Started streaming"
ssc.awaitTermination()  # Wait for the computation to terminate

