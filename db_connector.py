from pyspark.sql import SparkSession
import os
import math
from pyspark.sql.functions import *
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import ArrayType, IntegerType, BooleanType

class DBConnector:
    MONGO_DB_URI = "mongodb+srv://edwardwong:A1234567a@sparkcluster-vkhbx.azure.mongodb.net/MoviesRecommendation"
    
    def __init__(self):
        self.spark = SparkSession\
                    .builder \
                    .appName("db_connector")\
                    .config("spark.mongodb.input.uri", DBConnector.MONGO_DB_URI)\
                    .config("spark.mongodb.output.uri", DBConnector.MONGO_DB_URI)\
                    .getOrCreate()
        self.df_movies_info = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "MoviesInfo").load()
        self.df_movies_info.cache()
    
    def fetch_raw_data_from_user_ratings_table(self, get_small_dataset=True):
        if get_small_dataset:
            df_user_ratings = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "UserRatingsSmall").load()
        else:
            df_user_ratings = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "UserRatings").load()
        return df_user_ratings

    def get_recommendations(self, user_id):
        self.df_result = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection","PredictionResults").load()
        self.df_result.cache()
        movie_id_list = self.df_result.select('movies_id_list').where("userId = " + str(user_id)).collect()
        movies_list = movie_id_list[0][0]
        udf_filter = udf(lambda x: int(x) in movies_list, BooleanType())
        movies_title = self.df_movies_info.select("title").filter(udf_filter("movieId")).collect()
        return [x.title for x in movies_title]
    
    def run_model(self):
        ratings = self.fetch_raw_data_from_user_ratings_table()
        train, validation, test = ratings.randomSplit([0.7, 0.2, 0.1])
        als = ALS(maxIter=5, regParam=0.01, rank=4, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
        model = als.fit(train)
        #predictions = model.transform(validation)
        df_prediction = model.recommendForAllUsers(10)
        self.store_prediction_results(df_prediction)

    def store_prediction_results(self, df_prediction):
        def movie_id_list(x):
            return [int(val.movieId) for val in x]
        
        movie_id_list_udf = udf(lambda y: movie_id_list(y), ArrayType(IntegerType()))
        df_prediction = df_prediction.select('*', movie_id_list_udf('recommendations').alias("movies_id_list"))
        df_prediction = df_prediction.drop("recommendations")
        df_prediction.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("collection", "PredictionResults").save()
