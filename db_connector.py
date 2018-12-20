from pyspark.sql import SparkSession
import os
import math
from pyspark.sql.functions import *
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import ArrayType, IntegerType, BooleanType

class DBConnector:
    MONGO_DB_URI = "mongodb+srv://edwardwong:A1234567a@sparkcluster-vkhbx.azure.mongodb.net/MoviesRecommendation"
    
    def __init__(self, app_name):
        self.spark = SparkSession\
                    .builder \
                    .appName(app_name)\
                    .config("spark.mongodb.input.uri", DBConnector.MONGO_DB_URI)\
                    .config("spark.mongodb.output.uri", DBConnector.MONGO_DB_URI)\
                    .getOrCreate()
        # Get all the movie information from the database collection to a dataframe and cache it for later usage
        self.df_movies_info = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "MoviesInfoDemo").load()
        self.df_movies_info.cache()
    
    def fetch_raw_data_from_user_ratings_table(self, get_small_dataset=True):
        # Load existing and new user ratings from the MongoDB database to dataframes
        if get_small_dataset:
            df_user_ratings = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "UserRatingsDemo").load()
        else:
            df_user_ratings = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "UserRatings").load()
            
        df_new_user_ratings = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "UserRatingsNew").load()

        df_user_ratings = df_user_ratings.select("userId", "movieId", "rating")
        df_new_user_ratings = df_new_user_ratings.select("userId", "movieId", "rating")
        
        # Combine existing user ratings with new user ratings
        df_combined_user_ratings = df_new_user_ratings.union(df_user_ratings)
        df_combined_user_ratings.cache()

        return df_combined_user_ratings

    def _add_new_user_rating(self, user_id, movie_id, ratings):
        df_new_rating = self.spark.createDataFrame([(int(user_id), int(movie_id), float(ratings))], ["userId", "movieId", "rating"])
        self.add_new_user_rating(df_new_rating)

    def add_new_user_rating(self, df_new_rating):
        # Append new user rating data to the user ratings collection
        df_new_rating.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("collection", "UserRatingsNew").save()
    
    def get_recommendations(self, user_id):
        # Read the prediction result from the PredictionResults collection to a dataframe and cache it
        self.df_result = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection","PredictionResults").load()
        self.df_result.cache()

        # Get the recommended movie id list for a specific user ID
        movie_id_list = self.df_result.select('movies_id_list').where("userId = " + str(user_id)).collect()
        movies_list = movie_id_list[0][0]

        # Get the corresponding movie title for each movie ID
        udf_filter = udf(lambda x: int(x) in movies_list, BooleanType())
        movies_title = self.df_movies_info.select("title").filter(udf_filter("movieId")).collect()
        return [x.title for x in movies_title]
    
    def run_model(self):
        ratings = self.fetch_raw_data_from_user_ratings_table()
        train, validation, test = ratings.randomSplit([0.7, 0.2, 0.1])
        als = ALS(maxIter=20, regParam=0.01, rank=8, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
        model = als.fit(train)
        df_prediction = model.recommendForAllUsers(5)
        self.store_prediction_results(df_prediction)

    def store_prediction_results(self, df_prediction):
        def movie_id_list(x):
            return [int(val.movieId) for val in x]
        
        # Overwrite existing prediction result in the database with new prediction result from the model
        movie_id_list_udf = udf(lambda y: movie_id_list(y), ArrayType(IntegerType()))
        df_prediction = df_prediction.select('*', movie_id_list_udf('recommendations').alias("movies_id_list"))
        df_prediction = df_prediction.drop("recommendations")
        df_prediction.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("collection", "PredictionResults").save()
