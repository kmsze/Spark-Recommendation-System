import csv
import json
import pandas as pd
import pymongo
import sys, getopt, pprint
from pymongo import MongoClient

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb+srv://edwardwong:A1234567a@sparkcluster-vkhbx.azure.mongodb.net/test?retryWrites=true")
    db = client.MoviesRecommendation
    
    def get_recommendations(user_id):
        prediction_results = db['PredictionResults']
        movies_info = db['MoviesInfoDemo']
        user_prediction = prediction_results.find_one({'userId': user_id})
        if user_prediction is None:
            return ["Dummy 1", "Dummy 2", "Dummy 3", "Dummy 4", "Dummy 5"]
        movie_id_list = user_prediction["movies_id_list"]
        #print(movie_id_list)
        movies_list = []
        for id in movie_id_list:
            #print(movies_info.find_one({'movieId': id}))
            movies_list.append(str(movies_info.find_one({'movieId': id})['title']))
        return movies_list
    
    fun = ""
    while (fun != "exit"):
        fun = input("Get movies recommendation for which user (user ID)?\n")

        user_id = ""
        try:
            user_id = int(fun)
        except ValueError:
            pass

        if type(user_id) == int:
            movies_list = get_recommendations(user_id)
            for index, movie_title in enumerate(movies_list):
                print "{0}. {1}".format(index + 1, movie_title)
                #print("%d. %s" % (index + 1, movie_title))           
            print("")
        elif fun == "exit":
            pass
        else:
            #pass
            print "Please input an integer for user id"
