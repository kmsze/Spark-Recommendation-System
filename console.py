from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("spark://ks1:7077", "Console")
    sc.addPyFile('/home/kmsze/Spark-Recommendation-System/db_connector.py')
    sc.setLogLevel("ERROR")
    from db_connector import DBConnector
    db = DBConnector("console")

    scanner = sc._gateway.jvm.java.util.Scanner
    sys_in = getattr(sc._gateway.jvm.java.lang.System, 'in')
    fun = ""
    while (fun != "exit"):
        print "Get movies recommendation for which user (user ID)?"
        fun = scanner(sys_in).nextLine()
       
        user_id = ""
        try:
            user_id = int(fun)
        except ValueError:
            pass

        if type(user_id) == int:
            movies_list = db.get_recommendations(user_id)
            for index, movie_title in enumerate(movies_list):
                print "{0}. {1}".format(index + 1, movie_title)
            print ""
        elif fun == "run_model":
            db.run_model()
        elif len(fun.split(',')) == 3:
            print "add ratings"
            rating_info = fun.split(',')
            db._add_new_user_rating(rating_info[0], rating_info[1], rating_info[2])
        elif fun == "exit":
            pass
        else:
            print "Please input an integer for user id"
