# Spark-Recommendation-System

to run spark_streaming.py:

~/spark/bin/spark-submit --master "spark://ks1:7077" --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1 /home/tom/Spark-Recommendation-System/spark_streaming.py
