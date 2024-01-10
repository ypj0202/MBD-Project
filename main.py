from pyspark.sql import SparkSession
import os

# set hadoop binary
hadoop_bin = "C:\\Development\\hadoop-2.7.3"
# set python exec for win
python_executable_path = 'C:\\Development\\python_conda\\envs\\big_data\\python.exe'
# set environment vars
os.environ['HADOOP_HOME'] = hadoop_bin
os.environ['PYSPARK_PYTHON'] = python_executable_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable_path

data_path = "C:\\Users\\Vitaliy Fischuk\\PycharmProjects\\MBD-Project\\sample_data\\reviews_Video_Games.json"
# create a spark session (local for testing locally). to run in the dfs, remove master call.
spark = SparkSession.builder.appName("amazon_reviews").master("local[*]").getOrCreate()

reviews = spark.read.json(data_path)

reviews.show(truncate=False)

