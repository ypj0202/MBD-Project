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

data_path = "C:\\Users\\Vitaliy Fischuk\\PycharmProjects\\MBD-Project\\sample_data\\amazon_reviews"
category_names = [
    'Amazon_Instant_Video',
    'Apps_for_Android',
    'Automotive',
    'Books',
    'CDs_and_Vinyl',
    'Cell_Phones_and_Accessories',
    'Clothing_Shoes_and_Jewelry',
    'Digital_Music',
    'Electronics',
    'Health_and_Personal_Care',
    'Home_and_Kitchen',
    'Kindle_Store',
    'Movies_and_TV',
    'Musical_Instruments',
    'Office_Products',
    'Sports_and_Outdoors',
    'Tools_and_Home_Improvement',
    'Toys_and_Games',
    'Video_Games'
]
file_format = ".json"
tags = ["meta", "reviews"]

# create a spark session (local for testing locally). to run in the dfs, remove master call.
spark = SparkSession.builder.appName("amazon_reviews").master("local[*]").getOrCreate()

# load separately
for category in category_names:
    path_review = os.path.join(data_path, f"reviews_{category}.json")
    path_meta = os.path.join(data_path, f"meta_{category}.json")
    df_review = spark.read.json(path_review)
    df_meta = spark.read.json(path_meta)
    df_merged = df_review.join(df_meta, df_review["asin"] == df_meta["asin"], how="inner")
    df_merged.show(10, truncate=True)


