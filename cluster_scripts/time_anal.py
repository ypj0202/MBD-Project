from pyspark.sql.functions import lit, explode, collect_list, struct, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, ArrayType
from pyspark.sql.functions import from_unixtime, to_date, avg, count
import os
from datetime import datetime
import time


class Categories:
    categories = [
        "AMAZON_FASHION",
        "All_Beauty",
        "Appliances",
        "Arts_Crafts_and_Sewing",
        "Automotive",
        "Books",
        "CDs_and_Vinyl",
        "Cell_Phones_and_Accessories",
        "Clothing_Shoes_and_Jewelry",
        "Digital_Music",
        "Electronics",
        "Gift_Cards",
        "Grocery_and_Gourmet_Food",
        "Home_and_Kitchen",
        "Industrial_and_Scientific",
        "Kindle_Store",
        "Luxury_Beauty",
        "Magazine_Subscriptions",
        "Movies_and_TV",
        "Musical_Instruments",
        "Office_Products",
        "Patio_Lawn_and_Garden",
        "Pet_Supplies",
        "Prime_Pantry",
        "Software",
        "Sports_and_Outdoors",
        "Tools_and_Home_Improvement",
        "Toys_and_Games",
        "Video_Games"
    ]

    @staticmethod
    def get_categories():
        return Categories.categories


data_path = "/user/s2773430/data/Amazon_2018"
output_directory = "hdfs://spark-nn.eemcs.utwente.nl/user/s2426668/amazon_reviews_project/basic_analysis/time_anal"
category_names = Categories.get_categories()
file_format = "json.gz"

# create a spark session (local for testing locally). to run in the dfs, remove master call.
spark = SparkSession.builder.appName("amazon_reviews").getOrCreate()
# define a schema
review_schema = StructType([
    StructField("overall", DoubleType(), True),
    StructField("verified", BooleanType(), True),
    StructField("reviewTime", StringType(), True),
    StructField("reviewerID", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("style", StructType([
        StructField("Size", StringType(), True),
        StructField("style name", StringType(), True)
    ]), True),
    StructField("reviewerName", StringType(), True),
    StructField("reviewText", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("unixReviewTime", LongType(), True)
])

dfs = []
# load separately and then union (to create a separate column, which holds category)
for category in category_names:
    path_review = os.path.join(data_path, f"reviews_{category}.{file_format}")
    df_review = spark.read.schema(review_schema).json(path_review)
    df_review = df_review.withColumn("category", lit(category))  # Dataset category
    dfs.append(df_review)

# merge all the dataframes. (Cluster goes boom-boom)
df_reviews = dfs[0]
for df in dfs[1:]:
    df_reviews = df_reviews.unionByName(df)

# chipi, chipi, chapa, chapa, dubi, dubi, daba, daba
df_reviews = df_reviews.withColumn("reviewDate", to_date(from_unixtime("unixReviewTime")))

# get reviews and avg score per day
result = df_reviews.groupBy("reviewDate", "category").agg(count("*").alias("num_of_reviews"), avg("overall").alias("avg_score"))

# group by date and category
result = result.orderBy("reviewDate", "category")

result.show(1000)

# save results to a file
result.write.csv(output_directory, mode='overwrite', header=True)

print("SUCCESS")