from pyspark.sql.functions import lit, explode, collect_list, struct, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, ArrayType
from pyspark.sql.functions import from_unixtime, to_date, avg, count
from pyspark.sql import Window
from pyspark.sql.functions import rank, desc, first

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
output_directory = "hdfs://spark-nn.eemcs.utwente.nl/user/s2426668/amazon_reviews_project/time_products_anal"
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
meta_schema = StructType([
    StructField("category", ArrayType(StringType()), True),
    StructField("tech1", StringType(), True),
    StructField("description", ArrayType(StringType()), True),
    StructField("fit", StringType(), True),
    StructField("title", StringType(), True),
    StructField("also_buy", ArrayType(StringType()), True),
    StructField("tech2", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("feature", ArrayType(StringType()), True),
    StructField("rank", ArrayType(StringType()), True),
    StructField("also_view", ArrayType(StringType()), True),
    StructField("main_cat", StringType(), True),
    StructField("similar_item", StringType(), True),
    StructField("date", StringType(), True),
    StructField("price", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("imageURL", ArrayType(StringType()), True),
    StructField("imageURLHighRes", ArrayType(StringType()), True)
])

dfs = []
# load separately and then union (to create a separate column, which holds category)
for category in category_names:
    # paths
    path_review = os.path.join(data_path, f"reviews_{category}.{file_format}")
    path_meta = os.path.join(data_path, f"meta_{category}.{file_format}")
    # loading meta
    df_meta = spark.read.schema(meta_schema).json(path_meta)
    df_meta = df_meta.dropDuplicates(["asin"])
    df_meta = df_meta.drop("category")
    # loading reviews
    df_review = spark.read.schema(review_schema).json(path_review)
    df_review = df_review.withColumn("category", lit(category))  # Dataset category
    df_joined = df_review.join(df_meta, on="asin", how="inner")
    dfs.append(df_joined)

# merge all the dataframes. (Cluster goes boom-boom)
df_reviews = dfs[0]
for df in dfs[1:]:
    df_reviews = df_reviews.unionByName(df)

# chipi, chipi, chapa, chapa, dubi, dubi, daba, daba
df_reviews = df_reviews.withColumn("reviewDate", to_date(from_unixtime("unixReviewTime")))

# get total number of reviews per product per category per day (e.g. form combinations of date-category-product)
result = df_reviews.groupBy("reviewDate", "category", "asin").agg(count("*").alias("num_of_reviews"),
                                                                  first("title").alias("title"),
                                                                  first("date").alias("date"))


result_no_products = df_reviews.groupBy("reviewDate", "category").agg(count("*").alias("num_of_reviews"))
# use a window for each category-date combination, where the rows inside each window are ordered by number of reviews
# this allows us to perform ranking later.
window = Window.partitionBy("reviewDate", "category").orderBy(desc("num_of_reviews"))
# apply the window and rank all the rows based on number of reviews.
reviews_with_ranked_products = result.withColumn("rank", rank().over(window))
num_of_top_products_to_display = 10
# note: filter in pycharm gives a typing error, so i used where instead. both are interchangeable.
reviews_with_top_products = reviews_with_ranked_products.where(col("rank") <= num_of_top_products_to_display)

# convert to json
json_struct = struct("asin", "num_of_reviews", "title", "date")
top_products_in_json = reviews_with_top_products.groupBy("reviewDate", "category").agg(collect_list(json_struct)
                                                                                       .alias("top_products_json"))
# join with the original data
result = result_no_products.join(top_products_in_json, ["reviewDate", "category"], "left")
# group by date and category
result = result.orderBy("reviewDate", "category")

# save results to a file
result.write.json(output_directory, mode='overwrite')
print("SUCCESS")