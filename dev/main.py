from pyspark.sql import SparkSession
import os
from Categories import Categories
from events import events
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

# set hadoop binary
hadoop_bin = "C:\\Development\\hadoop-2.7.3"
# set python exec for win
python_executable_path = 'C:\\Development\\python_conda\\envs\\big_data\\python.exe'
# set environment vars
os.environ['HADOOP_HOME'] = hadoop_bin
os.environ['PYSPARK_PYTHON'] = python_executable_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable_path

data_path = "/sample_data/amazon_reviews"
output_directory = "C:\\Users\\Vitaliy Fischuk\\PycharmProjects\\MBD-Project\\sample_data\\result"
category_names = Categories.get_categories()
file_format = ".json"
tags = ["meta", "reviews"]

# create a spark session (local for testing locally). to run in the dfs, remove master call.
spark = SparkSession.builder.appName("amazon_reviews").master("local[*]").getOrCreate()
schema = StructType([
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

results_per_event = {event.name: [] for event in events}
# load separately
for category in category_names:
    path_review = os.path.join(data_path, f"reviews_{category}.json")
    df_review = spark.read.schema(schema).json(path_review)
    df_review.createOrReplaceTempView("reviews")
    df_review.show(2)
    for event in events:
        start = event.get_scrapping_start_date_unix()
        date = event.get_event_date_unix()
        end = event.get_scrapping_end_date_unix()
        query = f"""
        SELECT
            'Span Before' AS time_span,
            COUNT(*) as review_count,
            AVG(overall) as average_rating
        FROM
            reviews
        WHERE unixReviewTime BETWEEN {start} AND {date}
        UNION
        SELECT
            'Span After' AS time_span,
            COUNT(*) as review_count,
            AVG(overall) as average_rating
        FROM
            reviews
        WHERE unixReviewTime BETWEEN {date} AND {end}
        """
        result = spark.sql(query)
        results_per_event[event.name].append(result)


# save results to files
for event_name, dfs in results_per_event.items():
    combined_df = dfs[0]
    output_path = os.path.join(output_directory, f"{event_name}.csv")
    for df in dfs[1:]:
        combined_df = combined_df.union(df)
    combined_df.coalesce(1).write.csv(output_path, mode='overwrite', header=True)