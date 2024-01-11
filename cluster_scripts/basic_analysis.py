from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType
import os
from datetime import datetime
import time


class Event:
    def __init__(self, name, event_date, scrapping_start_date, scrapping_end_date):
        self.name = name
        self.event_date = event_date
        self.scrapping_start_date = scrapping_start_date
        self.scrapping_end_date = scrapping_end_date
        self.date_format = "%Y-%m-%d"

    def get_unix_time(self, human_time):
        date = datetime.strptime(human_time, self.date_format)
        return int(time.mktime(date.timetuple()))

    def get_event_date_unix(self):
        return self.get_unix_time(self.event_date)

    def get_scrapping_start_date_unix(self):
        return self.get_unix_time(self.scrapping_start_date)

    def get_scrapping_end_date_unix(self):
        return self.get_unix_time(self.scrapping_end_date)


events = [
    Event("2008_Recession", "2007-12-01", "2007-10-01", "2008-02-01"),
    Event("Boston_Marathon_bombing_2013", "2013-04-15", "2013-02-15", "2013-06-15")
]


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
output_directory = "hdfs://spark-nn.eemcs.utwente.nl/user/s2426668/amazon_reviews_project/basic_analysis"
category_names = Categories.get_categories()
file_format = "json.gz"

# create a spark session (local for testing locally). to run in the dfs, remove master call.
spark = SparkSession.builder.appName("amazon_reviews").getOrCreate()
# define a schema
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
    path_review = os.path.join(data_path, f"reviews_{category}.{file_format}")
    df_review = spark.read.schema(schema).json(path_review)
    df_review.createOrReplaceTempView("reviews")
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

