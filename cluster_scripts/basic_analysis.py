from pyspark.sql.functions import lit, explode, collect_list, struct, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, ArrayType
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
# output_directory = "hdfs://spark-nn.eemcs.utwente.nl/user/s2426668/amazon_reviews_project/basic_analysis"
output_directory = "hdfs://spark-nn.eemcs.utwente.nl/user/s2773430/amazon_reviews_project/basic_analysis"
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
results_per_event = {event.name: [] for event in events}
# load separately
for category in category_names:
    path_review = os.path.join(data_path, f"reviews_{category}.{file_format}")
    path_meta = os.path.join(data_path, f"meta_{category}.{file_format}")
    df_review = spark.read.schema(review_schema).json(path_review)
    df_review = df_review.withColumn("ds_category", lit(category)) # Dataset category
    df_meta = spark.read.schema(meta_schema).json(path_meta)
    df_meta = df_meta.dropDuplicates(["asin"])
    df_joined = df_review.join(df_meta, on="asin", how="inner") # This will duplicate the size of the data
    #df_review.createOrReplaceTempView("reviews")
    df_joined.createOrReplaceTempView("reviews")
    ### Join the review with the meta data, optimized approach
    # # Create a struct of all columns in the review DataFrame except 'asin'
    # review_struct = struct([col(f.name) for f in review_schema.fields if f.name != 'asin'])

    # # Aggregate Reviews by 'asin' into a list of structs
    # aggregated_reviews = df_review.groupBy("asin").agg(collect_list(review_struct).alias("reviews"))
    # # Join Aggregated Reviews with Meta Data
    # joined_df = df_meta.join(aggregated_reviews, "asin", "inner") # Use inner to drop products without reviews, left to keep them
    # # Obtain reviews from product meta
    # review_in_joined = joined_df.select(explode("reviews").alias("review")).select("review.overall", "review.verified",  "review.reviewTime", "review.reviewerID", "review.style.Size", "review.style.style name",  "review.reviewerName", "review.reviewText", "review.summary", "review.unixReviewTime")
    
    # Statistics per category
    print(f"<-----------Statistics for category {category}-------------->")
    print(f"Review count:{str(df_review.count())}")
    # print(f"Joined review count:{str(review_in_joined.count())}") # For optimized approach
    print(f"Joined review count:{str(df_joined.count())}")
    print(f"Unique product in meta:{str(df_meta.count())}")
    # print(f"Joined product count:{str(joined_df.count())}") # For optimized approach
    print(f"Joined product count:{str(df_joined.groupBy('asin').count().count())}")
    print(f"Unique product in review:{str(df_review.groupBy('asin').count().count())}")
    ### End of statistics
    for event in events:
        start = event.get_scrapping_start_date_unix()
        date = event.get_event_date_unix()
        end = event.get_scrapping_end_date_unix()
        query = f"""
        SELECT
            'Span Before' AS time_span,
            COUNT(*) as review_count,
            AVG(overall) as average_rating,
            ds_category
        FROM
            reviews
        WHERE unixReviewTime BETWEEN {start} AND {date}
        GROUP BY ds_category, 'Span Before'
        UNION
        SELECT
            'Span After' AS time_span,
            COUNT(*) as review_count,
            AVG(overall) as average_rating,
            ds_category
        FROM
            reviews
        WHERE unixReviewTime BETWEEN {date} AND {end}
        GROUP BY ds_category, 'Span After'
        """
        result = spark.sql(query)
        results_per_event[event.name].append(result)


# save results to files
for event_name, dfs in results_per_event.items():
    combined_df = dfs[0]
    output_path = os.path.join(output_directory, f"{event_name}")
    for df in dfs[1:]:
        combined_df = combined_df.union(df)
    combined_df.coalesce(1).write.csv(output_path, mode='overwrite', header=True)

