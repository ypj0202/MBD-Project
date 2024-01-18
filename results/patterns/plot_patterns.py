import json
import pandas as pd
import numpy as np
from datetime import datetime as dt
import matplotlib.pyplot as plt

data = pd.read_json("result.json", lines=True)
output_dir_day = "weekday_results/"
output_dir_cat_day = "category_weekday_results/"

categories = data["category"].unique() # 29
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
date_format = '%Y-%m-%d'

# change date to day of week
data["reviewDate"] = data["reviewDate"].apply(lambda x: dt.strptime(x,date_format).weekday())
# num of reviews to int
data["num_of_reviews"] = data["num_of_reviews"].apply(int)

data_list = [] # data for each weekday
total_list = [] # total amount of reviews per weekday
category_list = [[pd.DataFrame() for _ in range(7)] for _ in range(len(categories))] # per category per weekday
category_total_list = [[0 for _ in range(7)] for _ in range(len(categories))] # total reviews per weekday per category
# [row][col]

# reviewDate, category, num_of_reviews, top_products_json -> asin, num_of_reviews, title, date (often empty)

for i in range(7):
    data_list.append(data.loc[data["reviewDate"] == i])

    # total amount of reviews per day of the week
    total_list.append(data_list[i]["num_of_reviews"].sum())

    counter = 0
    for cat in categories:
        category_list[counter][i] = data_list[i].loc[data_list[i]["category"] == cat]
        category_total_list[counter][i] = category_list[counter][i]["num_of_reviews"].sum()
        counter = counter + 1



plt.figure(figsize=(12, 6))

# total reviews per day
plt.bar(days, [x/1e6 for x in total_list])
plt.title("total reviews per weekday")
plt.ylabel("number of reviews (millions)")
plt.savefig(output_dir_day+"tot_rev_day.png")
plt.close()

# total reviews per day per category
for i in range(len(categories)):
    plt.figure(figsize=(12, 6))
    plt.bar(days, [x/1e6 for x in category_total_list[i][:]])
    plt.title(f"total reviews per weekday {categories[i]}")
    plt.ylabel("number of reviews (millions)")
    plt.savefig(output_dir_cat_day+categories[i]+".png")
    plt.close()



# test = "1996-05-20"
# date = dt.strptime(test, date_format)
# print(date.weekday()==0)
# print(date)