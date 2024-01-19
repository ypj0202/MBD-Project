import pandas as pd
from datetime import datetime as dt
import matplotlib.pyplot as plt

data = pd.read_json("result.json", lines=True)
output_dir_total = "total_results/"
output_dir_cat_day = "category_weekday_results/"
output_dir_cat_month = "category_month_results/"

categories = data["category"].unique() # 29
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
date_format = '%Y-%m-%d'

# num of reviews to int
data["num_of_reviews"] = data["num_of_reviews"].apply(int)
# change date to day of week
data_days = data.copy()
data_days["reviewDate"] = data_days["reviewDate"].apply(lambda x: dt.strptime(x,date_format).weekday())

# change date to month
data_month = data.copy()
data_month["reviewDate"] = data_month["reviewDate"].apply(lambda x: dt.strptime(x,date_format).month)


data_day_list = [] # data for each weekday
total_day_list = [] # total amount of reviews per weekday
category_day_list = [[pd.DataFrame() for _ in range(7)] for _ in range(len(categories))] # per category per weekday
category_total_day_list = [[0 for _ in range(7)] for _ in range(len(categories))] # total reviews per weekday per category
# [row][col]

# reviewDate, category, num_of_reviews, top_products_json -> asin, num_of_reviews, title, date (often empty)

for i in range(7):
    data_day_list.append(data_days.loc[data_days["reviewDate"] == i])

    # total amount of reviews per day of the week
    total_day_list.append(data_day_list[i]["num_of_reviews"].sum())

    counter = 0
    for cat in categories:
        category_day_list[counter][i] = data_day_list[i].loc[data_day_list[i]["category"] == cat]
        category_total_day_list[counter][i] = category_day_list[counter][i]["num_of_reviews"].sum()
        counter = counter + 1


data_month_list = []
total_month_list = []
category_month_list = [[pd.DataFrame() for _ in range(12)] for _ in range(len(categories))]
category_total_month_list = [[0 for _ in range(12)] for _ in range(len(categories))]

for i in range(12):
    data_month_list.append(data_month.loc[data_month["reviewDate"] == i+1])

    # total amount of reviews per day of the week
    total_month_list.append(data_month_list[i]["num_of_reviews"].sum())

    counter = 0
    for cat in categories:
        category_month_list[counter][i] = data_month_list[i].loc[data_month_list[i]["category"] == cat]
        category_total_month_list[counter][i] = category_month_list[counter][i]["num_of_reviews"].sum()
        counter = counter + 1


plt.figure(figsize=(12, 6))

# total reviews per day
plt.bar(days, [x / 1e6 for x in total_day_list])
plt.title("total reviews per weekday")
plt.ylabel("number of reviews (millions)")
plt.savefig(output_dir_total+"tot_rev_day.png")
plt.close()

# total reviews per day per category
for i in range(len(categories)):
    plt.figure(figsize=(12, 6))
    plt.bar(days, [x / 1e6 for x in category_total_day_list[i][:]])
    plt.title(f"total reviews per weekday {categories[i]}")
    plt.ylabel("number of reviews (millions)")
    plt.savefig(output_dir_cat_day+categories[i]+".png")
    plt.close()


# total reviews per month
plt.bar(months, [x / 1e6 for x in total_month_list])
plt.title("total reviews per month")
plt.ylabel("number of reviews (millions)")
plt.savefig(output_dir_total+"tot_rev_month.png")
plt.close()

# total reviews per month per category
for i in range(len(categories)):
    plt.figure(figsize=(12, 6))
    plt.bar(months, [x / 1e6 for x in category_total_month_list[i][:]])
    plt.title(f"total reviews per month {categories[i]}")
    plt.ylabel("number of reviews (millions)")
    plt.savefig(output_dir_cat_month+categories[i]+".png")
    plt.close()


# test = "1996-05-20"
# date = dt.strptime(test, date_format)
# print(date.weekday()==0)
# print(date)