import csv
import pandas as pd
from datetime import datetime as dt
import numpy as np
import matplotlib.pyplot as plt

data = pd.read_json("result.json", lines=True)
output_dir_total = "total_results/"
output_dir_cat_day = "category_weekday_results/"
output_dir_cat_month = "category_month_results/"
output_dir_dec = "category_dec_results/"
output_popular = output_dir_total + "popular.csv"

plt.rcParams.update({'font.size': 20})
figsize = (15, 6)

categories = data["category"].unique() # 29
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
date_format = '%Y-%m-%d'

# num of reviews to int
data["num_of_reviews"] = data["num_of_reviews"].apply(int)
data["reviewDate"] = data["reviewDate"].apply(lambda x: dt.strptime(x, date_format))
data = data[(data["reviewDate"].dt.year >= 2010) & (data["reviewDate"].dt.year <= 2018)]

most_popular = np.zeros((len(categories)+1,3), dtype=int)
f = open(output_popular, 'w', newline='')
writer = csv.writer(f)

# [row][col]
# reviewDate, category, num_of_reviews, top_products_json -> asin, num_of_reviews, title, date (often empty)


def weekday_calc(data) :
    # change date to day of week
    data_days = data.copy()
    data_days["reviewDate"] = data_days["reviewDate"].apply(lambda x: x.weekday())

    data_day_list = []  # data for each weekday
    total_day_list = []  # total amount of reviews per weekday
    category_day_list = [[pd.DataFrame() for _ in range(7)] for _ in range(len(categories))]  # per category per weekday
    category_total_day_list = [[0 for _ in range(7)] for _ in
                               range(len(categories))]  # total reviews per weekday per category

    for i in range(7):
        data_day_list.append(data_days.loc[data_days["reviewDate"] == i])

        # total amount of reviews per day of the week
        total_day_list.append(data_day_list[i]["num_of_reviews"].sum())

        counter = 0
        for cat in categories:
            category_day_list[counter][i] = data_day_list[i].loc[data_day_list[i]["category"] == cat]
            category_total_day_list[counter][i] = category_day_list[counter][i]["num_of_reviews"].sum()
            counter = counter + 1

    plt.figure(figsize=figsize)

    # total reviews per day
    plt.bar(days, [x / 1e6 for x in total_day_list])
    plt.title("total reviews per weekday")
    plt.ylabel("number of reviews (millions)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir_total + "tot_rev_day.png")
    plt.close()
    popular_day = days[total_day_list.index(max(total_day_list))]
    print(f"all time most popular day is {popular_day}")


    # total reviews per day per category
    for i in range(len(categories)):
        plt.figure(figsize=figsize)
        plt.bar(days, [x / 1e6 for x in category_total_day_list[i][:]])
        plt.title(f"total reviews per weekday {categories[i]}")
        plt.ylabel("number of reviews (millions)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(output_dir_cat_day + categories[i] + ".png")
        plt.close()
        most_popular[i][0] = i # index category
        most_popular[i][1] = category_total_day_list[i][:].index(max(category_total_day_list[i][:])) # index most popular day

    most_popular[len(categories)][1] = np.bincount(most_popular[:len(categories),1]).argmax()



def month_calc(data):
    # change date to month
    data_month = data.copy()
    data_month["reviewDate"] = data_month["reviewDate"].apply(lambda x: x.month)

    data_month_list = []
    total_month_list = []
    category_month_list = [[pd.DataFrame() for _ in range(12)] for _ in range(len(categories))]
    category_total_month_list = [[0 for _ in range(12)] for _ in range(len(categories))]

    for i in range(12):
        data_month_list.append(data_month.loc[data_month["reviewDate"] == i + 1])

        # total amount of reviews per day of the week
        total_month_list.append(data_month_list[i]["num_of_reviews"].sum())

        counter = 0
        for cat in categories:
            category_month_list[counter][i] = data_month_list[i].loc[data_month_list[i]["category"] == cat]
            category_total_month_list[counter][i] = category_month_list[counter][i]["num_of_reviews"].sum()
            counter = counter + 1

    # total reviews per month
    plt.figure(figsize=figsize)
    plt.bar(months, [x / 1e6 for x in total_month_list])
    plt.title("total reviews per month")
    plt.ylabel("number of reviews (millions)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir_total + "tot_rev_month.png")
    plt.close()

    popular_month = months[total_month_list.index(max(total_month_list))]
    print(f"all time most popular month is {popular_month}")


    # total reviews per month per category
    for i in range(len(categories)):
        plt.figure(figsize=figsize)
        plt.bar(months, [x / 1e6 for x in category_total_month_list[i][:]])
        plt.title(f"total reviews per month {categories[i]}")
        plt.ylabel("number of reviews (millions)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(output_dir_cat_month + categories[i] + ".png")
        plt.close()

        most_popular[i][2] = category_total_month_list[i][:].index(max(category_total_month_list[i][:]))  # index most popular day

    most_popular[len(categories)][2] = np.bincount(most_popular[:len(categories)-1,2]).argmax()


def december_calc(data):
    data_month = data.copy()
    data_dec = data_month.loc[data_month["reviewDate"].dt.month == 12]

    dec_list = []
    dec_list_total = []
    category_dec_list = np.zeros((len(categories),31)) # days horizontal, categories vertical

    for i in range(31):
        # per day in december from all years
        dec_list.append(data_dec.loc[data_dec["reviewDate"].dt.day == i + 1])
        dec_list_total.append(dec_list[i]["num_of_reviews"].sum())

        counter = 0
        for cat in categories:
            category_dec_list[counter,i] = dec_list[i].loc[dec_list[i]["category"] == cat]["num_of_reviews"].sum()
            counter = counter + 1


    # total reviews in december
    plt.figure(figsize=figsize)
    plt.bar(list(range(1,32)), [y / 1e6 for y in dec_list_total])
    plt.title(f"total reviews per day in december total")
    plt.ylabel("number of reviews (millions")
    plt.xlabel("day of the month")
    plt.savefig(output_dir_total + "tot_rev_dec.png")


    # total reviews per day per category
    for i in range(len(categories)):
        plt.figure(figsize=figsize)
        plt.bar(list(range(1,32)), [y / 1e6 for y in category_dec_list[i][:]])
        plt.title(f"total reviews per day in december {categories[i]}")
        plt.ylabel("number of reviews (millions)")
        plt.xlabel("day of the month")
        plt.savefig(output_dir_dec + categories[i] + ".png")
        plt.close()




weekday_calc(data)
month_calc(data)
# december_calc(data)

print(most_popular)
writer.writerow(["category", "weekday", "month"])

for i in range(len(most_popular)-1):
    writer.writerow([categories[most_popular[i][0]], days[most_popular[i][1]], months[most_popular[i][2]]])

writer.writerow(["total", days[most_popular[len(categories)][1]], months[most_popular[len(categories)][2]]])

f.close()


# print the most popular days and months for each category