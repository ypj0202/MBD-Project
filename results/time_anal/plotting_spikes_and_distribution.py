
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
pd.options.mode.chained_assignment = None
import datetime
import calendar
from collections import Counter
# Load json file result.json and save in variable data
master_df = {}
input_data = pd.read_json('../../result.json', lines=True)
# Split data into categories and save in dictionary master_df
categories = input_data['category'].unique()
for category in categories:
    master_df[category] = input_data[input_data['category'] == category]

# Identify spikes and print top_product_json# Threshold for identifying spikes, in standard deviations
tmp = []
total = 0

for category_df in categories:
    df = master_df[category_df]
    df['rolling_mean'] = df['num_of_reviews'].rolling(window=7).mean()
    df['rolling_std'] = df['num_of_reviews'].rolling(window=7).std()
    # Identify spikes - reviews more than 2 standard deviations above the rolling mean
    df['threshold'] = df['rolling_mean'] + 2 * df['rolling_std']
    def extract_num_of_reviews(row):
        try:
            # If the data is in string format, uncomment the next line to parse it
            # row = json.loads(row)
            return row[0]['num_of_reviews'] if row else None
        except (IndexError, KeyError, TypeError):
            return None
    df['top_product_count'] = df['top_products_json'].apply(extract_num_of_reviews)
    df['spike'] = (df['num_of_reviews'] > df['threshold']) & (df['num_of_reviews'] > 500) & \
    (df['reviewDate'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d")) >= datetime.datetime.strptime('2010-01-01', "%Y-%m-%d")) \
    & (df['top_product_count'] > 500)
    total += df['spike'].sum()
    tmp.append({'category':category_df,'count':df['spike'].sum()})

    # Convert string dates to datetime objects
    date_objects = [datetime.datetime.strptime(date, '%Y-%m-%d') for date in df[df['spike']]['reviewDate']]

    # Extract months and weekdays
    months = [date.month for date in date_objects]
    weekdays = [date.weekday() for date in date_objects]

    # Count the frequencies
    month_counts = Counter(months)
    weekday_counts = Counter(weekdays)

    # Plot for monthly distribution
    plt.figure(figsize=(10, 5), dpi=1200)
    plt.bar(month_counts.keys(), month_counts.values())
    plt.xlabel('Month')
    plt.ylabel('Count')
    plt.title(f'Distribution of spikes by Month for category {category_df}')
    plt.xticks(range(1, 13), [calendar.month_abbr[i] for i in range(1, 13)])
    # plt.show()
    plt.savefig(f'./figures/spikes-distribution/spike-dis-Month-{category_df}.png')

    # Plot for weekday distribution
    plt.figure(figsize=(10, 5), dpi=1200)
    plt.bar(weekday_counts.keys(), weekday_counts.values())
    plt.xlabel('Weekday')
    plt.ylabel('Count')
    plt.title(f'Distribution of spikes by Weekday for category {category_df}')
    plt.xticks(range(7), [calendar.day_name[i] for i in range(7)])
    plt.savefig(f'./figures/spikes-distribution/spike-dis-Week-days-{category_df}.png')
    plt.show()

    for date in df[df['spike']]['reviewDate']:
        # Select data 20 days before and after the spike
            start_date = pd.to_datetime(date) - pd.Timedelta(days=7)
            end_date = pd.to_datetime(date) + pd.Timedelta(days=7)
            window_df = df[(pd.to_datetime(df['reviewDate']) >= start_date) & (pd.to_datetime(df['reviewDate']) <= end_date)]
            # Get item by date
            spike_dataPoint = window_df[window_df['reviewDate'] == date]
            # Plotting
            plt.figure(figsize=(10, 5), dpi=800)
            plt.xticks(rotation=45, ha="right")
            plt.plot(window_df['reviewDate'], window_df['num_of_reviews'], label='Number of Reviews')
            plt.plot(window_df['reviewDate'], window_df['rolling_mean'], label='7-Day Rolling Mean')
            plt.axvline(x=date, color='r', linestyle='--', label='Spike Date')
            plt.xlabel('Date')
            plt.ylabel('Number of Reviews')
            plt.title(f'Reviews 7 Days Before and After Spike on {date} for {category_df}')
            plt.annotate(f"Top product: {str(dict(list(spike_dataPoint['top_products_json'].iloc[0])[0])['title'])} with {str(dict(list(spike_dataPoint['top_products_json'].iloc[0])[0])['num_of_reviews'])} reviews\nTotal Reviews: {str(spike_dataPoint['num_of_reviews'].iloc[0])}",xy = (1.0, -0.3),xycoords='axes fraction',ha='right',va="center",fontsize=10)
            plt.legend()
            plt.tight_layout()
            plt.savefig(f'./figures/spikes-new/Spike-{date}-{category_df}.png', dpi=800)
            # plt.show()
print("Total:" + str(total))
# Print tmp in a pretty way
for i in tmp: print(i)


