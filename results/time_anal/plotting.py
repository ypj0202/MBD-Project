import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("result.csv")
df['reviewDate'] = pd.to_datetime(df['reviewDate'], dayfirst=True)
# filter from 2010 to 2018.
df = df[df['reviewDate'].dt.year >= 2010]
reviews_per_day = df.groupby('reviewDate')['num_of_reviews'].sum()
avg_score_per_day = (df.groupby('reviewDate').apply(lambda x: (x['avg_score'] * x['num_of_reviews']).sum()
                                                     / x["num_of_reviews"].sum()))

plot_df = pd.DataFrame({
    "total_reviews": reviews_per_day,
    "avg_score": avg_score_per_day
}).reset_index()


## CHATGPTPTTOOTPTPTPPTPTPT
date_range = pd.date_range(start=plot_df['reviewDate'].min(), end=plot_df['reviewDate'].max(), periods=33)

# Create 5 separate plots
for i in range(32):
    start_date = date_range[i]
    end_date = date_range[i + 1]

    # Filter data for the current interval
    mask = (plot_df['reviewDate'] >= start_date) & (plot_df['reviewDate'] < end_date)
    interval_df = plot_df[mask]

    # Create subplot for the current interval
    fig, axs = plt.subplots(2, figsize=(12, 8))

    # Total Reviews per Day
    axs[0].bar(interval_df['reviewDate'], interval_df['total_reviews'], color='blue')
    axs[0].set_title(f'Total Reviews per Day ({start_date.date()} to {end_date.date()})')
    axs[0].set_xlabel('Date')
    axs[0].set_ylabel('Number of Reviews')

    # Average Score per Day
    axs[1].plot(interval_df['reviewDate'], interval_df['avg_score'], marker='o', color='green')
    axs[1].set_title(f'Average Score per Day ({start_date.date()} to {end_date.date()})')
    axs[1].set_xlabel('Date')
    axs[1].set_ylabel('Average Score')

    plt.tight_layout()
    plt.savefig(f'figures/2010_2018/total_review_plots_{i+1}.png')
    plt.close()

