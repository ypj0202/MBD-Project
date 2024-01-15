import pandas as pd
import matplotlib.pyplot as plt

# getmerge from hadoop also merged headers, so we need to remove them (by hand).
df = pd.read_csv("result.csv")

df['reviewDate'] = pd.to_datetime(df['reviewDate'], dayfirst=True)
reviews_per_day = df.groupby('reviewDate')['num_of_reviews'].sum()
avg_score_per_day = (df.groupby('reviewDate').apply(lambda x: (x['avg_score'] * x['num_of_reviews']).sum()
                                                              / x["num_of_reviews"].sum()))

plot_df = pd.DataFrame({
    "total_reviews": reviews_per_day,
    "avg_score": avg_score_per_day
    }).reset_index()

# Plotting
fig, axs = plt.subplots(2, figsize=(25, 8))

# Total Reviews per Day
axs[0].bar(plot_df['reviewDate'], plot_df['total_reviews'], color='blue')
axs[0].set_title('Total Reviews per Day')
axs[0].set_xlabel('Date')
axs[0].set_ylabel('Number of Reviews')

# Average Score per Day
axs[1].plot(plot_df['reviewDate'], plot_df['avg_score'], marker='o', color='green')
axs[1].set_title('Average Score per Day')
axs[1].set_xlabel('Date')
axs[1].set_ylabel('Average Score')

plt.tight_layout()
plt.show()
plt.savefig('figures/total_review_plots.png')
