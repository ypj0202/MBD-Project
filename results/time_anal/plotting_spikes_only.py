import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

df = pd.read_csv("result.csv")
df['reviewDate'] = pd.to_datetime(df['reviewDate'], dayfirst=True)

reviews_per_day = df.groupby('reviewDate')['num_of_reviews'].sum()


####### CHATGPT CHATGPT CHATGPT CHATGPT CHATGPT COPYRIGHT COPYRIGHT NOT PLAGIARISM DONT KICK MY BALLS #######
# Identify spikes
spike_threshold = 1.5  # Threshold ratio
dataset_lookup = 20
spike_days = []

for i in range(dataset_lookup, len(reviews_per_day) - dataset_lookup):
    prev_day = reviews_per_day.iloc[i - 1]
    current_day = reviews_per_day.iloc[i]
    next_day = reviews_per_day.iloc[i + 1]

    if current_day > prev_day * spike_threshold and current_day > next_day * spike_threshold:
        spike_days.append(reviews_per_day.index[i])

# Create and save a plot for each spike
for day in spike_days:
    start_date = day - pd.Timedelta(days=10)
    end_date = day + pd.Timedelta(days=10)
    date_range = pd.date_range(start_date, end_date, freq='D')  # Generate a range of dates
    data_slice = reviews_per_day.reindex(date_range, fill_value=0)  # Reindex to include all dates

    plt.figure(figsize=(12, 6))
    # Plot data points with value > 0 in blue
    plt.plot(data_slice.index[data_slice > 0], data_slice[data_slice > 0], label='Reviews per Day', marker='o',
             color='blue')

    # Plot data points with value == 0 in red
    plt.scatter(data_slice.index[data_slice == 0], [0] * (data_slice == 0).sum(), color='red', label='Zero Reviews')
    plt.title(f"Spike on {day.strftime('%Y-%m-%d')}")
    plt.xlabel('Date')
    plt.ylabel('Number of Reviews')

    # Format the x-axis to show each date
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f'figures/spikes/spike_{day.strftime("%Y-%m-%d")}.png')
    plt.close()  # Close the figure to free memory
