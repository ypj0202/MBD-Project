####### CHATGPT GENERATED CONTENT ########

import pandas as pd
import matplotlib.pyplot as plt

# Assuming the data is in a CSV file named 'data.csv'
# Load the CSV data
data = pd.read_csv('Boston_Marathon_bombing_2013.csv')

# Separate the data into 'before' and 'after'
before_data = data[data['time_span'] == 'Span Before']
after_data = data[data['time_span'] == 'Span After']

# List of unique categories
categories = data['category'].unique()

# Plot for Average Rating
plt.figure(figsize=(10, 8))
for category in categories:
    # Get the average rating for 'before' and 'after'
    before_avg_rating = before_data[before_data['category'] == category]['average_rating']
    after_avg_rating = after_data[after_data['category'] == category]['average_rating']

    # Plot
    plt.barh(category + " Before", before_avg_rating, color='blue')
    plt.barh(category + " After", after_avg_rating, color='green')

plt.xlabel('Average Rating')
plt.title('Average Rating Before and After by Category')
plt.legend(['Before', 'After'])
plt.tight_layout()
plt.savefig('boston/average_rating_histogram.png')
plt.show()

# Plot for Number of Reviews
plt.figure(figsize=(10, 8))
for category in categories:
    # Get the review count for 'before' and 'after'
    before_review_count = before_data[before_data['category'] == category]['review_count']
    after_review_count = after_data[after_data['category'] == category]['review_count']

    # Plot
    plt.barh(category + " Before", before_review_count, color='blue')
    plt.barh(category + " After", after_review_count, color='green')

plt.xlabel('Number of Reviews')
plt.title('Number of Reviews Before and After by Category')
plt.legend(['Before', 'After'])
plt.tight_layout()
plt.savefig('boston/review_count_histogram.png')
plt.show()
