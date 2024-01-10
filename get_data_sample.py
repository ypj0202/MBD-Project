output_path = "sample_data/amazon_reviews"
input_path = "/data/doina/Amazon"
category_names = [
    'Amazon_Instant_Video',
    'Apps_for_Android',
    'Automotive',
    'Books',
    'CDs_and_Vinyl',
    'Cell_Phones_and_Accessories',
    'Clothing_Shoes_and_Jewelry',
    'Digital_Music',
    'Electronics',
    'Health_and_Personal_Care',
    'Home_and_Kitchen',
    'Kindle_Store',
    'Movies_and_TV',
    'Musical_Instruments',
    'Office_Products',
    'Sports_and_Outdoors',
    'Tools_and_Home_Improvement',
    'Toys_and_Games',
    'Video_Games'
]
file_format = "json.gz"
tags = ["meta", "reviews"]
commands = ""
for category in category_names:
    for tag in tags:
        input_file_path = f"{input_path}/{tag}_{category}.{file_format}"
        output_file_path = f"{output_path}/{tag}_{category}.json"
        command = f"hdfs dfs -text {input_file_path} | head -n 100 > {output_file_path}"
        commands += f"{command}; "
print(commands)