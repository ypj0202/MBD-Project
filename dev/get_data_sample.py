from Categories import Categories

output_path = "../sample_data/amazon_reviews"
input_path = "/user/s2773430/data/Amazon_2018"
category_names = Categories.get_categories()

file_format = "json.gz"
tags = ["reviews"]
commands = ""
for category in category_names:
    for tag in tags:
        input_file_path = f"{input_path}/{tag}_{category}.{file_format}"
        output_file_path = f"{output_path}/{tag}_{category}.json"
        command = f"hdfs dfs -text {input_file_path} | head -n 200 > {output_file_path}"
        commands += f"{command}; "
print(commands)