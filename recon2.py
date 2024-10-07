from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws
from functools import reduce

# Initialize Spark session
spark = SparkSession.builder.appName("CSV Differences").getOrCreate()

# Load the CSV files into DataFrames
df_old = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA.csv", header=True, inferSchema=True)
df_new = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA_4.csv", header=True, inferSchema=True)


# Alias the DataFrames for easier reference
df_old_alias = df_old.alias("old")
df_new_alias = df_new.alias("new")

# Define the key column(s) for joining (e.g., 'id')
key_column = 'id'  # Replace 'id' with your actual key column

# Step 1: Find Updates (Records present in both, but with changed data)
# Get the list of common columns except the key column
common_columns = [c for c in df_old.columns if c in df_new.columns and c != key_column]

# Dynamically build the comparison conditions for all common columns
conditions = [col(f"new.{c}") != col(f"old.{c}") for c in common_columns]

# Combine the conditions using logical OR
if conditions:
    comparison_expr = reduce(lambda x, y: x | y, conditions)
else:
    comparison_expr = lit(False)  # If no columns, no comparison is needed

# Find updates by performing an inner join on the key column and applying the dynamic comparison expression
updates = df_new_alias.join(df_old_alias, on=key_column, how='inner').filter(comparison_expr)

# Select the columns from `df_new` after the join (to avoid ambiguity)
updates = updates.select([col(f"new.{c}").alias(c) for c in df_new.columns]).withColumn("Change_Type", lit("Update"))

# Step 2: Find Insertions (Records present in new DataFrame but not in old DataFrame)
insertions = df_new_alias.join(df_old_alias, on=key_column, how='left_anti')
insertions = insertions.withColumn("Change_Type", lit("Insert"))

# Step 3: Find Deletions (Records present in old DataFrame but not in new DataFrame)
deletions = df_old_alias.join(df_new_alias, on=key_column, how='left_anti')
deletions = deletions.withColumn("Change_Type", lit("Delete"))

# Step 4: Prepare final output with gaps and labels
# Convert all rows to string format with separator for each value
updates_output = updates.withColumn("row", concat_ws(", ", *[col(c) for c in updates.columns]))
insertions_output = insertions.withColumn("row", concat_ws(", ", *[col(c) for c in insertions.columns]))
deletions_output = deletions.withColumn("row", concat_ws(", ", *[col(c) for c in deletions.columns]))

# Convert each DataFrame to a list of strings with a label
updates_list = ["*** UPDATES ***"] + [row['row'] for row in updates_output.collect()] + [""]  # Add a gap
insertions_list = ["*** INSERTIONS ***"] + [row['row'] for row in insertions_output.collect()] + [""]  # Add a gap
deletions_list = ["*** DELETIONS ***"] + [row['row'] for row in deletions_output.collect()] + [""]  # Add a gap

# Combine all lists
final_output_list = updates_list + insertions_list + deletions_list

# Step 5: Write the final output to a file
with open("final_changes_with_labels2.csv", "w") as file:
    for line in final_output_list:
        file.write(line + "\n")

print("Output file with labeled changes has been written successfully!")
