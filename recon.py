# from pyspark.sql import SparkSession
#
# # Initialize Spark session
# spark = SparkSession.builder.appName("CSV Differences").getOrCreate()
#
# # Load the CSV files into DataFrames
# df_old = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA.csv", header=True, inferSchema=True)
# df_new = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA_2.csv", header=True, inferSchema=True)
#
# # Find additions (rows in new file not in old file)
# additions = df_new.subtract(df_old)
#
# # Find deletions (rows in old file not in new file)
# deletions = df_old.subtract(df_new)
#
# # Assuming a primary key column 'id' to detect updates
# updates = df_new.join(df_old, on='id', how='inner').filter(df_new.col != df_old.col)
#
# # Save the results
# additions.write.csv("additions.csv")
# deletions.write.csv("deletions.csv")
# updates.write.csv("updates.csv")
# import logging
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit, col, reduce
#
# # Initialize Spark session
# spark = SparkSession.builder.appName("CSV Differences").getOrCreate()
#
# # Load the CSV files into DataFrames
# df_old = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA.csv", header=True, inferSchema=True)
# df_new = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA_2.csv", header=True, inferSchema=True)
#
# # Find additions (rows in new file not in old file)
# additions = df_new.subtract(df_old).withColumn("Change_Type", lit("Addition"))
#
# # Find deletions (rows in old file not in new file)
# deletions = df_old.subtract(df_new).withColumn("Change_Type", lit("Deletion"))
#
# # Get the list of common columns (assuming both DataFrames have the same schema)
# common_columns = [col for col in df_old.columns if col in df_new.columns]
#
# # Dynamically build the comparison conditions for all common columns
# conditions = [col(f"new.{c}") != col(f"old.{c}") for c in common_columns]
#
# # Combine the conditions with logical OR (to detect changes in any column)
# comparison_expr = reduce(lambda x, y: x | y, conditions)
#
# # Assuming a primary key column 'id' to detect updates
# # Compare specific columns (e.g., 'name' and 'value') between df_new and df_old
# updates = df_new.join(df_old, on='id', how='inner').filter(comparison_expr)
#
# # Add a "Change_Type" column to signify updates
# updates = updates.withColumn("Change_Type", lit("Update"))
#
# # Create gap rows (empty DataFrame with the same schema)
# gap = spark.createDataFrame([tuple([None for _ in df_new.columns])])
#
# # Label rows to indicate the start of each section
# addition_label = spark.createDataFrame([tuple(["--- Additions ---"] + [None]*(len(df_new.columns)-1))])
# deletion_label = spark.createDataFrame([tuple(["--- Deletions ---"] + [None]*(len(df_new.columns)-1))])
# update_label = spark.createDataFrame([tuple(["--- Updates ---"] + [None]*(len(df_new.columns)-1))])
#
# logging.info("creating final file")
# # Combine the results and gaps into one DataFrame
# combined = (addition_label.union(gap)
#             .union(additions)
#             .union(gap)
#             .union(deletion_label)
#             .union(gap)
#             .union(deletions)
#             .union(gap)
#             .union(update_label)
#             .union(gap)
#             .union(updates))
# logging.info("complete!!")
#
# # Write the combined result to a single CSV file
# combined.write.csv("combined_result.csv", header=True)
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from functools import reduce

# Initialize Spark session
spark = SparkSession.builder.appName("CSV Differences").getOrCreate()

# Load the CSV files into DataFrames
df_old = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA.csv", header=True, inferSchema=True)
df_new = spark.read.csv("C:/Users/somes/Downloads/MOCK_DATA_2.csv", header=True, inferSchema=True)

# Alias the DataFrames for easier reference
df_old_alias = df_old.alias("old")
df_new_alias = df_new.alias("new")

# Get the list of common columns (assuming both DataFrames have the same schema)
common_columns = [col for col in df_old.columns if col in df_new.columns]

# Dynamically build the comparison conditions for all common columns
conditions = [col(f"new.{c}") != col(f"old.{c}") for c in common_columns]

# Combine the conditions with logical OR (to detect changes in any column)
if conditions:
    comparison_expr = reduce(lambda x, y: x | y, conditions)
else:
    comparison_expr = lit(False)  # Default comparison if no columns to compare

# Perform the join on the 'id' column and apply the dynamic comparison expression
updates = df_new_alias.join(df_old_alias, on='id', how='inner').filter(comparison_expr)

# Add a "Change_Type" column to signify updates
updates = updates.withColumn("Change_Type", lit("Update"))

# Show the result (you can write it to a file later)
updates.show()

# Find additions (rows in new file not in old file)
additions = df_new.subtract(df_old).withColumn("Change_Type", lit("Addition"))
additions.show()

# Find deletions (rows in old file not in new file)
deletions = df_old.subtract(df_new).withColumn("Change_Type", lit("Deletion"))
deletions.show()


gap = spark.createDataFrame([tuple([None for _ in df_new.columns])])

# Label rows to indicate the start of each section
addition_label = spark.createDataFrame([tuple(["--- Additions ---"] + [None]*(len(df_new.columns)-1))])
deletion_label = spark.createDataFrame([tuple(["--- Deletions ---"] + [None]*(len(df_new.columns)-1))])
update_label = spark.createDataFrame([tuple(["--- Updates ---"] + [None]*(len(df_new.columns)-1))])

logging.info("creating final file")
# Combine the results and gaps into one DataFrame
combined = (addition_label.union(gap)
            .union(additions)
            .union(gap)
            .union(deletion_label)
            .union(gap)
            .union(deletions)
            .union(gap)
            .union(update_label)
            .union(gap)
            .union(updates))
logging.info("complete!!")

# Write the combined result to a single CSV file
combined.write.csv("combined_result.csv", header=True)
