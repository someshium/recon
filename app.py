import logging

from flask import Flask, request, jsonify, send_file
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws
from functools import reduce
import os

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("CSV Differences").getOrCreate()

UPLOAD_FOLDER = '/path/to/uploaded/files'
PROCESSED_FOLDER = '/path/to/processed/files'

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER

# Ensure upload and processed folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)


@app.route('/upload', methods=['POST'])
def upload_files():
    # Log request details for debugging
    app.logger.info("Received request: %s", request)

    # Log request headers and form data
    app.logger.info("Request headers: %s", request.headers)
    app.logger.info("Request form data: %s", request.form)
    app.logger.info("Request files: %s", request.files)
    # Ensure two files are uploaded
    if 'initialFile' not in request.files or 'finalFile' not in request.files:
        return jsonify({"error": "Please upload both 'old_file' and 'new_file'"}), 400

    old_file = request.files['initialFile']
    new_file = request.files['finalFile']

    old_file_path = os.path.join(app.config['UPLOAD_FOLDER'], old_file.filename)
    new_file_path = os.path.join(app.config['UPLOAD_FOLDER'], new_file.filename)

    # Save files to server
    old_file.save(old_file_path)
    new_file.save(new_file_path)

    # Process the files and create the diff output
    try:
        output_file = process_files(old_file_path, new_file_path)
        return send_file(output_file, as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def process_files(old_file_path, new_file_path):
    # Load the CSV files into DataFrames
    df_old = spark.read.csv(old_file_path, header=True, inferSchema=True)
    df_new = spark.read.csv(new_file_path, header=True, inferSchema=True)

    # Define the key column(s) for joining (e.g., 'id')
    key_column = 'id'  # Replace 'id' with your actual key column

    # Find Updates (Records present in both, but with changed data)
    common_columns = [c for c in df_old.columns if c in df_new.columns and c != key_column]
    conditions = [col(f"new.{c}") != col(f"old.{c}") for c in common_columns]

    if conditions:
        comparison_expr = reduce(lambda x, y: x | y, conditions)
    else:
        comparison_expr = lit(False)

    df_old_alias = df_old.alias("old")
    df_new_alias = df_new.alias("new")

    updates = df_new_alias.join(df_old_alias, on=key_column, how='inner').filter(comparison_expr)
    updates = updates.select([col(f"new.{c}").alias(c) for c in df_new.columns]).withColumn("Change_Type",
                                                                                            lit("Update"))

    insertions = df_new_alias.join(df_old_alias, on=key_column, how='left_anti')
    insertions = insertions.withColumn("Change_Type", lit("Insert"))

    deletions = df_old_alias.join(df_new_alias, on=key_column, how='left_anti')
    deletions = deletions.withColumn("Change_Type", lit("Delete"))

    updates_output = updates.withColumn("row", concat_ws(", ", *[col(c) for c in updates.columns]))
    insertions_output = insertions.withColumn("row", concat_ws(", ", *[col(c) for c in insertions.columns]))
    deletions_output = deletions.withColumn("row", concat_ws(", ", *[col(c) for c in deletions.columns]))

    updates_list = ["*** UPDATES ***"] + [row['row'] for row in updates_output.collect()] + [""]
    insertions_list = ["*** INSERTIONS ***"] + [row['row'] for row in insertions_output.collect()] + [""]
    deletions_list = ["*** DELETIONS ***"] + [row['row'] for row in deletions_output.collect()] + [""]

    final_output_list = updates_list + insertions_list + deletions_list

    output_file_path = os.path.join(app.config['PROCESSED_FOLDER'], 'difference.csv')

    # Write the final output to a CSV file
    with open(output_file_path, "w") as file:
        for line in final_output_list:
            file.write(line + "\n")

    return output_file_path


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
