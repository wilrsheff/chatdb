import csv
import json
from collections import defaultdict
from collections import Counter
import random
import pprint

# In-memory databases
mongo_db = {}
sql_data = {}

# Helper function: Choose a database
def database(dbms):
    if dbms == "sql":
        print("Choose from SQL databases:\n1. Books\n2. Smartphone Sales\n3. E-Commerce Sales\n4. Upload Dataset")
        choice = int(input("Enter your choice (1-4): "))
        user_data = ""
        if choice == 4:
            user_data = input("Enter your file name: ")
        file_map = {1: "books.csv", 2: "smartphones_sales.csv", 3: "e-commerce_sales.csv", 4: user_data}
        return file_map.get(choice, None)

    elif dbms == "mongodb":
        print("Choose from MongoDB collections:\n1. Phones\n2. Lottery Expenditures\n3. SpongeBob Squarepants Characters\n4. Upload Dataset")
        choice = int(input("Enter your choice (1-4): "))
        user_data = ""
        if choice == 4:
            user_data = input("Enter your file name: ")
        file_map = {1: "phones.json", 2: "lottery_expenditures.json", 3: "spongebob_characters.json", 4: user_data}
        return file_map.get(choice, None)

# Helper function: Ensure integer conversion
def convert_string_to_int(data):
    """
    Recursively converts string representations of integers or floats in a dictionary or list into their numeric types.
    
    :param data: JSON-like dictionary or list to process
    :return: Processed data with numeric fields converted
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str):
                try:
                    # Handle floating-point or integer conversion
                    data[key] = float(value) if '.' in value else int(value)
                except ValueError:
                    pass  # Leave as is if conversion fails
            elif isinstance(value, (dict, list)):
                convert_string_to_int(value)  # Recursively process nested structures
    elif isinstance(data, list):
        for i in range(len(data)):
            if isinstance(data[i], str):
                try:
                    data[i] = float(data[i]) if '.' in data[i] else int(data[i])
                except ValueError:
                    pass
            elif isinstance(data[i], (dict, list)):
                convert_string_to_int(data[i])  # Recursively process nested structures

def preprocess_csv_data(data):
    """
    Preprocess raw CSV data to clean and normalize values.
    Example tasks:
    - Convert numeric strings to integers or floats.
    - Remove leading/trailing whitespace.
    - Standardize null values.
    """
    for row in data:
        for key, value in row.items():
            if isinstance(value, str):
                value = value.strip()  # Trim whitespace
                if value.isdigit():
                    row[key] = int(value)  # Convert to integer
                else:
                    try:
                        row[key] = float(value)  # Convert to float if possible
                    except ValueError:
                        pass  # Leave as string if conversion fails
            elif value is None:
                row[key] = "NULL"  # Standardize null representation
    return data

# Helper function: Load CSV or JSON data
def open_file(file_name, user_name):
    if file_name.endswith(".csv"):
        if user_name.lower() == "wil":
            file_path = "/Users/wilrsheff/Dropbox/Mac/Documents/DSCI 351/Project/MySQL/" + file_name
        else:
            file_path = input("Enter the file path for " + file_name + ": ")
        with open(file_path, mode="r", encoding="ISO-8859-1") as csv_file:
            reader = csv.DictReader(csv_file)
            data = [row for row in reader]
        return preprocess_csv_data(data)
    elif file_name.endswith(".json"):
        if user_name.lower() == "wil":
            file_path = "/Users/wilrsheff/Dropbox/Mac/Documents/DSCI 351/Project/MongoDB/" + file_name
        else:
            file_path = input("Enter the file path for " + file_name + ": ")
        with open(file_path, mode="r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        # Apply conversion to all documents
        for doc in data:
            convert_string_to_int(doc)
        return data

# Initialize in-memory SQL-like structure
def initialize_sql_data(file_name, user_name):
    data = open_file(file_name, user_name)
    table_name = file_name.split(".")[0]
    if data:
        sql_data[table_name] = {
            "columns": list(data[0].keys()),
            "rows": data
        }
        print(f"SQL table '{table_name}' loaded into memory.")
    return table_name

# Helper function: Ensure keys are retrieved from nested JSON objects
def get_all_keys(data, prefix=""):
    """
    Recursively fetch all keys from nested JSON objects.

    :param data: JSON object or dictionary
    :param prefix: Current path in the JSON hierarchy
    :return: List of fully qualified keys
    """
    keys = []
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            keys.extend(get_all_keys(value, full_key))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            full_key = f"{prefix}[{i}]" if prefix else f"[{i}]"
            keys.extend(get_all_keys(item, full_key))
    else:
        keys.append(prefix)
    return keys

# Display database schema and sample data
def explore_database(choice, file_name, user_name):
    if choice == "sql":
        table_name = initialize_sql_data(file_name, user_name)
        columns = sql_data[table_name]["columns"]

        print(f"\nExploring SQL Database: {table_name}")
        print(f"Columns: {', '.join(columns)}")

        print("\nSample Data:")
        for row in sql_data[table_name]["rows"][:5]:
            pprint.pprint(row)
            
    elif choice == "mongodb":
        collection_name = file_name.split(".")[0]
        data = open_file(file_name, user_name)
        mongo_db[collection_name] = data

        print(f"\nExploring MongoDB Collection: {collection_name}")
        print("Attributes:")
        if data:
            keys = get_all_keys(data[0])
            print("\n".join(keys))

        print("\nSample Data:")
        for doc in data[:5]:
            pprint.pprint(doc)

# Helper functions: Preprocess for proper formatting in sql_queries function
def preprocess_data(table_name):
    """
    Infer and convert column types for data already in sql_data.
    Updates numeric_columns for advanced queries.
    """
    # Retrieve rows and columns for the table
    rows = sql_data[table_name]["rows"]
    columns = sql_data[table_name]["columns"]
    
    # Dictionary to store inferred data types for each column
    inferred_types = {}
    for col in columns:
        try:
            # Check if all values in the column can be cast to float
            if all(isinstance(float(row[col]), (int, float)) for row in rows):
                inferred_types[col] = float
        except ValueError:
            # Default to string type if conversion fails
            inferred_types[col] = str

    # Update rows with inferred types
    for row in rows:
        for col, col_type in inferred_types.items():
            try:
                # Apply the inferred type to each cell
                row[col] = col_type(row[col]) if col_type != str else row[col]
            except ValueError:
                pass  # Leave value as is if casting fails

    # Update sql_data with a list of numeric columns for easier querying
    sql_data[table_name]["numeric_columns"] = [
        col for col, col_type in inferred_types.items() if col_type != str
    ]

# SQL Query Generator
def sql_queries(table_name, construct=None, mode="sample"):
    """
    Dynamically generates SQL queries based on the table structure and selected constructs.
    Handles sample query generation and query-by-construct modes.

    :param table_name: Name of the SQL table.
    :param construct: Specific SQL construct to generate queries for (optional).
    :param mode: "sample" for generating sample queries, "construct" for query-by-construct.
    :return: List of tuples containing (query, natural language, simulated output).
    """
    preprocess_data(table_name)  # Ensure data types are inferred correctly

    columns = sql_data[table_name]["columns"]
    numeric_columns = sql_data[table_name].get("numeric_columns", [])
    rows = sql_data[table_name]["rows"]
    templates = []
    nl_templates = []
    outputs = []

    # Add templates dynamically based on constructs
    if not construct or construct == "group by":
        if columns:
            group_column = random.choice(columns)  # Select a random column for grouping
            query = f"SELECT {group_column}, COUNT(*) FROM {table_name} GROUP BY {group_column};"
            nl = f"Count the number of rows grouped by {group_column}."
            group_counts = Counter(row[group_column] for row in rows)
            simulated_output = dict(group_counts)
            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            templates.append("No query could be generated as there are no columns to group by.")
            nl_templates.append("Generalized GROUP BY query structure: SELECT column, COUNT(*) FROM table_name GROUP BY column;")
            outputs.append(None)

    if not construct or construct == "having":
        if numeric_columns:
            numeric_col = random.choice(numeric_columns)
            numeric_values = [float(row[numeric_col]) for row in rows if isinstance(row[numeric_col], (int, float))]
            if numeric_values:
                min_value = int(min(numeric_values))
                max_value = int(max(numeric_values))
                random_threshold = round(random.uniform(min_value, max_value), 2)
                group_column = random.choice(columns)

                query = f"SELECT {group_column}, AVG({numeric_col}) FROM {table_name} GROUP BY {group_column} HAVING AVG({numeric_col}) > {random_threshold};"
                nl = f"Find rows where the average of {numeric_col} is greater than {random_threshold}, grouped by {group_column}."

                simulated_output = {}
                for group in set(row[group_column] for row in rows if row[group_column] is not None):
                    group_rows = [row for row in rows if row[group_column] == group]
                    group_avg = sum(float(row[numeric_col]) for row in group_rows if row[numeric_col] is not None) / len(group_rows)
                    if group_avg > random_threshold:
                        simulated_output[group] = group_avg

                templates.append(query)
                nl_templates.append(nl)
                outputs.append(simulated_output)
            elif mode == "construct":
                templates.append("No query could be generated as there are no numeric values in the dataset.")
                nl_templates.append("Generalized HAVING query structure: SELECT column, AVG(numeric_column) FROM table_name GROUP BY column HAVING AVG(numeric_column) > threshold;")
                outputs.append(None)
        elif mode == "construct":
            templates.append("No query could be generated as there are no numeric columns in the dataset.")
            nl_templates.append("Generalized HAVING query structure: SELECT column, AVG(numeric_column) FROM table_name GROUP BY column HAVING AVG(numeric_column) > threshold;")
            outputs.append(None)

    if not construct or construct == "order by":
        if columns:
            order_column = random.choice(columns)  # Select a random column for ordering
            query = f"SELECT {order_column} FROM {table_name} ORDER BY {order_column} DESC;"
            nl = f"List all values of {order_column} in descending order."
            # Filter and normalize values for sorting
            valid_values = [row[order_column] for row in rows if isinstance(row[order_column], (int, float, str))]
            normalized_values = [str(value) for value in valid_values]  # Convert all values to strings for sorting
            simulated_output = sorted(normalized_values, reverse=True)
            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            templates.append("No query could be generated as there are no columns to order by.")
            nl_templates.append("Generalized ORDER BY query structure: SELECT column FROM table_name ORDER BY column DESC;")
            outputs.append(None)

    if not construct or construct == "where":
        if columns:
            filter_column = random.choice(columns)
            unique_values = list(set(row[filter_column] for row in rows if row[filter_column] is not None))
            if unique_values:
                selected_value = random.choice(unique_values)
                output_column = random.choice([col for col in columns if col != filter_column])
                query = f"SELECT {output_column} FROM {table_name} WHERE {filter_column} = '{selected_value}';"
                nl = f"Find rows where {filter_column} equals '{selected_value}' and display {output_column}."
                simulated_output = [row[output_column] for row in rows if row[filter_column] == selected_value]
                templates.append(query)
                nl_templates.append(nl)
                outputs.append(simulated_output)
            elif mode == "construct":
                templates.append(f"No query could be generated as the column {filter_column} has no valid values.")
                nl_templates.append("Generalized WHERE query structure: SELECT column FROM table_name WHERE column = 'value';")
                outputs.append(None)
        elif mode == "construct":
            templates.append("No query could be generated as there are no columns available.")
            nl_templates.append("Generalized WHERE query structure: SELECT column FROM table_name WHERE column = 'value';")
            outputs.append(None)

    if not construct or construct == "limit":
        if rows:
            max_limit = int(min(len(rows), 10))  # Limit the maximum to 10 or the number of rows in the dataset
            dynamic_limit = random.randint(1, max_limit)
            selected_columns = random.sample(columns, min(2, len(columns)))  # Choose up to 2 columns

            query = f"SELECT {', '.join(selected_columns)} FROM {table_name} LIMIT {dynamic_limit};"
            nl = f"Display the first {dynamic_limit} rows with columns {', '.join(selected_columns)}."
            simulated_output = [
                {col: row[col] for col in selected_columns} for row in rows[:dynamic_limit]
            ]
            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            templates.append("No query could be generated as there are no rows in the dataset.")
            nl_templates.append("Generalized LIMIT query structure: SELECT columns FROM table_name LIMIT number;")
            outputs.append(None)

    if not construct or construct == "join":
        if len(columns) >= 2:
            another_table_name = f"{table_name}_alias"
            join_column = random.choice(columns)
            selected_columns = random.sample(columns, min(2, len(columns)))

            query = (
                f"SELECT {table_name}.{selected_columns[0]}, {another_table_name}.{selected_columns[1]} "
                f"FROM {table_name} JOIN {another_table_name} "
                f"ON {table_name}.{join_column} = {another_table_name}.{join_column};"
            )
            nl = (
                f"Join the {table_name} table with an alias of itself ({another_table_name}) on the column {join_column}, "
                f"and display {selected_columns[0]} from the original table and {selected_columns[1]} from the alias table."
            )
            simulated_output = "Simulated output not available for JOIN queries because the alias table is virtual."
            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            templates.append("No query could be generated as there are not enough columns to perform a join.")
            nl_templates.append("Generalized JOIN query structure: SELECT table1.column, table2.column FROM table1 JOIN table2 ON table1.column = table2.column;")
            outputs.append(None)

    if not construct or construct == "like":
        text_columns = [col for col in columns if all(isinstance(row[col], str) for row in rows)]
        if text_columns:
            selected_column = random.choice(text_columns)
            unique_values = list(set(row[selected_column] for row in rows if row[selected_column] is not None))
            if unique_values:
                selected_value = random.choice(unique_values)
                substring = selected_value[:3] if len(selected_value) > 3 else selected_value
                query = f"SELECT {selected_column}, {random.choice(columns)} FROM {table_name} WHERE {selected_column} LIKE '%{substring}%';"
                nl = f"Find rows where {selected_column} contains the text '{substring}' and display {selected_column} and another column."
                simulated_output = [
                    {selected_column: row[selected_column], columns[1]: row[columns[1]]}
                    for row in rows if substring.lower() in str(row[selected_column]).lower()
                ]
                templates.append(query)
                nl_templates.append(nl)
                outputs.append(simulated_output)
            elif mode == "construct":
                templates.append(f"No query could be generated as the column {selected_column} has no valid text values.")
                nl_templates.append("Generalized LIKE query structure: SELECT column FROM table_name WHERE column LIKE '%text%';")
                outputs.append(None)
        elif mode == "construct":
            templates.append("No query could be generated as there are no text-based columns.")
            nl_templates.append("Generalized LIKE query structure: SELECT column FROM table_name WHERE column LIKE '%text%';")
            outputs.append(None)

    if not construct or construct == "range":
        if numeric_columns:
            # Dynamically select a numeric column
            numeric_col = random.choice(numeric_columns)

            # Extract numeric values for the selected column
            numeric_values = [float(row[numeric_col]) for row in rows if isinstance(row[numeric_col], (int, float))]
            if numeric_values:
                # Calculate min and max for the selected column
                min_value = int(min(numeric_values))
                max_value = int(max(numeric_values))

                # Generate a random range
                lower_bound = round(random.uniform(min_value, max_value - 1), 2)
                upper_bound = round(random.uniform(lower_bound + 1, max_value), 2)

                # Dynamically select an additional display column
                range_column = random.choice(columns)

                # Generate the query
                query = f"SELECT {numeric_col}, {range_column} FROM {table_name} WHERE {numeric_col} BETWEEN {lower_bound} AND {upper_bound};"
                nl = f"Find rows where {numeric_col} is between {lower_bound} and {upper_bound} and display {range_column}."

                # Simulate the query output
                simulated_output = [
                    {numeric_col: row[numeric_col], range_column: row[range_column]}
                    for row in rows if lower_bound <= float(row[numeric_col]) <= upper_bound
                ]

                # Append to templates
                templates.append(query)
                nl_templates.append(nl)
                outputs.append(simulated_output)
            elif mode == "construct":
                # Placeholder for when numeric values exist but are not suitable for a range
                templates.append("No query could be generated as the numeric column contains no valid range values.")
                nl_templates.append("Generalized RANGE query structure: SELECT numeric_column, column FROM table_name WHERE numeric_column BETWEEN lower_bound AND upper_bound;")
                outputs.append(None)
        elif mode == "construct":
            # Placeholder for when no numeric columns are available
            templates.append("No query could be generated as there are no numeric columns in the dataset.")
            nl_templates.append("Generalized RANGE query structure: SELECT numeric_column, column FROM table_name WHERE numeric_column BETWEEN lower_bound AND upper_bound;")
            outputs.append(None)

    if not construct or construct == "sum":
        if numeric_columns:
            # Dynamically select a column for grouping
            group_column = random.choice(columns)

            # Dynamically select a numeric column for summing
            numeric_col = random.choice(numeric_columns)

            # Generate the query
            query = f"SELECT {group_column}, SUM({numeric_col}) FROM {table_name} GROUP BY {group_column};"
            nl = f"Calculate the total sum of {numeric_col}, grouped by {group_column}."

            # Simulate the query output
            simulated_output = {
                group: sum(float(row[numeric_col]) for row in rows if row[group_column] == group and row[numeric_col] is not None)
                for group in set(row[group_column] for row in rows if row[group_column] is not None)
            }

            # Append to templates
            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            # Placeholder for when no numeric columns exist
            templates.append("No query could be generated as there are no numeric columns in the dataset.")
            nl_templates.append("Generalized SUM query structure: SELECT column, SUM(numeric_column) FROM table_name GROUP BY column;")
            outputs.append(None)

    # Handle sample query generation mode
    if mode == "sample":
        valid_indices = [i for i, output in enumerate(outputs) if output is not None]
        templates = [templates[i] for i in valid_indices]
        nl_templates = [nl_templates[i] for i in valid_indices]
        outputs = [outputs[i] for i in valid_indices]

    # Return the queries, descriptions, and outputs
    return [(templates[i], nl_templates[i], outputs[i]) for i in range(len(templates))]

# MongoDB Query Generator
def mongodb_queries(collection_name, construct=None, mode="sample"):
    """
    Dynamically generates MongoDB sample queries based on the collection structure and constructs.
    Handles sample query generation and query-by-construct modes.

    :param collection_name: Name of the MongoDB collection.
    :param construct: Specific MongoDB construct to generate queries for (optional).
    :param mode: "sample" for generating sample queries, "construct" for query-by-construct.
    :return: List of tuples containing (query, natural language, simulated output).
    """
    # Ensure the collection exists in the database
    if collection_name not in mongo_db or not mongo_db[collection_name]:
        return [("Error: Collection does not exist or is empty.", "Collection does not exist or is empty.", None)]

    # Get a sample document to infer fields
    sample_doc = mongo_db[collection_name][0]
    keys = list(sample_doc.keys())
    numeric_fields = [key for key in keys if isinstance(sample_doc[key], (int, float))]
    non_numeric_fields = [key for key in keys if key not in numeric_fields]
    templates = []
    nl_templates = []
    outputs = []

    # Add templates dynamically based on constructs
    if not construct or construct == "find":
        query = f"db.{collection_name}.find({{}})"
        nl = f"Find all documents in the {collection_name} collection."
        simulated_output = mongo_db[collection_name][:5]
        templates.append(query)
        nl_templates.append(nl)
        outputs.append(simulated_output)

    if not construct or construct == "projection":
        if keys:
            projection_fields = random.sample(keys, min(2, len(keys)))  # Dynamically select up to 2 fields
            query = f"db.{collection_name}.find({{}}, {{ {', '.join([f'{field}: 1' for field in projection_fields])}, _id: 0 }})"
            nl = f"Find all documents and display only {', '.join(projection_fields)}."
            simulated_output = [{key: doc.get(key) for key in projection_fields} for doc in mongo_db[collection_name][:5]]
            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            templates.append("No query could be generated as there are no fields in the collection.")
            nl_templates.append("Generalized PROJECTION query structure: db.collection.find({}, { field1: 1, field2: 1, _id: 0 });")
            outputs.append(None)

    if not construct or construct == "criteria":
        if numeric_fields:
            numeric_field = random.choice(numeric_fields)  # Dynamically select a numeric field
            numeric_values = [doc[numeric_field] for doc in mongo_db[collection_name] if isinstance(doc.get(numeric_field), (int, float))]
            if numeric_values:
                min_value = int(min(numeric_values))
                max_value = int(max(numeric_values))
                random_threshold = random.randint(min_value, max_value - 1)

                query = f"db.{collection_name}.find({{ {numeric_field}: {{ $gt: {random_threshold} }} }})"
                nl = f"Find documents where {numeric_field} is greater than {random_threshold}."
                simulated_output = [
                    doc for doc in mongo_db[collection_name]
                    if isinstance(doc.get(numeric_field), (int, float)) and doc.get(numeric_field) > random_threshold
                ]
                templates.append(query)
                nl_templates.append(nl)
                outputs.append(simulated_output)
            elif mode == "construct":
                templates.append("No query could be generated as no numeric values exist in the field.")
                nl_templates.append("Generalized CRITERIA query structure: db.collection.find({ numeric_field: { $gt: value } });")
                outputs.append(None)
        elif mode == "construct":
            templates.append("No query could be generated as there are no numeric fields in the collection.")
            nl_templates.append("Generalized CRITERIA query structure: db.collection.find({ numeric_field: { $gt: value } });")
            outputs.append(None)

    if not construct or construct == "conditions":
        if numeric_fields and non_numeric_fields:
            numeric_field = random.choice(numeric_fields)  # Select numeric field dynamically
            non_numeric_field = random.choice(non_numeric_fields)  # Select non-numeric field dynamically

            numeric_values = [doc[numeric_field] for doc in mongo_db[collection_name] if isinstance(doc.get(numeric_field), (int, float))]
            non_numeric_values = list(set(doc[non_numeric_field] for doc in mongo_db[collection_name] if doc.get(non_numeric_field)))

            if numeric_values and non_numeric_values:
                min_value = min(numeric_values)
                max_value = max(numeric_values)
                random_threshold = random.randint(min_value, max_value - 1)
                selected_value = random.choice(non_numeric_values)

                query = f"db.{collection_name}.find({{ {numeric_field}: {{ $gt: {random_threshold} }}, {non_numeric_field}: '{selected_value}' }})"
                nl = f"Find documents where {numeric_field} is greater than {random_threshold} and {non_numeric_field} equals '{selected_value}'."
                simulated_output = [
                    doc for doc in mongo_db[collection_name]
                    if isinstance(doc.get(numeric_field), (int, float)) and doc.get(numeric_field) > random_threshold 
                    and doc.get(non_numeric_field) == selected_value
                ]
                templates.append(query)
                nl_templates.append(nl)
                outputs.append(simulated_output)
            elif mode == "construct":
                templates.append("No query could be generated due to insufficient valid values in fields.")
                nl_templates.append("Generalized CONDITIONS query structure: db.collection.find({ numeric_field: { $gt: value }, non_numeric_field: 'value' });")
                outputs.append(None)
        elif mode == "construct":
            templates.append("No query could be generated as there are no numeric and non-numeric field combinations.")
            nl_templates.append("Generalized CONDITIONS query structure: db.collection.find({ numeric_field: { $gt: value }, non_numeric_field: 'value' });")
            outputs.append(None)

    if not construct or construct == "match":
        if numeric_fields:
            numeric_field = random.choice(numeric_fields)  # Dynamically select a numeric field
            numeric_values = [doc[numeric_field] for doc in mongo_db[collection_name] if isinstance(doc.get(numeric_field), (int, float))]
            if numeric_values:
                min_value = int(min(numeric_values))
                max_value = int(max(numeric_values))
                lower_bound = random.randint(min_value, max_value - 1)
                upper_bound = random.randint(lower_bound + 1, max_value)

                query = f"db.{collection_name}.aggregate([{{ $match: {{ {numeric_field}: {{ $gte: {lower_bound}, $lte: {upper_bound} }} }} }}])"
                nl = f"Find documents where {numeric_field} is between {lower_bound} and {upper_bound}."
                simulated_output = [
                    doc for doc in mongo_db[collection_name]
                    if isinstance(doc.get(numeric_field), (int, float)) and lower_bound <= doc.get(numeric_field) <= upper_bound
                ]
                templates.append(query)
                nl_templates.append(nl)
                outputs.append(simulated_output)
            elif mode == "construct":
                templates.append("No query could be generated as the numeric field has no valid range values.")
                nl_templates.append("Generalized MATCH query structure: db.collection.aggregate([ { $match: { numeric_field: { $gte: lower, $lte: upper } } } ]);")
                outputs.append(None)
        elif mode == "construct":
            templates.append("No query could be generated as there are no numeric fields in the collection.")
            nl_templates.append("Generalized MATCH query structure: db.collection.aggregate([ { $match: { numeric_field: { $gte: lower, $lte: upper } } } ]);")
            outputs.append(None)

    if not construct or construct in ["group", "sum"]:
        if numeric_fields and non_numeric_fields:
            # Dynamically select fields
            group_field = random.choice(non_numeric_fields)
            sum_field = random.choice(numeric_fields)

            # Generate the query
            query = (
                f"db.{collection_name}.aggregate(["
                f"{{ $group: {{ _id: '${group_field}', total: {{ $sum: '${sum_field}' }} }} }}])"
            )
            nl = f"Group documents by {group_field} and calculate the sum of {sum_field}."

            # Simulate the query output
            grouped_data = defaultdict(int)
            for doc in mongo_db[collection_name]:
                group_key = doc.get(group_field, "Unknown")
                numeric_value = doc.get(sum_field, 0)
                try:
                    numeric_value = float(numeric_value) if isinstance(numeric_value, str) else numeric_value
                except ValueError:
                    numeric_value = 0  # Default to 0 if conversion fails
                grouped_data[group_key] += numeric_value

            simulated_output = [{"_id": k, "total": v} for k, v in grouped_data.items()]
            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            # Placeholder for when no numeric and non-numeric fields exist
            templates.append("No query could be generated as there are no numeric and non-numeric fields in the collection.")
            nl_templates.append("Generalized GROUP query structure: db.collection.aggregate([ { $group: { _id: '$field', total: { $sum: '$numeric_field' } } } ]);")
            outputs.append(None)

    if not construct or construct in ["sort", "limit"]:
        if keys:
            # Dynamically select a field to sort by
            sort_field = random.choice(keys)

            # Dynamically determine a limit
            max_limit = min(len(mongo_db[collection_name]), 10)  # Limit to 10 or fewer documents
            dynamic_limit = random.randint(1, max_limit)

            # Generate the query
            query = (
                f"db.{collection_name}.aggregate(["
                f"{{ $sort: {{ {sort_field}: 1 }} }}, "  # Sort in ascending order
                f"{{ $limit: {dynamic_limit} }}])"
            )
            nl = f"Sort documents by {sort_field} in ascending order and return the top {dynamic_limit}."

            # Simulate the query output
            simulated_output = sorted(
                mongo_db[collection_name],
                key=lambda doc: str(doc.get(sort_field, "")),  # Convert all keys to strings for consistent sorting
            )[:dynamic_limit]

            templates.append(query)
            nl_templates.append(nl)
            outputs.append(simulated_output)
        elif mode == "construct":
            # Placeholder for when no fields exist
            templates.append("No query could be generated as there are no fields to sort or limit in the collection.")
            nl_templates.append("Generalized SORT/LIMIT query structure: db.collection.aggregate([ { $sort: { field: 1 } }, { $limit: number } ]);")
            outputs.append(None)

    # Handle sample query generation mode
    if mode == "sample":
        valid_indices = [i for i, output in enumerate(outputs) if output is not None]
        templates = [templates[i] for i in valid_indices]
        nl_templates = [nl_templates[i] for i in valid_indices]
        outputs = [outputs[i] for i in valid_indices]

    # Return the queries, descriptions, and outputs
    return [(templates[i], nl_templates[i], outputs[i]) for i in range(len(templates))]

# Main program
def main():
    print("Welcome to ChatDB, your SQL and MongoDB assistant!")
    user_name = input("Enter your name: ")
    db_type = input("Enter \"sql\" or \"mongodb\": ").lower().strip()
    while db_type not in ["sql", "mongodb"]:
        db_type = input("Invalid choice. Enter \"sql\" or \"mongodb\": ").lower().strip()
    print(f"You chose: {db_type}")

    while True:
        file_name = database(db_type)
        
        if db_type == "sql":
            table_name = initialize_sql_data(file_name, user_name)
        elif db_type == "mongodb":
            collection_name = file_name.split(".")[0]
            data = open_file(file_name, user_name)
            mongo_db[collection_name] = data
            print(f"MongoDB collection '{collection_name}' loaded into memory.")

        while True:
            print("\nOptions:")
            print("1. Explore Database")
            print("2. Generate General Sample Queries")
            print("3. Sample Queries by Construct")
            print("4. Exit to main menu")

            choice = input("Choose an option (1-4): ").strip()

            if choice == "1":  # Explore Database
                explore_database(db_type, file_name, user_name)
                continue

            elif choice == "2":  # General Sample Queries
                if db_type == "sql":
                    for sql_query, nl_desc, simulated_output in sql_queries(table_name, mode="sample"):
                        print(f"\n{nl_desc}")
                        print(f"SQL Query: {sql_query}")
                        execute_choice = input("Would you like to execute this query? (yes/no): ").lower().strip()
                        if execute_choice == "yes":
                            try:
                                print("Query Output:")
                                pprint.pprint(simulated_output)
                                break
                            except Exception as e:
                                print(f"Error executing query: {e}")
                        else:
                            break
                elif db_type == "mongodb":
                    for mongo_query, nl_desc, simulated_output in mongodb_queries(collection_name, mode="sample"):
                        print(f"\n{nl_desc}")
                        print(f"MongoDB Query: {mongo_query}")
                        execute_choice = input("Would you like to execute this query? (yes/no): ").lower().strip()
                        if execute_choice == "yes":
                            try:
                                print("Query Output:")
                                pprint.pprint(simulated_output)
                                break
                            except Exception as e:
                                print(f"Error executing query: {e}")
                        else:
                            break
                continue

            elif choice == "3":  # Sample Queries by Construct
                if db_type == "sql":
                    construct = input("Enter construct (e.g., 'group by', 'having', 'projection'): ").lower().strip()
                    for sql_query, nl_desc, simulated_output in sql_queries(table_name, construct=construct, mode="construct"):
                        print(f"\n{nl_desc}")
                        print(f"SQL Query: {sql_query}")
                        if simulated_output is not None:
                            execute_choice = input("Would you like to execute this query? (yes/no): ").lower().strip()
                            if execute_choice == "yes":
                                try:
                                    print("Query Output:")
                                    pprint.pprint(simulated_output)
                                    break
                                except Exception as e:
                                    print(f"Error executing query: {e}")
                            else:
                                break
                        else:
                            print("No results for this query.")
                            break
                elif db_type == "mongodb":
                    construct = input("Enter construct (e.g., 'find', 'projection', 'criteria'): ").lower().strip()
                    for mongo_query, nl_desc, simulated_output in mongodb_queries(collection_name, construct=construct, mode="construct"):
                        print(f"\n{nl_desc}")
                        print(f"MongoDB Query: {mongo_query}")
                        if simulated_output is not None:
                            execute_choice = input("Would you like to execute this query? (yes/no): ").lower().strip()
                            if execute_choice == "yes":
                                try:
                                    print("Query Output:")
                                    pprint.pprint(simulated_output)
                                    break
                                except Exception as e:
                                    print(f"Error executing query: {e}")
                            else:
                                break
                        else:
                            print("No results for this query.")
                            break
                continue

            elif choice == "4":  # Exit to Main Menu
                break

        continue_choice = input("Do you want to load another dataset or query again? (Enter 'q' to quit or any key to continue): ").strip().lower()
        if continue_choice == 'q':
            print("Exiting ChatDB. Goodbye!")
            break

if __name__ == "__main__":
    main()
