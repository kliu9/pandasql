import os
from pandasql import Pandasql

# USER SPECIFIED ARGUMENTS
file1_path = 'data/A.csv' # Absolute path to file 1
file2_path = 'data/B.csv' # Absolute path to file 2
output_path = 'ghjoin/A&B.csv' # Absolute path to where output is saved (must ensure that the subdirectories exist)

# Initialize Pandasql Object
my_pandasql = Pandasql("my_dataframe")

# --------- VARIOUS JOIN ALGORITHMS ----------
# View docstrings to specify additional arguments!
# Uncomment to run Grace Hash Join
# my_pandasql.pandasql_grace_hash_join(file1_path, file2_path, output_path)

# Uncomment to run "Naive Chunk Both Join"
# my_pandasql.chunked_merge(file1_path, file2_path, output_path)

# Uncomment to run "Naive Chunk One Join"
# my_pandasql.chunk_both_merge(file1_path, file2_path, output_path)

# Uncomment to run "Naive Join"
# my_pandasql.naiive_pandas_join(file1_path, file2_path)

# Uncomment to print statistics of join
# print(f"View result of joining {file1_path} & {file2_path} at {output_path}!")
# file_size = os.path.getsize(output_path)
# file_size_mb = file_size / (1024 * 1024) 
# with open(output_path, 'r') as file:
#     num_rows = sum(1 for _ in file) - 1
# print(f"The resulting CSV file is {file_size_mb:.2f} MB and has {num_rows} rows.")
