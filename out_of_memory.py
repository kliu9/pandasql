import gc
import pandas as pd
import psutil

from helpers import create_df1, create_df2, get_size_info, limit_memory_relative, process_csv_file

# -------------JOIN OPERATION FAILS IN CURRENT PANDAS IMPLEMENTATION-------------

# print("Creating dataframes...")
# df1 = create_df1(200000)
# df2 = create_df2(100000)

# print(get_size_info(df1, "DataFrame 1"))
# print(get_size_info(df2, "DataFrame 2"))

# df1.to_csv('data/A.csv', index=False)
# df2.to_csv('data/B.csv', index=False)

# del df1
# del df2

# Run garbage collection to free up memory
gc.collect()

# Limit memory
limit_memory_relative(400)

df1 = process_csv_file('data/A.csv', chunk_size=50)
df2 = process_csv_file('data/B.csv', chunk_size=50)

limit_memory_relative(75) # JOIN OPERATION FAILS WITh 75 MB LIMIT

current_process = psutil.Process()
current_memory_bytes = current_process.memory_info().rss
# current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024
# print(f"Current memory usage: {current_memory_gb:.2f} GB")
current_memory_mb = current_memory_bytes / 1024 / 1024  # Convert bytes to MB
print(f"Current memory usage: {current_memory_mb:.2f} MB")

# Join df1 and df2
try:
    print("\nAttempting join operation...")
    result = pd.merge(
        df1.sort_values(['key1', 'key2']),
        df2.sort_values(['key1', 'key2']),
        on=['key1', 'key2'],
        how='inner'
    )

    print("\nJoin completed successfully!")
    print(get_size_info(result, "Result DataFrame"))

    # Calculate some random statistics
    print("\nResult Statistics:")
    print(f"Number of unique key1 values: {result['key1'].nunique()}")
    print(f"Number of unique key2 values: {result['key2'].nunique()}")

except Exception as e:
    print("\nError during join operation:", str(e))