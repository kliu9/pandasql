import gc
import pandas as pd
import psutil
import os
from helpers import create_df1, create_df2, get_size_info, limit_memory_relative, process_csv_file
import pandasql

# print("Creating dataframes...")
# df1 = create_df1(10)
# df2 = create_df2(10)

# print(get_size_info(df1, "DataFrame 1"))
# print(get_size_info(df2, "DataFrame 2"))

# df1.to_csv('test.csv', index=False)
# df2.to_csv('test2.csv', index=False)

# del df1
# del df2
# gc.collect()
# Run garbage collection to free up memory
def try_pandasql(limit):
  #  limit_memory_relative(10) # We run out of memory for 50, but succeed for 60 MB
   # print("size df1", os.stat("fi1/fi10").st_size / (1024 * 1024))
    print("size df1", os.stat("A.csv").st_size / (1024 * 1024))
    fi = pandasql.Pandasql("fi1/fi1", column_types=[pandasql.CType.INT,
                                                    pandasql.CType.INT,
                                                    pandasql.CType.STRING,
                                                    pandasql.CType.FLOAT,
                                                    pandasql.CType.FLOAT,
                                                    pandasql.CType.STRING,
                                                    pandasql.CType.DATETIME_S])
    # fi.load_csv_pandasql("A.csv",1000000,[pandasql.CType.INT,pandasql.CType.INT
                                        #   ,pandasql.CType.STRING,pandasql.CType.FLOAT,
                                        #   pandasql.CType.FLOAT,pandasql.CType.STRING,
                                        #   pandasql.CType.DATETIME_S])
    # print(fi.load_chunk("fi1/fi10.csv"))

def try_regular(limit):
    limit_memory_relative(1500) # We run out of memory for 50, but succeed for 60 MB
    print("size df1", os.stat("A.csv").st_size / (1024 * 1024))
    df1 = process_csv_file('A.csv', chunk_size=500)
    df2 = process_csv_file('B.csv', chunk_size=500)

    current_process = psutil.Process()
    current_memory_bytes = current_process.memory_info().rss
    # current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024
    # print(f"Current memory usage: {current_memory_gb:.2f} GB")
    current_memory_mb = current_memory_bytes / 1024 / 1024  # Convert bytes to MB
    gc.collect()
    print(f"Current memoryd usage: {current_memory_mb:.2f} MB")
    limit_memory_relative(limit) # We run out of memory for 50, but succeed for 60 MB
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

def try_chunked(lim):
    # Limit memory
    limit_memory_relative(600) # We run out of memory for 50, but succeed for 60 MB
    print("size df1", os.stat("A.csv").st_size / (1024 * 1024))
    df1 = pandasql.Pandasql("1")
    #df2 = pandasql.Pandasql("2")
    df1.process_csv_file('A.csv', chunk_size=5000)
    #df2.process_csv_file('B.csv', chunk_size=5000)

    # current_process = psutil.Process()
    # current_memory_bytes = current_process.memory_info().rss
    # # current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024
    # # print(f"Current memory usage: {current_memory_gb:.2f} GB")
    # current_memory_mb = current_memory_bytes / 1024 / 1024  # Convert bytes to MB
    # gc.collect()
    # print(f"Current memoryd usage: {current_memory_mb:.2f} MB")
    # limit_memory_relative(lim) # We run out of memory for 50, but succeed for 60 MB
    # # # # Join df1 and df2
    # try:
    #     print("\nAttempting join operation...")
    #     result = df1.merge(df2,on=['key1', 'key2'])
    # #     result = pd.merge(
    # #         df1.sort_values(['key1', 'key2']),
    # #         df2.sort_values(['key1', 'key2']),
    # #         on=['key1', 'key2'],
    # #         how='inner'
    # #     )

    # #     print("\nJoin completed successfully!")
    #     print(result.get_size_info())

    #     # Calculate some random statistics
    #     print("\nResult Statistics:")
    #     print(f"Number of unique key1 values: {result['key1'].nunique()}")
    #     print(f"Number of unique key2 values: {result['key2'].nunique()}")

    #  except Exception as e:
    #     print("\nError during join operation:", str(e))

try_chunked(400)
#try_pandasql(100)