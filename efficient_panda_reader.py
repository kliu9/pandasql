import gc
import pandas as pd
# from memory_profiler import profile
# import psutil
import os
from filprofiler.api import profile
from helpers import create_df1, create_df2, get_size_info, limit_memory_absolute, limit_memory_relative, process_csv_file


def load_csv_by_row(file_path: str, row, length, **csv_kwargs,) -> pd.DataFrame:
    # print("size df1", os.stat("data/A.csv").st_size / (1024 * 1024))
    # print("size df1", os.stat("data/B.csv").st_size / (1024 * 1024))
    return pd.read_csv(file_path, skiprows=row, nrows=length, engine='python')

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


# @profile
def main():
    # df1 = process_csv_file('data/A.csv', chunk_size=500)
    df1 = load_csv_by_row('data/A.csv', 0, 1000)

    # df2 = process_csv_file('data/B.csv', chunk_size=500)
    df2 = load_csv_by_row('data/B.csv', 0, 1000)

    # print(get_size_info(df1, "DataFrame 1"))
    # print(get_size_info(df2, "DataFrame 2"))

    # # limit_memory_absolute(300) # JOIN OPERATION FAILS WITH 90 MB LIMIT, passes with 100 (resulting dataframe has shape 85.76 MB)

    # # Join df1 and df2
    # try:

    #     print("\nAttempting join operation...")
    #     result = pd.merge(
    #         df1.sort_values(['key1', 'key2']),
    #         df2.sort_values(['key1', 'key2']),
    #         on=['key1', 'key2'],
    #         how='inner'
    #     )

    #     print("\nJoin completed successfully!")

    #     print("\nResult Statistics:")
    #     print(f"Number of unique key1 values: {result['key1'].nunique()}")
    #     print(f"Number of unique key2 values: {result['key2'].nunique()}")

    # except Exception as e:
    #     print("\nError during join operation:", str(e))


if __name__ == "__main__":
    profile(lambda: main(), path="fil-stuff")
    # main()
    gc.collect()
