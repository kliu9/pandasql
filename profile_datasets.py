import pandasql
import timeit

import pandas as pd

# profile 1:1 fanout dataset
def custom_1_to_1_fanout():
    df1 = pandasql.Pandasql("OneToOne_1")
    df1.process_csv_file('data/OneToOne_1.csv', chunk_size=5000)

    df2 = pandasql.Pandasql("OneToOne_2")
    df2.process_csv_file('data/OneToOne_2.csv', chunk_size=5000)

    result = df1.merge(df2, "key")

def pandas_1_to_1_fanout():
    df1 = pd.read_csv('data/OneToOne_1.csv')
    df2 = pd.read_csv('data/OneToOne_2.csv')

    result = pd.merge(df1, df2, on="key", how="inner")

# custom_1_to_1_fanout() # used 24.8 MiB
# execution_time = timeit.timeit(custom_1_to_1_fanout, number=1)
# print(f"Execution time: {execution_time:.4f} seconds") # 0.0254 seconds

# pandas_1_to_1_fanout() # used 24.8 MiB
# execution_time = timeit.timeit(pandas_1_to_1_fanout, number=1)
# print(f"Execution time: {execution_time:.4f} seconds") # 0.0048 seconds

# profile 1:n fanout dataset
def custom_1_to_n_fanout():
    df1 = pandasql.Pandasql("OneToN_1")
    df1.process_csv_file('data/OneToN_1.csv', chunk_size=5000)

    df2 = pandasql.Pandasql("OneToN_2")
    df2.process_csv_file('data/OneToN_2.csv', chunk_size=5000)

    result = df1.merge(df2, "key")

# profile m:1 fanout dataset
def custom_m_to_1_fanout():
    df1 = pandasql.Pandasql("MToOne_1")
    df1.process_csv_file('data/MToOne_1.csv', chunk_size=5000)

    df2 = pandasql.Pandasql("MToOne_2")
    df2.process_csv_file('data/MToOne_2.csv', chunk_size=5000)

    result = df1.merge(df2, "key")
