import pandas as pd
import numpy as np
import pandasql
import pandasql_grace_hash
import time
from filprofiler.api import profile

# Build Datasets based on selectivity -------------------------------

df1_size = 20000
df2_size = 10000
join_key = 'key1'
df1_file = "benchmark/A.csv"
df2_file = "benchmark/B.csv"

df1 = pd.DataFrame({
        'key1': [1]*df1_size,
        'key2': np.random.randint(1, 100, size=df1_size),
        'text_data': np.random.choice(['A', 'B', 'C', 'D', 'E'] * 20, size=df1_size),
        'value1': np.random.randn(df1_size),
        'value2': np.random.randn(df1_size),
        'value3': np.random.choice(['A', 'B', 'C', 'D', 'E'] * 20, size=df1_size),
        # 'value3': [np.random.bytes(10) for _ in range(size)],
        'timestamp': pd.date_range(start='2020-01-01', periods=df1_size, freq='s')
    })
df1.to_csv(df1_file, index=False)


def create_df(selectivity):
    size = int(df2_size*selectivity)
    multiplier = [1]*size
    rest = [0]* int(df2_size-size)
    return pd.DataFrame({
        'key1': multiplier + rest,
        'key2': np.random.randint(1, 100, size=df2_size),
        'metric1': np.random.randn(df2_size),
        'metric2': np.random.randn(df2_size),
        'metric3': np.random.choice(['A', 'B', 'C', 'D', 'E'] * 20, size=df2_size),
        # 'metric3': [np.random.bytes(10) for _ in range(size)],
        'category': np.random.choice(['X', 'Y', 'Z'], size=df2_size),
        'date': pd.date_range(start='2020-01-01', periods=df2_size, freq='s')
    })

# Different Join Operations -------------------------------------------

def try_pandas_join(df1_path, df2_path):
    start_time = time.time()
    df1 = pd.read_csv(df1_path, engine='python')
    df2 = pd.read_csv(df2_path, engine='python')
    joined = df1.join(df2, [join_key,], how='inner')
    end_time = time.time()
    return end_time- start_time

def try_naive_join(df1_path, df2_path):
    start_time = time.time()
    A = pandasql.Pandasql("df1")
    A.join_chunks(df1_path, df2_path, "benchmark/naive_merged.csv",
                  join_key, join_key, chunk_size=10000)
    end_time = time.time()
    return end_time- start_time
    
def try_gh_join(df1_path, df2_path):
    start_time = time.time()
    pandasql_grace_hash.pandasql_grace_hash_join(df1_path, df2_path, "benchmark/gh_merged.csv", key=join_key)
    end_time = time.time()
    return end_time- start_time

def try_ms_join(df1_path, df2_path):
    start_time = time.time()
    A = pandasql.Pandasql("df1", column_types=[pandasql.CType.INT, pandasql.CType.INT, pandasql.CType.STRING, pandasql.CType.FLOAT,
                                                  pandasql.CType.FLOAT, pandasql.CType.STRING,
                                                  pandasql.CType.DATETIME_S])
    B = pandasql.Pandasql("df2", column_types=[pandasql.CType.INT, pandasql.CType.INT,  pandasql.CType.FLOAT,
                                                  pandasql.CType.FLOAT, pandasql.CType.STRING, pandasql.CType.STRING,
                                                  pandasql.CType.DATETIME_S])
    A.sort_merge(B, join_key, join_key, df1_path,
                 df2_path, "benchmark/ms_merged.csv", 10000)
    end_time = time.time()
    return end_time- start_time


# Run Memory Usage + Time


# for s in range(0,7): # selectivity from 10^(-6) to 1
s = 0
selectivity = 1/(10**(s))
df2 = create_df(selectivity)
df2.to_csv(df2_file, index=False)

# naive:
profile(lambda: try_naive_join(df1_file, df2_file), "fil-benchmark")
naive_time = try_naive_join(df1_file, df2_file)

# gh:
profile(lambda: try_gh_join(df1_file, df2_file), "fil-benchmark")
gh_time = try_gh_join(df1_file, df2_file)

# ms:
profile(lambda: try_ms_join(df1_file, df2_file), "fil-benchmark")
ms_time = try_ms_join(df1_file, df2_file)

# pandas:
profile(lambda: try_pandas_join(df1_file, df2_file), "fil-benchmark")
pd_time = try_pandas_join(df1_file, df2_file)


print(naive_time, gh_time, ms_time, pd_time)

