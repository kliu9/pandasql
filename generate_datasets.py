import pandasql

import pandas as pd
import numpy as np

from helpers import get_size_info

def generate_dataset(size_left, size_right, fanout="1:1", skewed=False):
    np.random.seed(42)

    left_keys = np.arange(size_left)
    left_values = np.random.rand(size_left)
    left_df = pd.DataFrame({"key": left_keys, "value_left": left_values})

    if fanout == "1:1":
        right_keys = left_keys[:size_right]
    elif fanout == "1:N":
        right_keys = np.repeat(left_keys[:size_left // 2], size_right // (size_left // 2))
    elif fanout == "M:1":
        right_keys = np.random.choice(left_keys, size=size_right, replace=True)
    else:
        raise ValueError("Unsupported fanout pattern")

    if skewed:
        skew_key = np.random.choice(left_keys, size=size_right, replace=True, p=[0.9 if k == 0 else 0.1/(size_left-1) for k in left_keys])
        right_keys = skew_key
    
    right_values = np.random.rand(size_right)
    right_df = pd.DataFrame({"key": right_keys, "value_right": right_values})
    
    return left_df, right_df

# GENERATE 1:N
left_df, right_df = generate_dataset(size_left=1000, size_right=2000, fanout="1:N")

print(get_size_info(left_df, "DataFrame 1"))
print(get_size_info(right_df, "DataFrame 2"))

left_df.to_csv('data/OneToN_1.csv', index=False)
right_df.to_csv('data/OneToN_2.csv', index=False)

df1 = pandasql.Pandasql("OneToN_1")
df1.process_csv_file('data/OneToN_1.csv', chunk_size=5000)

df2 = pandasql.Pandasql("OneToN_2")
df2.process_csv_file('data/OneToN_2.csv', chunk_size=5000)

print(f"{df1=}")
print(f"{df2=}")

result = df1.merge(df2, "key")

columns = ["key", "value_left", "value_right"]
actual_result = pd.DataFrame(result.chunks[0], columns=columns)
expected_result = pd.merge(left_df, right_df, on="key", how="inner")

# pd.set_option('display.max_rows', None)  # Show all rows
# pd.set_option('display.max_columns', None)  # Show all columns
# pd.set_option('display.width', 1000)  # Adjust the output width

print(f"{actual_result=}")
print(f"{expected_result=}")

# try to normalize stuffs
actual_result_reset = actual_result.reset_index(drop=True)
expected_result_reset = expected_result.reset_index(drop=True)

try:
    assert actual_result.equals(expected_result), "Join result does not match expected result!"
    print("The DataFrames are equal.")
except AssertionError:
    print("The DataFrames are NOT equal.")
    
    differences = actual_result.compare(expected_result)
    print("Differences between actual_result and expected_result:")
    print(differences)

    print("\nExtra rows in actual_result:")
    print(actual_result[~actual_result.isin(expected_result).all(axis=1)])
    print("\nExtra rows in expected_result:")
    print(expected_result[~expected_result.isin(actual_result).all(axis=1)])
