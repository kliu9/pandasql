import pandas as pd
import hashlib
import psutil
import gc
import os

from helpers import limit_memory_relative, limit_memory_absolute

def hash_partition(df, key_column, num_partitions):
    """
    Partitions a DataFrame based on a hash of the `key_column`.
    Args:
        df (pd.DataFrame): The DataFrame to be partitioned.
        key_column (str): The column to partition on.
        num_partitions (int): The number of partitions to create.
    Returns:
        List of DataFrames (partitions)
    """
    hashes = df[key_column].apply(lambda x: int(hashlib.md5(str(x).encode('utf-8')).hexdigest(), 16))
    print(f"{hashes=}")

    # Compute the partition index for each row by taking the modulo of the hash value
    partition_indices = hashes % num_partitions
    print(f"{partition_indices=}")

    # Split the DataFrame into partitions based on the computed partition indices
    partitions = [df[partition_indices == i] for i in range(num_partitions)]

    return partitions

def grace_hash_join(df1, df2, key_column, output_dir="output", num_partitions=100):
    os.makedirs(output_dir, exist_ok=True)

    partitions_df1 = hash_partition(df1, key_column, num_partitions)
    partitions_df2 = hash_partition(df2, key_column, num_partitions)

    # print(f"{partitions_df1=}")
    # print(f"{partitions_df2=}")
    
    current_process = psutil.Process()
    current_memory_bytes = current_process.memory_info().rss
    current_memory_mb = current_memory_bytes / 1024 / 1024
    print(f"Current memory usage: {current_memory_mb:.2f} MB")

    # result = []
    for i in range(num_partitions):
        partition1 = partitions_df1[i]
        partition2 = partitions_df2[i]
        
        joined_partition = pd.merge(partition1, partition2, on=key_column, how='inner')
        # result.append(joined_partition)

        output_file = os.path.join(output_dir, f"joined_partition_{i+1}.csv")
        joined_partition.to_csv(output_file, index=False)
        print(f"Partition {i+1} joined and saved to {output_file}")
        
        current_process = psutil.Process()
        current_memory_bytes = current_process.memory_info().rss
        current_memory_mb = current_memory_bytes / 1024 / 1024
        print(f"Processed partition {i+1}: Current memory usage: {current_memory_mb:.2f} MB")

        del joined_partition
        gc.collect()

    # final_result = pd.concat(result, ignore_index=True)
    print(f"\nGrace Hash Join completed.")
    # return final_result

file1 = "data/A.csv"
file2 = "data/B.csv"

size_file1 = os.path.getsize(file1)
size_file2 = os.path.getsize(file2)
size_file1_mb = size_file1 / (1024 * 1024)
size_file2_mb = size_file2 / (1024 * 1024)
print(f"Size of file 1: {size_file1_mb:.2f} MB")
print(f"Size of file 2: {size_file2_mb:.2f} MB")

df1 = pd.read_csv(file1)  
df2 = pd.read_csv(file2)

limit_memory_absolute(10000)

joined_df = grace_hash_join(df1, df2, key_column='key1')
