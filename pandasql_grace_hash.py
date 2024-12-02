import gc
import hashlib
import os
import pandas as pd
import pandasql

from collections import defaultdict

def custom_hash(x, num_partitions):
    if pd.isna(x):  # for NaN values
        return 0
    return int(hashlib.sha256(str(x).encode('utf-8')).hexdigest(), 16) % num_partitions

def pandasql_grace_hash_join(file1_path, file2_path, output_path, chunk_size=10000, key='key', num_partitions=100):
    """
    Chunks data located at both `file1_path` and `file2_path` and performs a grace hash join.

    Parameters:
    file1_path: Path to first CSV file
    file2_path: Path to second CSV file
    output_path: Path to save the merged output
    chunk_size: Number of rows to process at a time
    key: Name of column to join on
    num_partitions: Number of hash buckets
    """
    # hash all keys, bucket hashes 
    total_rows1 = sum(1 for _ in open(file1_path)) - 1
    total_rows2 = sum(1 for _ in open(file2_path)) - 1

    df1_columns = pd.read_csv(file1_path, nrows=0).columns
    df2_columns = pd.read_csv(file2_path, nrows=0).columns

    # bucket hashes for all rows of each file
    hash_buckets_file1 = defaultdict(list)
    hash_buckets_file2 = defaultdict(list)

    # helper function to process a file & populate hash buckets
    def process_file(file_path, columns, hash_buckets, total_rows, output_dir, file_prefix):
        for chunk_start in range(0, total_rows, chunk_size):
            chunk = pd.read_csv(
                file_path,
                skiprows=chunk_start + 1 if chunk_start > 0 else 0,
                nrows=chunk_size,
                engine='python',
                names=columns if chunk_start > 0 else None,
                header=0 if chunk_start == 0 else None
            )
            chunk['hash'] = chunk[key].apply(lambda x: custom_hash(x, num_partitions))
            for _, row in chunk.iterrows():
                row_hash = row['hash']
                del row['hash']
                hash_buckets[row_hash].append(row.to_dict())
            del chunk # free memory
        
        for hash_key, rows in hash_buckets.items():
            bucket_df = pd.DataFrame(rows)
            output_path = os.path.join(output_dir, f'{file_prefix}_bucket_{hash_key}.csv')
            print(f"saving bucketed rows to {output_path}...")
            bucket_df.to_csv(output_path, index=False)
            del bucket_df

    output_dir = f"{os.path.relpath(file1_path)}-{os.path.relpath(file2_path)}-ghjoin"
    print(f"{output_dir=}")
    os.makedirs(output_dir, exist_ok=True)

    process_file(file1_path, df1_columns, hash_buckets_file1, total_rows1, output_dir, "file1")
    process_file(file2_path, df2_columns, hash_buckets_file2, total_rows2, output_dir, "file2") 
    my_pandasql = pandasql.Pandasql(output_dir)

    # for every hash bucket, join the rows from file1 and file2
    first_chunk=True
    for hash_key in set(hash_buckets_file1.keys()).intersection(hash_buckets_file2.keys()):
        bucket1_path = f'{output_dir}/file1_bucket_{hash_key}.csv'
        bucket2_path = f'{output_dir}/file2_bucket_{hash_key}.csv'
        print(f"Processing hash_key: {hash_key}, extracting from bucket1_path: {bucket1_path} & bucket2_path: {bucket2_path}")

        # call nested join function
        my_pandasql.join_chunks(bucket1_path, bucket2_path, output_path, key, key, first_chunk=first_chunk)
        if first_chunk: # after first chunk, want to keep appending to same file
            first_chunk = False

output_dir = "ghjoin"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "OneToOne.csv")
pandasql_grace_hash_join("data/OneToOne_1.csv", "data/OneToOne_2.csv", output_path)

# file1_path = 'data/OneToOne_1.csv'
# file2_path = 'data/OneToOne_2.csv'

# # Read the CSV files into dataframes
# df1 = pd.read_csv(file1_path)
# df2 = pd.read_csv(file2_path)
# actual_output_path = os.path.join(output_dir, "PANDAS_OneToOne.csv")
# merged_df = pd.merge(df1, df2, on="key")
# merged_df.to_csv(actual_output_path, index=False)
