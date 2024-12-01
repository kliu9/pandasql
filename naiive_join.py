import gc
import pandas as pd
from filprofiler.api import profile



def chunk_both_merge(file1_path, file2_path, output_path, chunk_size=10000, key='key1'):
    #peak memory usage of 92.4 MiB
    """
    Chunks both and does pandas merge
    
    Parameters:
    - file1_path: Path to first CSV file
    - file2_path: Path to second CSV file
    - output_path: Where to save the merged results
    - chunk_size: Number of rows to process at a time
    - key: Column name to join on
    """

    total_rows1 = sum(1 for _ in open(file1_path)) - 1
    total_rows2 = sum(1 for _ in open(file2_path)) - 1

    df1_columns = pd.read_csv(file1_path, nrows=0).columns
    df2_columns = pd.read_csv(file2_path, nrows=0).columns

    first_chunk = True
    
    for chunk1_start in range(0, total_rows1, chunk_size):
        df1_chunk = pd.read_csv(
            file1_path,
            skiprows=chunk1_start + 1 if chunk1_start > 0 else 0,
            nrows=chunk_size,
            engine='python',
            names=df1_columns if chunk1_start > 0 else None,  # Use stored column names
            header=0 if chunk1_start == 0 else None
        )

        for chunk2_start in range(0, total_rows2, chunk_size):
            df2_chunk = pd.read_csv(
                file2_path,
                skiprows=chunk2_start + 1 if chunk2_start > 0 else 0,
                nrows=chunk_size,
                engine='python',
                names=df2_columns if chunk2_start > 0 else None,  # Use stored column names
                header=0 if chunk2_start == 0 else None
            )
            merged_chunk = pd.merge(df1_chunk, df2_chunk, on=key, how='inner')
            
            if not merged_chunk.empty:
                if first_chunk:
                    # First chunk: create new file with header
                    merged_chunk.to_csv(output_path, index=False, mode='w')
                    first_chunk = False
                else:
                    # Subsequent chunks: append without header
                    merged_chunk.to_csv(output_path, index=False, mode='a', header=False)
            
            del df2_chunk
            del merged_chunk
            
            progress1 = min(100, (chunk1_start + chunk_size) / total_rows1 * 100)
            progress2 = min(100, (chunk2_start + chunk_size) / total_rows2 * 100)
            print(f"Progress: File1 {progress1:.1f}%, File2 {progress2:.1f}%")
        
        # Free memory
        del df1_chunk

def chunked_merge(file1_path, file2_path, output_path, chunk_size=10000, key='key1'):
    #peak memory usage of 326.0 MiB
    """
    Chunks file1_path and does pandas merge
    
    Parameters:
    - file1_path: Path to first CSV file
    - file2_path: Path to second CSV file
    - output_path: Where to save the merged results
    - chunk_size: Number of rows to process at a time
    - key: Column name to join on
    """

    df1_columns = pd.read_csv(file1_path, nrows=0).columns
    df2 = pd.read_csv(file2_path)
    total_rows = sum(1 for _ in open(file1_path)) - 1 
    
    for chunk_start in range(0, total_rows, chunk_size):
        df1_chunk = pd.read_csv(
            file1_path,
            skiprows=chunk_start + 1 if chunk_start > 0 else 0,
            nrows=chunk_size,
            engine='python',
            names=df1_columns if chunk_start > 0 else None,  # Use stored column names
            header=0 if chunk_start == 0 else None
        )
        if chunk_start == 0 and key not in df1_chunk.columns:
            raise ValueError(f"Key column '{key}' not found in first file")
        
        merged_chunk = pd.merge(df1_chunk, df2, on=key, how='inner')
        if chunk_start == 0:
            merged_chunk.to_csv(output_path, index=False, mode='w')
        else:
            merged_chunk.to_csv(output_path, index=False, mode='a', header=False)
        
        del df1_chunk
        del merged_chunk
        
        progress = min(100, (chunk_start + chunk_size) / total_rows * 100)
        print(f"Progress: {progress:.1f}%")

def naiive_pandas_join(file1, file2):
    #roughly takes 4339.0 MiB according to fil
    df1 = pd.read_csv(file1, engine='python')
    df2 = pd.read_csv(file2, engine='python')
    
    return pd.merge(df1, df2, on='key1', how='inner')


def main():
    merged = chunked_merge('data/A.csv', 'data/B.csv', 'data/merged.csv', chunk_size=10000, key='key1')
    
    #print size
    print(merged.shape)


if __name__ == "__main__":
    #profile(lambda: main(), path="fil-stuff")
    main()
    gc.collect()