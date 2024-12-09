import gc
import pandas as pd
from filprofiler.api import profile



def chunk_both_merge(file1_path, file2_path, output_path, chunk_size=10000, key='key1', write_output=True):
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

    df1_columns = pd.read_csv(file1_path, nrows=0, engine='python').columns
    df2_columns = pd.read_csv(file2_path, nrows=0, engine='python').columns

    first_chunk = True
    
    skiprows_fn = lambda chunk_start: (lambda x: x <= chunk_start if chunk_start > 0 else None)
    header_fn = lambda chunk_start: 0 if chunk_start == 0 else None
    names_fn = lambda chunk_start, df1_columns: df1_columns if chunk_start > 0 else None
    for chunk1_start in range(0, total_rows1, chunk_size):
        df1_chunk = pd.read_csv(
            file1_path,
            skiprows=skiprows_fn(chunk1_start),
            nrows=chunk_size,
            engine='python',
            names=names_fn(chunk1_start, df1_columns),
            header=header_fn(chunk1_start)
        )

        for chunk2_start in range(0, total_rows2, chunk_size):
            df2_chunk = pd.read_csv(
                file2_path,
                skiprows=skiprows_fn(chunk2_start),
                nrows=chunk_size,
                engine='python',
                names=names_fn(chunk2_start, df2_columns),
                header=header_fn(chunk2_start)
            )
            merged_chunk = pd.merge(df1_chunk, df2_chunk, on=key, how='inner')
            
            if write_output and not merged_chunk.empty:
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

def chunked_merge(file1_path, file2_path, output_path, chunk_size=10000, key='key1', write_output=True):
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

    df1_columns = pd.read_csv(file1_path, nrows=0, engine='python').columns
    df2 = pd.read_csv(file2_path, engine='python')
    total_rows = sum(1 for _ in open(file1_path)) - 1 
        
    skiprows_fn = lambda chunk_start: (lambda x: x <= chunk_start if chunk_start > 0 else None)
    header_fn = lambda chunk_start: 0 if chunk_start == 0 else None
    names_fn = lambda chunk_start, df1_columns: df1_columns if chunk_start > 0 else None
    for chunk_start in range(0, total_rows, chunk_size):
        df1_chunk = pd.read_csv(
            file1_path,
            skiprows=skiprows_fn(chunk_start),
            nrows=chunk_size,
            engine='python',
            names=names_fn(chunk_start, df1_columns),
            header=header_fn(chunk_start)
        )
        if chunk_start == 0 and key not in df1_chunk.columns:
            raise ValueError(f"Key column '{key}' not found in first file")
        
        merged_chunk = pd.merge(df1_chunk, df2, on=key, how='inner')


        if(write_output):
            if chunk_start == 0:
                merged_chunk.to_csv(output_path, index=False, mode='w')
            else:
                merged_chunk.to_csv(output_path, index=False, mode='a', header=False)
        
        del df1_chunk
        del merged_chunk
        
        progress = min(100, (chunk_start + chunk_size) / total_rows * 100)
        print(f"Progress: {progress:.1f}%")

def naiive_pandas_join(file1, file2, write_output=True):
    #roughly takes 4339.0 MiB according to fil
    df1 = pd.read_csv(file1, engine='python')
    df2 = pd.read_csv(file2, engine='python')
    merged = pd.merge(df1, df2, on='key1', how='inner')
    if (write_output):
        merged.to_csv('data/merged_A.csv', index=False)
    return merged


def main():
    import time
    start = time.time()
    merged = naiive_pandas_join('data/A.csv', 'data/B.csv', write_output=False)
    print(f"Time taken: {time.time() - start:.2f} seconds")
    #print size
    #print(merged.shape)


if __name__ == "__main__":
    #profile(lambda: main(), path="fil-stuff")
    main()
    gc.collect()