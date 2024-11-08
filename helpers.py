import numpy as np
import os
import pandas as pd
import psutil
import resource

from typing import Optional, Iterator

def create_df1(size=500000):
    return pd.DataFrame({
        'key1': np.random.randint(1, 1000, size=size),
        'key2': np.random.randint(1, 100, size=size),
        'text_data': np.random.choice(['A', 'B', 'C', 'D', 'E'] * 20, size=size),
        'value1': np.random.randn(size),
        'value2': np.random.randn(size),
        'value3': [np.random.bytes(10) for _ in range(size)],
        'timestamp': pd.date_range(start='2020-01-01', periods=size, freq='s')
    })

def create_df2(size=400000):
    return pd.DataFrame({
        'key1': np.random.randint(1, 1000, size=size),
        'key2': np.random.randint(1, 100, size=size),
        'metric1': np.random.randn(size),
        'metric2': np.random.randn(size),
        'metric3': [np.random.bytes(10) for _ in range(size)], 
        'category': np.random.choice(['X', 'Y', 'Z'], size=size),
        'date': pd.date_range(start='2020-01-01', periods=size, freq='min')
    })

def get_size_info(df, name):
    memory_usage = df.memory_usage(deep=True).sum() / (1024**2)  # Convert to MB
    return f"{name} Shape: {df.shape}, Memory: {memory_usage:.2f} MB"

def limit_memory_absolute(mb=100):
    current_process = psutil.Process()
    current_memory_bytes = current_process.memory_info().rss
    current_memory_mb = current_memory_bytes / 1024 / 1024  # Convert bytes to MB

    total_limit_bytes = int(mb * 1024 * 1024)
    resource.setrlimit(resource.RLIMIT_AS, (total_limit_bytes, total_limit_bytes))

    print(f"Current memory usage: {current_memory_mb:.2f} MB")
    print(f"Absolute allowance: {mb:.2f} MB")
    print(f"Total memory limit set to: {mb:.2f} MB")

def limit_memory_relative(additional_mb=100): #additional_gb=0.1):
    """
    Limit the memory usage to current usage plus specified additional amount.
    Args:
        additional_gb (float): Additional memory allowance in gigabytes
    """
    # Get current memory usage
    current_process = psutil.Process()
    current_memory_bytes = current_process.memory_info().rss
    current_memory_mb = current_memory_bytes / 1024 / 1024  # Convert bytes to MB

    # Calculate new limit
    total_limit_mb = current_memory_mb + additional_mb
    total_limit_bytes = int(total_limit_mb * 1024 * 1024)  # Convert MB to bytes

    resource.setrlimit(resource.RLIMIT_AS, (total_limit_bytes, total_limit_bytes))

    print(f"Current memory usage: {current_memory_mb:.2f} MB")
    print(f"Additional allowance: {additional_mb:.2f} MB")
    print(f"Total memory limit set to: {total_limit_mb:.2f} MB")
    # current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024

    # # Calculate new limit
    # total_limit_gb = current_memory_gb + additional_gb
    # total_limit_bytes = int(total_limit_gb * 1024 * 1024 * 1024)

    # resource.setrlimit(resource.RLIMIT_AS, (total_limit_bytes, total_limit_bytes))

    # print(f"Current memory usage: {current_memory_gb:.2f} GB")
    # print(f"Additional allowance: {additional_gb:.2f} GB")
    # print(f"Total memory limit set to: {total_limit_gb:.2f} GB")

def load_csv_chunked(
    file_path: str,
    chunk_size: int = 100000,
    columns: Optional[list] = None,
    **csv_kwargs
) -> Iterator[pd.DataFrame]:
    """
    Load a CSV file in chunks to manage memory usage.

    Parameters:
    -----------
    file_path : str
        Path to the CSV file
    chunk_size : int, default 100000
        Number of rows to load in each chunk
    columns : list, optional
        Specific columns to load. If None, loads all columns.
    csv_kwargs : dict
        Additional arguments to pass to pd.read_csv
    """
    # Create CSV reader iterator
    csv_iter = pd.read_csv(
        file_path,
        usecols=columns,
        chunksize=chunk_size,
        **csv_kwargs
    )
    current_process = psutil.Process()
    current_memory_bytes = current_process.memory_info().rss
    current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024
    # print(f"Current memory usage: {current_memory_gb:.2f} GB\n")
    i = 0
    # Yield chunks
    for chunk in csv_iter:
        yield chunk
        i+=1

def process_csv_file(
    file_path: str,
    chunk_size: int = 100000,
    columns: Optional[list] = None,
    **csv_kwargs
) -> pd.DataFrame:
    """
    Process a CSV file in chunks and combine results.

    Parameters:
    -----------
    file_path : str
        Path to the CSV file
    chunk_size : int, default 100000
        Number of rows to load in each chunk
    columns : list, optional
        Specific columns to load. If None, loads all columns.
    csv_kwargs : dict
        Additional arguments to pass to pd.read_csv
    """
    chunks = []
    total_rows = 0

    # Get total file size for progress monitoring
    file_size = os.path.getsize(file_path)

    for i, chunk in enumerate(load_csv_chunked(file_path, chunk_size, columns, **csv_kwargs)):
        chunks.append(chunk)
        total_rows += len(chunk)
        # Print progress
        current_process = psutil.Process()
        current_memory_bytes = current_process.memory_info().rss
        current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024
        # print(f"Current memory usage: {current_memory_gb:.2f} GB\n")
        print(f"Processed chunk {i+1}: {total_rows:,} rows", end='\r')

    print(f"\nCompleted loading {total_rows:,} total rows")
    return pd.concat(chunks, ignore_index=True)
