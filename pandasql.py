import pandas as pd
from typing import Optional, Iterator
import numpy as np
import os
import psutil
class Pandasql:
    def __init__(self,initchunks=None):
        if initchunks:
            self.chunks=initchunks
        else:
            self.chunks=[]
        
    def merge(self,other, on):
        newchunks=[]
        for k,chunk1 in enumerate(self.chunks):
            for chunk2 in other.chunks:
                newchunks.append(pd.merge(chunk1,chunk2,on=['key1', 'key2'],
                how='inner'))
                del chunk2
            current_memory_mb = psutil.Process().memory_info().rss / 1024 / 1024  # Convert bytes to MB
            print(f"{k} Current memory usage: {current_memory_mb:.2f} MB")
        return Pandasql(newchunks)
    def load_csv_chunked(self,
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

    def get_size_info(self):
        memory_usage=0
        for chunk in self.chunks:
            memory_usage += chunk.memory_usage(deep=True).sum() / (1024**2)  # Convert to MB
        return f" Memory: {memory_usage:.2f} MB"
    def process_csv_file(self,
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
        total_rows = 0

        # Get total file size for progress monitoring
        file_size = os.path.getsize(file_path)

        for i, chunk in enumerate(self.load_csv_chunked(file_path, chunk_size, columns, **csv_kwargs)):
            self.chunks.append(pd.DataFrame(chunk))
            total_rows += len(chunk)
            # Print progress
            current_process = psutil.Process()
            current_memory_bytes = current_process.memory_info().rss
            current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024
            # print(f"Current memory usage: {current_memory_gb:.2f} GB\n")
            print(f"Processed chunk {i+1}: {total_rows:,} rows", end='\r')

        print(f"\nCompleted loading {total_rows:,} total rows")
        
