import pandas as pd
import heapq
import os
import time
from concurrent.futures import ProcessPoolExecutor
# from helpers import limit_memory_relative, limit_memory_absolute

def process_chunk(args):
    """
    For passed in args, reads df_file into pd dataframe and creates data chunk from
    given boundaries, add a column that stores index in original dataframe, only
    extracts given col_key, writes out to csv file
    Args:
        args: [df_file: str of file,
                chunk_start: row of start of chunk,
                chunksize: size of chunk,
                col_key: sort key to extract,
                count: which chunk is being processed]
    Returns:
        string of tempfile stored chunk
    """
    df_file, chunk_start, chunksize, col_key, count = args
    df_cols = pd.read_csv(df_file, nrows=0).columns
    chunk = pd.read_csv(
        df_file,
        skiprows=chunk_start + 1 if chunk_start > 0 else 0,
        nrows=chunksize,
        usecols=[col_key,],
        engine='python',
        names=df_cols,
        header=0 if chunk_start == 0 else None
    )
    chunk['row_idx'] = chunk_start + chunk.index
    sorted_chunk = chunk.sort_values(by=col_key)
    temp_file_path = f"sorted_chunk_{count}.csv"
    sorted_chunk.to_csv(temp_file_path, index=False)
    return temp_file_path

class heapNode:
    """
    heapnode of MinHeap
    Args:
        df_row: row of dataframe
        file_handler: the filehandler of the file that stores the number
    """
    def __init__(self, df_row, col_val, file_handler) -> None:
        self.row = df_row
        self.file_handler = file_handler
        self.col = col_val
        self.item = df_row[col_val]

    def __lt__(self, other):
        """
        defines less than for min heap 
        Arg:
            other: heapNode instance
        Returns: 
            bool True if this heapNode is less than other
        """
        return self.item < other.item

class externalMergeSort:
    """
    Merge sort large DataFrame based on 'col_key' value

    split large dataframe into smaller files, sort small files, 
    then merge different files into new df. Files loaded as []
    """
    def __init__(self, col_key) -> None:
        self.sorted_temp_files = []
        self.sort_key = col_key
        self.getCurrentDir()
        
    def getCurrentDir(self):
        self.cwd = os.getcwd()
    
    # def split_and_sort_DataFrame(self, df_file, chunksize = 10000):
    #     # splits large df file into chunks, sorts & stores them as temp csv files
    #    total_rows = sum(1 for _ in open(df_file))-1
    #    count = 0
    #    for chunk_start in range(0, total_rows, chunksize):
    #     chunk = pd.read_csv(
    #         df_file,
    #         skiprows= chunk_start +1 if chunk_start>0 else 0,
    #         nrows = chunksize,
    #         usecols=[self.sort_key,],
    #         engine='python',
    #         names = [self.sort_key,] if chunk_start >0 else None,
    #         header=0 if chunk_start == 0 else None
    #         )

    #     chunk['row_idx'] = chunk_start + chunk.index
    #     sorted_chunk = chunk.sort_values(by=self.sort_key)
    #     temp_file_path = os.path.join(self.cwd, f"sorted_chunk_{count}.csv")
    #     sorted_chunk.to_csv(temp_file_path, index=False)
    #     # add to internal list
    #     self.sorted_temp_files.append(temp_file_path)
    #     count+=1


    def split_and_sort_DataFrame(self, df_file, chunksize=10000):
        """
        parallel implementation of splitting DF into chunks, sort chunks, and save to temp files
        Args:
            df_file: str of relative file path
            chunksize: int size of chunks
        Returns: None
        """
        total_rows = sum(1 for _ in open(df_file)) - 1
        args_list = [
            (df_file, chunk_start, chunksize, self.sort_key, count)
            for count, chunk_start in enumerate(range(0, total_rows, chunksize))
        ]
        with ProcessPoolExecutor() as executor:
            self.sorted_temp_files = list(executor.map(process_chunk, args_list))

    def build_heap(self, arr):
        """
        heapify "heap" arr
        Arg:
            arr: array that represents heap
        returns:
            None
        """
        l = len(arr) - 1
        mid = l / 2
        while mid >= 0:
            self.heapify(arr, mid, l)
            mid -= 1


    def heapify(self, arr, i, n):
        """
        Min heap 
        Args:
            arr: list repr heap, each element is a heapNode instance
            i: index of current node in heap to be heapified
            n: total num elements in heap
        """
        # children
        left = 2 * i + 1
        right = 2 * i + 2
        # get smallest
        if left < n and arr[left].item < arr[i].item:
            smallest = left
        else:
            smallest = i

        if right < n and arr[right].item < arr[smallest].item:
            smallest = right

        # if not smallest, swap
        if i != smallest:
            (arr[i], arr[smallest]) = (arr[smallest], arr[i])
            self.heapify(arr, smallest, n)


    def merge_sorted_files(self):
        """
        Low-level merge of sorted temp (csv) files
        """
        output_file = os.path.join(self.cwd, f"sorted_df.csv")

        file_handlers = [open(f, 'r') for f in self.sorted_temp_files]
        readers = [pd.read_csv(
            fh, 
            chunksize=1,
            engine='python'
            ) for fh in file_handlers]

        # Initialize heap
        heap = []
        for i, reader in enumerate(readers):
            try:
                row = next(reader).iloc[0]
                heapq.heappush(heap, heapNode(row, self.sort_key ,reader))
            except StopIteration:
                file_handlers[i].close()
        
        # Open the output file
        with open(output_file, 'w') as out_f:
            # Write header
            if self.sorted_temp_files:
                sample_df = pd.read_csv(self.sorted_temp_files[0], nrows=1)
                out_f.write(','.join(sample_df.columns) + '\n')
            
            # Merge process
            while heap:
                smallest_node = heapq.heappop(heap)
                smallest_row = smallest_node.row
                # Write the row to the output file
                out_f.write(','.join(map(str, smallest_row.values)) + '\n')
                
                # Read the next row from the same file
                try:
                    next_row = next(smallest_node.file_handler).iloc[0]
                    heapq.heappush(heap, heapNode(next_row, self.sort_key, smallest_node.file_handler))
                except StopIteration:
                    # Close the file if no more rows
                    smallest_node.file_handler.close()
        # print(f"Successfully merged sorted chunks into {output_file}")

    def cleanup(self):
        """Deletes all temporary sorted chunk files."""
        for f in self.sorted_temp_files:
            try:
                os.remove(f)
            except OSError as e:
                print(f"Error deleting file {f}: {e}")
        self.sorted_temp_files = []

if __name__ == "__main__":
    start_time = time.time()
    sorter = externalMergeSort(col_key = 'key1')
    sorter.split_and_sort_DataFrame("data/A.csv")
    split_time = time.time()
    sorter.merge_sorted_files()
    end_time = time.time()
    print("opt1: new split, old merge, chunk 10000")
    print("split + sort time:", split_time - start_time, "s")
    print("merge time:", end_time - split_time, "s")
    print("Elapsed time:", end_time-start_time, "s")

    sorter.cleanup()



    
