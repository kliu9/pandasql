import csv
import gc
import hashlib
import math
import psutil
from datetime import datetime

import os
import psutil
import sys
import heapq
import numpy as np
import pandas as pd
import shutil


from datetime import datetime
from enum import Enum
from typing import Optional, Iterator


class Row:
    def __init__(self, val, row, chunk):
        self.val = val
        self.row = row
        self.chunk = chunk

    def __lt__(self, other):
        return self.val < other.val

    def __repr__(self):
        return f"Row(val={self.val}, row={self.row}, chunk={self.chunk})"


class CType(Enum):
    INT = 1
    FLOAT = 2
    STRING = 3
    DATETIME_S = 4

# prestore row num?


class Pandasql:
    def __init__(self, name, initchunks=None, columns=None, column_types=None):
        """
        Initialize a Pandasql object.

        name: Name of this Pandasql object
        initchunks: Initial list of chunks
        columns:
        column_types:
        """
        self.header = []
        self.name = name
        self.chunks = initchunks if initchunks else []
        self.columns = columns
        self.column_types = column_types

    def sort_merge(self, other: "Pandasql", onSelf, onOther, file_path1, file_path2, output_path, chunk_size):
        sort_path1 = self.name + "_sorted.csv"
        sort_path2 = other.name + "_sorted.csv"
        self.sortCSVReaderFast(file_path=file_path1, on=onSelf,
                           chunk_size=chunk_size, out_path=sort_path1)
        other.sortCSVReaderFast(file_path=file_path2, on=onOther,
                            chunk_size=chunk_size, out_path=sort_path2)
        reader1 = open(sort_path1, 'r', newline='')
        reader2 = open(sort_path2, 'r', newline='')
        idx1 = self.columns[onSelf]
        idx2 = other.columns[onOther]
        next(reader1)
        next(reader2)
        itr1 = 1
        itr2 = 0
        chunk2 = []
        chunk1 = []
        first_chunk = True
        nextOther = other.load_line(reader2)
        currVal = nextOther[idx2]
        while (nextOther[idx2] == currVal):
            chunk2.append(nextOther)
            itr2 += 1
            if itr2 >= other.size:
                break
            nextOther = other.load_line(reader2)
        nxtRow = self.load_line(reader1)
        while itr1 < self.size:
            chunk1 = []
            currVal = nxtRow[idx1]
            while (nxtRow[idx1] == currVal):
                chunk1.append(nxtRow)
                itr1 += 1
                if itr1 >= self.size:
                    break
                nxtRow = self.load_line(reader1)
            currVal = nextOther[idx2]
            prevVal = -1
            # make sure prevVal is set to a diff value than currVal
            if (prevVal == currVal):
                prevVal -= 1
            while (chunk1[0][idx1] > chunk2[0][idx2]):
                chunk2 = []
                if currVal == prevVal:
                    break
                prevVal = currVal
                currVal = nextOther[idx2]
                while (nextOther[idx2] == currVal):
                    chunk2.append(nextOther)
                    itr2 += 1
                    if itr2 >= other.size:
                        break
                    nextOther = other.load_line(reader2)
            if len(chunk2) == 0:
                break
            if chunk1[0][idx1] < chunk2[0][idx2]:
                continue
            if chunk1[0][idx1] == chunk2[0][idx2]:
                df1 = pd.DataFrame(chunk1, columns=self.get_column_list())
                df2 = pd.DataFrame(chunk2, columns=other.get_column_list())
                merged_chunk = pd.merge(df1, df2, how='cross')
                if first_chunk:
                    # First chunk: create new file with header
                 #   merged_chunk.to_csv(output_path, index=False, mode='w')
                    first_chunk = False
                else:
                    pass
                    # Subsequent chunks: append without header
                 #   merged_chunk.to_csv(
                  #      output_path, index=False, mode='a', header=False)
                chunk2 = []
                currVal = nextOther[idx2]
                while (nextOther[idx2] == currVal):
                    chunk2.append(nextOther)
                    itr2 += 1
                    if itr2 >= other.size:
                        break
                    nextOther = other.load_line(reader2)
            else:
                break

    def load_line(self, reader):
        line = next(reader).strip("\n\r").split(",")
        row = [0] * len(self.column_types)
        for k, column_type in enumerate(self.column_types):

            row[k] = self.convert(line[k], column_type)
        return row

    def load_lines(self, file_path, reader, lines):
        """
        Loads a chunk of data from a CSV file.

        file_path: Path to the CSV file
        Returns a list of lists, where each sublist is a row of data with the columns
        converted to the specified types.
        """
        chunk = []
        cnt = 0
        for i in range(lines):
            line = next(reader).strip("\n").split(",")
            cnt += 1
            row = [0] * len(self.column_types)
            for k, column_type in enumerate(self.column_types):

                row[k] = self.convert(line[k], column_type)
            chunk.append(row)
            # current_memory_bytes = current_process.memory_info().rss
            # current_memory_gb = current_memory_bytes / 1024 / 1024
            # print(f"Current memoryd usage: {current_memory_gb:.2f} MB {cnt}")
            # gc.collect()
        return chunk

    def sortCSVReaderFast(self, file_path, on, chunk_size, out_path):
        total_rows = sum(1 for _ in open(file_path)) - 1
        chunks = []
        df1_columns = pd.read_csv(file_path, nrows=0,engine = 'python').columns
        self.columns = {}
        self.header = df1_columns
        for i in range(len(df1_columns)):
            self.columns[df1_columns[i]] = i
        idx = self.columns[on]
        os.mkdir(f"{file_path}_tmp")
        cnt = 0
        for chunk_start in range(0, total_rows, chunk_size):
            if (chunk_start == 0):
                df1_chunk = pd.read_csv(
                    file_path,
                    nrows=chunk_size,
                    engine='python',
                    names=df1_columns if chunk_start > 0 else None,  # Use stored column names
                    header=0 if chunk_start == 0 else None
                )
            else:
                df1_chunk = pd.read_csv(
                    file_path,
                    skiprows=lambda x: x <= chunk_start,
                    nrows=chunk_size,
                    engine='python',
                    names=df1_columns if chunk_start > 0 else None,  # Use stored column names
                    header=0 if chunk_start == 0 else None
                )

            df1_chunk = df1_chunk.sort_values(by=on)
            chunks.append(len(df1_chunk))
            df1_chunk.to_csv(file_path+"_tmp/"+str(cnt) +
                             ".csv", mode='a', index=False, header=False)
            del df1_chunk
            cnt += 1
        heap = []
        ptrs = [0]*len(chunks)

        readers = []
        for i in range(len(chunks)):
            reader = open(file_path+"_tmp/"+str(i)+".csv", 'r', newline='')

            row = self.load_line(reader)

            heapq.heappush(heap, Row(row[idx], row, i))
            ptrs[i] += 1
            readers.append(reader)
        cnt = 0
        fi = open(out_path, 'w', newline='')
        writer = csv.writer(fi, delimiter=',')
        writer.writerow(self.header)
        while len(heap) > 0:
            item = heapq.heappop(heap)
            if ptrs[item.chunk] < chunks[item.chunk]:
                row = self.load_line(readers[item.chunk])
                heapq.heappush(heap, Row(row[idx], row, item.chunk))
                ptrs[item.chunk] += 1
            cnt += 1
            if (cnt % 10000 == 0):
                print(cnt)

            writer.writerow(item.row)
        # Remove a directory and its contents
        self.size = cnt
       # print(ptrs, cooldowns, chunks)
        shutil.rmtree(file_path+"_tmp")

    def sortCSVReader(self, file_path, on, chunk_size, out_path):
        total_rows = sum(1 for _ in open(file_path)) - 1
        chunks = []
        df1_columns = pd.read_csv(file_path, nrows=0, engine='python').columns
        self.columns = {}
        self.header = df1_columns
        for i in range(len(df1_columns)):
            self.columns[df1_columns[i]] = i
        idx = self.columns[on]
        os.mkdir(f"{file_path}_tmp")
        cnt = 0

        for chunk_start in range(0, total_rows, chunk_size):
            if (chunk_start == 0):
                df1_chunk = pd.read_csv(
                    file_path,
                    nrows=chunk_size,
                    engine='python',
                    names=df1_columns if chunk_start > 0 else None,  # Use stored column names
                    header=0 if chunk_start == 0 else None
                )
            else:
                df1_chunk = pd.read_csv(
                    file_path,
                    skiprows=lambda x: x <= chunk_start,
                    nrows=chunk_size,
                    engine='python',
                    names=df1_columns if chunk_start > 0 else None,  # Use stored column names
                    header=0 if chunk_start == 0 else None
                )

            df1_chunk = df1_chunk.sort_values(by=on)
            chunks.append(len(df1_chunk))
            df1_chunk.to_csv(file_path+"_tmp/"+str(cnt) +
                             ".csv", mode='a', index=False, header=False)
            del df1_chunk
            cnt += 1
        multiplicity = max(1, chunk_size//len(chunks))
        heap = []
        ptrs = [0]*len(chunks)
        cooldowns = [multiplicity]*len(chunks)

        readers = []
        for i in range(len(chunks)):
            reader = open(file_path+"_tmp/"+str(i)+".csv", 'r', newline='')

            rows = self.load_lines(file_path+"_tmp/"+str(i) +
                                   ".csv", reader, min(chunks[i], multiplicity))
            for row in rows:
                heapq.heappush(heap, Row(row[idx], row, i))
            ptrs[i] += multiplicity
            readers.append(reader)
        write_chunks = None
        cnt = 0
        firstchunk = True
        fi = open(out_path, 'w', newline='')
        writer = csv.writer(fi, delimiter=',')
        writer.writerow(self.header)
        while len(heap) > 0:
            item = heapq.heappop(heap)
            cooldowns[item.chunk] -= 1
            if cooldowns[item.chunk] <= 0 and ptrs[item.chunk] < chunks[item.chunk]:
                rows = self.load_lines(file_path+"_tmp/"+str(item.chunk) +
                                       ".csv", readers[item.chunk], min(chunks[i]-ptrs[item.chunk], multiplicity))
                ptrs[item.chunk] += multiplicity
                for row in rows:
                    heapq.heappush(heap, Row(row[idx], row, item.chunk))
                cooldowns[item.chunk] = multiplicity

            cnt += 1
            if (cnt % 10000 == 0):
                print(cnt)

            writer.writerow(item.row)
        # Remove a directory and its contents
        self.size = cnt
       # print(ptrs, cooldowns, chunks)
        shutil.rmtree(file_path+"_tmp")

    def sort(self, file_path, on, chunk_size, out_path):
        total_rows = sum(1 for _ in open(file_path)) - 1
        chunks = []
        df1_columns = pd.read_csv(file_path, nrows=0).columns
        os.mkdir(f"{file_path}_tmp")
        cnt = 0
        for chunk_start in range(0, total_rows, chunk_size):
            df1_chunk = pd.read_csv(
                file_path,
                skiprows=chunk_start + 1 if chunk_start > 0 else 0,
                nrows=chunk_size,
                engine='python',
                names=df1_columns if chunk_start > 0 else None,  # Use stored column names
                header=0 if chunk_start == 0 else None
            )

            df1_chunk = df1_chunk.sort_values(by=on)
            chunks.append(len(df1_chunk))
            df1_chunk.to_csv(file_path+"_tmp/"+str(cnt) +
                             ".csv", mode='a', index=False, header=False)
            cnt += 1
        heap = []
        index = [0]*len(chunks)
        multiplicity = max(1, chunk_size//len(chunks))
        for i in range(len(chunks)):
            rows = pd.read_csv(file_path+"_tmp/"+str(i) +
                               ".csv", skiprows=0, nrows=multiplicity, header=None, names=df1_columns)
            heapq.heappush(heap, Row(row.iloc[0][on], row, i))
        write_chunks = None
        cnt = 0
        firstchunk = True
        while len(heap) > 0:
            row = heapq.heappop(heap)
            index[row.chunk] += 1
            if index[row.chunk] < chunks[row.chunk]:
                row2 = pd.read_csv(file_path+"_tmp/"+str(row.chunk) +
                                   ".csv", skiprows=index[row.chunk], nrows=1, header=None, names=df1_columns)
                heapq.heappush(heap, Row(row2.iloc[0][on], row2, row.chunk))
            cnt += 1
            if (cnt % 10000 == 0):
                print(cnt)
            if write_chunks is not None:
                write_chunks = pd.concat(
                    [write_chunks, row.row], ignore_index=True)
                if len(write_chunks) >= chunk_size:
                    if firstchunk:
                        write_chunks.to_csv(
                            out_path, mode='w', index=False, header=True)
                        firstchunk = False
                    else:
                        write_chunks.to_csv(
                            out_path, mode='a', index=False, header=False)
                    write_chunks = None
            else:
                write_chunks = row.row
        # Remove a directory and its contents
        shutil.rmtree(file_path+"_tmp")

    def get_column_list(self):
        lst = []
        res = []
        for name in (self.columns):
            lst.append([self.columns[name], name])
        lst.sort()
        for pair in lst:
            res.append(pair[1])
        return res

    def get_size_info(self):
        memory_usage = 0
        for chunk in self.chunks:
            # Convert to MB
            memory_usage += chunk.memory_usage(deep=True).sum() / (1024**2)
        return f"Memory: {memory_usage:.2f} MB"

    def convert(self, obj, column_type):
        if (column_type == CType.INT):
            return int(obj)
        if (column_type == CType.FLOAT):
            return float(obj)
        if (column_type == CType.STRING):
            return obj
        if (column_type == CType.DATETIME_S):
            return datetime.strptime(obj, "%Y-%m-%d %H:%M:%S")

    def join_chunks(self, file1_path, file2_path, output_path, key1, key2, chunk_size=10000, first_chunk=True, write_output=True):
        # peak memory usage of 92.4 MiB
        """
        Chunks both and does pandas merge

        Parameters:
        - file1_path: Path to first CSV file
        - file2_path: Path to second CSV file
        - output_path: Where to save the merged results
        - chunk_size: Number of rows to process at a time
        - key: Column name to join on
        - first_chunk: Variable used to allow us to write to the same file upon calling this function multiple times (used in Grace Hash)
        """
        # print(f"[Pandasql join_chunks] joining {file1_path} on {key1} with {file2_path} on {key2} to {output_path} with chunk size {chunk_size}")
        total_rows1 = sum(1 for _ in open(file1_path)) - 1
        total_rows2 = sum(1 for _ in open(file2_path)) - 1

        df1_columns = pd.read_csv(file1_path, nrows=0, engine="python").columns
        df2_columns = pd.read_csv(file2_path, nrows=0, engine="python").columns


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
                merged_chunk = pd.merge(
                    df1_chunk, df2_chunk, left_on=key1, right_on=key2, how='inner')

                # if write_output and not merged_chunk.empty:
                #     if first_chunk:
                #         # First chunk: create new file with header
                #         merged_chunk.to_csv(output_path, index=False, mode='w')
                #         first_chunk = False
                #     else:
                #         # Subsequent chunks: append without header
                #         merged_chunk.to_csv(
                #             output_path, index=False, mode='a', header=False)

                del df2_chunk
                del merged_chunk

                progress1 = min(
                    100, (chunk1_start + chunk_size) / total_rows1 * 100)
                progress2 = min(
                    100, (chunk2_start + chunk_size) / total_rows2 * 100)
                print(
                    f"Progress: File1 {progress1:.1f}%, File2 {progress2:.1f}%")

            # Free memory
            del df1_chunk

    def load_csv_pandasql(self, file_path, chunk_size, column_types=None):
        """
        Loads a CSV file and split it into chunks.

        Args:
            file_path (str): The path to the CSV file.
            chunk_size (int): The size of each chunk in bytes.
            column_types (list[CType], optional): The types of columns. Defaults to None.

        Returns:
            None
        """
        self.chunkrows = []
        if (column_types):
            self.column_types = column_types
        current_process = psutil.Process()
        current_memory_bytes = current_process.memory_info().rss
        current_memory_gb = current_memory_bytes / 1024 / 1024
        print(f"Current memory usage: {current_memory_gb:.2f} MB")
        csvFile = open(file_path, 'r', newline='')
        reader = csv.reader(csvFile)
        if 1 == 1:
            current_chunk_size = 0
            rows = 0
            chunk_no = 0
            os.makedirs(self.name, exist_ok=True)

            fi = open(self.name+"/"+str(chunk_no)+".csv", 'w', newline='')
            self.chunks = [self.name+"/"+str(chunk_no)+".csv"]
            writer = csv.writer(fi, delimiter=',')
            firstLine = True
            columns = next(reader)
            print("columns", columns)
            self.columns = {}
            for i in range(len(columns)):
                self.columns[columns[i]] = i
            for line in reader:
                # if firstLine:
                #     self.columns = line
                #
                #     firstLine = False
                #     continue
                line_size = 0
                for obj in line:
                    # line_size += sys.getsizeof(obj)
                    line_size += len(obj)
                if (current_chunk_size + line_size > chunk_size):
                    fi.close()
                    current_chunk_size = 0
                    self.chunkrows.append(rows)
                    rows = 0
                    chunk_no += 1

                    fi = open(self.name + "/"+str(chunk_no) +
                              ".csv", 'w', newline='')
                    self.chunks.append(self.name+"/"+str(chunk_no)+".csv")
                    writer = csv.writer(fi, delimiter=',')
                    gc.collect()
                    if (chunk_no % 30 == 0):
                        current_memory_bytes = current_process.memory_info().rss
                        current_memory_gb = current_memory_bytes / 1024 / 1024
                        print(
                            f"Current memory usage: {current_memory_gb:.2f} MB")
                writer.writerow(line)
                current_chunk_size += line_size
                rows += 1
            fi.close()
        csvFile.close()

    def process_csv_file(self,
                         file_path: str,
                         chunk_size: int = 100000,
                         columns: Optional[list] = None,
                         **csv_kwargs
                         ) -> pd.DataFrame:
        """
        Process a CSV file in chunks and combine results.

        file_path: Path to the CSV file
        chunk_size: Number of rows to load in each chunk
        columns: Specific columns to load. If None, loads all columns.
        csv_kwargs: Additional arguments to pass to pd.read_csv
        """
        total_rows = 0
        if 1 == 1:
            print("hi")
        # Get total file size for progress monitoring
        file_size = os.path.getsize(file_path)

        for i, chunk in enumerate(self.load_csv_chunked(file_path, chunk_size, columns, **csv_kwargs)):
            self.chunks.append(pd.DataFrame(chunk))
            total_rows += len(chunk)
            # current_process = psutil.Process()
            # current_memory_bytes = current_process.memory_info().rss
            # current_memory_gb = current_memory_bytes / 1024 / 1024 / 1024
            # print(f"Current memory usage: {current_memory_gb:.2f} GB\n")
            print(f"Processed chunk {i+1}: {total_rows:,} rows", end='\r')
        print(f"\nCompleted loading {total_rows:,} total rows")

        # print("len(self.chunks):", len(self.chunks))
        # print("self.chunks:", self.chunks)
    
    def chunk_both_merge(self, file1_path, file2_path, output_path, chunk_size=10000, key='key1', write_output=True):
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

    def chunked_merge(self, file1_path, file2_path, output_path, chunk_size=10000, key='key1', write_output=True):
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

    def naiive_pandas_join(self, file1, file2, write_output=True):
        #roughly takes 4339.0 MiB according to fil
        df1 = pd.read_csv(file1, engine='python')
        df2 = pd.read_csv(file2, engine='python')
        merged = pd.merge(df1, df2, on='key1', how='inner')
        if (write_output):
            merged.to_csv('data/merged_A.csv', index=False)
        return merged

    def custom_hash(self, x, num_partitions):
        if pd.isna(x):  # for NaN values
            return 0
        return int(hashlib.sha256(str(x).encode('utf-8')).hexdigest(), 16) % num_partitions

    # def pandasql_grace_hash_join(self, file1_path, file2_path, output_path, chunk_size=10000, key='key1', num_partitions=100):
    #     """
    #     Chunks data located at both `file1_path` and `file2_path` and performs a grace hash join.

    #     Parameters:
    #     file1_path: Path to first CSV file
    #     file2_path: Path to second CSV file
    #     output_path: Path to save the merged output
    #     chunk_size: Number of rows to process at a time
    #     key: Name of column to join on
    #     num_partitions: Number of hash buckets
    #     """
    #     # hash all keys, bucket hashes 
    #     total_rows1 = sum(1 for _ in open(file1_path)) - 1
    #     total_rows2 = sum(1 for _ in open(file2_path)) - 1

    #     df1_columns = pd.read_csv(file1_path, nrows=0).columns
    #     df2_columns = pd.read_csv(file2_path, nrows=0).columns

    #     # Path to where hash bucket CSVs are stored -- ensures that it is cleared if already existing
    #     bucket_path = f"{os.path.relpath(file1_path)}-{os.path.relpath(file2_path)}-ghjoin" 
    #     if os.path.exists(bucket_path):
    #         shutil.rmtree(bucket_path)

    #     output_dir = "ghjoin" # SPECIFY OUTPUT DIR OF FILE
    #     os.makedirs(output_dir, exist_ok=True)

    #     # helper function to process a file & populate hash buckets
    #     def process_file(file_path, columns, total_rows, output_dir, file_prefix):
    #         for i in range(num_partitions):
    #             output_path = os.path.join(output_dir, f'{file_prefix}_bucket_{i}.csv')
    #             with open(output_path, mode='w', newline='') as file:
    #                 writer = csv.writer(file)
    #                 writer.writerow(columns)

    #         for chunk_start in range(0, total_rows, chunk_size):
    #             chunk = pd.read_csv(
    #                 file_path,
    #                 skiprows=chunk_start + 1 if chunk_start > 0 else 0,
    #                 nrows=chunk_size,
    #                 engine='python',
    #                 names=columns if chunk_start > 0 else None,
    #                 header=0 if chunk_start == 0 else None
    #             )
    #             chunk['hash'] = chunk[key].apply(lambda x: self.custom_hash(x, num_partitions))
    #             for _, row in chunk.iterrows():
    #                 row_hash = row['hash']
    #                 del row['hash']
    #                 output_path = os.path.join(output_dir, f'{file_prefix}_bucket_{row_hash}.csv')
    #                 with open(output_path, mode='a', newline='') as file:
    #                     writer = csv.writer(file)
    #                     writer.writerow(row)
    #             del chunk # free memory
            
    #     output_dir = f"{os.path.relpath(file1_path)}-{os.path.relpath(file2_path)}-ghjoin"
    #     print(f"{output_dir=}")
    #     os.makedirs(output_dir, exist_ok=True)

    #     process_file(file1_path, df1_columns, total_rows1, output_dir, "file1")
    #     process_file(file2_path, df2_columns, total_rows2, output_dir, "file2") 

    #     # for every hash bucket, join the rows from file1 and file2
    #     first_chunk=True
    #     # for hash_key in set(hash_buckets_file1.keys()).intersection(hash_buckets_file2.keys()):
    #     for hash_key in range(num_partitions):
    #         bucket1_path = f'{output_dir}/file1_bucket_{hash_key}.csv'
    #         bucket2_path = f'{output_dir}/file2_bucket_{hash_key}.csv'
    #         # print(f"Processing hash_key: {hash_key}, extracting from bucket1_path: {bucket1_path} & bucket2_path: {bucket2_path}")

    #         # call nested join function
    #         self.join_chunks(bucket1_path, bucket2_path, output_path, key, key, first_chunk=first_chunk, chunk_size=math.floor(chunk_size ** 0.5))
    #         if first_chunk: # after first chunk, want to keep appending to same file
    #             first_chunk = False

    #         os.remove(bucket1_path)
    #         os.remove(bucket2_path)

    def pandasql_grace_hash_join(self, file1_path, file2_path, output_path, chunk_size=10000, join_chunk_size=10000, key='key1', num_partitions=100, write_output=True):
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

        df1_columns = pd.read_csv(file1_path, nrows=0, engine='python').columns
        df2_columns = pd.read_csv(file2_path, nrows=0, engine='python').columns

        # Path to where hash bucket CSVs are stored -- ensures that it is cleared if already existing
        bucket_path = f"{os.path.relpath(file1_path)}-{os.path.relpath(file2_path)}-ghjoin" 
        if os.path.exists(bucket_path):
            shutil.rmtree(bucket_path)

        output_dir = "ghjoin" # SPECIFY OUTPUT DIR OF FILE
        os.makedirs(output_dir, exist_ok=True)

        # helper function to process a file & populate hash buckets
        skiprows_fn = lambda chunk_start: (lambda x: x <= chunk_start if chunk_start > 0 else None)
        header_fn = lambda chunk_start: 0 if chunk_start == 0 else None
        names_fn = lambda chunk_start, df1_columns: df1_columns if chunk_start > 0 else None
        def process_file(file_path, columns, total_rows, output_dir, file_prefix):
            for i in range(num_partitions):
                output_path = os.path.join(output_dir, f'{file_prefix}_bucket_{i}.csv')
                with open(output_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(columns)

            for chunk_start in range(0, total_rows, chunk_size):
                chunk = pd.read_csv(
                    file_path,
                    skiprows=skiprows_fn(chunk_start),
                    nrows=chunk_size,
                    engine='python',
                    names=names_fn(chunk_start, columns),
                    header=header_fn(chunk_start)
                )
                chunk['hash'] = chunk[key].apply(lambda x: self.custom_hash(x, num_partitions))
                for hash_value, group in chunk.groupby('hash'):
                    output_path = os.path.join(output_dir, f'{file_prefix}_bucket_{hash_value}.csv')

                    group.drop('hash', axis=1).to_csv(
                        output_path,
                        mode='a',
                        header=False,
                        index=False
                    )
                del chunk # free memory

        output_dir = f"{os.path.relpath(file1_path)}-{os.path.relpath(file2_path)}-ghjoin"
        print(f"{output_dir=}")
        os.makedirs(output_dir, exist_ok=True)

        import time
        start_time = time.time()
        process_file(file1_path, df1_columns, total_rows1, output_dir, "file1")
        process_file(file2_path, df2_columns, total_rows2, output_dir, "file2") 

        print(f"Time taken to process files: {time.time() - start_time}")
        print("Processing hash buckets...", time.time() - start_time)

        # for every hash bucket, join the rows from file1 and file2
        first_chunk=True
        for hash_key in range(num_partitions):
            bucket1_path = f'{output_dir}/file1_bucket_{hash_key}.csv'
            bucket2_path = f'{output_dir}/file2_bucket_{hash_key}.csv'
            print(f"Processing hash_key: {hash_key}, extracting from bucket1_path: {bucket1_path} & bucket2_path: {bucket2_path}")

            # call nested join function
            self.join_chunks(bucket1_path, bucket2_path, output_path, key, key, chunk_size=join_chunk_size, first_chunk=first_chunk, write_output=write_output)
            if first_chunk: # after first chunk, want to keep appending to same file
                first_chunk = False

            os.remove(bucket1_path)
            os.remove(bucket2_path)
        print(f"Grace hash join completed", time.time() - start_time)

