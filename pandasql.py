import csv
import gc
import psutil
from datetime import datetime

import os
import psutil
import sys

import numpy as np
import pandas as pd

from datetime import datetime
from enum import Enum
from typing import Optional, Iterator


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
        self.name = name
        self.chunks = initchunks if initchunks else []
        self.columns = columns
        self.column_types = column_types

    def merge(self, other, on):
        """
        Merge a Pandasql object with another Pandasql object on specified keys.

        other: The other Pandasql object to merge with
        on: List of keys to perform the merge operation on
        """
        # print(f"MERGING DF {self.name} WITH OTHER DF {other.name} ON COLS {on}")
        newchunks = []
        for k, chunk1 in enumerate(self.chunks):
            # print("PROCESSING CHUNK1", chunk1)
            for chunk2 in other.chunks:
                # print("PROCESSING CHUNK2", chunk2)
                newchunks.append(pd.merge(chunk1, chunk2, on=on, how='inner'))
                del chunk2
            current_memory_mb = psutil.Process().memory_info().rss / 1024 / \
                1024  # Convert bytes to MB
            print(f"{k} Current memory usage: {current_memory_mb:.2f} MB")
     #   return Pandasql(newchunks)

        return Pandasql(f"{self.name}-{other.name}", initchunks=newchunks)

    def join(self, other: "Pandasql", onSelf, onOther, name, chunk_size):
        if (self.columns is None) or (other.columns is None):
            return None

        ind1 = self.columns[onSelf]
        ind2 = other.columns[onOther]
        if (self.column_types[ind1] != other.column_types[ind2]):
            print("JoinERROR: DIFFERENT TYPE")
            return None
        newColumns = self.columns.copy()
        for c in other.columns:
            if c not in newColumns:
                newColumns[c] = len(newColumns)+other.columns[c]
            else:
                newColumns[c+"_"+other.name] = len(newColumns)+other.columns[c]

        new_column_types = self.column_types[:]+other.column_types[:]

        chunk_no = 0
        current_chunk_size = 0
        os.makedirs(name, exist_ok=True)
        chunks = [name+"/"+str(chunk_no)+".csv"]
        fi = open(name+"/"+str(chunk_no)+".csv", 'w', newline='')

        writer = csv.writer(fi, delimiter=',')
        for f1 in self.chunks:
            chunk1 = self.load_chunk(f1)
            for f2 in other.chunks:
                chunk2 = other.load_chunk(f2)
                for c1 in chunk1:
                    for c2 in chunk2:
                        if (c1[ind1] == c2[ind2]):
                          #  line = c1[:ind1]+c1[ind1+1:]+[c1[ind1]]+c2[:ind2]+c2[ind2+1:]
                            line = c1+c2
                            line_size = 0
                            for obj in line:
                                # line_size += sys.getsizeof(obj)
                                line_size += len(str(obj))
                            if (current_chunk_size + line_size > chunk_size):
                                fi.close()
                                current_chunk_size = 0
                                chunk_no += 1
                                fi = open(name+"/" + str(chunk_no) +
                                          ".csv", 'w', newline='')
                                writer = csv.writer(fi, delimiter=',')
                                chunks.append(name+"/"+str(chunk_no)+".csv")
                                gc.collect()
                            writer.writerow(line)
                            current_chunk_size += line_size
        fi.close()
        return Pandasql(name, initchunks=chunks, columns=newColumns, column_types=new_column_types)

    def load_csv_chunked(self,
                         file_path: str,
                         chunk_size: int = 100000,
                         columns: Optional[list] = None,
                         **csv_kwargs
                         ) -> Iterator[pd.DataFrame]:
        """
        Load a CSV file in chunks to manage memory usage.

        file_path: Path to the CSV file
        chunk_size: Number of rows to load in each chunk
        columns: Specific columns to load. If None, loads all columns.
        csv_kwargs: Additional arguments to pass to pd.read_csv
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
            i += 1

    def get_size_info(self):
        memory_usage = 0
        for chunk in self.chunks:
            # Convert to MB
            memory_usage += chunk.memory_usage(deep=True).sum() / (1024**2)
        return f"Memory: {memory_usage:.2f} MB"

    def convert(self, obj, column_type):
        # match column_type:
        #     case CType.INT:
        #         return int(obj)
        #     case CType.FLOAT:
        #         return float(obj)
        #     case CType.STRING:
        #         return obj
        #     case CType.DATETIME_S:
        #         return datetime.strptime(obj, "%Y-%m-%d %H:%M:%S")
        #     case _:
        #         raise ValueError(f"Unsupported column type: {column_type}")

        if (column_type == CType.INT):
            return int(obj)
        if (column_type == CType.FLOAT):
            return float(obj)
        if (column_type == CType.STRING):
            return obj
        if (column_type == CType.DATETIME_S):
            return datetime.strptime(obj, "%Y-%m-%d %H:%M:%S")

    def load_chunk(self, file_path):
        csvFile = open(file_path, 'r', newline='')
        reader = csv.reader(csvFile)
        chunk = []
        current_process = psutil.Process()
        cnt = 0
        for line in reader:
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

    def load_csv_pandasql(self, file_path, chunk_size, column_types=None):
        self.column_types = column_types
        current_process = psutil.Process()
        current_memory_bytes = current_process.memory_info().rss
        current_memory_gb = current_memory_bytes / 1024 / 1024
        print(f"Current memory usage: {current_memory_gb:.2f} MB")
        csvFile = open(file_path, 'r', newline='')
        reader = csv.reader(csvFile)
        if 1 == 1:
            current_chunk_size = 0
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
