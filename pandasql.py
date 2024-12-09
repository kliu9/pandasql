import csv
import gc
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

    def join(self, other: "Pandasql", onSelf, onOther, name, chunk_size):
        """
        Join this Pandasql with another Pandasql on given columns.

        Parameters
        ----------
        other : Pandasql
            Other Pandasql object to join with
        onSelf : str
            Column name in this Pandasql to join on
        onOther : str
            Column name in `other` Pandasql to join on
        name : str
            Name of the new Pandasql object
        chunk_size : int
            Size of each chunk in bytes

        Returns
        -------
        newPandasql : Pandasql
            New Pandasql object with the joined data
        """
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
            break
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
        """
        Loads a chunk of data from a CSV file.

        file_path: Path to the CSV file
        Returns a list of lists, where each sublist is a row of data with the columns
        converted to the specified types.
        """
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
        return pd.DataFrame(chunk, columns=self.get_column_list())

    # def join_chunks(self, other, onSelf, onOther, name1, name2, chunk_size, rows, orows):

    #     print(chunk.head())
    #     index = 0
    #     while index < rows:
    #         chunk = pd.read_csv(name1, skiprows=range(0, index), nrows=min(chunk_size, rows-index),
    #                             names=self.get_column_list(), engine='python')
    #         index += chunk_size
    #         oindex = 0
    #         while oindex < orows:
    #             ochunk = pd.read_csv(name2, skiprows=range(0, oindex), nrows=min(chunk_size, orows-oindex),
    #                                  names=self.get_column_list(), engine='python')
    #             index += chunk_size

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

                if write_output and not merged_chunk.empty:
                    if first_chunk:
                        # First chunk: create new file with header
                        merged_chunk.to_csv(output_path, index=False, mode='w')
                        first_chunk = False
                    else:
                        # Subsequent chunks: append without header
                        merged_chunk.to_csv(
                            output_path, index=False, mode='a', header=False)

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
