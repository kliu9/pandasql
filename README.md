# PandaSQL
PandaSQL uses the existing Pandas infrastructure and leverages NumPy’s parallel data processing abilities to implement out-of-memory joins with efficient data loading.

## Running Join Algorithms
PandaSQL currently supports four join algorithms: Naive Join, Naive Chunk Both Join, Naive Chunk One Join, Grace Hash Join. See paper for descriptions of algorithms and implementation details.

The entrypoint to PandaSQL is `join.py`. At the top, specify any arguments (i.e. path to files to join, path to store joined output). The bulk of the logic lives in `pandasql.py` -- please consult docstrings of the corresponding functions for additional arguments (i.e. chunk size, whether to write to output, number of partitions). Ensure that any user-specified files and directories exist.

To run any join algorithm on our sample datasets (`data/A.csv` & `data/B.csv`), simply uncomment any line (15, 18, 21, 24) in `join.py`. With the correct versions of these datasets, verify that the statistics (uncommenting the last few lines) output "The resulting CSV file is 255.97 MB and has 2020000 rows.".

## Profile Code
### Latency:
Use the `timeit` package to wrap a function call. For example, for `pandasql_grace_hash_join`:
```
execution_time = timeit.timeit(lambda: pandasql_grace_hash_join(file1_path, file2_path, output_path), number=1)
```

### Memory: 
To profile memory usage of a given file (i.e. `join.py`):
1. Install fil by following these installation instructions for your development environment: https://pythonspeed.com/fil.
2. In a terminal, run `fil-profile run join.py`, which will open a webpage in your browser demonstrating the memory usage of the program.
