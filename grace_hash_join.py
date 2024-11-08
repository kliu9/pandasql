from helpers import process_csv_file, limit_memory_relative

def grace_hash_join(file1, file2):
    df1 = process_csv_file(file1, chunk_size=50)
    df2 = process_csv_file(file2, chunk_size=50)

    limit_memory_relative(75)

    # implement join and show that it works for 75 mb when out_of_memory.py doesn't

    print(df1)
    print(df2)

grace_hash_join("data/A.csv", "data/B.csv")