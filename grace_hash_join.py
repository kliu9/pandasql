from out_of_memory import process_csv_file

def grace_hash_join(file1, file2):
    df1 = process_csv_file(file1, chunk_size=500)
    df2 = process_csv_file(file2, chunk_size=500)

    pass


grace_hash_join("A.csv", "B.csv")