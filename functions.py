import pandas as pd

def count_csv_rows(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Count the number of lines in the file
        row_count = sum(1 for _ in file)
    # Subtract 1 for the header row (if present)
    return row_count - 1  # Adjust if there's no header


def get_unique_values(file_path, column_name):
    chunk_size = 10**6  # Number of rows per chunk
    unique_values = set()

    for chunk in pd.read_csv(file_path, usecols=[column_name], chunksize=chunk_size):
        unique_values.update(chunk[column_name].dropna().unique())
    
    return unique_values

def get_column_names(file_path):
    # Read only the header row
    column_names = pd.read_csv(file_path, nrows=0).columns.tolist()
    return column_names