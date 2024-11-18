import time
from functions import (
    count_csv_rows,
    count_csv_rows_dask,
    get_column_names,
    get_unique_values_by_column,
    display_top_rows,
    get_random_sample,
)

file_path = "test_data/2020_Yellow_Taxi_Trip_Data.csv"

# column_names = get_column_names(file_path)
# print(f"Column names: {column_names}")

# unique_values = get_unique_values_by_column(file_path, "VendorID")
# print(f"Unique values: {str(unique_values)}")

# display_top_rows(file_path, 10)

# p_start_time = time.time()
rows_pandas = count_csv_rows(file_path)
# pandas_time = time.time() - p_start_time

print(f"Total rows: {rows_pandas}")
# print(f"Time (Pandas): {pandas_time}")

# # Time the Dask approach
# d_start_time = time.time()
# rows_dask = count_csv_rows_dask(file_path)
# dask_time = time.time() - d_start_time

# print(f"Total rows (Dask): {rows_dask}")
# print(f"Time (Dask): {dask_time}")
sample = get_random_sample(file_path)
print(sample)