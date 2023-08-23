import duckdb
import pandas as pd
import os
import logging
import time


def initialize_logging():
    logging.basicConfig(level=logging.INFO)
    logging.info('Logging initialized')


def connect_to_database(db_path=':memory:'):
    conn = duckdb.connect(database=db_path, read_only=False)
    logging.info('Connected to database')
    return conn


# Main initialization
initialize_logging()
conn = connect_to_database()


def read_fixed_length_file(file_path, structure):
    logging.info(f'Reading fixed-length file: {file_path}')
    start_time = time.time()
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            row = {}
            for start, end, column_name in structure:
                row[column_name] = line[start:end].strip()
            data.append(row)
    logging.info(f'Read fixed-length file completed in {time.time() - start_time} seconds')
    return data


def load_data_into_duckdb(conn, file_path, table_name, mapping_file=None):
    logging.info(f'Loading data into DuckDB from file: {file_path}')
    start_time = time.time()
    extension = os.path.splitext(file_path)[1].lower()
    if extension == '.dat':
        # Read the structure from the Mapping.csv file
        mapping_df = pd.read_csv(mapping_file)
        # Trim spaces from the field_name column
        mapping_df['IGP Base Field'] = mapping_df['IGP Base Field'].str.strip()
        column_names = mapping_df['IGP Base Field'].tolist()
        structure = list(zip(mapping_df["'Start Position'"], mapping_df['End Position'], column_names))
        data = read_fixed_length_file(file_path, structure)
        conn.register(table_name, pd.DataFrame(data, columns=column_names))
    elif extension == '.csv':
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
    elif extension == '.xml':
        data = pd.read_xml(file_path)
        conn.register(table_name, data)
    elif extension == '.json':
        data = pd.read_json(file_path)
        conn.register(table_name, data)
    else:
        raise ValueError(f"Unsupported file format: {extension}")
    # Return the column names for the loaded table
    logging.info(f'Loading data into DuckDB completed in {time.time() - start_time} seconds')
    column_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [col[1] for col in column_info]


def run_data_comparison(conn, src_path, trg_path, primary_keys, table_src='table_src', table_trg='table_trg'):
    logging.info('Running data comparison')
    start_time = time.time()

    # Loading data into DuckDB
    columns_src_names = load_data_into_duckdb(conn, src_path, table_src)
    columns_trg_names = load_data_into_duckdb(conn, trg_path, table_trg)

    # Define keys string for SQL queries
    # keys_str = ' AND '.join([f"a.{key} = b.{key}" for key in PRIMARY_KEYS])
    keys_str = ' AND '.join([f'a."{key}" = b."{key}"' for key in primary_keys])

    # List of columns to compare excluding primary keys
    compare_columns = [col for col in columns_src_names if col not in primary_keys]

    # Data breaks
    start_time = time.time()
    total_data_break_query = f"""
    SELECT a.*, b.*
    FROM {table_src} AS a
    LEFT JOIN {table_trg} AS b ON ({keys_str})
    """
    total_data_break_data = conn.execute(total_data_break_query).fetchall()
    logging.info(f"Total Data Break Query completed in {time.time() - start_time} seconds.")

    # Determine the indices for the source and target columns
    src_indices = [columns_src_names.index(col) for col in compare_columns]
    trg_indices = [columns_trg_names.index(col) + len(columns_src_names) for col in compare_columns]

    # Prepare the data for CSV
    total_data_break = []
    for row in total_data_break_data:
        mismatched_columns = {}
        for key in primary_keys:
            key_index = columns_src_names.index(key)
            mismatched_columns[key] = row[key_index]
        for idx, col in enumerate(compare_columns):
            src_value = row[src_indices[idx]]
            # Check if the target value exists
            if trg_indices[idx] < len(row):
                trg_value = row[trg_indices[idx]]
            else:
                trg_value = None  # Set to None if target value doesn't exist
            # Check for mismatch and capture the column name and differences
            if src_value != trg_value:
                mismatched_columns[f"{col}_Expected"] = src_value
                mismatched_columns[f"{col}_Actual"] = trg_value
        if len(mismatched_columns) > len(primary_keys):
            total_data_break.append(mismatched_columns)

    # Write the data to CSV
    pd.DataFrame(total_data_break).to_csv('total_data_break.csv', index=False)

    # Only in Source
    start_time = time.time()
    only_in_source_query = f"""
    SELECT a.*
    FROM {table_src} AS a
    LEFT JOIN {table_trg} AS b ON ({keys_str})
    WHERE b.{primary_keys[0]} IS NULL
    """
    only_in_source = conn.execute(only_in_source_query).fetchall()
    logging.info(f"Only in Source Query completed in {time.time() - start_time} seconds.")
    pd.DataFrame(only_in_source, columns=columns_src_names).to_csv("only_in_source.csv", index=False)

    # Only in Target
    start_time = time.time()
    only_in_target_query = f"""
    SELECT b.*
    FROM {table_trg} AS b
    LEFT JOIN {table_src} AS a ON ({keys_str})
    WHERE a.{primary_keys[0]} IS NULL
    """
    only_in_target = conn.execute(only_in_target_query).fetchall()
    logging.info(f"Only in Target Query completed in {time.time() - start_time} seconds.")
    pd.DataFrame(only_in_target, columns=columns_trg_names).to_csv("only_in_target.csv", index=False)

    # Compute metrics and write to separate CSVs
    keys_str_no_alias = ', '.join(primary_keys)

    # Source Key Duplicate
    start_time = time.time()
    source_key_duplicate_query = f"""
    SELECT *
    FROM {table_src}
    WHERE ({keys_str_no_alias}) IN
        (SELECT {keys_str_no_alias}
         FROM {table_src}
         GROUP BY ({keys_str_no_alias})
         HAVING COUNT(*) > 1)
    """

    source_key_duplicate = conn.execute(source_key_duplicate_query).fetchall()
    logging.info(f"Source Key Duplicate Query completed in {time.time() - start_time} seconds.")
    pd.DataFrame(source_key_duplicate, columns=columns_src_names).to_csv('source_key_duplicate.csv', index=False)

    # Target Key Duplicate
    start_time = time.time()
    target_key_duplicate_query = f"""
    SELECT *
    FROM {table_trg}
    WHERE ({keys_str_no_alias}) IN
        (SELECT {keys_str_no_alias}
         FROM {table_trg}
         GROUP BY ({keys_str_no_alias})
         HAVING COUNT(*) > 1)
    """
    target_key_duplicate = conn.execute(target_key_duplicate_query).fetchall()
    logging.info(f"Target Key Duplicate Query completed in {time.time() - start_time} seconds.")
    pd.DataFrame(target_key_duplicate, columns=columns_trg_names).to_csv('target_key_duplicate.csv', index=False)

    # Metrics for the summary
    source_total = conn.execute(f"SELECT COUNT(*) FROM {table_src}").fetchone()[0]
    target_total = conn.execute(f"SELECT COUNT(*) FROM {table_trg}").fetchone()[0]
    total_key_matched = \
        conn.execute(f"SELECT COUNT(*) FROM {table_src} AS a JOIN {table_trg} AS b ON ({keys_str})").fetchone()[0]
    only_in_source_count = len(only_in_source)
    only_in_target_count = len(only_in_target)

    # Detect duplicates in source and target based on primary keys
    source_key_duplicate_count = \
        conn.execute(f"SELECT COUNT(DISTINCT {', '.join(primary_keys)}) FROM {table_src}").fetchone()[0]
    target_key_duplicate_count = \
        conn.execute(f"SELECT COUNT(DISTINCT {', '.join(primary_keys)}) FROM {table_trg}").fetchone()[0]

    # Calculate total data breaks
    total_data_break_count = len(total_data_break)

    # Create summary dictionary
    summary_data = {
        'Source Total': source_total,
        'Target Total': target_total,
        'Total Data Breaks': total_data_break_count,
        'Only in Source': only_in_source_count,
        'Only in Target': only_in_target_count,
        'Source Key Duplicate': source_key_duplicate_count,
        'Target Key Duplicate': target_key_duplicate_count
    }

    # Write the summary data to CSV
    pd.DataFrame([summary_data]).to_csv("summary.csv", index=False)

    logging.info(f'Data comparison completed in {time.time() - start_time} seconds')


def main():
    src_path = 'C:/Users/ranji/OneDrive/PycharmProjects/pythontest/src/Org.csv'
    trg_path = 'C:/Users/ranji/OneDrive/PycharmProjects/pythontest/trg/Org.csv'
    primary_keys = ['Organization_Id']
    run_data_comparison(conn, src_path, trg_path, primary_keys)


if __name__ == "__main__":
    main()
