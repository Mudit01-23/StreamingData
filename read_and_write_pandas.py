"""
This script extracts a zip folder containing CSV files, creates separate dataframes for each room, merges them, and saves the final dataframe to a CSV file.
"""

import os
import pandas as pd
import zipfile
from functools import reduce

# Define directories
zip_folder = '/Users/muditjoshi/Downloads/elk_stack_mac/sensors_instrumented_in_an_office_building_dataset.zip'
extracted_folder = '/Users/muditjoshi/Desktop/StreamingData/KETI_clean'
output_csv = '/Users/muditjoshi/Desktop/StreamingData/sensors.csv'

# Columns to read from CSV files
columns = ['co2', 'humidity', 'light', 'pir', 'temperature']


def extract_zip(zip_file, extracted_folder):
    """
    Extracts the zip file to the specified directory.
    """
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)


def create_separate_dataframes(directory):
    """
    Creates a dictionary that includes room numbers as keys and dataframes per room as values.
    """
    dataframes = {}  # Initialize dataframes dictionary
    dataframes_room = {}
    for filename in os.listdir(directory):
        new_directory = os.path.join(directory, filename)
        if os.path.isdir(new_directory):  # Check if it's a directory
            dfs = []
            count = 0  # Initialize count here
            for new_files in os.listdir(new_directory):
                f = os.path.join(new_directory, new_files)
                if os.path.isfile(f) and f.endswith('.csv'):
                    my_path = filename + '_' + new_files.split('.')[0]  # e.g., 656_co2
                    dataframes[my_path] = pd.read_csv(f, names=['ts_min_bignt', columns[count]])
                    dfs.append(dataframes[my_path])
                    count += 1  # Increment count for the next column
            if dfs:
                dataframes_room[filename] = reduce(
                    lambda left, right: pd.merge(left, right, on='ts_min_bignt', how='inner'), dfs)
                dataframes_room[filename]['room'] = filename  # Adds room number as column

    return dataframes_room


def create_main_dataframe(separate_dataframes):
    """
    Concatenates all per-room dataframes vertically. Creates final dataframe.
    """
    dataframes_to_concat = list(separate_dataframes.values())
    df = pd.concat(dataframes_to_concat, ignore_index=True)
    df = df.sort_values('ts_min_bignt')  # Sort data by timestamp
    df.dropna(inplace=True)  # Drop rows with NaN values
    df["event_ts_min"] = pd.to_datetime(df["ts_min_bignt"], unit='s')  # Convert timestamp to datetime

    return df


def write_main_dataframe(df, output_csv):
    """
    Writes the final dataframe to a CSV file.
    """
    df.to_csv(output_csv, index=False)


if __name__ == '__main__':
    # Extract zip file
    extract_zip(zip_folder, extracted_folder)

    # Create separate dataframes per room
    all_dataframes = create_separate_dataframes(extracted_folder)

    # Create main dataframe by merging all rooms
    main_dataframe = create_main_dataframe(all_dataframes)

    # Write main dataframe to CSV
    write_main_dataframe(main_dataframe, output_csv)
