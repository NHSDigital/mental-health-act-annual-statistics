"""
This script checks whether pairs of CSVs are the same as each other.

To use:
    files_to_compare: [(String, String)] is imported from params.py. It contains pairs of filenames to be tested.
    OUTPUT_DIR: String and GROUND_TRUTH_DIR: String are also imported from params.py. They are the respective locations of the pair of files.

"""

import pandas as pd
import pathlib2
from backtesting_params import bt_params

for (target_filename, gtruth_filename) in bt_params['files_to_compare']:
    target_df = pd.read_csv(pathlib2.Path(bt_params['OUTPUT_DIR']) / target_filename)
    gtruth_df = pd.read_csv(pathlib2.Path(bt_params['GROUND_TRUTH_DIR']) / gtruth_filename)

    # Filter to get November stats only
    filtered_gtruth_df = gtruth_df[(gtruth_df['DATE'].isin(['30/11/2021']))] # {date_field}

    # remove DATE columns due to being unable to compare columns
    filtered_gtruth_df = filtered_gtruth_df.drop(['DATE'], axis=1)
    target_df = target_df.drop(['DATE'], axis=1)

    # Fix order of the rows - NOTE: BREAKDOWN_TYPE, and columns containing values will fail the last test.
    # NOTE: This will make TEST 2 pass, might need to add a condition e.g. if TEST 2 fails run the below
    filtered_gtruth_df = filtered_gtruth_df.sort_values(by=filtered_gtruth_df.columns.tolist()).reset_index(drop=True)
    target_df = target_df.sort_values(by=target_df.columns.tolist()).reset_index(drop=True)

    filtered_gtruth_df = filtered_gtruth_df.round(2)
    target_df = target_df.round(2)

    # replace blanks with zeros in the rates column
    target_df = target_df.fillna(0)

    print(f"\n Testing file: {gtruth_filename} against {target_filename}")

    try:
        # test csvs have the same number of rows and cols       
        assert target_df.shape == filtered_gtruth_df.shape
        print(f"\nTest 1 status: PASS \nTest 1 Details: CSV ground truth: {gtruth_filename} and CSV output: {target_filename} have the same number of rows and cols.\n")
    except AssertionError:
        print(f"Test 1 status: FAILED \nTest 1 Details: Rows, cols {target_df.shape} in {target_filename} is different to {filtered_gtruth_df.shape} in {gtruth_filename}.\n")

    try:
        #test the columns have the same names and same order
        assert target_df.columns.tolist() == filtered_gtruth_df.columns.tolist()
        print(f"Test 2 status: PASS \nTest 2 Details: CSV ground truth: {gtruth_filename} and CSV output: {target_filename} have the same column names and order.\n")
    except AssertionError:
        print(f"Test 2 status: FAILED \nTest 2 Details: {target_filename} has different column names to {gtruth_filename}. {target_df.columns.tolist()} compared to {gtruth_df.columns.tolist()}.\n")

    try:
        # test the contents of each column are the same
        for each_col in filtered_gtruth_df.columns:
            assert (filtered_gtruth_df[each_col].equals(target_df[each_col]))
        print(f"Test 3 status: PASS \nTest 3 Details: CSV ground truth: {gtruth_filename} and CSV output: {target_filename} have the same column contents.\n")
    except AssertionError:
        print(f'Test 3 status: FAILED \nTest 3 Details: The contents in col {each_col} are different between {target_filename} and {gtruth_filename}.')
