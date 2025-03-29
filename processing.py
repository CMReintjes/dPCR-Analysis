import pandas as pd
import os
from typing import List, Dict
import re

def process_replicate_wells(df: pd.DataFrame, replicates: Dict[str, List[str]], id_col: str, value_columns: List[str]) -> pd.DataFrame:
    '''
    Averages values across replicate wells defined in the metadata for each measurement point (e.g., Temperature or Cycle).

    Args:
        df (pd.DataFrame): Raw data including 'Well' and an ID column like 'Temperature' or 'Cycle'.
        replicates (dict): Mapping of replicate group names to well lists (from metadata).
        id_col (str): The column name representing measurement (e.g. 'Temperature', 'Cycle').
        value_columns (List[str]): The measurement columns to average (e.g. 'Fluorescence').

    Returns:
        pd.DataFrame: Averages and std of values per replicate group and ID column.
    '''
    results = []
    print(replicates.items())
    for group, wells in replicates.items():
        #print(group)
        subset = df[df['Well Position'].isin(wells)]
        #print(subset)
        if subset.empty:
            continue

        grouped = subset.groupby(["Reading", id_col])[value_columns].agg(['mean', 'std']).reset_index()
        grouped.columns = ['Reading', id_col] + [f"{col}_{stat}" for col in value_columns for stat in ['mean', 'std']]
        grouped['Replicate Group'] = group
        results.append(grouped)
        print(results)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def process_replicates(df: pd.DataFrame, group_by: List[str], value_columns: List[str]) -> pd.DataFrame:
    '''
    Groups replicate entries by specified keys and computes mean and standard deviation for the value columns.

    Args:
        df (pd.DataFrame): Raw data with possible replicate rows.
        group_by (List[str]): Column names to group by (e.g. ['Sample Name', 'Temperature']).
        value_columns (List[str]): Numeric columns to aggregate (e.g. ['Fluorescence']).

    Returns:
        pd.DataFrame: DataFrame with mean and std columns added.
    '''
    agg_funcs = {col: ['mean', 'std'] for col in value_columns}
    processed = df.groupby(group_by).agg(agg_funcs).reset_index()
    processed.columns = ['_'.join(col).strip('_') for col in processed.columns.values]
    return processed

def load_multiple_runs(run_dirs: List[str], data_type: str = "melt") -> pd.DataFrame:
    '''
    Loads and combines data from multiple run folders for comparison.

    Args:
        run_dirs (List[str]): List of paths to run directories.
        data_type (str): Either 'melt' or 'amplification' to determine which file to load.

    Returns:
        pd.DataFrame: Combined data with an added 'run_id' column for tracking source.
    '''
    all_data = []
    filename = "melt_curve_data.csv" if data_type == "melt" else "amplification_data.csv"

    for run_dir in run_dirs:
        path = os.path.join(run_dir, filename)
        if os.path.exists(path):
            df = pd.read_csv(path)
            df['run_id'] = os.path.basename(run_dir)
            all_data.append(df)

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
