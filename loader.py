import os
import json
import argparse
import pandas as pd
from datetime import datetime
import re

# Constants
ETL_VERSION = "v1.0.0"
DEFAULT_INPUT_DIR = "inputs"
DEFAULT_OUTPUT_DIR = "runs"
DEFAULT_METADATA = {
    "created_by_etl_version": ETL_VERSION,
    "block_type": "Unknown",
    "chemistry": "Not specified",
    "passive_reference": "None",
    "date_created": None,
    "experiment_type": "Unknown",
    "quantification_cycle_method": "Standard",
    "signal_smoothing_on": False,
    "experiment_run_end_time": None,
    #"calibration": {},
    #"num_wells": 0,
    #"targets_detected": [],
    #"num_amplification_cycles": 0,
    "samples": []
}

# Metadata Extraction
def extract_metadata(df: pd.DataFrame) -> dict:
    '''
    Extracts metadata from the Sample Setup DataFrame.

    Args:
        df (pd.DataFrame): DataFrame parsed from the 'Sample Setup' Excel sheet.

    Returns:
        dict: A dictionary containing extracted metadata fields with default fallbacks.
    '''
    metadata = DEFAULT_METADATA.copy()

    try:
        # Extract block type from the second column header
        metadata["block_type"] = df.columns[1] if df.shape[1] > 1 else metadata["block_type"]

        flat_dict = dict(zip(df.iloc[:, 0].astype(str).str.strip(), df.iloc[:, 1]))

        metadata["chemistry"] = flat_dict.get("Chemistry", metadata["chemistry"])
        metadata["passive_reference"] = flat_dict.get("Passive Reference", metadata["passive_reference"])
        date_created_raw = flat_dict.get("Date Created", metadata["date_created"])
        if date_created_raw:
            date_created_clean = re.sub(r"\b(AM|PM|EDT|PST|CST|EST|UTC)\b", "", date_created_raw, flags=re.IGNORECASE).strip()
            metadata["date_created"] = date_created_clean
        else:
            metadata["date_created"] = metadata["date_created"]
        metadata["experiment_type"] = flat_dict.get("Experiment Type", metadata["experiment_type"])
        metadata["quantification_cycle_method"] = flat_dict.get("Quantification Cycle Method", metadata["quantification_cycle_method"])
        metadata["signal_smoothing_on"] = flat_dict.get("Signal Smoothing On", metadata["signal_smoothing_on"])

        # Experiment Run End Time (used for folder naming)
        run_time_str = flat_dict.get("Experiment Run End Time")
        if run_time_str:
            try:
                # Remove any unrecognized timezone abbreviations like 'EDT', 'PST', and AM/PM indicators
                run_time_str = re.sub(r"\b(AM|PM|EDT|PST|CST|EST|UTC)\b", "", run_time_str, flags=re.IGNORECASE).strip()
                run_time = pd.to_datetime(run_time_str)
                metadata["experiment_run_end_time"] = run_time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                metadata["experiment_run_end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            metadata["experiment_run_end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        print(f"[WARN] Failed to extract metadata from Sample Setup: {e}")

    return metadata


def create_output_dir(run_time: str, base_output: str) -> str:
    '''
    Creates a directory for the current run based on timestamp and returns its path.

    Args:
        run_time (str): The experiment run end time as a formatted string.
        base_output (str): Base path where the run directory should be created.

    Returns:
        str: Full path to the created run-specific output directory.
    '''
    folder_name = f"run_{run_time.replace(':', '').replace(' ', '_')}"
    output_path = os.path.join(base_output, folder_name)
    os.makedirs(output_path, exist_ok=True)
    return output_path


def save_metadata(metadata: dict, output_path: str):
    '''
    Saves metadata dictionary as a JSON file in the specified output directory.

    Args:
        metadata (dict): Metadata to be saved.
        output_path (str): Directory where the metadata file will be written.
    '''
    try:
        metadata_path = os.path.join(output_path, "metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)
        print(f"[INFO] Metadata saved to {metadata_path}")
    except Exception as e:
        print(f"[ERROR] Failed to save metadata: {e}")


def load_melt_curve_data(xls: pd.ExcelFile) -> pd.DataFrame:
    '''
    Loads and validates the 'Melt Curve Raw Data' sheet from the Excel file.

    Args:
        xls (pd.ExcelFile): Parsed Excel file object.

    Returns:
        pd.DataFrame: Cleaned DataFrame containing melt curve measurements.

    Raises:
        ValueError: If required columns are missing or the sheet is not found.
    '''
    if "Melt Curve Raw Data" not in xls.sheet_names:
        raise ValueError("'Melt Curve Raw Data' sheet not found.")

    df = xls.parse("Melt Curve Raw Data")
    expected_columns = [
        "Well", "Well Position", "Reading", "Temperature",
        "Fluorescence", "Derivative", "Target Name"
    ]

    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns in Melt Curve Raw Data: {missing_cols}")

    return df


def load_amplification_data(xls: pd.ExcelFile) -> pd.DataFrame:
    '''
    Loads and validates the 'Amplification Data' sheet from the Excel file.

    Args:
        xls (pd.ExcelFile): Parsed Excel file object.

    Returns:
        pd.DataFrame: Cleaned DataFrame containing amplification measurements.

    Raises:
        ValueError: If required columns are missing or the sheet is not found.
    '''
    if "Amplification Data" not in xls.sheet_names:
        raise ValueError("'Amplification Data' sheet not found.")

    df = xls.parse("Amplification Data")
    expected_columns = [
        "Well", "Well Position", "Cycle", "Target Name",
        "Rn", "Delta Rn"
    ]

    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns in Amplification Data: {missing_cols}")

    # Drop rows with missing well or measurement data
    df.dropna(subset=["Well", "Rn", "Delta Rn"], inplace=True)

    return df


# Load Results Data
def load_results_data(xls: pd.ExcelFile) -> pd.DataFrame:
    '''
    Loads and returns the results table from the 'Results' sheet starting around row 35.

    Args:
        xls (pd.ExcelFile): Excel file object.

    Returns:
        pd.DataFrame: Cleaned results DataFrame.
    '''
    if "Results" not in xls.sheet_names:
        raise ValueError("'Results' sheet not found.")

    df = xls.parse("Results", skiprows=35)
    df = df.dropna(how="all")  # Drop fully empty rows
    return df


# Main ETL
def run_etl(input_file: str, output_dir: str, verbose: bool = False, dry_run: bool = False, skip_metadata: bool = False, skip_summary: bool = False):
    '''
    Main entry point for the ETL pipeline.

    Args:
        input_file (str): Path to the input Excel file.
        output_dir (str): Directory where output files should be saved.
        verbose (bool, optional): If True, prints debug info to console.
        dry_run (bool, optional): If True, processes data without saving outputs.
        skip_metadata (bool, optional): If True, skips saving metadata.json.
    '''
    try:
        if not os.path.isfile(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")

        xls = pd.ExcelFile(input_file)
        if "Sample Setup" not in xls.sheet_names:
            raise ValueError("'Sample Setup' sheet not found.")

        setup_df = xls.parse("Sample Setup")
        metadata = extract_metadata(setup_df)

        # Load additional sample name info from extended Sample Setup table
        try:
            sample_setup_extended = xls.parse("Sample Setup", skiprows=35)
            if "Sample Name" in sample_setup_extended.columns and "Well Position" in sample_setup_extended.columns:
                # Store sample names
                unique_sample_names = sample_setup_extended["Sample Name"].dropna().unique().tolist()
                metadata["samples"] = unique_sample_names

                # Build replicate groupings
                replicate_map = {}
                for _, row in sample_setup_extended.dropna(subset=["Sample Name", "Well Position"]).iterrows():
                    sample = row["Sample Name"]
                    well = row["Well Position"]
                    match = re.match(r"([A-Z]+)(\d+)", str(well).strip(), re.IGNORECASE)
                    if match:
                        row_num = match.group(2)
                        group_key = f"{sample}_{row_num}"
                        replicate_map.setdefault(group_key, []).append(well)

                metadata["replicates"] = replicate_map

                if verbose:
                    print(f"[INFO] Found {len(unique_sample_names)} unique sample names.")
                    print(f"[INFO] Built {len(replicate_map)} replicate groups.")

        except Exception as e:
            print(f"[WARN] Could not extract sample names from extended Sample Setup: {e}")
        run_output_dir = create_output_dir(metadata["experiment_run_end_time"], output_dir)

        melt_df = load_melt_curve_data(xls)
        amp_df = load_amplification_data(xls)
        results_df = load_results_data(xls)

        if not skip_summary:
            metadata["summary"] = {
                "melt_curve": {
                    "num_wells": melt_df['Well'].nunique(),
                    "temperature_range": [melt_df['Temperature'].min(), melt_df['Temperature'].max()],
                    "unique_targets": melt_df['Target Name'].dropna().unique().tolist()
                },
                "amplification": {
                    "num_cycles": amp_df['Cycle'].max() if not amp_df.empty else 0,
                    "num_amplified_wells": amp_df['Well'].nunique(),
                    "unique_targets": amp_df['Target Name'].dropna().unique().tolist()
                }
            }

        # Melt Curve Sheet
        if not skip_metadata and not dry_run:
            save_metadata(metadata, run_output_dir)

        if verbose:
            print(f"[INFO] Loaded Melt Curve Raw Data with shape: {melt_df.shape}")
            print(melt_df.head())

        if not dry_run:
            melt_output_path = os.path.join(run_output_dir, "melt_curve_data.csv")
            melt_df.to_csv(melt_output_path, index=False)
            print(f"[INFO] Melt Curve data saved to {melt_output_path}")

        # Amplification Sheet
        if verbose:
            print(f"[INFO] Loaded Amplification Data with shape: {amp_df.shape}")
            print(amp_df.head())

        if not dry_run:
            amp_output_path = os.path.join(run_output_dir, "amplification_data.csv")
            amp_df.to_csv(amp_output_path, index=False)
            print(f"[INFO] Amplification data saved to {amp_output_path}")

        # Results Sheet
        if verbose:
            print(f"[INFO] Loaded Results Data with shape: {results_df.shape}")
            #print(results_df.info())

        if not dry_run:
            results_output_path = os.path.join(run_output_dir, "results_table.csv")
            results_df.to_csv(results_output_path, index=False)
            print(f"[INFO] Results data saved to {results_output_path}")

        print(f"[INFO] Run directory initialized at: {run_output_dir}")
    except Exception as e:
        print(f"[ERROR] ETL process failed: {e}")


# Command Line Arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL pipeline for dPCR Excel files.")
    parser.add_argument("-i", "--input", type=str, default=os.path.join(DEFAULT_INPUT_DIR, "input.xlsx"), help="Path to the input Excel file")
    parser.add_argument("-o", "--output", type=str, default=DEFAULT_OUTPUT_DIR, help="Base output directory")
    parser.add_argument("-v", "--version", action="store_true", help="Print the script version and exit")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--dry-run", action="store_true", help="Run without saving any output")
    parser.add_argument("--skip-metadata", action="store_true", help="Skip saving metadata.json")
    parser.add_argument("--skip-summary", action="store_true", help="Skip loading additional summary stats in metadata.json")

    args = parser.parse_args()

    if args.version:
        print(f"ETL Script Version: {ETL_VERSION}")
    else:
        run_etl(
            input_file=args.input,
            output_dir=args.output,
            verbose=args.verbose,
            dry_run=args.dry_run,
            skip_metadata=args.skip_metadata,
            skip_summary=args.skip_summary
        )
