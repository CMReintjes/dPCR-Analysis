import os
import json
import argparse
import pandas as pd
from datetime import datetime

# ---- Constants ----
ETL_VERSION = "v1.0.0"
DEFAULT_INPUT_DIR = "inputs"
DEFAULT_OUTPUT_DIR = "runs"
DEFAULT_METADATA = {
    "block_type": "Unknown",
    "chemistry": "Not specified",
    "passive_reference": "None",
    "date_created": None,
    "experiment_type": "Unknown",
    "quantification_cycle_method": "Standard",
    "signal_smoothing_on": False,
    "experiment_run_end_time": None,
    "calibration": {},
    "num_wells": 0,
    "targets_detected": [],
    "num_amplification_cycles": 0,
    "created_by_etl_version": ETL_VERSION
}

# ---- Utility Functions ----
def extract_sample_setup_metadata(df: pd.DataFrame) -> dict:
    metadata = DEFAULT_METADATA.copy()
    
    try:
        flat_dict = dict(zip(df.iloc[:, 0].astype(str).str.strip(), df.iloc[:, 1]))
        
        metadata["block_type"] = flat_dict.get("Block Type", metadata["block_type"])
        metadata["chemistry"] = flat_dict.get("Chemistry", metadata["chemistry"])
        metadata["passive_reference"] = flat_dict.get("Passive Reference", metadata["passive_reference"])
        metadata["date_created"] = flat_dict.get("Date Created", metadata["date_created"])
        metadata["experiment_type"] = flat_dict.get("Experiment Type", metadata["experiment_type"])
        metadata["quantification_cycle_method"] = flat_dict.get("Quantification Cycle Method", metadata["quantification_cycle_method"])
        metadata["signal_smoothing_on"] = flat_dict.get("Signal Smoothing On", metadata["signal_smoothing_on"])

        # Experiment Run End Time (used for folder naming)
        run_time_str = flat_dict.get("Experiment Run End Time")
        if run_time_str:
            try:
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
    folder_name = f"run_{run_time.replace(':', '').replace(' ', '_')}"
    output_path = os.path.join(base_output, folder_name)
    os.makedirs(output_path, exist_ok=True)
    return output_path


def save_metadata(metadata: dict, output_path: str):
    try:
        metadata_path = os.path.join(output_path, "metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)
        print(f"[INFO] Metadata saved to {metadata_path}")
    except Exception as e:
        print(f"[ERROR] Failed to save metadata: {e}")


def load_melt_curve_data(xls: pd.ExcelFile) -> pd.DataFrame:
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


# ---- Main ETL Runner ----
def main(input_file: str, output_dir: str, verbose: bool = False, dry_run: bool = False, skip_metadata: bool = False):
    try:
        if not os.path.isfile(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")

        xls = pd.ExcelFile(input_file)
        if "Sample Setup" not in xls.sheet_names:
            raise ValueError("'Sample Setup' sheet not found.")

        setup_df = xls.parse("Sample Setup")
        metadata = extract_sample_setup_metadata(setup_df)
        run_output_dir = create_output_dir(metadata["experiment_run_end_time"], output_dir)

        if not skip_metadata and not dry_run:
            save_metadata(metadata, run_output_dir)

        melt_df = load_melt_curve_data(xls)
        if verbose:
            print(f"[INFO] Loaded Melt Curve Raw Data with shape: {melt_df.shape}")
            print(melt_df.head())

        print(f"[INFO] Run directory initialized at: {run_output_dir}")

    except Exception as e:
        print(f"[ERROR] ETL process failed: {e}")


# ---- CLI Entry Point ----
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL pipeline for dPCR Excel files.")
    parser.add_argument("-i", "--input", type=str, default=os.path.join(DEFAULT_INPUT_DIR, "input.xlsx"), help="Path to the input Excel file")
    parser.add_argument("-o", "--output", type=str, default=DEFAULT_OUTPUT_DIR, help="Base output directory")
    parser.add_argument("-v", "--version", action="store_true", help="Print the script version and exit")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--dry-run", action="store_true", help="Run without saving any output")
    parser.add_argument("--skip-metadata", action="store_true", help="Skip saving metadata.json")

    args = parser.parse_args()

    if args.version:
        print(f"ETL Script Version: {ETL_VERSION}")
    else:
        main(
            input_file=args.input,
            output_dir=args.output,
            verbose=args.verbose,
            dry_run=args.dry_run,
            skip_metadata=args.skip_metadata
        )
