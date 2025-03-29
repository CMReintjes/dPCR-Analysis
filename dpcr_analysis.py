import argparse
import os
import json
import pandas as pd
from loader import ETL_VERSION, DEFAULT_INPUT_DIR, run_etl
from processing import load_multiple_runs, process_replicate_wells


def load_metadata(run_dir):
    metadata_path = os.path.join(run_dir, 'metadata.json')
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata not found in {run_dir}")
    with open(metadata_path) as f:
        return json.load(f)


def main(args):
    if args.replicate_average and args.runs:
        df = load_multiple_runs(args.runs, data_type=args.data_type)
        metadata = load_metadata(args.runs[0])
        replicates = metadata.get("replicates", {})
        id_col = "Temperature" if args.data_type == "melt" else "Cycle"
        print(id_col)
        value_columns = ["Fluorescence"] if args.data_type == "melt" else ["Delta Rn"]
        avg = process_replicate_wells(df, replicates, id_col, value_columns)
        avg.to_csv(args.output, index=False)
        print(f"[INFO] Saved replicate-averaged data to {args.output}")


# Command Line Arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master CLI for dPCR tools")

    # ETL
    parser.add_argument("--etl", action="store_true", help="Run the ETL process")
    parser.add_argument("--input", type=str, default=DEFAULT_INPUT_DIR, help="Input .xlsx file for ETL")
    parser.add_argument("--skip-summary", action="store_true", help="Include summary stats in metadata")
    parser.add_argument("--skip-metadata", action="store_true", help="Skip metadata output")
    parser.add_argument("--dry-run", action="store_true", help="Run ETL without writing files")
    
    # Replicate processing
    parser.add_argument("--runs", nargs="+", help="Run folders for processing")
    parser.add_argument("--data-type", choices=["melt", "amplification"], default="melt")
    parser.add_argument("--replicate-average", action="store_true", help="Run replicate averaging")
    parser.add_argument("--output", type=str, default="combined_output.csv")

    # General
    parser.add_argument("--version", action="store_true", help="Show ETL version and exit")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    if args.version:
        print(f"ETL Script Version: {ETL_VERSION}")

    else:
        if args.etl:
            run_etl(
                input_file=args.input,
                output_dir="runs/",
                verbose=args.verbose,
                dry_run=args.dry_run,
                skip_metadata=args.skip_metadata,
                skip_summary=args.skip_summary
            )
    
        main(args)
        