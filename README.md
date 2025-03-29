# dPCR-Analysis

You can now run your ETL pipeline directly from the command line like this:

```python dpcr_etl_setup.py -i inputs/Paola-Run1.xlsx --verbose```

## Available CLI Options:

`-i, --input`: Path to the .xlsx file (defaults to inputs/input.xlsx)

`-o, --output`: Base output directory (defaults to output/)

`-v, --version`: Print the script version and exit

`--verbose`: Show parsed metadata in the terminal

`--dry-run`: Run without saving any files

`--skip-metadata`: Skip writing the metadata file
