# Next Steps and Features

## 🔍 1. **Data Validation & Cleaning**

- **Melt Curve**: Validate that all numeric columns are proper `float`/`int`.
- **Sanitize decimals** if `,` instead of `.` (common in instrument outputs).
- Drop empty rows, or flag strange values (e.g., negative fluorescence).
- Detect duplicate wells or misaligned readings.

## 📊 2. **Summary Report**

- Auto-generate a small `.txt` or `.json` report with:
  - Number of valid wells
  - Targets detected
  - Min/max temps
  - Max cycles in amplification
- Great for quick QC checks.

## 📈 3. **Basic Visualization**

- Add `--plot` CLI flag to auto-generate:
  - Melt curves by well/target
  - Amplification curves
- Use `matplotlib` or `seaborn`, and save as `.png` in the output folder.

## 🧪 4. **Unit Tests**

- Begin laying out `pytest` or similar structure for validation.
- Ensures loaders and transformations don't break with new files.

## 🔁 5. **Comparison Mode**

- Enable `--compare` to pass in two runs and generate diffs:
  - Metadata differences
  - QC stats delta
  - Overlayed plots

## 📦 6. **Packaging the Project**

- Add `README.md` with CLI usage
- Add `environment.yml` if not already committed
- Organize into module structure (`etl/`, `tests/`, `data/`, etc.)
