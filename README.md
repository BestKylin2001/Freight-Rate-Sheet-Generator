# Freight Rate Sheet Generator

This project provides a Streamlit web application and supporting scripts for cleaning, standardizing, and uploading freight rate Excel files to Google BigQuery, as well as querying and generating shipping rate sheets.

## Features
- **Streamlit App**: User-friendly web interface for querying and filtering shipping rates.
- **Excel Data Cleaning**: Python scripts and notebooks to standardize, clean, and merge rate sheet Excel files.
- **BigQuery Integration**: Uploads cleaned data to Google BigQuery for centralized access and analytics.
- **Fuzzy Matching**: Standardizes port, carrier, and city names using fuzzy matching and alias dictionaries.
- **Export**: Download filtered results as CSV.

## Getting Started

### Prerequisites
- Python 3.8+
- Google Cloud service account JSON credentials (for BigQuery access)
- Required Python packages (see below)

### Installation
1. Clone or download this repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   Or manually install:
   ```bash
   pip install streamlit pandas numpy thefuzz pyarrow pandas-gbq google-cloud-bigquery openpyxl rapidfuzz
   ```
3. Place your Google Cloud service account JSON file in the project root (e.g., `rate-sheet-sql-c341ef092d19.json`).

### Usage
#### 1. Data Cleaning and Upload (Optional)
- Use the provided Jupyter notebook (`RateGeneratorMay26_19_47 (1).ipynb`) to clean and standardize Excel files, then upload them to BigQuery.
- The notebook includes:
  - Header detection
  - Column name standardization
  - Fuzzy matching for ports, carriers, and cities
  - Date normalization
  - BigQuery upload scripts

#### 2. Run the Streamlit App
- Start the app:
  ```bash
  streamlit run "streamlit_app (5).py"
  ```
- The app allows you to:
  - Select and filter routes by origin, destination, carrier, and date
  - Adjust rate fields
  - Download the filtered results as CSV

#### 3. Packaging as an Executable (Windows)
- Use PyInstaller to create a `.exe`:
  ```bash
  pip install pyinstaller
  pyinstaller --onefile --add-data "rate-sheet-sql-c341ef092d19.json;." "streamlit_app (5).py"
  ```
- The executable will be in the `dist` folder.

#### 4. Launcher Script
- Use `launcher.py` to launch the `.exe` from Python:
  ```python
  import subprocess
  import os
  exe_path = os.path.join(os.path.dirname(__file__), "dist", "streamlit_app (5).exe")
  subprocess.Popen([exe_path])
  ```

## Project Structure
- `streamlit_app (5).py` — Main Streamlit web app
- `bigquery_utils (3).py` — BigQuery helper functions
- `RateGeneratorMay26_19_47 (1).ipynb` — Data cleaning and upload notebook
- `rate-sheet-sql-c341ef092d19.json` — Google Cloud credentials
- `build/`, `dist/` — Build and distribution folders (after packaging)

## Notes
- Ensure your Google Cloud project has billing enabled for BigQuery uploads.
- Adjust file and folder names as needed for your environment.

## License
MIT License
