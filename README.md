# Freight Rate Sheet Generator

This project is a rate-sheet processing pipeline that consolidates multiple Excel rate sheets with inconsistent content, formats, and data types from different agents all over the world, cleans and standardizes the data, and uploads cleaned tables into Google BigQuery for downstream analysis. 

<img width="250" alt="project 1" src="https://github.com/user-attachments/assets/c5d4406a-2339-4837-8e07-29fa38ccbb4d" />
Picture 1: Page details.
<br>

<img width="250" alt="project 2" src="https://github.com/user-attachments/assets/a445d477-3bc8-426f-9dfa-31959540d7ab" />
Picture 2: Select input tables to preview.
<br>

<img width="250" alt="project 3" src="https://github.com/user-attachments/assets/f1854356-9949-4329-b49f-fc0fc2eee121" />
Picture 3: Select origin ports X destinations, and carriers perferences.
<br>

<img width="250" alt="project 4" src="https://github.com/user-attachments/assets/ed3c17c3-36d1-4d98-99ef-4ee584addfa1" />
Picture 4: Allow both dropdown bar selection and typing match.
<br>

<img width="250" alt="project 5" src="https://github.com/user-attachments/assets/bf2b61e3-c777-4b18-868d-9a718d1619dd" />
Picture 5: Carriers and other selections that allow you to customize more information.
<br>

<img width="250" alt="project 6" src="https://github.com/user-attachments/assets/8f2a12a7-f634-4456-9843-e1c170a961ed" />
Picture 6: Final rate sheet.
<br>

## Inputs and Outputs
Input raw files under RateSheet_Project/RateSheetFiles. RateGenerator exe reads these Excel files, detects and normalizes headers, standardizes field names and values, exports cleaned Excel files into a Cleaned folder, and loads the final tables into a BigQuery dataset.

## Main Dependencies

pandas: core table processing and read/write.
rapidfuzz (or thefuzz): high-performance fuzzy matching for ports, carriers, and city names.
openpyxl: extract computed values from Excel with formulas.
google-cloud-bigquery / google-auth: authentication and uploading DataFrames to BigQuery.
pytz / datetime: date parsing and timezone handling.

## Key Techniques & Algorithms

Header detection: scan the top N rows to find the real header row (e.g., searching for "POL"), enabling robust handling of varying source formats.
Column normalization: strip whitespace/special characters, convert separators to underscores, deduplicate columns, and rename common fields to a canonical set.
Alias dictionaries + fuzzy matching: maintain dictionaries for port aliases, carrier codes, and city keywords; prefer exact alias/code matches and fall back to fuzzy-match with a threshold (e.g., 75%) to resolve ambiguous names.
Date standardization: handle both Excel serial dates and string dates, try multiple formats, coerce invalid values to NaT, and use filename-inferred year as a calibration heuristic when needed.
Formula extraction: use openpyxl with data_only=True to capture computed values rather than raw formulas before exporting/uploading.
BigQuery load: normalize file names to safe table IDs and use google-cloud-bigquery's DataFrame load API with WRITE_TRUNCATE to ensure idempotent uploads.

# Running the App - Streamlit Cloud 👇

https://freight-rate-sheet-generator-hgpxnwxp67ycj7d9kdwv3e.streamlit.app/

## Project Structure

- `streamlit_app.py` – Main Streamlit application
- `RateGeneratorJuly15.py` – Data cleaning and transformation scripts
- `bigquery_utils.py` – BigQuery integration helpers
- `requirements.txt` – Python dependencies
- `json` – Google Cloud service account credentials （for safty issues, json is not provided in GitHub and codes)

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License.

## Contact

For questions or support, please contact [kylinsyt@gmail.com].
