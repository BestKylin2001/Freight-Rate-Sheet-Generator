# Freight Rate Sheet Generator

An automated tool for cleaning and standardizing ocean freight rate sheets with incorrect and inconsistent content, formats, and data types, using BigQuery for centralized analysis for future analysis and visulization. An automated interactive design via Streamlit for interactive access and customization data using.



## Features

- **Upload & Parse Rate Sheets:** Easily upload Excel or CSV files containing ocean freight rates.
- **Automated Data Cleaning:** Remove duplicates, handle missing values, identify varies headrows, head title, and their data, and standardize formats for ports, carriers, rates, and date.
- **Fuzzy Matching:** Intelligent matching of port and carrier names using rapidfuzz.
- **BigQuery Integration:** Store cleaned data at scale using Google BigQuery for any future analytic and visualize purpose.
- **Interactive Dashboard:** Supports querying all rate sheets simultaneously. Visualize rates by different routes and carriers. Supports selecting different routing modes and adjusting rates for ocean and inland transportation. Supports cross-matching mode to generate all possible routes from each origin to every destination simultaneously. Supports route quantity control and keyword-based filtering or exclusion.
- **Download Cleaned Data:** Export standardized rate sheets for further use or sharing.

## Technologies Used

- [Python 3.8+](https://www.python.org/)
- [Streamlit](https://streamlit.io/) – Rapid web app development
- [Google BigQuery](https://cloud.google.com/bigquery) – Scalable cloud data warehouse
- [Pandas](https://pandas.pydata.org/) – Data cleaning and manipulation
- [RapidFuzz](https://github.com/maxbachmann/RapidFuzz) – Fuzzy string matching
- [Openpyxl](https://openpyxl.readthedocs.io/) – Excel file handling

## Getting Started

> ⚠️ **Note:** This project is actively used with private data. Therefore, the service account JSON key is not included.  
> You can freely replace the path with your own credentials and use your own Google BigQuery instance to achieve the same functionality.

### Prerequisites

- Python 3.8+
- Google Cloud account with BigQuery access
- Service account credentials (JSON)

### Installation

1. Clone the repository:
   ```powershell
   git clone <repo-url>
   cd streamlit_launcher
   ```
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
3. Set up your Google Cloud credentials:
   - Place your service account JSON in the project directory.
   - Set the environment variable:
     ```powershell
     $env:GOOGLE_APPLICATION_CREDENTIALS = "path\to\your\credentials.json"
     ```

### Running the App

```powershell
streamlit run streamlit_app.py
```

## Usage

1. Upload your ocean freight rate sheet (Excel/CSV).
2. Review and clean the data using the interactive interface.
3. Save cleaned data to BigQuery and explore analytics.
4. Download standardized rate sheets as needed.

## Project Structure

- `streamlit_app.py` – Main Streamlit application
- `RateGeneratorJuly15.py` – Data cleaning and transformation scripts
- `bigquery_utils.py` – BigQuery integration helpers
- `requirements.txt` – Python dependencies
- `rate-sheet-sql-465312-c347c0f5adb3.json` – Google Cloud service account credentials

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License.

## Contact

For questions or support, please contact [kylinsyt@gmail.com].
