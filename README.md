# Fetching Script

This script fetches financial data (OHLCV) and news sentiment for a given stock ticker using the Polygon.io API. The data is saved as partitioned Parquet datasets for further analysis.

## Prerequisites

Before running the script, ensure you have the following installed:
- Python 3.8 or higher
- `pip` (Python package manager)

## Setup Instructions

### 1. Clone the Repository
Clone this repository to your local machine or download the script.

### 2. Create a Virtual Environment
It is recommended to use a virtual environment to manage dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows, use .venv\Scripts\activate
```

### 3. Install Dependencies
Install the required Python packages:

```bash
pip install requests pandas pyarrow python-dotenv
```

### 4. Configure the `.env` File
Create a `.env` file in the same directory as the script and add your Polygon.io API key:

```env
POLYGON_API_KEY=your_polygon_api_key_here
```

Replace `your_polygon_api_key_here` with your actual API key.

### 5. Run the Script
To fetch data for a specific stock ticker, run the script with the ticker as an argument:

```bash
python fetching.py <ticker>
```

For example, to fetch data for `AAPL`:

```bash
python fetching.py AAPL
```

### 6. Output
The script will save the fetched data in the `data/` directory as a partitioned Parquet dataset. The file structure will look like this:

```
data/
└── <ticker>_intraday/
    ├── partition_date=YYYY-MM/
    │   └── part-*.parquet
```

## Notes
- The script handles API rate-limiting by retrying requests after a delay.
- Ensure your API key has sufficient permissions to access the required endpoints.
- The free plan allows up to 5 request every minute. the script will handle the limits automatically.

## Troubleshooting
- If you encounter issues with dependencies, ensure you are using the virtual environment (`.venv`).
- If the script fails to fetch data, verify your API key and internet connection.

## License
This script is provided as-is for educational purposes. Use at your own risk.
