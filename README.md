## Fetching Script

This script fetches financial data (OHLCV) and news sentiment for a given stock ticker using the Polygon.io API. The data is saved as partitioned Parquet datasets for further analysis.

The Free API provides access to historical aggregate data with a 2-year lookback period and a limit of 5 requests per minute.

The script automatically handles these rate limits. By listing the stock tickers you wish to extract in the `run_fetching.sh` file, you can run the process unattended until all data is retrieved. 

Additionally, you can customize the script's parameters in the Configuration section of `fetching.py`.

You can get your own API key on polygon.io

---

#### Prerequisites

Before running the script, ensure you have the following installed:
- Python 3.8 or higher
- `pip` (Python package manager)

#### Setup Instructions

##### 1. Clone the Repository
Clone this repository to your local machine or download the script.

##### 2. Create a Virtual Environment
It is recommended to use a virtual environment to manage dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows, use .venv\Scripts\activate
```

##### 3. Install Dependencies
Install the required Python packages:

```bash
pip install requests pandas pyarrow python-dotenv
```

##### 4. Configure the `.env` File
Create a `.env` file in the same directory as the script and add your Polygon.io API key:

```env
POLYGON_API_KEY=your_polygon_api_key_here
```

Replace `your_polygon_api_key_here` with your actual API key.

##### 5. Run the Script
To fetch data for a specific stock ticker, run the script with the ticker as an argument:

```bash
python fetching.py <ticker>
```

For example, to fetch data for `AAPL`:

```bash
python fetching.py AAPL
```

##### 6. Output
The script will save the fetched data in the `data/` directory as a partitioned Parquet dataset. The file structure will look like this:

```
data/
└── <ticker>_intraday/
    ├── partition_date=YYYY-MM/
    │   └── part-*.parquet
```

---

#### License
This script is provided as-is for educational purposes. Use at your own risk.
