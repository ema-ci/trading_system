import os
import sys
import time
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BASE_URL        = "https://api.polygon.io"
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "YOUR_API_KEY_HERE")

TIMESPAN        = "minute"
MULTIPLIER      = 1        # data granularity (1 minute)
MONTHS_BACK     = 12

# ─── DATE RANGE ───────────────────────────────────────────────────────────────

end_date   = datetime.today()
start_date = end_date - timedelta(days=MONTHS_BACK * 30)

START = start_date.strftime("%Y-%m-%d")
END   = end_date.strftime("%Y-%m-%d")

# ─── HELPER ──────────────────────────────────────────────────────────────────

def get(url, params):
    """
    Execute a GET request to the given URL with the provided parameters, including the API key.
    Implements a simple retry mechanism in case of rate-limiting (HTTP 429).
    """

    params["apiKey"] = POLYGON_API_KEY
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code == 429:
        print("Rate-limit, waiting 60s…")
        time.sleep(60)
        return get(url, params)
    resp.raise_for_status()
    return resp.json()

# ─── 1. FETCH CORE OHLCV ─────────────────────────────────────────────────────────

def fetch_aggs(ticker, multiplier, timespan, start, end):
    """
    Download OHLCV aggregates for a specific ticker and return a DataFrame.
    Handles pagination using 'next_url' and implements a simple retry mechanism in case of rate-limiting.
    """
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}
    rows = []
    while url:
        data = get(url, params)
        rows.extend(data.get("results", []))
        url = data.get("next_url")
        params = {}
        if url:
            time.sleep(0.2)
    if not rows:
        return pd.DataFrame()
    
    # Rename columns
    df = pd.DataFrame(rows).rename(columns={
        "t": "timestamp", "o": "open", "h": "high",
        "l": "low",       "c": "close", "v": "volume",
        "vw": "vwap",     "n": "transactions"
    })

    # Add column 'z' = 1 if within NY market hours, 0 otherwise

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)

# ─── 2. FETCH NEWS SENTIMENT ────────────────────────────────────────────────────────

SENTIMENT_MAP = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}

def fetch_daily_sentiment(ticker: str) -> pd.DataFrame:
    """
    Download news and return a DataFrame with:
    - timestamp rounded to the nearest minute
    - sentiment_raw: average of numerical values for each minute
    - news_count: number of news items for each minute
    """
    # print(f"  Downloading news sentiment for {ticker} …")
    url    = f"{BASE_URL}/v2/reference/news"
    params = {
        "ticker":            ticker,
        "published_utc.gte": START,
        "published_utc.lte": END,
        "limit":             1000,
        "sort":              "published_utc",
        "order":             "asc",
    }

    articles = []
    while url:
        data = get(url, params)
        articles.extend(data.get("results", []))
        url    = data.get("next_url")
        params = {}
        if url:
            time.sleep(0.2)

    if not articles:
        return pd.DataFrame(columns=["timestamp", "sentiment_raw", "news_count"])

    records = []
    for art in articles:
        pub_time = art.get("published_utc", "")
        insights = art.get("insights", [])
        scores = []
        for ins in insights:
            if ins.get("ticker") == ticker:
                # Use None as default to avoid ambiguity with neutral=0.0
                s = SENTIMENT_MAP.get(ins.get("sentiment"))
                if s is not None:
                    scores.append(s)
        # Average of valid insights for that article
        if pub_time and scores:
            records.append({
                "timestamp": pd.to_datetime(pub_time, utc=True),
                "sentiment_raw": sum(scores) / len(scores)
            })

    if not records:
        return pd.DataFrame(columns=["timestamp", "sentiment_raw", "news_count"])

    df_news = pd.DataFrame(records).sort_values("timestamp").reset_index(drop=True)

    # B. Temporal alignment: round to the nearest minute
    df_news["timestamp"] = df_news["timestamp"].dt.floor("min")

    # If multiple news items in the same minute → average sentiment and count news items
    df_news = (
        df_news
        .groupby("timestamp")
        .agg(sentiment_raw=("sentiment_raw", "mean"), news_count=("sentiment_raw", "size"))
        .reset_index()                # make 'timestamp' a column
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    return df_news


# ─── BUILD AND SAVE ───────────────────────────────────────────────────────────────

def main(ticker):
    os.makedirs("data", exist_ok=True)

    print("\nfetching core OHLCV data for {ticker} …")
    df_core = fetch_aggs(ticker, MULTIPLIER, TIMESPAN, START, END)

    print("\nfetching news sentiment data for {ticker} …")
    df_news = fetch_daily_sentiment(ticker)

    # Standardize the precision of all timestamps to nanoseconds to avoid MergeError
    df_core["timestamp"] = pd.to_datetime(df_core["timestamp"]).dt.as_unit("ns")
    df_news["timestamp"] = pd.to_datetime(df_news["timestamp"]).dt.as_unit("ns")

    print("\nmerging core data with news sentiment …")
    df = df_core.copy().sort_values("timestamp")
    if not df_news.empty:
        df = pd.merge_asof(
            df,
            df_news,
            on="timestamp",
            direction="backward"
        )
        print(f" merge completed with df_news. total rows: {len(df):,}")

    # Add the ticker name as the first field
    df.insert(0, "ticker", ticker)

    ticker_path = f"data/{ticker}_intraday.parquet"

    print("\nsaving final DataFrames as partitioned Parquet datasets …")
    df["partition_date"] = df["timestamp"].dt.strftime("%Y-%m")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=ticker_path,
        partition_cols=["partition_date"],
        compression="snappy",
    )
    print(f" ticker Parquet saved at: data/{ticker}_intraday")

# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 fetching.py <ticker>")
        sys.exit(1)

    ticker = sys.argv[1].strip()

    main(ticker)
