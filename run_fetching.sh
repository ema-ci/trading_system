#!/bin/bash

# Array of tickers to fetch data for
tickers=("AAPL" "MSFT" "NVDA" "TSLA" "AMD" "JPM" "BAC" "GS" "V" "MS" "XOM" "CVX" "SLB" "COP" "EOG")

# Execute fetching.py for each ticker
for i in "${!tickers[@]}"; do
    echo "Esecuzione ${i}: python3 fetching.py ${tickers[$i]}"
    python3 fetching.py "${tickers[$i]}"
done
