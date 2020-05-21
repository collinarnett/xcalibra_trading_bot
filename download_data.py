from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm


def get_data(interval, pair):
    base = f"https://app.xcalibra.com/api/public/v1/price-history/{pair}"
    payload = {"interval": interval, "limit": 1000}

    r = requests.get(base, params=payload).json()
    data = []
    pbar = tqdm()
    # The following two lines are bad code and I should feel bad but at least they work
    if interval == "day":
        data.append(r)
    else:
        data.append(r)
        while r[0]["open"] != 0:
            to_timestamp = r[0]["timestamp"]
            payload = {
                "interval": interval,
                "to_timestamp": to_timestamp,
                "limit": 1000,
            }
            r = requests.get(base, params=payload).json()
            data.append(r)
            pbar.update()

    # Flatten list for easy dataframe import
    cleaned_data = [item for sublist in data for item in sublist]
    df = pd.DataFrame(cleaned_data)
    # Fill None values
    df.fillna(value=0, inplace=True)
    # Flip DataFrame
    df.iloc[:] = df.iloc[::-1].values
    # Change 'volume' and 'quantity' from string to numeric
    df[["volume", "quantity"]] = df[["volume", "quantity"]].apply(pd.to_numeric)
    # Set the index as the timestamp column
    df.set_index("timestamp", inplace=True)
    # Typecast the index to datetime from string
    df.index = pd.to_datetime(df.index)
    # Drop rows with all zeros
    df = df[(df.T != 0).any()]
    # Save
    df.to_csv(f"./data/{pair}_{interval}.csv")


if __name__ == "__main__":
    pairs = [
        "BTC_RSD",
        "SFX_RSD",
        "SFT_RSD",
        "ETH_RSD",
        "SFX_BTC",
        "SFT_BTC",
        "ETH_BTC",
        "SFT_SFX",
    ]
    intervals = [
        "day",
        "hour",
        "minute",
    ]
    p = Path('data')
    p.mkdir(exists_ok=True)
    for interval in tqdm(intervals, desc="interval"):
        for pair in tqdm(pairs, desc="pairs"):
            if Path(f"../data/{pair}_{interval}.csv").exists() is False:
                get_data(interval, pair)
            else:
                print(f"../xcalibra_market_data/{pair}_{interval}.csv: Exists")
