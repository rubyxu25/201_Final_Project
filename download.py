"""
Module 1: Data Collection
Person in Charge: Bojun Zhang
Tasks: 
- Calling the Exchange Rate API (ExchangeRate-API) 
- Calling the Stock Market API (MarketStack) 
- Performing initial data organization, and exporting as CSV
"""

import requests
import csv
import json
import os
from datetime import datetime, timedelta
from database import set_up_database, store_exchange_rates_to_db, store_stock_data_to_db, get_date_range
from sqlite3 import Cursor, Connection

# https://marketplace.apilayer.com/exchangerates_data-api
EXCHANGE_RATE_API_KEY = "whASJjGoRh KeuJrFk3Djfh00eIXYw473" 
# https://marketstack.com/
STOCK_MARKET_API_KEY = "e9673061b18d2e07a68e0db9bd5eeaa5"
# sqlite database file
DB_PATH = "data.db"

# get the data of a range of days
def get_exchange_rate_data(base_currency: str, target_currencies: list[str], 
                          start_date: str, end_date: str, api_key: str) -> str:
    """
    Fetch exchange rate data from ExchangeRate-API
    the maximum allowed timeframe is 365 days.

    Args:
        base_currency: Base currency code (e.g., 'USD')
        target_currencies: list of target currency codes (e.g., ['EUR', 'GBP', 'JPY'])
        start_date: Start Date in format 'YYYY-MM-DD'
        end_date: End Date in format 'YYYY-MM-DD'
        api_key: ExchangeRate-API key
        
    Returns:
        tuple(json, url)
        json: JSON string containing exchange rate data
        url: URL of the request
    """
    # doc: https://marketplace.apilayer.com/exchangerates_data-api#endpoints

    headers= {"apikey": api_key}
    url = f"https://api.apilayer.com/exchangerates_data/timeseries?start_date={start_date}&end_date={end_date}&base={base_currency}&symbols={','.join(target_currencies)}"
    response = requests.get(url, headers=headers)
    json_data = json.loads(response.text)

    if response.status_code != 200:
        if "error" in json_data:
            raise Exception(f"Error(get_exchange_rate_data): {json_data['error']['message']}")
        elif "message" in json_data:
            raise Exception(f"Error(get_exchange_rate_data): {json_data['message']}")
        else:
            raise Exception(f"Error(get_exchange_rate_data): {json_data}")

    if not json_data["success"]:
        raise Exception(f"Error(get_exchange_rate_data): : {json_data['error']['message']}")

    return json_data, url

def organize_exchange_rate_data(exchange_data: str) -> list[tuple]:
    """
    Organize exchange rate(json) data into tuples
    
    Args:
        exchange_data: JSON string containing exchange rate data
        
    Returns:
        list of tuples with organized exchange rate data

    Example:
    {
        "success": true,
        "timeseries": true,
        "start_date": "2012-05-01",
        "end_date": "2012-05-03",
        "base": "EUR",
        "rates": {
            "2012-05-01":{
            "USD": 1.322891,
            "AUD": 1.278047,
            "CAD": 1.302303
            },
            "2012-05-02": {
            "USD": 1.315066,
            "AUD": 1.274202,
            "CAD": 1.299083
            },
            "2012-05-03": {
            "USD": 1.314491,
            "AUD": 1.280135,
            "CAD": 1.296868
            },
            [...]
        }
    }
    """
    result = []

    # data is a timeseries
    base = exchange_data["base"]
    for date, target_currencies in exchange_data["rates"].items():
        for target_currency, rate in target_currencies.items():
            result.append((date, base, target_currency, rate))

    return result

def export_exchange_rate_to_csv(data: list[tuple], filename: str) -> bool:
    """
    Export organized data to CSV file
    
    Args:
        data: list of tuples to export
        filename: Output CSV filename
        
    Returns:
        True if successful, False otherwise
    """
    # get the directory of the file
    dir_path = os.path.dirname(filename)

    # create the directory if it doesn't exist
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path)

    csv_file = open(filename, "w", newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["date", "base_currency", "target_currency", "exchange_rate"])
    for row in data:
        csv_writer.writerow(row)
    csv_file.close()
    return True

def get_stock_data(symbols: list[str], start_date: str, end_date: str, api_key: str) -> str:
    """
    Fetch stock market data from MarketStack API
    
    Args:
        symbols: list of stock symbols (e.g., ['AAPL', 'GOOGL'])
        date: Date in format 'YYYY-MM-DD'
        api_key: MarketStack API key
        
    Returns:
        JSON string containing stock market data
    """
    headers= {"Accept": "application/json"}
    url = f"https://api.marketstack.com/v2/eod?access_key={api_key}&symbols={','.join(symbols)}&date_from={start_date}&date_to={end_date}&limit=1000"
    response = requests.get(url, headers=headers)
    json_data = json.loads(response.text)

    if response.status_code != 200 or "error" in json_data:
        raise Exception(f"Error(get_stock_data): {json_data['error']['message']}")

    return json_data, url

def organize_stock_data(stock_data: str) -> list[tuple]:
    """
    Organize stock market data into tuples
    
    Args:
        stock_data: JSON string containing stock market data
        
    Returns:
        list of tuples with organized stock data
    Example:
    {
        "pagination": {
            "limit": 1000,
            "offset": 0,
            "count": 2,
            "total": 2
        },
        "data": [
            {
                "open": 248.93,
                "high": 249.1,
                "low": 241.82,
                "close": 243.85,
                "volume": 55558000.0,
                "adj_high": 249.1,
                "adj_low": 241.8201,
                "adj_close": 243.85,
                "adj_open": 248.93,
                "adj_volume": 55740731.0,
                "split_factor": 1.0,
                "dividend": 0.0,
                "name": "Apple Inc",
                "exchange_code": "NASDAQ",
                "asset_type": "Stock",
                "price_currency": "USD",
                "symbol": "AAPL",
                "exchange": "XNAS",
                "date": "2025-01-02T00:00:00+0000"
            },
            {
                "open": 390.1,
                "high": 392.7299,
                "low": 373.04,
                "close": 379.28,
                "volume": 109710749.0,
                "adj_high": 392.7299,
                "adj_low": 373.04,
                "adj_close": 379.28,
                "adj_open": 390.1,
                "adj_volume": 109710749.0,
                "split_factor": 1.0,
                "dividend": 0.0,
                "name": "Tesla Inc",
                "exchange_code": "NASDAQ",
                "asset_type": "Stock",
                "price_currency": "usd",
                "symbol": "TSLA",
                "exchange": "XNAS",
                "date": "2025-01-02T00:00:00+0000"
            }
        ]
    }
    """
    
    result = []
    for data in stock_data["data"]:
        currency = "unknown"
        if "price_currency" in data and data["price_currency"]:
            currency = data["price_currency"].upper()
        result.append((data["date"][:10], data["symbol"], data["open"], data["high"], data["low"], data["close"], data["volume"], data["exchange"], 
            currency))
    return result

def export_stock_to_csv(data: list[tuple], filename: str) -> bool:
    """
    Export organized data to CSV file
    
    Args:
        data: list of tuples to export
        filename: Output CSV filename
        
    Returns:
        True if successful, False otherwise
    """
    # get the directory of the file
    dir_path = os.path.dirname(filename)

    # create the directory if it doesn't exist
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path)

    csv_file = open(filename, "w", newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["date", "symbol", "open", "high", "low", "close", "volume", "exchange", "currency"])
    for row in data:
        csv_writer.writerow(row)
    csv_file.close()
    return True

    
def download_exchange_rate_data(base_currency: str, target_currencies: list[str], 
                              start_date: str, end_date: str, cur:Cursor, conn:Connection, debug: bool = False) -> None:
    """
    Download exchange rate data from ExchangeRate-API
    
    Args:
        base_currency: Base currency code (e.g., 'USD')
        target_currencies: list of target currency codes (e.g., ['EUR', 'GBP', 'JPY'])
        start_date: Start Date in format 'YYYY-MM-DD'
        end_date: End Date in format 'YYYY-MM-DD'
        
    Returns:                
    """
    try:
        json_data, url = get_exchange_rate_data(base_currency, target_currencies, start_date, end_date, EXCHANGE_RATE_API_KEY)
        if debug:
            print('----url----\n', url)
            print('----json----\n', json.dumps(json_data, indent=4))

        organized_data = organize_exchange_rate_data(json_data)
        if debug:
            print('----organized_data----')
            for data in organized_data:
                print(data)
        
        #export_exchange_rate_to_csv(organized_data, f"exchange_rate_{start_date}_{end_date}.csv")
        store_exchange_rates_to_db(organized_data, cur, conn)
    except Exception as e:
        print(f"{e}")

def download_stock_data(symbols: list[str], 
                              start_date: str, end_date: str, cur:Cursor, conn:Connection, debug: bool = False) -> None:
    """
    Download exchange rate data from ExchangeRate-API
    
    Args:
        base_currency: Base currency code (e.g., 'USD')
        target_currencies: list of target currency codes (e.g., ['EUR', 'GBP', 'JPY'])
        start_date: Start Date in format 'YYYY-MM-DD'
        end_date: End Date in format 'YYYY-MM-DD'
        
    Returns:                
    """
    try:
        json_data, url = get_stock_data(symbols, start_date, end_date, STOCK_MARKET_API_KEY)
        if debug:
            print('----url----\n', url)
            print('----json----\n', json.dumps(json_data, indent=4))

        organized_data = organize_stock_data(json_data)
        if debug:
            print('----organized_data----')
            for data in organized_data:
                print(data)
        
        #export_stock_to_csv(organized_data, f"stock_{start_date}_{end_date}.csv")
        store_stock_data_to_db(organized_data, cur, conn)
    except Exception as e:
        print(f"{e}")

def download_exchange_rate_limit_25(start_date: str, db_path: str) :
    base = "USD"
    targets = ["CNY", "EUR", "GBP"]
    cur, conn = set_up_database(db_path)

    start_date = datetime.strptime(start_date, '%Y-%m-%d').date() # convert string to datetime object
    i = 0
    while i + 3 < 25:
        download_exchange_rate_data(base, targets, start_date, start_date, cur, conn, False)
        i += 3
        start_date = start_date + timedelta(days=1)

def download_stock_limit_25(start_date: str, db_path: str) :
    symbols = ["AAPL", "NVDA", "TSLA"]
    cur, conn = set_up_database(db_path)

    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    i = 0
    while i + 3 < 25:
        download_stock_data(symbols, start_date, start_date, cur, conn, False)
        i += 3
        start_date = start_date + timedelta(days=1)

def exchange_data_collection():
    base = "USD"
    targets = ["CNY", "EUR", "GBP"]

    # download 25 items data each time
    download_exchange_rate_limit_25("2025-11-10", DB_PATH)

def stock_data_collection():
    symbols = ["AAPL", "NVDA", "TSLA"]

    # download 25 items data each time
    download_stock_limit_25("2025-11-10", DB_PATH)

def download_exchange_rate_months(
    start_date: str,
    end_date: str,
    db_path: str
) -> None:
    """
    Download exchange rate data in batches (<25 records per batch),
    covering multiple months by repeatedly shifting the start date.
    """

    current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    while current_date <= end_date:
        download_exchange_rate_limit_25(
            current_date.strftime("%Y-%m-%d"),
            db_path
        )
        # Each batch covers ~8 days (24 records)
        current_date += timedelta(days=8)
        
def download_stock_months(
    start_date: str,
    end_date: str,
    db_path: str
) -> None:
    """
    Download stock market data in batches (<25 records per batch),
    covering multiple months by repeatedly shifting the start date.
    """

    current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    while current_date <= end_date:
        download_stock_limit_25(
            current_date.strftime("%Y-%m-%d"),
            db_path
        )
        current_date += timedelta(days=8)

def demo():
    start_date = "2025-10-01"
    download_exchange_rate_limit_25(start_date, "test.db")
    download_stock_limit_25(start_date, "test.db")
    
if __name__ == "__main__":
    demo()
