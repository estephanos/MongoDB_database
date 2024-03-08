import requests
import pymongo
import os
import time
import json

# MongoDB setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["financial_db"]


# Ensure an index on the 'symbol' field for each collection
def create_indexes():
    """Create unique indexes on the 'symbol' field for all financial collections."""

    collections = ["BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW"]
    for collection_name in collections:
        collection = db[collection_name]
        collection.create_index([("symbol", pymongo.ASCENDING)], unique=True)



def fetch_financial_data(symbol, report_type):
    """
    Fetch financial data for a given symbol and report type from Alpha Vantage API.

    :param symbol: The stock symbol to fetch data for.
    :param report_type: The type of financial report (BALANCE_SHEET, INCOME_STATEMENT, CASH_FLOW).
    :return: A list of dictionaries containing the financial data.
    """

    api_key = os.getenv('ALPHA_VANTAGE_API_KEY', 'YOUR_DEFAULT_API_KEY')  # Replace YOUR_DEFAULT_API_KEY with your actual key if not using an env variable
    url = f"https://www.alphavantage.co/query?function={report_type}&symbol={symbol}&apikey={api_key}&datatype=json"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        return []

    data = json.loads(response.text)
    if "quarterlyReports" in data:
        return data["quarterlyReports"]
    else:
        print(f"No quarterlyReports found in response for {symbol} - {report_type}. Response: {data}")
        return []
    


def store_data(symbol, report_type, data):
    """
    Store financial data in MongoDB with upsert functionality.

    :param symbol: The stock symbol for the data.
    :param report_type: The type of financial report (BALANCE_SHEET, INCOME_STATEMENT, CASH_FLOW).
    :param data: The financial data to be stored.
    """

    collection = db[report_type]
    for report in data:
        collection.update_one(
            {"symbol": symbol},
            {"$set": report},
            upsert=True
        )

def main():
    """
    Main function to fetch and store financial data for specified symbols and report types.
    """
     # Prompt user for the symbol
    user_symbol = input("Enter the symbol of the company you want to import data for: ").upper()
    
    # Add the symbol to the symbols list
    symbols = ["AAPL", "PYPL"]  # Default list of symbols
    if user_symbol not in symbols:
        symbols.append(user_symbol)

    report_types = ["BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW"]

    for symbol in symbols:
        for report_type in report_types:
            print(f"Fetching {report_type} for {symbol}")
            data = fetch_financial_data(symbol, report_type)
            if data:
                store_data(symbol, report_type, data)
            time.sleep(12)  # Pause to avoid hitting API rate limits too quickly

if __name__ == "__main__":
    main()
