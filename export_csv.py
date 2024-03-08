import csv
import pymongo

def export_to_csv(db, collection_name, symbol, csv_file_name):
    """
    Export a document's balance sheet data to a CSV file.

    :param db: The database connection object.
    :param collection_name: The name of the collection to export from.
    :param symbol: The symbol of the company to export data for.
    :param csv_file_name: The desired name of the CSV file.
    """
    collection = db[collection_name]
    
    # Fetch the document for the specified symbol
    doc = collection.find_one({"symbol": symbol})
    
    if doc is None or 'balanceSheets' not in doc:
        print(f"No data found for symbol {symbol} in {collection_name}.")
        return
    
    # Define the CSV file path
    csv_file_path = f"{csv_file_name}_{symbol}.csv"

    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Extract and write the header based on the keys of the first balance sheet entry
        header = ['symbol'] + list(doc['balanceSheets'][0].keys())
        writer.writerow(header)

        # Write data rows for each balance sheet entry
        for entry in doc['balanceSheets']:
            row = [symbol] + [entry.get(field, '') for field in header[1:]]
            writer.writerow(row)
    
    print(f"Data for {symbol} exported successfully to {csv_file_path}.")

def main_export():
    """
    Main function to handle user input and call the export function.
    """
    symbol = input("Enter the symbol of the company to export data for: ").upper()
    collection_name = input("Enter the collection name (e.g., BALANCE_SHEET): ").upper()
    csv_file_name = f"{collection_name}"

    export_to_csv(db, collection_name, symbol, csv_file_name)

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["financial_db"]
    
    main_export()
