import pymongo

# Setup MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["financial_db"]

def delete_documents(symbol, collection_name):
    """
    Delete documents for a specific symbol from a specified collection.

    :param symbol: The symbol of the company whose documents are to be deleted.
    :param collection_name: The name of the collection to delete documents from.
    """
    collection = db[collection_name]
    result = collection.delete_many({"symbol": symbol})
    
    if result.deleted_count > 0:
        print(f"Deleted {result.deleted_count} documents for symbol {symbol} from {collection_name}.")
    else:
        print(f"No documents found for symbol {symbol} in {collection_name}.")

def main_delete():
    """
    Main function to handle user input for deleting documents.
    """
    collection_name = input("Enter the collection name from which to delete documents (BALANCE_SHEET, INCOME_STATEMENT, CASH_FLOW): ").upper()
    symbol = input("Enter the symbol of the company to delete documents for: ").upper()

    delete_documents(symbol, collection_name)

if __name__ == "__main__":
    main_delete()
