import csv
import datetime
import time
import logging
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONNECTION_STRING = "YOUR_COSMOS_DB_CONNECTION_STRING"
DATABASE_NAME = "your_database"
COLLECTION_NAME = "your_collection"
CSV_FILE = "mongo-db/SAMPLE_MSISDN.csv"
BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_DELAY = 5

def process_csv_to_cosmos():
    client = None
    total_processed = 0
    total_duplicates = 0
    start_time = time.time()
    
    try:
        client = MongoClient(CONNECTION_STRING)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        db.command('ping')
        logging.info("Connected to Cosmos DB successfully")
        
        batch = []
        
        with open(CSV_FILE, 'r') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            
            for i, line in enumerate(csv_reader):
                if not line:
                    continue
                    
                msisdn = line[0].strip('"')
                current_time = datetime.datetime.utcnow()  # Native datetime object, MongoDB driver converts to BSON Date
                
                doc = {
                    "_id": msisdn,
                    "allocationDate": current_time,
                    "createdDate": current_time,
                    "requestId": "batch-import-old-app",
                    "simNumber": "N/A",
                    "simType": "N/A",
                    "status": "completed"
                }
                
                batch.append(InsertOne(doc))
                
                if len(batch) >= BATCH_SIZE:
                    success, duplicates = write_batch_with_retry(collection, batch, MAX_RETRIES)
                    total_duplicates += duplicates
                    if success:
                        total_processed += len(batch) - duplicates
                        logging.info(f"Progress: {total_processed} records processed, {total_duplicates} duplicates skipped")
                    batch = []
                    
            if batch:
                success, duplicates = write_batch_with_retry(collection, batch, MAX_RETRIES)
                total_duplicates += duplicates
                if success:
                    total_processed += len(batch) - duplicates
        
        elapsed_time = time.time() - start_time
        records_per_second = total_processed / elapsed_time if elapsed_time > 0 else 0
        logging.info(f"Import completed. {total_processed} records inserted, {total_duplicates} duplicates skipped in {elapsed_time:.2f}s ({records_per_second:.2f} records/s)")
        
    except ConnectionFailure as e:
        logging.error(f"Failed to connect to Cosmos DB: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if client:
            client.close()
            logging.info("Connection closed")

def write_batch_with_retry(collection, batch, max_retries):
    duplicate_count = 0
    
    for attempt in range(max_retries):
        try:
            result = collection.bulk_write(batch, ordered=False)
            return True, duplicate_count
        except BulkWriteError as bwe:
            write_errors = bwe.details.get('writeErrors', [])
            
            # Count duplicate key errors (code 11000)
            new_duplicates = sum(1 for err in write_errors if err.get('code') == 11000)
            duplicate_count += new_duplicates
            other_errors = len(write_errors) - new_duplicates
            
            if other_errors > 0:
                logging.warning(f"Bulk write: {new_duplicates} duplicates, {other_errors} other errors on attempt {attempt+1}/{max_retries}")
            else:
                logging.info(f"Bulk write: {new_duplicates} duplicates ignored")
                
            # If only duplicate errors occurred, consider this a success
            if other_errors == 0:
                return True, duplicate_count
                
            if attempt == max_retries - 1:
                return False, duplicate_count
                
            time.sleep(RETRY_DELAY)
            
        except Exception as e:
            logging.error(f"Error on attempt {attempt+1}/{max_retries}: {e}")
            if attempt == max_retries - 1:
                return False, duplicate_count
            time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            
    return False, duplicate_count

if __name__ == "__main__":
    process_csv_to_cosmos() 