import csv
import datetime
import time
import logging
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONNECTION_STRING = "CONNECTION_STRING"
DATABASE_NAME = "dxlrewardsdb"
COLLECTION_NAME = "msisdn_records"
CSV_FILE = "SAMPLE_MSISDN_slim.csv"
BATCH_SIZE = 10000
LOG_FREQUENCY = 10
MAX_RETRIES = 3
RETRY_DELAY = 5

def process_csv_to_cosmos():
    client = None
    total_processed = 0
    total_duplicates = 0
    start_time = time.time()
    batch_count = 0
    
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
                current_time = datetime.datetime.utcnow()
                
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
                        batch_count += 1
                        
                        if batch_count % LOG_FREQUENCY == 0:
                            elapsed = time.time() - start_time
                            rate = total_processed / elapsed if elapsed > 0 else 0
                            logging.info(f"Progress: {total_processed:,} records processed, {total_duplicates:,} duplicates skipped ({rate:.2f} records/s)")
                    batch = []
                    
            if batch:
                success, duplicates = write_batch_with_retry(collection, batch, MAX_RETRIES)
                total_duplicates += duplicates
                if success:
                    total_processed += len(batch) - duplicates
        
        elapsed_time = time.time() - start_time
        records_per_second = total_processed / elapsed_time if elapsed_time > 0 else 0
        logging.info(f"Import completed. {total_processed:,} records inserted, {total_duplicates:,} duplicates skipped in {elapsed_time:.2f}s ({records_per_second:.2f} records/s)")
        
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
            
            new_duplicates = sum(1 for err in write_errors if err.get('code') == 11000)
            duplicate_count += new_duplicates
            other_errors = len(write_errors) - new_duplicates
            
            if other_errors > 0:
                logging.warning(f"Bulk write: {new_duplicates} duplicates, {other_errors} other errors on attempt {attempt+1}/{max_retries}")
            else:
                if attempt == 0 and new_duplicates > 100:
                    logging.info(f"Bulk write: {new_duplicates} duplicates ignored")
                
            if other_errors == 0:
                return True, duplicate_count
                
            if attempt == max_retries - 1:
                return False, duplicate_count
                
            time.sleep(RETRY_DELAY)
            
        except Exception as e:
            logging.error(f"Error on attempt {attempt+1}/{max_retries}: {e}")
            if attempt == max_retries - 1:
                return False, duplicate_count
            time.sleep(RETRY_DELAY * (attempt + 1))
            
    return False, duplicate_count

if __name__ == "__main__":
    process_csv_to_cosmos() 