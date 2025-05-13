#!/bin/bash

# Configuration
CSV_FILE="/Users/webster.muchefa/Downloads/INCENTIVE/SAMPLE_MSISDN.csv"
CONNECTION_STRING="mongodb://localhost:27017"
DATABASE="dxlrewardsdb"
TEMP_COLLECTION="msisdn_records_temp"
TARGET_COLLECTION="msisdn_records"
BATCH_SIZE=100000  # Increased batch size for mongoimport
PROCESS_BATCH_SIZE=10000  # Increased batch size for processing
MAX_INSERTION_WORKERS=4  # Increased for parallel processing
PAUSE_INTERVAL=10  # Pause after this many batches
PAUSE_DURATION=1000  # Milliseconds to pause

echo "Starting import process for large dataset..."

# Step 1: Import CSV directly with projection to only pull MSISDN column
echo "Step 1: Importing CSV data into temporary collection..."
mongoimport --uri "$CONNECTION_STRING" \
  --db "$DATABASE" \
  --collection "$TEMP_COLLECTION" \
  --type csv \
  --headerline \
  --fields "MSISDN" \
  --file "$CSV_FILE" \
  --numInsertionWorkers $MAX_INSERTION_WORKERS \
  --batchSize $BATCH_SIZE \
  --drop

# Step 2: Transform and insert into the final collection
echo "Step 2: Transforming data and inserting into final collection..."
mongo --quiet "$CONNECTION_STRING/$DATABASE" <<EOF
// Create an index on the temp collection to speed up the find operation
db.$TEMP_COLLECTION.createIndex({ "MSISDN": 1 });

// Ensure index on target collection
db.$TARGET_COLLECTION.createIndex({ "_id": 1 });

// Process records in batches
var batchSize = $PROCESS_BATCH_SIZE;
var totalProcessed = 0;
var batchCount = 0;
var now = new Date();
var startTime = new Date();

// Use a more efficient batch processing approach
var cursor = db.$TEMP_COLLECTION.find({}, { MSISDN: 1 }).noCursorTimeout().batchSize(batchSize);
var batch = [];

try {
    while(cursor.hasNext()) {
        var doc = cursor.next();
        var msisdn = doc.MSISDN.toString();
        
        batch.push({
            updateOne: {
                filter: { _id: msisdn },
                update: {
                    \$set: {
                        simType: "N/A",
                        simNumber: "N/A",
                        status: "completed",
                        requestId: "batch-import-old-app",
                        allocationDate: now,
                        createdDate: now
                    }
                },
                upsert: true
            }
        });

        if (batch.length >= batchSize) {
            try {
                db.$TARGET_COLLECTION.bulkWrite(batch, { ordered: false });
                totalProcessed += batch.length;
                batchCount++;

                // Log less frequently to reduce overhead
                if (batchCount % 200 === 0) {
                    var currentTime = new Date();
                    var elapsedSecs = (currentTime - startTime)/1000;
                    var recordsPerSec = Math.round(totalProcessed/elapsedSecs);
                    print("Processed " + totalProcessed + " records (" + recordsPerSec + " records/sec)");
                }

                batch = [];

                // Pause more frequently with longer duration for Cosmos DB RU management
                if (batchCount % $PAUSE_INTERVAL === 0) {
                    sleep($PAUSE_DURATION);
                    print("Pausing to respect RU limits...");
                }
            } catch (e) {
                print("Error processing batch: " + e);
                // Implement exponential backoff for failures
                sleep(2000);
                batch = [];
            }
        }
    }

    // Process remaining records
    if (batch.length > 0) {
        try {
            db.$TARGET_COLLECTION.bulkWrite(batch, { ordered: false });
            totalProcessed += batch.length;
        } catch (e) {
            print("Error processing final batch: " + e);
        }
    }

    var totalTimeSecs = (new Date() - startTime)/1000;
    print("Import complete. Total records processed: " + totalProcessed + " in " + totalTimeSecs + " seconds");
} catch (e) {
    print("Error during import: " + e);
} finally {
    cursor.close();
    
    if (totalProcessed > 0) {
        print("Cleaning up temporary collection...");
        db.$TEMP_COLLECTION.drop();
    }
}
EOF

echo "Import process completed."