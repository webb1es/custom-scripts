#!/bin/bash

# Configuration for millions of records with Cosmos DB RU constraints
CSV_FILE="/Users/webster.muchefa/Downloads/INCENTIVE/SAMPLE_MSISDN.csv"
CONNECTION_STRING="mongodb://localhost:27017"
DATABASE="dxlrewardsdb"
TEMP_COLLECTION="msisdn_records_temp"
TARGET_COLLECTION="msisdn_records"
BATCH_SIZE=200000      # Larger batch for initial import
PROCESS_BATCH_SIZE=5000   # Smaller batches for Cosmos DB processing
MAX_INSERTION_WORKERS=12  # More workers for parallel CSV import
PAUSE_INTERVAL=10      # Frequent pauses for RU management
PAUSE_DURATION=5000    # Longer pauses (5 seconds) to respect RU limits
MEMORY_RESET=200       # Reset memory tracking every X batches

echo "Starting import process for large dataset (optimized for 50M records)..."

# Step 1: Modify import parameters to ensure strings remain strings
echo "Step 1: Importing CSV data into temporary collection with string type preservation..."
# Add --columnsHaveTypes and --type=csv to specify string type for MSISDN
mongoimport --uri "$CONNECTION_STRING" \
  --db "$DATABASE" \
  --collection "$TEMP_COLLECTION" \
  --type csv \
  --columnsHaveTypes \
  --fields "MSISDN.string()" \
  --file "$CSV_FILE" \
  --numInsertionWorkers $MAX_INSERTION_WORKERS \
  --batchSize $BATCH_SIZE \
  --drop

# Create index silently
echo "Creating index on temporary collection..."
mongo --quiet "$CONNECTION_STRING/$DATABASE" --eval "db.$TEMP_COLLECTION.createIndex({ \"MSISDN\": 1 }, { background: true })" > /dev/null 2>&1

# Step 2: Transform and insert - optimized for RU constraints
echo "Step 2: Processing records with RU-optimized strategy..."
mongo --quiet "$CONNECTION_STRING/$DATABASE" <<EOF
print("[INFO] Starting data processing...");

// Use statistics object for tracking
var stats = {
    processed: 0,
    skipped: 0,
    duplicates: 0,
    batchCount: 0,
    startTime: new Date()
};

// Get initial count
var initialCount = db.$TARGET_COLLECTION.count();
print("[INFO] Target collection currently has " + initialCount + " documents");

// Use object instead of array for better memory handling with 50M records
var msisdnSet = {};
var now = new Date();

try {
    // Process in optimized batches
    var cursor = db.$TEMP_COLLECTION.find().noCursorTimeout().batchSize($PROCESS_BATCH_SIZE);
    var batch = [];

    // Add a reminder about import options
    print("[INFO] Using direct string import with --columnsHaveTypes and MSISDN.string() type specification");

    while(cursor.hasNext()) {
        var doc = cursor.next();
        var msisdn = doc.MSISDN ? doc.MSISDN.toString().trim() : null;

        // Log information about the MSISDN type in the first few records
        if (stats.batchCount === 0 && stats.processed < 10) {
            print("[DEBUG] MSISDN #" + stats.processed + " Value: " + msisdn + " Type: " + typeof doc.MSISDN);
        }

        // Skip invalid/duplicate records
        if (!msisdn || msisdn === "") { stats.skipped++; continue; }
        if (msisdnSet[msisdn]) { stats.duplicates++; continue; }

        // Mark as processed and add to batch
        msisdnSet[msisdn] = 1;

        // Create the document to insert - match Go struct mapping
        var newDoc = {
            _id: msisdn,  // Store MSISDN directly as _id to match Go struct
            simType: "N/A",
            simNumber: "N/A",
            status: "completed",
            requestId: "batch-import-old-app",
            allocationDate: now,
            createdDate: now
        };

        // Add to batch
        batch.push({
            insertOne: {
                document: newDoc
            }
        });

        // Process batch when full
        if (batch.length >= $PROCESS_BATCH_SIZE) {
            try {
                db.$TARGET_COLLECTION.bulkWrite(batch, { ordered: false });
                stats.processed += batch.length;
                stats.batchCount++;

                // Status reporting
                if (stats.batchCount % 50 === 0) {
                    var elapsedSecs = (new Date() - stats.startTime)/1000;
                    var recordsPerSec = Math.round(stats.processed/elapsedSecs);
                    print("[INFO] Processed: " + stats.processed.toLocaleString() +
                          " | Skipped: " + (stats.duplicates + stats.skipped).toLocaleString() +
                          " | Speed: " + recordsPerSec.toLocaleString() + " rec/sec");
                }

                // Memory management for large datasets
                if (stats.batchCount % $MEMORY_RESET === 0) {
                    msisdnSet = {};
                    try { gc(); } catch(e) {}
                }

                // RU management pauses
                if (stats.batchCount % $PAUSE_INTERVAL === 0) {
                    print("[INFO] Pausing for RU management...");
                    sleep($PAUSE_DURATION);
                }

                batch = [];
            } catch (e) {
                print("[ERROR] Batch processing failed: " + e);
                sleep(10000); // Long pause after error
                batch = [];
            }
        }
    }

    // Process final batch
    if (batch.length > 0) {
        try {
            db.$TARGET_COLLECTION.bulkWrite(batch, { ordered: false });
            stats.processed += batch.length;
        } catch (e) {
            print("[ERROR] Final batch failed: " + e);
        }
    }

    // Final stats
    var finalCount = db.$TARGET_COLLECTION.count();
    var netNewRecords = finalCount - initialCount;
    var totalTimeSecs = (new Date() - stats.startTime)/1000;
    var throughput = Math.round(stats.processed/totalTimeSecs);

    print("\n[SUCCESS] Import completed - Final statistics");
    print("┌───────────────────────────────────────────────┐");
    print("│ Total records processed:  " + stats.processed.toLocaleString().padStart(12) + "        │");
    print("│ Duplicates skipped:       " + stats.duplicates.toLocaleString().padStart(12) + "        │");
    print("│ Invalid records skipped:  " + stats.skipped.toLocaleString().padStart(12) + "        │");
    print("│ Initial collection count: " + initialCount.toLocaleString().padStart(12) + "        │");
    print("│ Final collection count:   " + finalCount.toLocaleString().padStart(12) + "        │");
    print("│ Net new documents added:  " + netNewRecords.toLocaleString().padStart(12) + "        │");
    print("│ Total execution time:     " + totalTimeSecs.toLocaleString().padStart(12) + " seconds │");
    print("│ Average throughput:       " + throughput.toLocaleString().padStart(12) + " rec/sec │");
    print("└───────────────────────────────────────────────┘");
} catch (e) {
    print("[ERROR] Import process failed: " + e);
} finally {
    if (cursor) cursor.close();

    if (stats.processed > 0) {
        print("[INFO] Cleaning up temporary collection...");
        db.$TEMP_COLLECTION.drop();
    }

    msisdnSet = null;
}
EOF

echo "Import process completed."