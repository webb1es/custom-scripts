#!/bin/bash

# Configuration - Optimized for Cosmos DB with RU constraints (50M records)
CSV_FILE="/path/to/your/SAMPLE_MSISDN.csv" # Update this path for your Linux VPS
const connectionString = "$CONN_STRING" // mongodb://localhost:27017/
DATABASE="dxlrewardsdb"
TEMP_COLLECTION="msisdn_records_temp"
TARGET_COLLECTION="msisdn_records"
BATCH_SIZE=200000      # Import batch size - higher values improve throughput
PROCESS_BATCH_SIZE=5000   # Processing batch size - lower values better respect RU limits
MAX_WORKERS=8         # Adjusted for typical VPS CPU cores
PAUSE_INTERVAL=10      # Batches between RU pauses
PAUSE_DURATION=5       # Pause duration in seconds - increase for stricter RU limits
MEMORY_RESET=200       # Memory cleanup interval - lower for limited RAM

# Check if MongoDB tools are installed
command -v mongoimport >/dev/null 2>&1 || { echo "MongoDB tools not found. Install using: apt-get install -y mongodb-database-tools"; exit 1; }
command -v mongo >/dev/null 2>&1 || { echo "MongoDB shell not found. Install using: apt-get install -y mongodb-org-shell"; exit 1; }

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: CSV file not found at $CSV_FILE"
    exit 1
fi

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Starting MSISDN import (50M records to Cosmos DB)"

# Step 1: Import CSV to temporary collection with proper data typing
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Importing CSV to temporary collection..."
mongoimport --uri "$CONNECTION_STRING" \
  --db "$DATABASE" \
  --collection "$TEMP_COLLECTION" \
  --type csv \
  --columnsHaveTypes \
  --fields "MSISDN.string()" \
  --file "$CSV_FILE" \
  --numInsertionWorkers $MAX_WORKERS \
  --batchSize $BATCH_SIZE \
  --drop

# Create indexed temporary collection for efficient processing
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Creating index for faster processing..."
mongo --quiet "$CONNECTION_STRING/$DATABASE" --eval "db.$TEMP_COLLECTION.createIndex({ \"MSISDN\": 1 }, { background: true })" > /dev/null 2>&1

# Step 2: Process temp collection and insert to target with RU optimization
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing records with RU optimization..."
mongo --quiet "$CONNECTION_STRING/$DATABASE" <<EOF
var stats = { processed: 0, skipped: 0, duplicates: 0, batchCount: 0, startTime: new Date() };
var initialCount = db.$TARGET_COLLECTION.count();
print("[$(date +"%Y-%m-%d %H:%M:%S")] Starting data transformation. Current target count: " + initialCount);

var msisdnSet = {};  // For duplicate detection
var now = new Date();
var batch = [];

try {
    // Use cursor with noCursorTimeout to prevent timeout on large collections
    var cursor = db.$TEMP_COLLECTION.find().noCursorTimeout().batchSize($PROCESS_BATCH_SIZE);
    
    while(cursor.hasNext()) {
        var doc = cursor.next();
        var msisdn = doc.MSISDN ? doc.MSISDN.toString().trim() : null;
        
        // Only log the first few records to verify data format
        if (stats.batchCount === 0 && stats.processed < 3) {
            print("[DEBUG] Sample MSISDN: " + msisdn + " (Type: " + typeof doc.MSISDN + ")");
        }
        
        // Data validation - skip invalid records
        if (!msisdn || msisdn === "") { 
            stats.skipped++; 
            continue; 
        }
        
        // Deduplication - skip duplicates within current import
        if (msisdnSet[msisdn]) { 
            stats.duplicates++; 
            continue; 
        }
        
        // Track seen MSISDNs and prepare document for insertion
        msisdnSet[msisdn] = 1;
        batch.push({
            insertOne: {
                document: {
                    _id: msisdn,  // Use MSISDN as document ID
                    simType: "N/A",
                    simNumber: "N/A",
                    status: "completed",
                    requestId: "batch-import-old-app",
                    allocationDate: now,
                    createdDate: now
                }
            }
        });
        
        // Process batch when full - respects RU limits
        if (batch.length >= $PROCESS_BATCH_SIZE) {
            try {
                // Use unordered bulk writes for better performance
                db.$TARGET_COLLECTION.bulkWrite(batch, { ordered: false });
                stats.processed += batch.length;
                stats.batchCount++;
                
                // Status reporting at regular intervals
                if (stats.batchCount % 50 === 0) {
                    var elapsed = (new Date() - stats.startTime)/1000;
                    var speed = Math.round(stats.processed/elapsed);
                    var percent = initialCount > 0 ? Math.round((stats.processed / initialCount) * 100) : 0;
                    
                    print("[PROGRESS] " + 
                          stats.processed.toLocaleString() + " processed | " + 
                          (stats.duplicates + stats.skipped).toLocaleString() + " skipped | " + 
                          speed.toLocaleString() + " rec/sec | " +
                          "~" + percent + "% complete");
                }
                
                // Memory management - critical for 50M+ records
                if (stats.batchCount % $MEMORY_RESET === 0) {
                    print("[MEMORY] Freeing memory after " + ($PROCESS_BATCH_SIZE * $MEMORY_RESET) + " records");
                    msisdnSet = {};
                    try { gc(); } catch(e) {}
                }
                
                // RU management - pause to avoid throttling
                if (stats.batchCount % $PAUSE_INTERVAL === 0) {
                    print("[RU MGMT] Pausing for " + $PAUSE_DURATION + "s to respect RU limits");
                    sleep($PAUSE_DURATION * 1000);
                }
                
                batch = [];
            } catch (e) {
                print("[ERROR] Batch failed: " + e + " - Retrying after pause");
                sleep(10000);
                batch = [];
            }
        }
    }
    
    // Process final batch
    if (batch.length > 0) {
        try {
            db.$TARGET_COLLECTION.bulkWrite(batch, { ordered: false });
            stats.processed += batch.length;
            print("[INFO] Final batch of " + batch.length + " records processed");
        } catch (e) {
            print("[ERROR] Final batch failed: " + e);
        }
    }
    
    // Final statistics
    var finalCount = db.$TARGET_COLLECTION.count();
    var totalTime = (new Date() - stats.startTime)/1000;
    var newRecords = finalCount - initialCount;
    
    print("\n[SUCCESS] Import completed at $(date +"%Y-%m-%d %H:%M:%S")");
    print("┌────────────────────────────────────────┐");
    print("│ Records processed: " + stats.processed.toLocaleString().padStart(10) + "               │");
    print("│ Duplicates skipped: " + stats.duplicates.toLocaleString().padStart(10) + "               │");
    print("│ Invalid records: " + stats.skipped.toLocaleString().padStart(10) + "               │");
    print("│ Net new records: " + newRecords.toLocaleString().padStart(10) + "               │");
    print("│ Processing time: " + totalTime.toLocaleString().padStart(10) + " sec            │");
    print("│ Import speed: " + Math.round(stats.processed/totalTime).toLocaleString().padStart(10) + " rec/sec        │");
    print("└────────────────────────────────────────┘");
} catch (e) {
    print("[CRITICAL] Process failed with error: " + e);
} finally {
    // Ensure cursor is closed and temp collection cleaned up
    if (cursor) cursor.close();
    
    if (stats.processed > 0) {
        print("[CLEANUP] Removing temporary collection");
        db.$TEMP_COLLECTION.drop();
    }
}
EOF

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Import process completed"