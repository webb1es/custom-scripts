#!/bin/bash

# Configuration
CSV_FILE="/Users/webster.muchefa/Downloads/INCENTIVE/SAMPLE_MSISDN.csv"
CONNECTION_STRING="mongodb://localhost:27017"
DATABASE="dxlrewardsdb"
TEMP_COLLECTION="msisdn_records_temp"
TARGET_COLLECTION="msisdn_records"
BATCH_SIZE=500000           # Increased batch size for faster imports
PROCESS_BATCH_SIZE=50000    # Smaller batches to avoid memory pressure
MAX_WORKERS=16              # Increased worker threads
PAUSE_INTERVAL=5            # More frequent pauses
PAUSE_DURATION=1            # Shorter pauses
MEMORY_RESET=20             # More frequent memory cleanup
PROGRESS_INTERVAL=10
LOG_THRESHOLD=1000
MONGODB_FLAGS="--quiet --norc"  # MongoDB shell flags

# Cleanup function to handle Ctrl+C gracefully
cleanup() {
    echo -e "\n\n⚠️  Script interrupted. Cleaning up resources..."
    rm -f "$TEMP_JS_FILE" 2>/dev/null

    echo -e "Do you want to drop the temporary collection? (y/n) \c"
    read -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Dropping temporary collection..."
        mongosh $MONGODB_FLAGS "$CONNECTION_STRING/$DATABASE" --eval "db.$TEMP_COLLECTION.drop()" >/dev/null 2>&1
    fi

    echo "Cleanup complete. Exiting."
    exit 1
}

trap cleanup SIGINT SIGTERM

# Check prerequisites
command -v mongoimport >/dev/null 2>&1 || { echo "MongoDB tools not found. Install using: apt-get install -y mongodb-database-tools"; exit 1; }
command -v mongosh >/dev/null 2>&1 || { echo "MongoDB shell not found. Install using: apt-get install -y mongodb-org-shell"; exit 1; }

if [ ! -f "$CSV_FILE" ]; then
    echo "Error: CSV file not found at $CSV_FILE"
    exit 1
fi

echo "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓"
echo "┃              Starting MSISDN Import Process                ┃"
echo "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"

# PHASE 1: Initial setup - create target collection index first if it doesn't exist
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│ PHASE 1: Setting up target collection and indexes           │"
echo "└─────────────────────────────────────────────────────────────┘"

SETUP_JS_FILE=$(mktemp)

cat > "$SETUP_JS_FILE" << 'EOF'
const target = process.env.TARGET_COLLECTION;

// Create target collection if it doesn't exist
if (!db.getCollectionNames().includes(target)) {
    db.createCollection(target);
    print("Created target collection");
}

// Check if index exists
const indexes = db[target].getIndexes();
const hasIdIndex = indexes.some(idx => {
    return idx.key && idx.key._id === 1;
});

if (!hasIdIndex) {
    print("Creating _id index on target collection...");
    db[target].createIndex({ "_id": 1 });
    print("Index created");
} else {
    print("_id index already exists on target collection");
}
EOF

TARGET_COLLECTION="$TARGET_COLLECTION" mongosh $MONGODB_FLAGS "$CONNECTION_STRING/$DATABASE" "$SETUP_JS_FILE"
rm -f "$SETUP_JS_FILE" 2>/dev/null

# PHASE 2: Import CSV to temporary collection
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│ PHASE 2: Importing CSV to temporary collection              │"
echo "└─────────────────────────────────────────────────────────────┘"

# Stream directly to mongo with optimized parameters
mongoimport --uri "$CONNECTION_STRING" \
  --db "$DATABASE" \
  --collection "$TEMP_COLLECTION" \
  --type csv \
  --columnsHaveTypes \
  --fields "MSISDN.string()" \
  --file "$CSV_FILE" \
  --numInsertionWorkers $MAX_WORKERS \
  --batchSize $BATCH_SIZE \
  --maintainInsertionOrder false \
  --stopOnError false \
  --drop

# PHASE 3: Process and import records in a optimized way
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│ PHASE 3: Processing and importing to main collection        │"
echo "└─────────────────────────────────────────────────────────────┘"

TEMP_JS_FILE=$(mktemp)

cat > "$TEMP_JS_FILE" << 'EOF'
const targetCollection = process.env.TARGET_COLLECTION;
const tempCollection = process.env.TEMP_COLLECTION;
const processBatchSize = parseInt(process.env.PROCESS_BATCH_SIZE, 10);
const pauseInterval = parseInt(process.env.PAUSE_INTERVAL, 10);
const pauseDuration = parseInt(process.env.PAUSE_DURATION, 10);
const memoryReset = parseInt(process.env.MEMORY_RESET, 10);
const currentDateTime = process.env.CURRENT_DATETIME;
const progressInterval = parseInt(process.env.PROGRESS_INTERVAL, 10);
const logThreshold = parseInt(process.env.LOG_THRESHOLD, 10);

// Fast estimated count
const totalRecords = db[tempCollection].estimatedDocumentCount();
const initialCount = db[targetCollection].estimatedDocumentCount();

print(`[${currentDateTime}] Found ${totalRecords.toLocaleString()} records to process`);
print(`Current collection size: ${initialCount.toLocaleString()} records`);
print("Press Ctrl+C to interrupt\n");

var stats = { processed: 0, skipped: 0, duplicates: 0, batchCount: 0, startTime: new Date() };

function getProgressBar(percent, length = 30) {
    const filledLength = Math.round(length * percent / 100);
    const emptyLength = length - filledLength;
    return `[${'█'.repeat(filledLength)}${'░'.repeat(emptyLength)}] ${percent}%`;
}

var now = new Date();
var batch = [];
var sampleMSISDNs = [];
var reportedBatches = 0;
var lastObjectId = null;

try {
    // Create a temp index if it doesn't exist to enable efficient pagination
    const tempIndexes = db[tempCollection].getIndexes();
    if (!tempIndexes.some(idx => idx.key && idx.key._id === 1)) {
        print("Creating temporary _id index for efficient cursor pagination...");
        db[tempCollection].createIndex({ "_id": 1 }, { background: true });
        print("Temporary index created");
    }

    // Process the collection in chunks to avoid cursor timeouts and memory issues
    var moreRecords = true;
    var query = {};

    while (moreRecords) {
        if (lastObjectId) {
            query = { _id: { $gt: lastObjectId } };
        }

        var cursor = db[tempCollection].find(query)
            .sort({ _id: 1 })
            .limit(processBatchSize * 10)
            .batchSize(processBatchSize);

        var recordsInChunk = 0;

        while(cursor.hasNext()) {
            var doc = cursor.next();
            recordsInChunk++;
            lastObjectId = doc._id;

            var msisdn = doc.MSISDN ? doc.MSISDN.toString().trim() : null;

            if (!msisdn || msisdn === "" || msisdn === "MSISDN") {
                stats.skipped++;
                continue;
            }

            if (sampleMSISDNs.length < 3) {
                sampleMSISDNs.push(msisdn);
            }

            // Use updateOne with upsert instead of insertOne to handle duplicates at DB level
            batch.push({
                updateOne: {
                    filter: { _id: msisdn },
                    update: {
                        $setOnInsert: {
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

            if (batch.length >= processBatchSize) {
                try {
                    const result = db[targetCollection].bulkWrite(batch, {
                        ordered: false,
                        writeConcern: { w: 0 }  // Fire and forget for speed
                    });

                    stats.processed += batch.length;
                    stats.batchCount++;

                    if (stats.batchCount === 1 && sampleMSISDNs.length > 0) {
                        print(`\n📱 Sample MSISDNs: ${sampleMSISDNs.join(", ")}\n`);
                    }

                    if (stats.batchCount % progressInterval === 0) {
                        var elapsed = (new Date() - stats.startTime)/1000;
                        var speed = Math.round(stats.processed/elapsed);
                        var percent = Math.min(100, Math.round((stats.processed / totalRecords) * 100));

                        print(`\n📊 Batch #${stats.batchCount} Complete ${getProgressBar(percent)}`);
                        print(`   ✓ Processed: ${stats.processed.toLocaleString()} of ${totalRecords.toLocaleString()} (${speed.toLocaleString()} rec/sec)`);
                        print(`   ✓ Skipped: ${stats.skipped.toLocaleString()} records`);

                        const remaining = Math.round((totalRecords - stats.processed) / speed);
                        if (remaining > 0) {
                            print(`   ⏱ Est. remaining: ${Math.floor(remaining/60)}m ${remaining%60}s\n`);
                        }

                        reportedBatches = stats.batchCount;
                    }

                    if (stats.batchCount % memoryReset === 0) {
                        if (stats.batchCount % progressInterval !== 0) {
                            print(`\n🧹 Memory cleanup after ${processBatchSize * memoryReset} records`);
                        }
                        try { gc(); } catch(e) {}
                    }

                    if (stats.batchCount % pauseInterval === 0) {
                        if (stats.batchCount % progressInterval !== 0 && stats.batchCount !== reportedBatches) {
                            print(`\n⏸ Pause (${pauseDuration}s)`);
                        }
                        sleep(pauseDuration * 1000);
                    }

                    batch = [];
                } catch (e) {
                    print(`\n❌ Batch #${stats.batchCount} failed: ${e}\n   Retrying...`);
                    sleep(3000);
                    batch = [];
                }
            }
        }

        cursor.close();

        // If we processed fewer records than our limit, we're done
        moreRecords = (recordsInChunk >= processBatchSize * 10);

        if (moreRecords) {
            print(`\n⏳ Processed ${recordsInChunk} records in this chunk, continuing to next chunk...`);
            try { gc(); } catch(e) {}
            sleep(pauseDuration * 1000);
        }
    }

    // Process final batch
    if (batch.length > 0) {
        try {
            db[targetCollection].bulkWrite(batch, {
                ordered: false,
                writeConcern: { w: 1 }  // Ensure the final batch is written
            });
            stats.processed += batch.length;

            if (batch.length >= logThreshold) {
                print(`\n📤 Final batch of ${batch.length} records processed`);
            }
        } catch (e) {
            print(`\n❌ Final batch failed: ${e}`);
        }
    }

    var finalCount = db[targetCollection].estimatedDocumentCount();
    var totalTime = (new Date() - stats.startTime)/1000;
    var newRecords = finalCount - initialCount;

    var timeString;
    if (totalTime < 60) {
        timeString = `${totalTime.toFixed(1)} seconds`;
    } else if (totalTime < 3600) {
        timeString = `${Math.floor(totalTime/60)}m ${Math.round(totalTime%60)}s`;
    } else {
        timeString = `${Math.floor(totalTime/3600)}h ${Math.floor((totalTime%3600)/60)}m ${Math.round(totalTime%60)}s`;
    }

    print(`\n✅ Import completed at ${currentDateTime}`);
    print("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓");
    print("┃                     IMPORT SUMMARY                          ┃");
    print("┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫");
    print(`┃ Records processed          ┃ ${stats.processed.toLocaleString().padStart(28)} ┃`);
    print(`┃ Invalid records            ┃ ${stats.skipped.toLocaleString().padStart(28)} ┃`);
    print(`┃ Net new records            ┃ ${newRecords.toLocaleString().padStart(28)} ┃`);
    print(`┃ Processing time            ┃ ${timeString.padStart(28)} ┃`);
    print(`┃ Import speed               ┃ ${Math.round(stats.processed/totalTime).toLocaleString().padStart(24)} rec/sec ┃`);
    print("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛");
} catch (e) {
    print(`\n❌ Process failed with error: ${e}`);
} finally {
    if (stats.processed > 0) {
        print("\n🧹 Cleaning up temporary collection");
        db[tempCollection].drop();
    }
}
EOF

CURRENT_DATETIME=$(date +"%Y-%m-%d %H:%M:%S")
TARGET_COLLECTION="$TARGET_COLLECTION" \
TEMP_COLLECTION="$TEMP_COLLECTION" \
PROCESS_BATCH_SIZE="$PROCESS_BATCH_SIZE" \
PAUSE_INTERVAL="$PAUSE_INTERVAL" \
PAUSE_DURATION="$PAUSE_DURATION" \
MEMORY_RESET="$MEMORY_RESET" \
CURRENT_DATETIME="$CURRENT_DATETIME" \
PROGRESS_INTERVAL="$PROGRESS_INTERVAL" \
LOG_THRESHOLD="$LOG_THRESHOLD" \
mongosh $MONGODB_FLAGS "$CONNECTION_STRING/$DATABASE" "$TEMP_JS_FILE"

rm -f "$TEMP_JS_FILE" 2>/dev/null

echo "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓"
echo "┃              MSISDN Import Process Complete                 ┃" 
echo "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"