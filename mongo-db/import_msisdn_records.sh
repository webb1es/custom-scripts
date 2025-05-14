#!/bin/bash

# Configuration
CSV_FILE="/Users/webster.muchefa/Downloads/INCENTIVE/MSISDN.csv"
CONNECTION_STRING="mongodb://localhost:27017"
DATABASE="dxlrewardsdb"
TEMP_COLLECTION="msisdn_records_temp"
TARGET_COLLECTION="msisdn_records"
BATCH_SIZE=200000
PROCESS_BATCH_SIZE=100000
MAX_WORKERS=8
PAUSE_INTERVAL=10
PAUSE_DURATION=5
MEMORY_RESET=200
PROGRESS_INTERVAL=10
LOG_THRESHOLD=100

# Check if MongoDB tools are installed
command -v mongoimport >/dev/null 2>&1 || { echo "MongoDB tools not found. Install using: apt-get install -y mongodb-database-tools"; exit 1; }
command -v mongosh >/dev/null 2>&1 || { echo "MongoDB shell not found. Install using: apt-get install -y mongodb-org-shell"; exit 1; }

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: CSV file not found at $CSV_FILE"
    exit 1
fi

echo "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓"
echo "┃                 Starting MSISDN Import Process             ┃"
echo "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"

# PHASE 1: Import CSV to temporary collection
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│ PHASE 1: Importing CSV to temporary collection              │"
echo "└─────────────────────────────────────────────────────────────┘"

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
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Creating index on temporary collection..."
mongosh --quiet "$CONNECTION_STRING/$DATABASE" --eval "db.$TEMP_COLLECTION.createIndex({ \"MSISDN\": 1 }, { background: true })" > /dev/null 2>&1

# PHASE 2: Create a temporary JavaScript file for processing records
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│ PHASE 2: Processing and importing to main collection        │"
echo "└─────────────────────────────────────────────────────────────┘"

TEMP_JS_FILE=$(mktemp)

cat > "$TEMP_JS_FILE" << 'EOF'
// Get parameters from environment variables
const targetCollection = process.env.TARGET_COLLECTION;
const tempCollection = process.env.TEMP_COLLECTION;
const processBatchSize = parseInt(process.env.PROCESS_BATCH_SIZE, 10);
const pauseInterval = parseInt(process.env.PAUSE_INTERVAL, 10);
const pauseDuration = parseInt(process.env.PAUSE_DURATION, 10);
const memoryReset = parseInt(process.env.MEMORY_RESET, 10);
const currentDateTime = process.env.CURRENT_DATETIME;
const progressInterval = parseInt(process.env.PROGRESS_INTERVAL, 10);
const logThreshold = parseInt(process.env.LOG_THRESHOLD, 10);

var stats = { processed: 0, skipped: 0, duplicates: 0, batchCount: 0, startTime: new Date() };
var initialCount = db[targetCollection].countDocuments();
print(`[${currentDateTime}] Starting main import. Current collection size: ${initialCount} records`);

// Progress bar function
function getProgressBar(percent, length = 30) {
    const filledLength = Math.round(length * percent / 100);
    const emptyLength = length - filledLength;
    const bar = '█'.repeat(filledLength) + '░'.repeat(emptyLength);
    return `[${bar}] ${percent}%`;
}

var msisdnSet = {};
var now = new Date();
var batch = [];
var sampleMSISDNs = [];
var reportedBatches = 0;

try {
    var cursor = db[tempCollection].find().noCursorTimeout().batchSize(processBatchSize);
    
    while(cursor.hasNext()) {
        var doc = cursor.next();
        var msisdn = doc.MSISDN ? doc.MSISDN.toString().trim() : null;
        
        if (!msisdn || msisdn === "" || msisdn === "MSISDN") { 
            stats.skipped++; 
            continue; 
        }
        
        if (sampleMSISDNs.length < 3) {
            sampleMSISDNs.push(msisdn);
        }
        
        if (msisdnSet[msisdn]) { 
            stats.duplicates++; 
            continue; 
        }
        
        msisdnSet[msisdn] = 1;
        batch.push({
            insertOne: {
                document: {
                    _id: msisdn,
                    simType: "N/A",
                    simNumber: "N/A",
                    status: "completed",
                    requestId: "batch-import-old-app",
                    allocationDate: now,
                    createdDate: now
                }
            }
        });
        
        if (batch.length >= processBatchSize) {
            try {
                db[targetCollection].bulkWrite(batch, { ordered: false });
                stats.processed += batch.length;
                stats.batchCount++;
                
                if (stats.batchCount === 1 && sampleMSISDNs.length > 0) {
                    print(`\n📱 Sample MSISDNs: ${sampleMSISDNs.join(", ")}\n`);
                }
                
                if (stats.batchCount % progressInterval === 0) {
                    var elapsed = (new Date() - stats.startTime)/1000;
                    var speed = Math.round(stats.processed/elapsed);
                    var percent = initialCount > 0 ? Math.round((stats.processed / initialCount) * 100) : 0;
                    if (percent > 100) percent = 100;
                    
                    var progressBar = getProgressBar(percent);
                    var timeRemaining = speed > 0 ? Math.round((initialCount - stats.processed) / speed) : "?";
                    
                    print(`\n📊 Batch #${stats.batchCount} Complete ${progressBar}`);
                    print(`   ✓ Processed: ${stats.processed.toLocaleString()} records at ${speed.toLocaleString()} rec/sec`);
                    print(`   ✓ Skipped: ${(stats.duplicates + stats.skipped).toLocaleString()} records`);
                    if (timeRemaining !== "?") {
                        print(`   ⏱ Est. time remaining: ${Math.floor(timeRemaining/60)}m ${timeRemaining%60}s\n`);
                    }
                    
                    reportedBatches = stats.batchCount;
                }
                
                if (stats.batchCount % memoryReset === 0) {
                    if (stats.batchCount % progressInterval !== 0) {
                        print(`\n🧹 Memory cleanup after ${processBatchSize * memoryReset} records`);
                    }
                    msisdnSet = {};
                    try { gc(); } catch(e) {}
                }
                
                if (stats.batchCount % pauseInterval === 0) {
                    if (stats.batchCount % progressInterval !== 0 && stats.batchCount !== reportedBatches) {
                        print(`\n⏸ RU management pause (${pauseDuration}s)`);
                    }
                    sleep(pauseDuration * 1000);
                }
                
                batch = [];
            } catch (e) {
                print(`\n❌ Batch #${stats.batchCount} failed: ${e}\n   Retrying after pause...`);
                sleep(10000);
                batch = [];
            }
        }
    }
    
    if (batch.length > 0) {
        try {
            const bulkOps = batch.map(op => {
                const doc = op.insertOne.document;
                return {
                    updateOne: {
                        filter: { _id: doc._id },
                        update: { $setOnInsert: doc },
                        upsert: true
                    }
                };
            });
            
            db[targetCollection].bulkWrite(bulkOps, { ordered: false });
            stats.processed += batch.length;
            
            if (batch.length >= logThreshold) {
                print(`\n📤 Final batch of ${batch.length} records processed`);
            }
        } catch (e) {
            print(`\n❌ Final batch failed: ${e}`);
        }
    }
    
    var finalCount = db[targetCollection].countDocuments();
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
    print(`┃ Duplicates skipped         ┃ ${stats.duplicates.toLocaleString().padStart(28)} ┃`);
    print(`┃ Invalid records            ┃ ${stats.skipped.toLocaleString().padStart(28)} ┃`);
    print(`┃ Net new records            ┃ ${newRecords.toLocaleString().padStart(28)} ┃`);
    print(`┃ Processing time            ┃ ${timeString.padStart(28)} ┃`);
    print(`┃ Import speed               ┃ ${Math.round(stats.processed/totalTime).toLocaleString().padStart(24)} rec/sec ┃`);
    print("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛");
} catch (e) {
    print(`\n❌ Process failed with error: ${e}`);
} finally {
    if (cursor) cursor.close();
    
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
mongosh --quiet --norc "$CONNECTION_STRING/$DATABASE" "$TEMP_JS_FILE"

rm "$TEMP_JS_FILE"

echo "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓"
echo "┃              MSISDN Import Process Complete                 ┃" 
echo "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"