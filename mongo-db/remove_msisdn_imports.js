#!/usr/bin/env mongosh
// Hardcoded settings - edit these values directly if needed
// 
// To run on Windows:
// 1. Open PowerShell and navigate to MongoDB bin directory (e.g., C:\Program Files\MongoDB\Server\6.0\bin)
// 2. Run: .\mongosh.exe "$CONN_STRING" --file "path\to\remove_msisdn_imports.js"
// 
// To run on Linux:
// 1. Open terminal
// 2. Run: mongosh "$CONN_STRING" --file "/path/to/remove_msisdn_imports.js"

const connectionString = "$CONN_STRING" // mongodb://localhost:27017/

const batchSize = 1000;     // Number of records per batch
const pauseMs = 1000;       // Pause between batches (milliseconds)
const dbName = "dxlrewardsdb";
const collectionName = "msisdn_records";
const filterField = "MSISDN";
const logInterval = 10;     // Log progress every N batches (higher = fewer log lines)

// Helper function to create compact progress bar
function makeProgressBar(percent, width = 20) {
    const completed = Math.round(width * (percent / 100));
    const remaining = width - completed;
    return '[' + '='.repeat(completed) + ' '.repeat(remaining) + ']';
}

print(`Starting batch deletion with connection to ${dbName}.${collectionName}`);
print(`Will delete records where ${filterField} exists`);
print(`Processing ${batchSize} records per batch with ${pauseMs}ms pause`);

try {
    // Connect to MongoDB
    let conn = new Mongo(connectionString);
    const db = conn.getDB(dbName);
    
    // Count total documents to track progress percentage
    const totalToDelete = db[collectionName].countDocuments({ [filterField]: { $exists: true } });
    print(`Found ${totalToDelete} documents to delete`);
    
    if (totalToDelete === 0) {
        print("No matching documents found. Nothing to delete.");
        quit();
    }
    
    // Print header
    print("\nBatch  | Progress | Deleted/Total | Speed | ETA");
    print("-------+---------+--------------+-------+--------");
    
    // Track progress
    let totalDeleted = 0;
    let batchCount = 0;
    let startTime = new Date();
    let lastBatchTime = startTime;
    let totalBatchDuration = 0;
    
    // Process in batches until no matching documents remain
    while (true) {
        // Find and delete documents directly using deleteMany with a limit
        const result = db[collectionName].deleteMany(
            { [filterField]: { $exists: true } },
            { limit: batchSize }
        );
        
        const deletedCount = result.deletedCount;
        
        // Exit if no more matching documents
        if (deletedCount === 0) break;
        
        totalDeleted += deletedCount;
        batchCount++;
        
        // Calculate progress percentage
        const percentComplete = Math.round((totalDeleted / totalToDelete) * 100);
        
        // Calculate timing
        const currentTime = new Date();
        const batchDuration = (currentTime - lastBatchTime) / 1000;
        totalBatchDuration += batchDuration;
        const recordsPerSecond = Math.round(deletedCount / batchDuration);
        
        // Estimate remaining time
        const remainingRecords = totalToDelete - totalDeleted;
        const estimatedSecondsLeft = remainingRecords > 0 ? Math.round(remainingRecords / recordsPerSecond) : 0;
        let timeRemaining;
        
        if (estimatedSecondsLeft > 3600) {
            const hours = Math.floor(estimatedSecondsLeft / 3600);
            const mins = Math.floor((estimatedSecondsLeft % 3600) / 60);
            timeRemaining = `${hours}h ${mins}m`;
        } else if (estimatedSecondsLeft > 60) {
            const mins = Math.floor(estimatedSecondsLeft / 60);
            const secs = estimatedSecondsLeft % 60;
            timeRemaining = `${mins}m ${secs}s`;
        } else {
            timeRemaining = `${estimatedSecondsLeft}s`;
        }
        
        // Only log periodically to reduce output lines
        if (batchCount % logInterval === 0 || batchCount === 1 || deletedCount < batchSize) {
            print(`${batchCount.toString().padStart(5)} | ${percentComplete.toString().padStart(3)}%     | ${totalDeleted.toString().padStart(10)}/${totalToDelete} | ${recordsPerSecond}/s | ${timeRemaining}`);
        }
        
        // Update batch time
        lastBatchTime = currentTime;
        
        // Pause between batches to reduce resource consumption
        sleep(pauseMs);
    }
    
    // Report final statistics
    const totalDurationSec = (new Date() - startTime) / 1000;
    const totalDurationMin = (totalDurationSec / 60).toFixed(1);
    const avgSpeed = Math.round(totalDeleted / totalDurationSec);
    
    print(`\nOperation complete. Total documents deleted: ${totalDeleted}`);
    print(`Time taken: ${totalDurationMin} minutes (avg ${avgSpeed} records/sec)`);
    print(`Total batches: ${batchCount}`);
} catch (error) {
    print(`Error: ${error.message}`);
    print(`Stack trace: ${error.stack || "No stack trace available"}`);
}
