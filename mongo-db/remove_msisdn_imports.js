// Hardcoded settings - edit these values directly if needed
const connectionString = "mongodb://localhost:27017/";  // MongoDB connection string
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

print(`Starting batch deletion with connection: ${connectionString}`);
print(`Database: ${dbName}, Collection: ${collectionName}`);
print(`Will delete records where ${filterField} exists`);
print(`Processing ${batchSize} records per batch with ${pauseMs}ms pause`);

try {
    // Connect to MongoDB using connection string
    const conn = new Mongo(connectionString);
    const db = conn.getDB(dbName);
    
    // Count total documents to track progress percentage
    const totalToDelete = db[collectionName].count({ [filterField]: { $exists: true } });
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
        // Find records to delete (only get IDs to save memory)
        const idsToDelete = db[collectionName]
            .find({ [filterField]: { $exists: true } }, { _id: 1 })
            .limit(batchSize)
            .map(doc => doc._id);
        
        // Exit if no more matching documents
        if (idsToDelete.length === 0) break;
        
        // Delete the batch
        const result = db[collectionName].deleteMany({ _id: { $in: idsToDelete } });
        totalDeleted += result.deletedCount;
        batchCount++;
        
        // Calculate progress percentage
        const percentComplete = Math.round((totalDeleted / totalToDelete) * 100);
        
        // Calculate timing
        const currentTime = new Date();
        const batchDuration = (currentTime - lastBatchTime) / 1000;
        totalBatchDuration += batchDuration;
        const avgBatchDuration = totalBatchDuration / batchCount;
        const recordsPerSecond = Math.round(result.deletedCount / batchDuration);
        
        // Estimate remaining time
        const remainingRecords = totalToDelete - totalDeleted;
        const estimatedSecondsLeft = Math.round(remainingRecords / recordsPerSecond);
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
        if (batchCount % logInterval === 0 || batchCount === 1 || idsToDelete.length < batchSize) {
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
}
