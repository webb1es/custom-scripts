// Hardcoded settings - edit these values directly if needed
const connectionString = "mongodb://localhost:27017/";  // MongoDB connection string
const batchSize = 1000;     // Number of records per batch
const pauseMs = 1000;       // Pause between batches (milliseconds)
const dbName = "dxlrewardsdb";
const collectionName = "msisdn_records";
const filterField = "MSISDN";

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

    // Track progress
    let totalDeleted = 0;
    let startTime = new Date();
    let lastBatchTime = startTime;

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

        // Calculate progress percentage
        const percentComplete = Math.round((totalDeleted / totalToDelete) * 100);

        // Calculate timing
        const currentTime = new Date();
        const batchDuration = (currentTime - lastBatchTime) / 1000;
        const totalDuration = (currentTime - startTime) / 1000;
        const recordsPerSecond = Math.round(result.deletedCount / batchDuration);

        // Estimate remaining time
        const remainingRecords = totalToDelete - totalDeleted;
        const estimatedSecondsLeft = Math.round(remainingRecords / recordsPerSecond);
        const estimatedMinutesLeft = Math.round(estimatedSecondsLeft / 60);

        // Log progress
        print(`Batch: ${result.deletedCount} records in ${batchDuration.toFixed(1)}s (${recordsPerSecond}/sec)`);
        print(`Progress: ${totalDeleted}/${totalToDelete} (${percentComplete}%) - Est. ${estimatedMinutesLeft} min remaining`);

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
} catch (error) {
    print(`Error: ${error.message}`);
}
