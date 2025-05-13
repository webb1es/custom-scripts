// Set batch size (adjust as needed based on your RU capacity)
const batchSize = 1000;
// Initial count to track progress
let totalDeleted = 0;
let stillHasDocuments = true;

// Connect to the database
db = db.getSiblingDB("dxlrewardsdb");

// Continue deleting in batches until no documents remain
while (stillHasDocuments) {
    // Find a batch of documents that match the filter
    const docsToDelete = db.msisdn_records
        .find({ MSISDN: { $exists: true } })
        .limit(batchSize)
        .toArray();

    // Check if we found any documents
    if (docsToDelete.length === 0) {
        stillHasDocuments = false;
        print(`Finished. Total deleted: ${totalDeleted}`);
        break;
    }

    // Extract the _id values to use for deletion
    const idsToDelete = docsToDelete.map((doc) => doc._id);

    // Delete the batch using the _id values
    const result = db.msisdn_records.deleteMany({ _id: { $in: idsToDelete } });

    // Update progress
    totalDeleted += result.deletedCount;
    print(
        `Deleted batch of ${result.deletedCount}. Total so far: ${totalDeleted}`,
    );

    // Optional: Add a delay between batches to further reduce RU consumption
    sleep(1000); // Pause for 1 second between batches
}
