/**
 * Copyright Deusty LLC.
**/

#import "YapDatabaseCloudCoreOperation.h"

#import "YapDatabaseCloudCoreOptions.h"
#import "YapDatabaseCloudCorePipeline.h"

@class YapDatabaseCloudCore;


NS_INLINE BOOL YDB_IsEqualOrBothNil(id obj1, id obj2)
{
	if (obj1)
		return [obj1 isEqual:obj2];
	else
		return (obj2 == nil);
}

@interface YapDatabaseCloudCoreOperation ()

#pragma mark Internal Properties

/**
 * Represents the operation's rowid (primary key) in the queue table (that stores all operations).
 * This property is set automatically once the operation has been written to disk.
 *
 * This property does NOT need to be included during serialization.
 * It gets it own separate column in the database table (obviously).
**/
@property (nonatomic, assign, readwrite) int64_t operationRowid;

/**
 * The snapshot value is stored in its own dedicated row in the database,
 * and is used to restore the graph's & graph order.
 *
 * YapDatabaseCloudCoreTransaction is responsible for setting this value when:
 * - restoring operations from disk
 * - adding/inserting/modifying operations
**/
@property (nonatomic, assign, readwrite) uint64_t snapshot;

#pragma mark Transactional Changes

/**
 * Set 'needsDeleteDatabaseRow' (within a read-write transaction) to have the operation deleted from the database.
 * Set 'needsModifyDatabaseRow' (within a read-write transaction) to have the operation rewritten to the database.
 * 
 * As one would expect, 'needsDeleteDatabaseRow' trumps 'needsModifyDatabaseRow'.
 * So if both are set, the operation will be deleted from the database.
**/
@property (nonatomic, assign, readwrite) BOOL needsDeleteDatabaseRow;
@property (nonatomic, assign, readwrite) BOOL needsModifyDatabaseRow;

/**
 * The status that will get synced to the pipeline after the transaction is committed.
**/
@property (nonatomic, strong, readwrite) NSNumber *pendingStatus;

@property (nonatomic, readonly) BOOL pendingStatusIsCompletedOrSkipped;
@property (nonatomic, readonly) BOOL pendingStatusIsCompleted;
@property (nonatomic, readonly) BOOL pendingStatusIsSkipped;

- (void)clearTransactionVariables;

@end
