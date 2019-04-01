/**
 * Copyright Deusty LLC.
**/

#import <Foundation/Foundation.h>

#import "YapDatabaseCloudCorePipeline.h"
#import "YapDatabaseCloudCoreGraph.h"
#import "YapDatabaseCloudCore.h"


@interface YapDatabaseCloudCorePipeline ()

/**
 * All pipelines are stored in the 'pipelines' table, which includes the following information:
 * - rowid (int64_t)
 * - name (of pipeline)
 * - algorithm
 * 
 * This information is used when storing operations.
 * Operations in non-default pipelines store the pipeline's rowid, rather than the pipeline's name.
 * In addition to saving a small amount of space, this makes changing pipelines significantly easier:
 * - renaming a pipeline
 * - changing a pipeline's algorithm
**/
@property (nonatomic, assign, readwrite) int64_t rowid;

- (BOOL)setOwner:(YapDatabaseCloudCore *)owner;

- (NSArray<NSArray<YapDatabaseCloudCoreOperation *> *> *)graphOperations;

- (BOOL)getStatus:(YDBCloudCoreOperationStatus *)statusPtr
         isOnHold:(BOOL *)isOnHoldPtr
 forOperationUUID:(NSUUID *)opUUID;

- (void)restoreGraphs:(NSArray<YapDatabaseCloudCoreGraph *> *)graphs previousAlgorithm:(NSNumber *)algorithm;

- (BOOL)getSnapshot:(uint64_t *)snapshotPtr forGraphIndex:(NSUInteger)graphIdx;
- (BOOL)getGraphIndex:(NSUInteger *)graphIdxPtr forSnapshot:(uint64_t)snapshot;

- (void)processAddedGraph:(YapDatabaseCloudCoreGraph *)graph
		 insertedOperations:(NSDictionary<NSNumber *, NSArray<YapDatabaseCloudCoreOperation *> *> *)insertedOperations
       modifiedOperations:(NSDictionary<NSUUID *, YapDatabaseCloudCoreOperation *> *)modifiedOperations;

/**
 * All of the public methods that return an operation (directly, or via enumeration block),
 * always return a copy of the internally held operation.
 *
 * Internal methods can avoid the copy overhead by using the underscore versions below.
**/

- (YapDatabaseCloudCoreOperation *)_operationWithUUID:(NSUUID *)uuid;

- (void)_enumerateOperationsUsingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation,
                                                            NSUInteger graphIdx, BOOL *stop))enumBlock;

@end
