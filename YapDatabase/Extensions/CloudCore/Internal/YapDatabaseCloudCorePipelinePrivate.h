/**
 * Copyright Deusty LLC.
**/

#import <Foundation/Foundation.h>

#import "YapDatabaseCloudCorePipeline.h"
#import "YapDatabaseCloudCoreGraph.h"
#import "YapDatabaseCloudCore.h"


@interface YapDatabaseCloudCorePipeline ()

/**
 * Non-default pipelines are stored in the 'pipelines' table, which includes the following information:
 * - rowid (int64_t)
 * - name (of pipeline)
 * 
 * This information is used when storing operations.
 * Operations in non-default pipelines store the pipeline's rowid, rather than the pipeline's name.
 * In addition to saving a small amount of space, this makes renaming pipelines significantly easier.
**/
@property (nonatomic, assign, readwrite) int64_t rowid;

- (BOOL)setOwner:(YapDatabaseCloudCore *)owner;

- (NSArray<NSArray<YapDatabaseCloudCoreOperation *> *> *)graphOperations;

- (BOOL)getStatus:(YDBCloudCoreOperationStatus *)statusPtr
         isOnHold:(BOOL *)isOnHoldPtr
 forOperationUUID:(NSUUID *)opUUID;

- (void)restoreGraphs:(NSArray<YapDatabaseCloudCoreGraph *> *)graphs previousAlgorithm:(NSNumber *)algorithm;

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

- (void)_enumerateOperationsUsingBlock:(void (^)(YapDatabaseCloudCoreOperation *operation,
                                                 NSUInteger graphIdx, BOOL *stop))enumBlock;

@end
