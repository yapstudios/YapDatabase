/**
 * Copyright Deusty LLC.
  */

#import <Foundation/Foundation.h>

#import "YapDatabaseCloudCorePipeline.h"
#import "YapDatabaseCloudCoreGraph.h"
#import "YapDatabaseCloudCore.h"


@interface YapDatabaseCloudCoreGraph ()

- (instancetype)initWithSnapshot:(uint64_t)snapshot
                      operations:(NSArray<YapDatabaseCloudCoreOperation *> *)operations;

@property (nonatomic, assign, readonly) uint64_t snapshot;
@property (nonatomic, copy, readonly) NSArray<YapDatabaseCloudCoreOperation *> *operations;

/**
 * The graph needs access to its parent pipeline so it can ask for operation status.
 */
@property (nonatomic, unsafe_unretained, readwrite) YapDatabaseCloudCorePipeline *pipeline;

/**
 * This property is set for pipelines using the FlatGraph algorithm.
 * When in this configuration, an operation in commit B might depend upon an operation in commit A.
 * So graphs are setup as a linked-list.
 */
@property (nonatomic, weak, readwrite) YapDatabaseCloudCoreGraph *previousGraph;

- (void)insertOperations:(NSArray<YapDatabaseCloudCoreOperation *> *)insertedOperations
        modifyOperations:(NSDictionary<NSUUID *, YapDatabaseCloudCoreOperation *> *)modifiedOperations
                modified:(NSMutableArray<YapDatabaseCloudCoreOperation *> *)matchedModifiedOperations;

- (NSArray *)removeCompletedAndSkippedOperations;

- (YapDatabaseCloudCoreOperation *)nextReadyOperation:(NSNumber *)minPriority;

@end
