/**
 * Copyright Deusty LLC.
**/

#import "YapDatabaseCloudCoreGraph.h"
#import "YapDatabaseCloudCorePrivate.h"
#import "YapDatabaseCloudCoreOperationPrivate.h"
#import "YapDatabaseLogging.h"

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
**/
#if DEBUG
  static const int ydbLogLevel = YDB_LOG_LEVEL_INFO;
#else
  static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif
#pragma unused(ydbLogLevel)


@implementation YapDatabaseCloudCoreGraph

@synthesize snapshot = snapshot;
@synthesize operations = operations;
@synthesize pipeline = pipeline;
@synthesize previousGraph = previousGraph;

- (instancetype)initWithSnapshot:(uint64_t)inSnapshot
                      operations:(NSArray<YapDatabaseCloudCoreOperation *> *)inOperations
{
	if ((self = [super init]))
	{
		snapshot = inSnapshot;
		operations = [[self class] sortOperationsByPriority:inOperations];
	
		if ([self hasCircularDependency])
		{
			@throw [self circularDependencyException];
		}
	}
	return self;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Utilities
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

+ (NSArray *)sortOperationsByPriority:(NSArray *)operations
{
	return [operations sortedArrayWithOptions:NSSortStable usingComparator:^NSComparisonResult(id obj1, id obj2) {
		
		__unsafe_unretained YapDatabaseCloudCoreOperation *op1 = obj1;
		__unsafe_unretained YapDatabaseCloudCoreOperation *op2 = obj2;
		
		int32_t priority1 = op1.priority;
		int32_t priority2 = op2.priority;
		
		// From the docs:
		//
		// NSOrderedAscending  : The left operand is smaller than the right operand.
		// NSOrderedDescending : The left operand is greater than the right operand.
		//
		// HOWEVER - NSArray's sort method will order the items in Ascending order.
		// But we want the highest priority item to be at index 0.
		// So we're going to reverse this.
		
		if (priority1 < priority2) return NSOrderedDescending;
		if (priority1 > priority2) return NSOrderedAscending;
		
		return NSOrderedSame;
	}];
}

- (YapDatabaseCloudCoreOperation *)operationWithUUID:(NSUUID *)opUUID
{
	for (YapDatabaseCloudCoreOperation *op in operations)
	{
		if ([op.uuid isEqual:opUUID])
		{
			return op;
		}
	}
	
	__strong YapDatabaseCloudCoreGraph *previousGraph = self.previousGraph;
	if (previousGraph) {
		return [previousGraph operationWithUUID:opUUID];
	}
	else {
		return nil;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark General
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method allows the graph to be updated by inserting & modifying operations.
 * 
 * After modification, the graph will automatically re-sort itself.
 *
 * @param insertedOperations
 *   An array of operations that are to be added to the graph.
 * 
 * @param modifiedOperations
 *   A mapping from operationUUID to modified operation.
 *   The dictionary can include operations that don't apply to this graph.
 *   E.g. it may contain a list of every modified/replaced operation from a recent transaction.
 *   Any that don't apply to this graph will be ignored.
 * 
 * @param matchedModifiedOperations
 *   Each modified operation may or may not belong to this graph.
 *   When the method identifies ones that do, they are added to matchedOperations.
**/
- (void)insertOperations:(NSArray<YapDatabaseCloudCoreOperation *> *)insertedOperations
        modifyOperations:(NSDictionary<NSUUID *, YapDatabaseCloudCoreOperation *> *)modifiedOperations
                modified:(NSMutableArray<YapDatabaseCloudCoreOperation *> *)matchedModifiedOperations
{
	__block NSMutableIndexSet *indexesToReplace = nil;
	
	[operations enumerateObjectsUsingBlock:^(YapDatabaseCloudCoreOperation *operation, NSUInteger index, BOOL *stop) {
		
		if ([modifiedOperations objectForKey:operation.uuid])
		{
			if (indexesToReplace == nil)
				indexesToReplace = [NSMutableIndexSet indexSet];
			
			[indexesToReplace addIndex:index];
		}
	}];
	
	if ((insertedOperations.count > 0) || indexesToReplace)
	{
		NSMutableArray *newOperations = [operations mutableCopy];
		
		if (insertedOperations)
			[newOperations addObjectsFromArray:insertedOperations];
		
		[indexesToReplace enumerateIndexesUsingBlock:^(NSUInteger index, BOOL *stop) {
		#pragma clang diagnostic push
		#pragma clang diagnostic ignored "-Wimplicit-retain-self"
			
			YapDatabaseCloudCoreOperation *oldOperation = operations[index];
			YapDatabaseCloudCoreOperation *newOperation = modifiedOperations[oldOperation.uuid];
			
			[newOperations replaceObjectAtIndex:index withObject:newOperation];
			[matchedModifiedOperations addObject:newOperation];
			
		#pragma clang diagnostic pop
		}];
		
		operations = [[self class] sortOperationsByPriority:newOperations];
	}
}

/**
 * Removes any operations from the graph that have been marked as completed.
**/
- (NSArray *)removeCompletedAndSkippedOperations
{
	NSMutableIndexSet *indexesToRemove = [NSMutableIndexSet indexSet];
	NSMutableArray *removedOperations = [NSMutableArray arrayWithCapacity:1];
	
	NSUInteger index = 0;
	for (YapDatabaseCloudCoreOperation *operation in operations)
	{
		YDBCloudCoreOperationStatus status = [pipeline statusForOperationWithUUID:operation.uuid];
		
		if (status == YDBCloudOperationStatus_Completed ||
		    status == YDBCloudOperationStatus_Skipped)
		{
			[indexesToRemove addIndex:index];
			[removedOperations addObject:operation];
		}
		
		index++;
	}
	
	if (indexesToRemove.count > 0)
	{
		NSMutableArray *newOperations = [operations mutableCopy];
		[newOperations removeObjectsAtIndexes:indexesToRemove];
		
		operations = [newOperations copy];
	}
	
	return removedOperations;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Dequeue Logic
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method searches for the next operation that can immediately be started.
 *
 * If found, sets the isStarted property to YES, and returns the next operation.
 * Otherwise returns nil.
 *
 * @param minPriority
 *   If non-nil, the returned operation must have a priorty > minPriority
**/
- (YapDatabaseCloudCoreOperation *)nextReadyOperation:(NSNumber *)minPriority
{
	YDBLogVerbose(@"[graph:%llu] - nextReadyOperation", self.snapshot);
	
	YapDatabaseCloudCoreOperation *nextOpToStart = nil;
	
	for (YapDatabaseCloudCoreOperation *op in operations)
	{
		if (minPriority != nil)
		{
			// Note:
			//   The operations are already sorted by priority.
			//   This sorting was done in the init method.
			
			if (op.priority <= minPriority.intValue) {
				break;
			}
		}
		
		if ([self hasUnmetDependency:op])
		{
			YDBLogVerbose(@"[graph:%llu] - op(%@) - has unmet dependency",
							  self.snapshot, op.uuid);
		}
		else
		{
			YDBLogVerbose(@"[graph:%llu] - op(%@) - checking op status",
				self.snapshot, op.uuid);
			
			YDBCloudCoreOperationStatus status = YDBCloudOperationStatus_Pending;
			BOOL isOnHold = NO;
			[pipeline getStatus:&status isOnHold:&isOnHold forOperationUUID:op.uuid];
			
			if ((status == YDBCloudOperationStatus_Pending) && !isOnHold)
			{
				nextOpToStart = op;
				break;
			}
		}
	}
	
	YDBLogVerbose(@"[graph:%llu] - nextReadyOperation - nextOpToStart(%@)",
		self.snapshot, nextOpToStart.uuid);
	return nextOpToStart;
}

/**
 * Recursive helper method.
 *
 * A baseOperation cannot be started until all of its dependencies have completed (or been skipped).
 * So all we need to know (about the baseOperation) is:
 *
 * - is there a dependency that isn't finished yet
**/
- (BOOL)hasUnmetDependency:(YapDatabaseCloudCoreOperation *)baseOp
{
	for (NSUUID *depUUID in baseOp.dependencies)
	{
		YapDatabaseCloudCoreOperation *dependentOp = [self operationWithUUID:depUUID];
		if (dependentOp)
		{
			YDBCloudCoreOperationStatus status = YDBCloudOperationStatus_Pending;
			[pipeline getStatus:&status isOnHold:NULL forOperationUUID:dependentOp.uuid];
			
			if (status == YDBCloudOperationStatus_Completed ||
			    status == YDBCloudOperationStatus_Skipped)
			{
				// Dependency completed
			}
			else
			{
				YDBLogVerbose(@"[graph:%llu] - op(%@) - waiting for dependency(%@)",
					self.snapshot, baseOp.uuid, depUUID);
				
				return YES;
			}
		}
		else
		{
			// Dependency completed
		}
	}
	
	return NO;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Cicular Dependency Logic
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (BOOL)hasCircularDependency
{
	BOOL result = NO;
	
	NSMutableSet<NSUUID *> *visitedOps = [NSMutableSet setWithCapacity:operations.count];
	
	for (YapDatabaseCloudCoreOperation *op in operations)
	{
		if ([self _hasCircularDependency:op withVisitedOps:visitedOps])
		{
			result = YES;
			break;
		}
	}
	
	return result;
}

/**
 * Recursive helper method.
**/
- (BOOL)_hasCircularDependency:(YapDatabaseCloudCoreOperation *)op
                withVisitedOps:(NSMutableSet<NSUUID *> *)visitedOps
{
	if ([visitedOps containsObject:op.uuid])
	{
		return YES;
	}
	else
	{
		BOOL result = NO;
		
		[visitedOps addObject:op.uuid];
		
		for (NSUUID *depUUID in op.dependencies)
		{
			YapDatabaseCloudCoreOperation *depOp = [self operationWithUUID:depUUID];
			if (depOp)
			{
				if ([self _hasCircularDependency:depOp withVisitedOps:visitedOps])
				{
					result = YES;
					break;
				}
			}
		}
		
		[visitedOps removeObject:op.uuid];
		
		return result;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Exceptions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSException *)circularDependencyException
{
	NSString *reason = @"Circular dependency found in operations!";
	
	return [NSException exceptionWithName:@"YapDatabaseCloudCore" reason:reason userInfo:nil];
}

@end
