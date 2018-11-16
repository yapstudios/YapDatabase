/**
 * Copyright Deusty LLC.
**/

#import "YapDatabaseCloudCorePipeline.h"
#import "YapDatabaseCloudCorePrivate.h"
#import "YapDatabaseLogging.h"

#import <libkern/OSAtomic.h>

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
**/
#if DEBUG && robbie_hanson
  static const int ydbLogLevel = YDB_LOG_LEVEL_VERBOSE; // | YDB_LOG_FLAG_TRACE;
#elif DEBUG
  static const int ydbLogLevel = YDB_LOG_LEVEL_INFO;
#else
  static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif
#pragma unused(ydbLogLevel)

NSString *const YDBCloudCorePipelineQueueChangedNotification =
              @"YDBCloudCorePipelineQueueChangedNotification";

NSString *const YDBCloudCorePipelineSuspendCountChangedNotification =
              @"YDBCloudCorePipelineSuspendCountChangedNotification";

NSString *const YDBCloudCorePipelineActiveStatusChangedNotification =
              @"YDBCloudCorePipelineActiveStatusChangedNotification";

NSString *const YDBCloudCore_EphemeralKey_Status   = @"status";
NSString *const YDBCloudCore_EphemeralKey_Hold     = @"hold";


@implementation YapDatabaseCloudCorePipeline
{
	NSUInteger suspendCount;
	OSSpinLock suspendCountLock;
	
	dispatch_queue_t queue;
	void *IsOnQueueKey;
	
	id ephemeralInfoSharedKeySet;
	
	NSMutableDictionary<NSUUID *, NSMutableDictionary *> *ephemeralInfo; // must only be accessed/modified within queue
	NSMutableArray<YapDatabaseCloudCoreGraph *> *graphs;                 // must only be accessed/modified within queue
	NSMutableSet<NSUUID *> *startedOpUUIDs;                              // must only be accessed/modified within queue
	
	int needsStartNextOperationFlag; // access/modify via OSAtomic
	
	dispatch_source_t holdTimer;
	BOOL holdTimerSuspended;
	
	__weak YapDatabaseCloudCore *_atomic_owner;
	
	BOOL isActive;
}

@synthesize name = name;
@synthesize algorithm = algorithm;
@synthesize delegate = delegate;
@dynamic owner;

@synthesize previousNames = previousNames;
@synthesize maxConcurrentOperationCount = _atomic_maxConcurrentOperationCount;

@synthesize rowid = rowid;

- (instancetype)init
{
	// Empty init not allowed
	return [self initWithName: nil
	                algorithm: YDBCloudCorePipelineAlgorithm_CommitGraph
	                 delegate: nil];
}

- (instancetype)initWithName:(NSString *)inName
                    delegate:(id<YapDatabaseCloudCorePipelineDelegate>)inDelegate
{
	return [self initWithName: inName
	                algorithm: YDBCloudCorePipelineAlgorithm_CommitGraph
	                 delegate: inDelegate];
}

- (instancetype)initWithName:(NSString *)inName
                   algorithm:(YDBCloudCorePipelineAlgorithm)inAlgorithm
                    delegate:(id<YapDatabaseCloudCorePipelineDelegate>)inDelegate
{
	if (!inName)
	{
		YDBLogWarn(@"Init method requires non-nil name !");
		return nil;
	}
	
	if (inAlgorithm != YDBCloudCorePipelineAlgorithm_CommitGraph &&
	    inAlgorithm != YDBCloudCorePipelineAlgorithm_FlatGraph)
	{
		YDBLogWarn(@"Init method requires valid algorithm !");
		return nil;
	}
	
	if (!inDelegate)
	{
		YDBLogWarn(@"Init method requires non-nil delegate !");
		return nil;
	}
	
	if ((self = [super init]))
	{
		name = [inName copy];
		algorithm = inAlgorithm;
		delegate = inDelegate;
		
		suspendCountLock = OS_SPINLOCK_INIT;
		
		queue = dispatch_queue_create("YapDatabaseCloudCorePipeline", DISPATCH_QUEUE_SERIAL);
		
		IsOnQueueKey = &IsOnQueueKey;
		dispatch_queue_set_specific(queue, IsOnQueueKey, IsOnQueueKey, NULL);
		
		ephemeralInfoSharedKeySet = [NSDictionary sharedKeySetForKeys:@[
		  YDBCloudCore_EphemeralKey_Status,
		  YDBCloudCore_EphemeralKey_Hold
		]];
		
		ephemeralInfo    = [[NSMutableDictionary alloc] initWithCapacity:8];
		graphs           = [[NSMutableArray alloc] initWithCapacity:8];
		
		startedOpUUIDs   = [[NSMutableSet alloc] initWithCapacity:8];
		
		self.maxConcurrentOperationCount = 8;
	}
	return self;
}

- (void)dealloc
{
	[[NSNotificationCenter defaultCenter] removeObserver:self];
	
	// Deallocating a suspended timer will cause a crash
	if (holdTimer && holdTimerSuspended) {
		dispatch_resume(holdTimer);
		holdTimerSuspended = NO;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Ownership
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (YapDatabaseCloudCore *)owner
{
	__block YapDatabaseCloudCore *owner = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		owner = _atomic_owner;
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return owner;
}

- (BOOL)setOwner:(YapDatabaseCloudCore *)inOwner
{
	__block BOOL wasOwnerSet = NO;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		if (!_atomic_owner && inOwner)
		{
			_atomic_owner = inOwner;
			wasOwnerSet = YES;
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return wasOwnerSet;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Operation Searching
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (YapDatabaseCloudCoreOperation *)operationWithUUID:(NSUUID *)uuid
{
	return [[self _operationWithUUID:uuid] copy];
}

- (YapDatabaseCloudCoreOperation *)_operationWithUUID:(NSUUID *)uuid
{
	if (uuid == nil) return nil;
	
	__block YapDatabaseCloudCoreOperation *match = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			for (YapDatabaseCloudCoreOperation *operation in graph.operations)
			{
				if ([operation.uuid isEqual:uuid])
				{
					match = operation;
					return;
				}
			}
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return match;
}

/**
 * Returns a list of operations in state 'YDBCloudOperationStatus_Active'.
**/
- (NSArray<YapDatabaseCloudCoreOperation *> *)activeOperations
{
	NSUInteger capacity = self.maxConcurrentOperationCount;
	if (capacity == 0) {
		capacity = 8;
	}
	
	NSMutableArray<YapDatabaseCloudCoreOperation *> *results = [NSMutableArray arrayWithCapacity:capacity];
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableSet<NSUUID *> *activeOpUUIDs = [NSMutableSet setWithCapacity:capacity];
		
		[ephemeralInfo enumerateKeysAndObjectsUsingBlock:
			^(NSUUID *opUUID, NSMutableDictionary *opInfo, BOOL *stop)
		{
			NSNumber *statusNum = opInfo[YDBCloudCore_EphemeralKey_Status];
			if (statusNum)
			{
				YDBCloudCoreOperationStatus status = (YDBCloudCoreOperationStatus)[statusNum integerValue];
				if (status == YDBCloudOperationStatus_Active)
				{
					[activeOpUUIDs addObject:opUUID];
				}
			}
		}];
		
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			for (YapDatabaseCloudCoreOperation *op in graph.operations)
			{
				if ([activeOpUUIDs containsObject:op.uuid])
				{
					[results addObject:[op copy]];
				}
			}
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return results;
}

- (void)enumerateOperationsUsingBlock:(void (^)(YapDatabaseCloudCoreOperation *operation,
                                                NSUInteger graphIdx, BOOL *stop))enumBlock
{
	[self _enumerateOperationsUsingBlock:^(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop) {
		
		enumBlock([operation copy], graphIdx, stop);
	}];
}

- (void)_enumerateOperationsUsingBlock:(void (^)(YapDatabaseCloudCoreOperation *operation,
                                                 NSUInteger graphIdx, BOOL *stop))enumBlock
{
	__block NSMutableArray<NSArray<YapDatabaseCloudCoreOperation *> *> *graphOperations = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		graphOperations = [NSMutableArray arrayWithCapacity:graphs.count];
		
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			[graphOperations addObject:graph.operations];
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	
	NSUInteger graphIdx = 0;
	BOOL stop = NO;
	
	for (NSArray<YapDatabaseCloudCoreOperation *> *operations in graphOperations)
	{
		for (YapDatabaseCloudCoreOperation *operation in operations)
		{
			enumBlock(operation, graphIdx, &stop);
			
			if (stop) break;
		}
		
		if (stop) break;
		graphIdx++;
	}
}

- (NSUInteger)graphCount
{
	__block NSUInteger graphCount = 0;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		graphCount = graphs.count;
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return graphCount;
}

- (NSArray<NSArray<YapDatabaseCloudCoreOperation *> *> *)graphOperations
{
	__block NSMutableArray<NSArray<YapDatabaseCloudCoreOperation *> *> *graphOperations = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		graphOperations = [NSMutableArray arrayWithCapacity:graphs.count];
		
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			[graphOperations addObject:graph.operations];
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return graphOperations;
}

- (BOOL)getSnapshot:(uint64_t *)snapshotPtr forGraphIndex:(NSUInteger)graphIdx
{
	__block BOOL found = NO;
	__block uint64_t snapshot = 0;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		if (graphIdx <= graphs.count)
		{
			found = YES;
			snapshot = graphs[graphIdx].snapshot;
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	if (snapshotPtr) *snapshotPtr = snapshot;
	return found;
}

- (BOOL)getGraphIndex:(NSUInteger *)graphIdxPtr forSnapshot:(uint64_t)snapshot
{
	__block BOOL found = NO;
	__block uint64_t graphIdx = 0;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSUInteger idx = 0;
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			if (graph.snapshot == snapshot)
			{
				found = YES;
				graphIdx = idx;
				
				break;
			}
			else
			{
				idx++;
			}
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	if (graphIdxPtr) *graphIdxPtr = graphIdx;
	return found;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Utility Methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Internal method to fetch root key/value pairs from the ephemeralInfo dictionay.
**/
- (id)_ephemeralInfoForKey:(NSString *)key operationUUID:(NSUUID *)opUUID
{
	if (key == nil) return nil;
	if (opUUID == nil) return nil;
	
	__block id result = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[opUUID];
		result = opInfo[key];
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return result;
}

/**
 * Internal method to modify root key/value pairs in the ephemeralInfo dictionay.
**/
- (void)_setEphemeralInfo:(id)object forKey:(NSString *)key operationUUID:(NSUUID *)uuid
{
	if (key == nil) return;
	if (uuid == nil) return;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[uuid];
		if (opInfo)
		{
			opInfo[key] = object;
			
			if (!object && (opInfo.count == 0))
			{
				ephemeralInfo[uuid] = nil;
			}
		}
		else if (object)
		{
			opInfo = [NSMutableDictionary dictionaryWithSharedKeySet:ephemeralInfoSharedKeySet];
			ephemeralInfo[uuid] = opInfo;
			
			opInfo[key] = object;
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
}

/**
 * Returns whether or not the change was allowed (not necesarily whether it changed status).
 *
 * Once an operation transitions to completed or skipped,
 *  it's not allowed to transition to pending or active.
**/
- (BOOL)_setStatus:(YDBCloudCoreOperationStatus)status forOperationUUID:(NSUUID *)uuid
{
	__block BOOL allowed = YES;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[uuid];
		if (opInfo == nil)
		{
			opInfo = [NSMutableDictionary dictionaryWithSharedKeySet:ephemeralInfoSharedKeySet];
			ephemeralInfo[uuid] = opInfo;
		}
		
		NSNumber *existingStatusNum = opInfo[YDBCloudCore_EphemeralKey_Status];
		if (existingStatusNum != nil)
		{
			YDBCloudCoreOperationStatus existingStatus = (YDBCloudCoreOperationStatus)[existingStatusNum integerValue];
			
			if (existingStatus == YDBCloudOperationStatus_Completed ||
			    existingStatus == YDBCloudOperationStatus_Skipped)
			{
				// Cannot change status after its been marked completed or skipped
				allowed = NO;
			}
		}
		
		if (allowed)
		{
			opInfo[YDBCloudCore_EphemeralKey_Status] = @(status);
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return allowed;
}

/**
 * Internal method to fetch status & hold in an atomic manner.
**/
- (BOOL)getStatus:(YDBCloudCoreOperationStatus *)statusPtr
         isOnHold:(BOOL *)isOnHoldPtr
 forOperationUUID:(NSUUID *)opUUID
{
	__block BOOL found = NO;
	__block NSNumber *status = nil;
	__block NSDate *hold = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[opUUID];
		if (opInfo)
		{
			found  = YES;
			status = opInfo[YDBCloudCore_EphemeralKey_Status];
			hold   = opInfo[YDBCloudCore_EphemeralKey_Hold];
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	if (statusPtr)
	{
		if (status != nil)
			*statusPtr = (YDBCloudCoreOperationStatus)[status integerValue];
		else
			*statusPtr = YDBCloudOperationStatus_Pending;
	}
	if (isOnHoldPtr)
	{
		if (hold)
			*isOnHoldPtr = ([hold timeIntervalSinceNow] > 0.0);
		else
			*isOnHoldPtr = NO;
	}
	
	return found;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Operation Status
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Returns the current status for the given operation.
**/
- (YDBCloudCoreOperationStatus)statusForOperationWithUUID:(NSUUID *)opUUID
{
	NSNumber *status = [self _ephemeralInfoForKey:YDBCloudCore_EphemeralKey_Status operationUUID:opUUID];
	if (status != nil)
		return (YDBCloudCoreOperationStatus)[status integerValue];
	else
		return YDBCloudOperationStatus_Pending;
}

/**
 * Typically you are strongly discouraged from manually starting an operation.
 * You should allow the pipeline to mange the queue, and only start operations when told to.
 *
 * However, there is one particular edge case in which is is unavoidable: background network tasks.
 * If the app is relaunched, and you discover there are network tasks from a previously app session,
 * you'll obviously want to avoid starting the corresponding operation again.
 * In this case, you should use this method to inform the pipeline that the operation is already started.
**/
- (void)setStatusAsActiveForOperationWithUUID:(NSUUID *)opUUID
{
	if (opUUID == nil) return;
	
	__weak YapDatabaseCloudCorePipeline *weakSelf = self;
	
	dispatch_block_t block = ^{ @autoreleasepool {
		
		__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		BOOL allowed = [strongSelf _setStatus:YDBCloudOperationStatus_Active forOperationUUID:opUUID];
		if (allowed)
		{
			[strongSelf->startedOpUUIDs addObject:opUUID];
			
			[strongSelf _checkForActiveStatusChange]; // may have transitioned from inactive to active
		}
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_async(queue, block); // ASYNC
}

/**
 * The PipelineDelegate may invoke this method to reset a failed operation.
 * This gives control over the operation back to the pipeline,
 * and it will dispatch it back to the PipelineDelegate again when ready.
**/
- (void)setStatusAsPendingForOperationWithUUID:(NSUUID *)opUUID
{
	if (opUUID == nil) return;
	
	__weak YapDatabaseCloudCorePipeline *weakSelf = self;
	
	dispatch_block_t block = ^{ @autoreleasepool {
		
		__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		BOOL allowed = [strongSelf _setStatus:YDBCloudOperationStatus_Pending forOperationUUID:opUUID];
		if (allowed)
		{
			[strongSelf->startedOpUUIDs removeObject:opUUID];
			
			[strongSelf _checkForActiveStatusChange]; // may have transitioned from active to inactive
			[strongSelf startNextOperationIfPossible];
		}
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_async(queue, block); // ASYNC
}

/**
 * The PipelineDelegate may invoke this method to reset a failed operation,
 * and simultaneously tell the pipeline to delay retrying it again for a period of time.
 *
 * This is typically used when implementing retry logic such as exponential backoff.
 * It works by setting a hold on the operation to [now dateByAddingTimeInterval:delay].
**/
- (void)setStatusAsPendingForOperationWithUUID:(NSUUID *)opUUID
                                    retryDelay:(NSTimeInterval)delay
{
	NSDate *hold = nil;
	if (delay > 0.0) {
		hold = [NSDate dateWithTimeIntervalSinceNow:delay];
	}
	
	__weak YapDatabaseCloudCorePipeline *weakSelf = self;
	
	dispatch_block_t block = ^{ @autoreleasepool {
		
		__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		BOOL allowed = [strongSelf _setStatus:YDBCloudOperationStatus_Pending forOperationUUID:opUUID];
		if (allowed)
		{
			[strongSelf _setEphemeralInfo:hold
			                       forKey:YDBCloudCore_EphemeralKey_Hold
			                operationUUID:opUUID];
			
			[strongSelf->startedOpUUIDs removeObject:opUUID];
			[strongSelf updateHoldTimer];
			
			[strongSelf _checkForActiveStatusChange]; // may have transitioned from active to inactive
			[strongSelf startNextOperationIfPossible];
		}
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_async(queue, block); // ASYNC
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Operation Hold
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Returns the current hold for the operation, or nil if there is no hold.
**/
- (NSDate *)holdDateForOperationWithUUID:(NSUUID *)opUUID
{
	return [self _ephemeralInfoForKey:YDBCloudCore_EphemeralKey_Hold operationUUID:opUUID];
}

/**
 * And operation can be put on "hold" until a specified date.
 * This is typically used in conjunction with retry logic such as exponential backoff.
 *
 * The operation won't be delegated again until the given date.
 * You can pass a nil date to remove a hold on an operation.
 *
 * @see setStatusAsPendingForOperation:withRetryDelay:
**/
- (void)setHoldDate:(NSDate *)date forOperationWithUUID:(NSUUID *)opUUID
{
	if (opUUID == nil) return;
	
	dispatch_block_t block = ^{ @autoreleasepool {
		
		[self _setEphemeralInfo:date
		                 forKey:YDBCloudCore_EphemeralKey_Hold
		          operationUUID:opUUID];
		
		[self updateHoldTimer];
		[self startNextOperationIfPossible];
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_async(queue, block); // ASYNC
}

/**
 * The pipeline manages its own timer, that's configured to fire when the next "hold" for an operation expires.
 * Having a single timer is more efficient when multiple operations are on hold.
**/
- (void)updateHoldTimer
{
	NSAssert(dispatch_get_specific(IsOnQueueKey), @"Must be executed within queue");
	
	// Create holdTimer (if needed)
	
	if (holdTimer == NULL)
	{
		holdTimer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, queue);
		
		__weak typeof(self) weakSelf = self;
		dispatch_source_set_event_handler(holdTimer, ^{ @autoreleasepool {
		
			__strong typeof(self) strongSelf = weakSelf;
			if (strongSelf)
			{
				[strongSelf fireHoldTimer];
			}
		}});
		
		holdTimerSuspended = YES;
	}
	
	// Calculate when to fire next
	
	__block NSDate *nextFireDate = nil;
	
	[ephemeralInfo enumerateKeysAndObjectsUsingBlock:^(NSUUID *uuid, NSMutableDictionary *opInfo, BOOL *stop) {
		
		NSDate *hold = opInfo[YDBCloudCore_EphemeralKey_Hold];
		if (hold)
		{
			if (nextFireDate == nil)
				nextFireDate = hold;
			else
				nextFireDate = [nextFireDate earlierDate:hold];
		}
	}];
	
	// Update timer
	
	if (nextFireDate)
	{
		NSTimeInterval startOffset = [nextFireDate timeIntervalSinceNow];
		if (startOffset < 0.0)
			startOffset = 0.0;
		
		dispatch_time_t start = dispatch_time(DISPATCH_TIME_NOW, (startOffset * NSEC_PER_SEC));
		
		uint64_t interval = DISPATCH_TIME_FOREVER;
		uint64_t leeway = (0.1 * NSEC_PER_SEC);
		
		dispatch_source_set_timer(holdTimer, start, interval, leeway);
		
		if (holdTimerSuspended) {
			holdTimerSuspended = NO;
			dispatch_resume(holdTimer);
		}
	}
	else
	{
		if (!holdTimerSuspended) {
			holdTimerSuspended = YES;
			dispatch_suspend(holdTimer);
		}
	}
}

/**
 * Invoked when the hold timer fires.
 * This means that one or more operations are no longer on hold, and may be re-started.
**/
- (void)fireHoldTimer
{
	NSAssert(dispatch_get_specific(IsOnQueueKey), @"Must be executed within queue");
	
	// Remove the stored hold date for any items in which: hold < now
	
	NSDate *now = [NSDate date];
	__block NSMutableArray *uuidsToRemove = nil;
	
	[ephemeralInfo enumerateKeysAndObjectsUsingBlock:^(NSUUID *uuid, NSMutableDictionary *opInfo, BOOL *stop) {
		
		NSDate *hold = opInfo[YDBCloudCore_EphemeralKey_Hold];
		if (hold)
		{
			NSTimeInterval interval = [hold timeIntervalSinceDate:now];
			if (interval <= 0)
			{
				opInfo[YDBCloudCore_EphemeralKey_Hold] = nil;
				
				if (opInfo.count == 0)
				{
					if (uuidsToRemove == nil)
						uuidsToRemove = [[NSMutableArray alloc] initWithCapacity:4];
					
					[uuidsToRemove addObject:uuid];
				}
			}
		}
	}];
	
	if (uuidsToRemove)
	{
		[ephemeralInfo removeObjectsForKeys:uuidsToRemove];
	}
	
	[self updateHoldTimer];
	[self startNextOperationIfPossible];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Suspend & Resume
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (BOOL)isSuspended
{
	return ([self suspendCount] > 0);
}

- (NSUInteger)suspendCount
{
	NSUInteger currentSuspendCount = 0;
	
	OSSpinLockLock(&suspendCountLock);
	{
		currentSuspendCount = suspendCount;
	}
	OSSpinLockUnlock(&suspendCountLock);
	
	return currentSuspendCount;
}

/**
 * Increments the suspendCount.
 * All calls to 'suspend' need to be matched with an equal number of calls to 'resume'.
 *
 * @return
 *   The new suspend count.
 *   This will be 1 if the pipeline was previously active, and is now suspended due to this call.
 *   Otherwise it will be greater than one, meaning it was previously suspended,
 *   and you just incremented the suspend count.
 *
 * @see resume
 * @see suspendCount
**/
- (NSUInteger)suspend
{
	return [self suspendWithCount:1];
}

/**
 * This method operates the same as invoking the suspend method the given number of times.
 * That is, it increments the suspend count by the given number.
 *
 * If you invoke this method with a zero parameter,
 * it will simply return the current suspend count, without modifying it.
 *
 * @see suspend
 * @see suspendCount
**/
- (NSUInteger)suspendWithCount:(NSUInteger)suspendCountIncrement
{
	BOOL overflow = NO;
	NSUInteger oldSuspendCount = 0;
	NSUInteger newSuspendCount = 0;
	
	OSSpinLockLock(&suspendCountLock);
	{
		oldSuspendCount = suspendCount;
		
		if (suspendCount <= (NSUIntegerMax - suspendCountIncrement))
			suspendCount += suspendCountIncrement;
		else {
			suspendCount = NSUIntegerMax;
			overflow = YES;
		}
		
		newSuspendCount = suspendCount;
	}
	OSSpinLockUnlock(&suspendCountLock);
	
	if (overflow) {
		YDBLogWarn(@"%@ - The suspendCount has reached NSUIntegerMax!", THIS_METHOD);
	}
	else if (suspendCountIncrement > 0) {
		YDBLogInfo(@"=> SUSPENDED : incremented suspendCount == %lu", (unsigned long)newSuspendCount);
	}
	
	[self postSuspendCountChangedNotification];
	if ((oldSuspendCount == 0) && (newSuspendCount > 0))
	{
		[self checkForActiveStatusChange]; // may have transitioned from active to inactive
	}
	
	return newSuspendCount;
}

/**
 * Decrements the suspendCount.
 * All calls to 'suspend' need to be matched with an equal number of calls to 'resume'.
 *
 * @return
 *   The current suspend count.
 *   This will be 0 if the extension was previously suspended, and is now resumed due to this call.
 *   Otherwise it will be greater than one, meaning it's still suspended,
 *   and you just decremented the suspend count.
 *
 * @see suspend
 * @see suspendCount
**/
- (NSUInteger)resume
{
	BOOL underflow = 0;
	NSUInteger newSuspendCount = 0;
	
	OSSpinLockLock(&suspendCountLock);
	{
		if (suspendCount > 0)
			suspendCount--;
		else
			underflow = YES;
		
		newSuspendCount = suspendCount;
	}
	OSSpinLockUnlock(&suspendCountLock);
	
	if (underflow) {
		YDBLogWarn(@"%@ - Attempting to resume with suspendCount already at zero.", THIS_METHOD);
	}
	else
	{
		if (newSuspendCount == 0) {
			YDBLogInfo(@"=> RESUMED");
		}
		else {
			YDBLogInfo(@"=> SUSPENDED : decremented suspendCount == %lu", (unsigned long)newSuspendCount);
		}
		
		[self postSuspendCountChangedNotification];
		if (newSuspendCount == 0)
		{
			[self checkForActiveStatusChange]; // may have transitioned from inactive to active
			[self queueStartNextOperationIfPossible];
		}
	}
	
	return newSuspendCount;
}

- (void)postSuspendCountChangedNotification
{
	dispatch_block_t block = ^{
		
		[[NSNotificationCenter defaultCenter] postNotificationName:YDBCloudCorePipelineSuspendCountChangedNotification
		                                                    object:self];
	};
	
	if ([NSThread isMainThread])
		block();
	else
		dispatch_async(dispatch_get_main_queue(), block);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Active & Inactive
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (BOOL)isActive
{
	__block BOOL status = NO;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		status = isActive;
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return status;
}

- (void)checkForActiveStatusChange
{
	dispatch_block_t block = ^{ @autoreleasepool {
		[self _checkForActiveStatusChange];
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
}

- (void)_checkForActiveStatusChange
{
	NSAssert(dispatch_get_specific(IsOnQueueKey), @"Must be executed within queue");
	
	__block BOOL hasOps = NO;
	__block BOOL hasActiveOps = NO;
	
	[self _enumerateOperationsUsingBlock:
		^(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		if (!hasOps) {
			hasOps = YES;
		}
		
		if (graphIdx == 0)
		{
			if ([self statusForOperationWithUUID:operation.uuid] == YDBCloudOperationStatus_Active)
			{
				hasActiveOps = YES;
				*stop = YES;
			}
		}
		else
		{
			*stop = YES;
		}
	}];
	
	if (isActive) // current reported state is active
	{
		// Transition to inactive when:
		// - There are 0 operations in 'YDBCloudOperationStatus_Active' mode
		// - AND (the pipeline is suspended OR there are no more operations)
		
		if (!hasActiveOps)
		{
			if (!hasOps || self.isSuspended)
			{
				isActive = NO;
				[self postActiveStatusChanged:isActive];
			}
		}
	}
	else // current reported state is inactive
	{
		// Transition to active when:
		// - There are 1 or more operations in 'YDBCloudOperationStatus_Active' mode.
		
		if (hasActiveOps)
		{
			isActive = YES;
			[self postActiveStatusChanged:isActive];
		}
	}
}

- (void)postActiveStatusChanged:(BOOL)_isActive
{
	dispatch_block_t block = ^{
		
		NSDictionary *userInfo = @{ @"isActive" : @(_isActive) };
		
		[[NSNotificationCenter defaultCenter] postNotificationName: YDBCloudCorePipelineActiveStatusChangedNotification
		                                                    object: self
		                                                  userInfo: userInfo];
	};
	
	if ([NSThread isMainThread])
		block();
	else
		dispatch_async(dispatch_get_main_queue(), block);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Graph Management
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)restoreGraphs:(NSArray<YapDatabaseCloudCoreGraph *> *)inGraphs
    previousAlgorithm:(NSNumber *)prvAlgorithm
{
	YDBLogAutoTrace();
	
	__weak YapDatabaseCloudCorePipeline *weakSelf = self;
	
	dispatch_block_t block = ^{ @autoreleasepool {
		
		__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		for (YapDatabaseCloudCoreGraph *graph in inGraphs)
		{
			graph.pipeline = self;
		}
		
		BOOL migratingFromCommitGraphToFlatGraph =
		    (prvAlgorithm != nil)
		 && (prvAlgorithm.integerValue == YDBCloudCorePipelineAlgorithm_CommitGraph)
		 && (strongSelf->algorithm == YDBCloudCorePipelineAlgorithm_FlatGraph);
		
		if (migratingFromCommitGraphToFlatGraph)
		{
			// We have to assume that all previously stored operations
			// only have the proper dependencies set within their respective graph.
			//
			// So we need to implicitly maintain the CommitGraph
			// for these restored operations (but not for future operations).
			
			YapDatabaseCloudCoreGraph *lastNonEmptyGraph = nil;
			for (YapDatabaseCloudCoreGraph *graph in inGraphs)
			{
				if (lastNonEmptyGraph)
				{
					for (YapDatabaseCloudCoreOperation *laterOp in graph.operations)
					{
						for (YapDatabaseCloudCoreOperation *earlierOp in lastNonEmptyGraph.operations)
						{
							[laterOp addDependency:earlierOp];
						}
					}
				}
				
				if (graph.operations.count > 0) {
					lastNonEmptyGraph = graph;
				}
			}
		}
		
		[strongSelf->graphs addObjectsFromArray:inGraphs];
		
		if (strongSelf->graphs.count > 0) {
			[strongSelf startNextOperationIfPossible];
		}
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_async(queue, block); // ASYNC
}

- (void)processAddedGraph:(YapDatabaseCloudCoreGraph *)graph
		 insertedOperations:(NSDictionary<NSNumber *, NSArray<YapDatabaseCloudCoreOperation *> *> *)insertedOperations
       modifiedOperations:(NSDictionary<NSUUID *, YapDatabaseCloudCoreOperation *> *)modifiedOperations
{
	YDBLogAutoTrace();
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		if (graph)
		{
			graph.pipeline = self;
			if (algorithm == YDBCloudCorePipelineAlgorithm_FlatGraph) {
				graph.previousGraph = [graphs lastObject];
			}
			
			[graphs addObject:graph];
			
			for (YapDatabaseCloudCoreOperation *operation in graph.operations)
			{
				[operation clearTransactionVariables];
			}
		}
		
		NSMutableArray *modifiedOperationsInPipeline = nil;
		
		if ((insertedOperations.count > 0) || (modifiedOperations.count > 0))
		{
			// The modifiedOperations dictionary contains a list of every pre-existing operation
			// that was modified/replaced in the read-write transaction.
			//
			// Each operation may or may not belong to this pipeline.
			// When we identify the ones that do, we need to add them to matchedOperations.
			
			modifiedOperationsInPipeline = [NSMutableArray array];
			
			NSUInteger graphIdx = 0;
			for (YapDatabaseCloudCoreGraph *graph in graphs)
			{
				NSArray<YapDatabaseCloudCoreOperation *> *insertedInGraph = insertedOperations[@(graphIdx)];
				
				if (insertedInGraph)
					[modifiedOperationsInPipeline addObjectsFromArray:insertedInGraph];
				
				[graph insertOperations:insertedInGraph
				       modifyOperations:modifiedOperations
				               modified:modifiedOperationsInPipeline];
				
				graphIdx++;
			}
			
			for (YapDatabaseCloudCoreOperation *operation in modifiedOperationsInPipeline)
			{
				NSNumber *pendingStatus = operation.pendingStatus;
				if (pendingStatus != nil)
				{
					YDBCloudCoreOperationStatus status = (YDBCloudCoreOperationStatus)[pendingStatus integerValue];
					
					[self _setStatus:status forOperationUUID:operation.uuid];
				}
				
				[operation clearTransactionVariables];
			}
		}
		
		if (graph || (modifiedOperationsInPipeline.count > 0))
		{
			// Although we could do this synchronously here (since we're inside the queue),
			// it may be better to perform this task async so we don't delay
			// the readWriteTransaction (which invoked this method).
			//
			[self queueStartNextOperationIfPossible];
			
			// Notify listeners that the operation list in the queue changed.
			[self postQueueChangedNotification];
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Dequeue Logic
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method may be invoked from any thread.
 * It uses an efficient mechanism to consolidate invocations of the 'startNextOperationIfPossible' method.
 * 
 * That is, invoking this method 50 times may result in only a single invocation of 'startNextOperationIfPossible'.
**/
- (void)queueStartNextOperationIfPossible
{
	int const flagOff = 0;
	int const flagOn  = 1;
	
	BOOL didSetFlagOn = OSAtomicCompareAndSwapInt(flagOff, flagOn, &needsStartNextOperationFlag);
	if (didSetFlagOn)
	{
		__weak YapDatabaseCloudCorePipeline *weakSelf = self;
		
		dispatch_async(queue, ^{ @autoreleasepool {
			
			__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
			if (strongSelf)
			{
				OSAtomicCompareAndSwapInt(flagOn, flagOff, &strongSelf->needsStartNextOperationFlag);
				
				[strongSelf startNextOperationIfPossible];
			}
		}});
	}
}

/**
 * Core logic for starting operations via the PipelineDelegate.
**/
- (void)startNextOperationIfPossible
{
	YDBLogAutoTrace();
	NSAssert(dispatch_get_specific(IsOnQueueKey), @"Must be executed within queue");
	
	if (algorithm == YDBCloudCorePipelineAlgorithm_CommitGraph)
		[self _startNextOperationIfPossible_CommitGraph];
	else
		[self _startNextOperationIfPossible_FlatGraph];
}

- (void)_startNextOperationIfPossible_CommitGraph
{
	YDBLogAutoTrace();
	
	// Step 1 of 3:
	// Purge any completed/skipped operations
	
	BOOL queueChanged = NO;
	YapDatabaseCloudCoreGraph *currentGraph = [graphs firstObject];
	while (currentGraph)
	{
		NSArray *removedOperations = [currentGraph removeCompletedAndSkippedOperations];
		if (removedOperations.count > 0)
		{
			queueChanged = YES;
			
			for (YapDatabaseCloudCoreOperation *operation in removedOperations)
			{
				[startedOpUUIDs removeObject:operation.uuid];
				[ephemeralInfo removeObjectForKey:operation.uuid];
			}
		}
		
		if (currentGraph.operations.count == 0)
		{
			[graphs removeObjectAtIndex:0];
			currentGraph = [graphs firstObject];
		}
		else
		{
			break;
		}
	}
	
	if (queueChanged) {
		[self postQueueChangedNotification];
	}
	
	if (currentGraph == nil)
	{
		// Waiting for another graph to be added
		
		[self _checkForActiveStatusChange]; // may have transitioned from active to inactive
		return;
	}
	
	// Step 2 of 3:
	// Are we allowed to start any more operations ?
	
	if ([self isSuspended])
	{
		// Waiting to be resumed
		return;
	}
	
	NSUInteger maxConcurrentOperationCount = self.maxConcurrentOperationCount;
	if (maxConcurrentOperationCount == 0)
		maxConcurrentOperationCount = NSUIntegerMax;
	
	if (startedOpUUIDs.count >= maxConcurrentOperationCount)
	{
		// Waiting for one or more operations to complete
		return;
	}
	
	// Step 3 of 3:
	// Start as many operations as we can.
	
	YapDatabaseCloudCoreOperation *nextOp = [currentGraph dequeueNextOperation];
	if (nextOp)
	{
		__weak YapDatabaseCloudCorePipeline *weakSelf = self;
		dispatch_queue_t globalQueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
		
		do {
			
			[self _setStatus:YDBCloudOperationStatus_Active forOperationUUID:nextOp.uuid];
			
			YapDatabaseCloudCoreOperation *opToStart = nextOp;
			dispatch_async(globalQueue, ^{ @autoreleasepool {
				
				__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
				if (strongSelf) {
					[strongSelf.delegate startOperation:opToStart forPipeline:strongSelf];
				}
			}});
			
			[startedOpUUIDs addObject:nextOp.uuid];
			if (startedOpUUIDs.count >= maxConcurrentOperationCount) {
				break;
			}
			
			nextOp = [currentGraph dequeueNextOperation];
			
		} while (nextOp);
		
		[self _checkForActiveStatusChange]; // may have transitioned from inactive to active
	}
}

- (void)_startNextOperationIfPossible_FlatGraph
{
	YDBLogAutoTrace();
	
	// Step 1 of 3:
	// Purge any completed/skipped operations
	
	BOOL queueChanged = NO;
	NSUInteger i = 0;
	while (i < graphs.count)
	{
		YapDatabaseCloudCoreGraph *graph = graphs[i];
		
		NSArray *removedOperations = [graph removeCompletedAndSkippedOperations];
		if (removedOperations.count > 0)
		{
			queueChanged = YES;
			
			for (YapDatabaseCloudCoreOperation *operation in removedOperations)
			{
				[startedOpUUIDs removeObject:operation.uuid];
				[ephemeralInfo removeObjectForKey:operation.uuid];
			}
		}
		
		if (graph.operations.count == 0)
		{
			[graphs removeObjectAtIndex:i];
			
			// Careful: Graphs (in FlatCommit mode) are setup in a linked-list,
			// where each graph has a (weak) pointer to the previous graph.
			// So we need to fixup the link.
			if (i < graphs.count)
			{
				YapDatabaseCloudCoreGraph *nextGraph = graphs[i];
				if (i == 0) {
					nextGraph.previousGraph = nil;
				}
				else {
					nextGraph.previousGraph = graphs[i - 1];
				}
			}
		}
		else
		{
			i++;
		}
	}
	
	if (queueChanged) {
		[self postQueueChangedNotification];
	}
	
	if (graphs.count == 0)
	{
		// Waiting for another graph to be added
		
		[self _checkForActiveStatusChange]; // may have transitioned from active to inactive
		return;
	}
	
	// Step 2 of 3:
	// Are we allowed to start any more operations ?
	
	if ([self isSuspended])
	{
		// Waiting to be resumed
		return;
	}
	
	NSUInteger maxConcurrentOperationCount = self.maxConcurrentOperationCount;
	if (maxConcurrentOperationCount == 0)
		maxConcurrentOperationCount = NSUIntegerMax;
	
	if (startedOpUUIDs.count >= maxConcurrentOperationCount)
	{
		// Waiting for one or more operations to complete
		return;
	}
	
	// Step 3 of 3:
	// Start as many operations as we can.
	//
	// Notes:
	//
	// In a FlatGraph configuration:
	// - Operations in commit C may have dependencies on other operations from commit C, or
	//   from earlier commits (such as B or A).
	// - We are allowed to start operations from any commit (so long as dependecies are fullfilled).
	// - If priorities are equal, we implicitly prefer operations that were queued earlier.
	
	YapDatabaseCloudCoreOperation* (^dequeueNextOperation)(void) = ^ YapDatabaseCloudCoreOperation*(){
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		YapDatabaseCloudCoreOperation *result = nil;
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			YapDatabaseCloudCoreOperation *next = [graph dequeueNextOperation];
			if (next)
			{
				if ((result == nil) || (result.priority < next.priority))
				{
					result = next;
				}
			}
		}
		
		return result;
		
	#pragma clang diagnostic pop
	};
	
	YapDatabaseCloudCoreOperation *nextOp = dequeueNextOperation();
	if (nextOp)
	{
		__weak YapDatabaseCloudCorePipeline *weakSelf = self;
		dispatch_queue_t globalQueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
		
		do {
			
			[self _setStatus:YDBCloudOperationStatus_Active forOperationUUID:nextOp.uuid];
			
			YapDatabaseCloudCoreOperation *opToStart = nextOp;
			dispatch_async(globalQueue, ^{ @autoreleasepool {
				
				__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
				if (strongSelf) {
					[strongSelf.delegate startOperation:opToStart forPipeline:strongSelf];
				}
			}});
			
			[startedOpUUIDs addObject:nextOp.uuid];
			if (startedOpUUIDs.count >= maxConcurrentOperationCount) {
				break;
			}
			
			nextOp = dequeueNextOperation();
			
		} while (nextOp);
		
		[self _checkForActiveStatusChange]; // may have transitioned from inactive to active
	}
}

- (void)postQueueChangedNotification
{
	dispatch_block_t block = ^{
		
		[[NSNotificationCenter defaultCenter] postNotificationName:YDBCloudCorePipelineQueueChangedNotification
		                                                    object:self];
	};
	
	if ([NSThread isMainThread])
		block();
	else
		dispatch_async(dispatch_get_main_queue(), block);
}

@end
