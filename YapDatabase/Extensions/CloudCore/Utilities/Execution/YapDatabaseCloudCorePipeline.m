/**
 * Copyright Deusty LLC.
**/

#import "YapDatabaseCloudCorePipeline.h"
#import "YapDatabaseCloudCorePrivate.h"

#import "YapDatabaseAtomic.h"
#import "YapDatabaseLogging.h"

#import <stdatomic.h>

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
**/
#if DEBUG && robbie_hanson
  static const int ydbLogLevel = YDBLogLevelVerbose; // | YDBLogFlagTrace;
#elif DEBUG
  static const int ydbLogLevel = YDBLogLevelInfo;
#else
  static const int ydbLogLevel = YDBLogLevelWarning;
#endif
#pragma unused(ydbLogLevel)

NSString *const YDBCloudCorePipelineQueueChangedNotification = @"YDBCloudCorePipelineQueueChangedNotification";
NSString *const YDBCloudCorePipelineQueueChangedKey_addedOperationUUIDs    = @"added";
NSString *const YDBCloudCorePipelineQueueChangedKey_modifiedOperationUUIDs = @"modified";
NSString *const YDBCloudCorePipelineQueueChangedKey_insertedOperationUUIDs = @"inserted";
NSString *const YDBCloudCorePipelineQueueChangedKey_removedOperationUUIDs  = @"removed";

NSString *const YDBCloudCorePipelineSuspendCountChangedNotification =
              @"YDBCloudCorePipelineSuspendCountChangedNotification";

NSString *const YDBCloudCorePipelineActiveStatusChangedNotification =
              @"YDBCloudCorePipelineActiveStatusChangedNotification";

NSString *const YDBCloudCore_EphemeralKey_Status   = @"status";
NSString *const YDBCloudCore_EphemeralKey_Hold     = @"hold";


@implementation YapDatabaseCloudCorePipeline
{
	id ephemeralInfoSharedKeySet;
	
	dispatch_queue_t queue;
	void *IsOnQueueKey;
	
	YAPUnfairLock spinLock;
	
	//
	// These variables must only be accessed/modified within queue:
	//
	
	NSMutableDictionary<NSUUID *, NSMutableDictionary *> *ephemeralInfo;
	NSMutableArray<YapDatabaseCloudCoreGraph *> *graphs;
	NSMutableSet<NSUUID *> *startedOpUUIDs;
	
	dispatch_source_t holdTimer;
	BOOL holdTimerSuspended;
	
	__weak YapDatabaseCloudCore *_atomic_setOnce_owner;
	
	BOOL isActive;
	
	//
	// These variables must only be accessed/modified from within spinLock
	//
	
	NSUInteger suspendCount;
	NSUInteger _atomic_maxConcurrentOperationCount;
	
	//
	// These variable must only be accessed/modified via atomic_x():
	//
	
	atomic_flag needsStartNextOperationFlag;
}

@synthesize name = name;
@synthesize algorithm = algorithm;
@synthesize delegate = delegate;
@dynamic owner;

@synthesize previousNames = previousNames;
@dynamic maxConcurrentOperationCount;

@dynamic isSuspended;
@dynamic suspendCount;
@dynamic isActive;

@synthesize rowid = rowid;

- (instancetype)init
{
	// Empty init not allowed
	return nil;
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
		
		spinLock = YAP_UNFAIR_LOCK_INIT;
		
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
		
		_atomic_maxConcurrentOperationCount = 8;
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
		
		owner = _atomic_setOnce_owner;
		
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
		
		if (!_atomic_setOnce_owner && inOwner)
		{
			_atomic_setOnce_owner = inOwner;
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
#pragma mark Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSUInteger)maxConcurrentOperationCount
{
	NSUInteger result = 0;
	
	YAPUnfairLockLock(&spinLock);
	{
		result = _atomic_maxConcurrentOperationCount;
	}
	YAPUnfairLockUnlock(&spinLock);
	
	return result;
}

- (void)setMaxConcurrentOperationCount:(NSUInteger)value
{
	BOOL changed = NO;
	
	YAPUnfairLockLock(&spinLock);
	{
		if (_atomic_maxConcurrentOperationCount != value) {
			_atomic_maxConcurrentOperationCount = value;
			changed = YES;
		}
	}
	YAPUnfairLockUnlock(&spinLock);
	
	if (changed)
	{
		[self queueStartNextOperationIfPossible];
	}
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
 * Searches for a list of operations.
 *
 * @return A dictionary with all the found operations.
 *         Operations which were not found won't be present in the returned dictionary.
**/
- (NSDictionary<NSUUID*, YapDatabaseCloudCoreOperation*> *)operationsWithUUIDs:(NSArray<NSUUID*> *)uuids
{
	if (uuids.count == 0) return [NSDictionary dictionary];
	
	NSSet<NSUUID*> *uuids_set = [NSSet setWithArray:uuids];
	
	NSMutableDictionary<NSUUID*, YapDatabaseCloudCoreOperation*> *results =
		[NSMutableDictionary dictionaryWithCapacity:uuids.count];
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
	
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			for (YapDatabaseCloudCoreOperation *operation in graph.operations)
			{
				if ([uuids_set containsObject:operation.uuid])
				{
					results[operation.uuid] = [operation copy];
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
			if (statusNum != nil)
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

- (void)enumerateOperationsUsingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation,
                                                           NSUInteger graphIdx, BOOL *stop))enumBlock
{
	[self _enumerateOperationsUsingBlock:^(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop) {
		
		enumBlock([operation copy], graphIdx, stop);
	}];
}

- (void)_enumerateOperationsUsingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation,
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
		
		if (graphIdx < graphs.count)
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
	__block NSUInteger graphIdx = 0;
	
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

- (NSDate *)earliestDate:(NSDictionary<NSString*, NSDate*> *)dates
{
	NSDate *earliestDate = nil;
	
	for (NSDate *date in [dates objectEnumerator])
	{
		if (earliestDate == nil) {
			earliestDate = date;
		}
		else {
			earliestDate = [earliestDate earlierDate:date];
		}
	}
	
	return earliestDate;
}

- (NSDate *)latestDate:(NSDictionary<NSString*, NSDate*> *)dates
{
	NSDate *latestDate = nil;
	
	for (NSDate *date in [dates objectEnumerator])
	{
		if (latestDate == nil) {
			latestDate = date;
		}
		else {
			latestDate = [latestDate laterDate:date];
		}
	}
	
	return latestDate;
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
	NSAssert(dispatch_get_specific(IsOnQueueKey), @"Must be executed within queue");
	
	BOOL found = NO;
	
	NSMutableDictionary *opInfo = ephemeralInfo[opUUID];
	if (opInfo)
	{
		found = YES;
		
		if (statusPtr)
		{
			NSNumber *statusNum = opInfo[YDBCloudCore_EphemeralKey_Status];
			if (statusNum != nil)
				*statusPtr = (YDBCloudCoreOperationStatus)[statusNum integerValue];
			else
				*statusPtr = YDBCloudOperationStatus_Pending;
		}
		if (isOnHoldPtr)
		{
			NSDate *latestHold = nil;
			
			NSDictionary<NSString*, NSDate*> *holdDict = opInfo[YDBCloudCore_EphemeralKey_Hold];
			if (holdDict) {
				latestHold = [self latestDate:holdDict];
			}
			
			if (latestHold)
				*isOnHoldPtr = ([latestHold timeIntervalSinceNow] > 0.0);
			else
				*isOnHoldPtr = NO;
		}
		
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
	__block NSNumber *status = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[opUUID];
		status = opInfo[YDBCloudCore_EphemeralKey_Status];
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Operation Hold
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Returns the current hold for the operation (with the given context), or nil if there is no hold.
 *
 * Different context's allow different parts of the system to operate in parallel.
 * For example, if an operation requires several different subsystems to each complete an action,
 * then each susbsystem can independently place a hold on the operation.
 * Once all holds are lifted, the pipeline can dispatch the operation again.
**/
- (NSDate *)holdDateForOperationWithUUID:(NSUUID *)opUUID context:(NSString *)inContext
{
	NSString *context = inContext ? [inContext copy] : @"";
	
	__block NSDate *date = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[opUUID];
		NSDictionary<NSString*, NSDate*> *holdDict = opInfo[YDBCloudCore_EphemeralKey_Hold];
		
		date = holdDict[context];
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return date;
}

/**
 * And operation can be put on "hold" until a specified date.
 *
 * There are multiple uses for this. For example:
 * - An operation may require various preparation tasks to complete before it can be started.
 * - A failed operation may use a holdDate in conjunction with retry logic, such as exponential backoff.
 *
 * The operation won't be started again until all associated holdDate's have expired.
 * You can pass a nil date to remove a hold on an operation (for a given context).
**/
- (void)setHoldDate:(NSDate *)date forOperationWithUUID:(NSUUID *)opUUID context:(NSString *)inContext
{
	if (opUUID == nil) return;
	NSString *context = inContext ? [inContext copy] : @"";
	
	__weak YapDatabaseCloudCorePipeline *weakSelf = self;
	
	dispatch_block_t block = ^{ @autoreleasepool {
		
		__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		NSMutableDictionary *opInfo = strongSelf->ephemeralInfo[opUUID];
		if (!opInfo && date)
		{
			opInfo = [NSMutableDictionary dictionaryWithSharedKeySet:strongSelf->ephemeralInfoSharedKeySet];
			strongSelf->ephemeralInfo[opUUID] = opInfo;
		}
		
		NSMutableDictionary<NSString*, NSDate*> *holds = opInfo[YDBCloudCore_EphemeralKey_Hold];
		if (!holds && date)
		{
			holds = [NSMutableDictionary dictionaryWithCapacity:1];
			opInfo[YDBCloudCore_EphemeralKey_Hold] = holds;
		}
		
		holds[context] = date;
		
		[strongSelf updateHoldTimer];
		[strongSelf startNextOperationIfPossible];
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_async(queue, block); // ASYNC
}

/**
 * Returns the latest hold date for the given operation.
 *
 * If there are no holdDates for the operation, returns nil.
 * If there are 1 or more holdDates, returns the latest date.
**/
- (NSDate *)latestHoldDateForOperationWithUUID:(NSUUID *)opUUID
{
	__block NSDate *latestDate = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[opUUID];
		NSDictionary<NSString*, NSDate*> *holdDict = opInfo[YDBCloudCore_EphemeralKey_Hold];
		
		if (holdDict) {
			latestDate = [self latestDate:holdDict];
		}
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return latestDate;
}

/**
 * Returns a dictionary of all the hold dates associated with an operation.
**/
- (NSDictionary<NSString*, NSDate*> *)holdDatesForOperationWithUUID:(NSUUID *)opUUID
{
	__block NSDictionary<NSString*, NSDate*> *holdDict = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableDictionary *opInfo = ephemeralInfo[opUUID];
		holdDict = [opInfo[YDBCloudCore_EphemeralKey_Hold] copy];
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return holdDict;
}

/**
 * Returns a dictionary of all the hold dates associated with a particular context.
**/
- (NSDictionary<NSUUID*, NSDate*> *)holdDatesForContext:(NSString *)inContext
{
	NSString *context = inContext ? [inContext copy] : @"";
	
	__block NSMutableDictionary<NSUUID*, NSDate*> *results = nil;
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		[ephemeralInfo enumerateKeysAndObjectsUsingBlock:^(NSUUID *opUUID, NSDictionary *opInfo, BOOL *stop) {
			
			NSDictionary<NSString*, NSDate*> *holdDict = opInfo[YDBCloudCore_EphemeralKey_Hold];
			NSDate *holdDate = holdDict[context];
			
			if (holdDate)
			{
				if (results == nil) {
					results = [NSMutableDictionary dictionary];
				}
				
				results[opUUID] = holdDate;
			}
		}];
		
	#pragma clang diagnostic pop
	}};
	
	if (dispatch_get_specific(IsOnQueueKey))
		block();
	else
		dispatch_sync(queue, block);
	
	return results;
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
		
		NSDictionary<NSString*, NSDate*> *holdDict = opInfo[YDBCloudCore_EphemeralKey_Hold];
		if (holdDict)
		{
			NSDate *earliestHold = [self earliestDate:holdDict];
			
			if (nextFireDate == nil)
				nextFireDate = earliestHold;
			else
				nextFireDate = [nextFireDate earlierDate:earliestHold];
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
	
	// Remove the stored hold date for any items in which: hold <= now
	
	NSDate *now = [NSDate date];
	__block NSMutableArray<NSUUID *> *uuidsToRemove = nil;
	
	[ephemeralInfo enumerateKeysAndObjectsUsingBlock:^(NSUUID *uuid, NSMutableDictionary *opInfo, BOOL *stop) {
		
		NSMutableDictionary<NSString*, NSDate*> *holdDict = opInfo[YDBCloudCore_EphemeralKey_Hold];
		if (holdDict)
		{
			__block NSMutableArray<NSString *> *contextsToRemove = nil;
			
			[holdDict enumerateKeysAndObjectsUsingBlock:^(NSString *context, NSDate *holdDate, BOOL *stop) {
				
				NSTimeInterval interval = [holdDate timeIntervalSinceDate:now];
				if (interval <= 0)
				{
					if (contextsToRemove == nil) {
						contextsToRemove = [[NSMutableArray alloc] initWithCapacity:4];
					}
					
					[contextsToRemove addObject:context];
				}
			}];
			
			if (contextsToRemove.count > 0)
			{
				[holdDict removeObjectsForKeys:contextsToRemove];
				
				if (holdDict.count == 0)
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
	
	YAPUnfairLockLock(&spinLock);
	{
		currentSuspendCount = suspendCount;
	}
	YAPUnfairLockUnlock(&spinLock);
	
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
	
	YAPUnfairLockLock(&spinLock);
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
	YAPUnfairLockUnlock(&spinLock);
	
	if (overflow) {
		YDBLogWarn(@"The suspendCount has reached NSUIntegerMax!");
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
	
	YAPUnfairLockLock(&spinLock);
	{
		if (suspendCount > 0)
			suspendCount--;
		else
			underflow = YES;
		
		newSuspendCount = suspendCount;
	}
	YAPUnfairLockUnlock(&spinLock);
	
	if (underflow) {
		YDBLogWarn(@"Attempting to resume with suspendCount already at zero.");
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
		
		if (strongSelf->algorithm == YDBCloudCorePipelineAlgorithm_FlatGraph)
		{
			// Graphs in FlatCommit mode are setup in a linked-list,
			// where each graph has a (weak) pointer to the previous graph.
			
			YapDatabaseCloudCoreGraph *prvGraph = nil;
			for (YapDatabaseCloudCoreGraph *graph in inGraphs)
			{
				graph.previousGraph = prvGraph;
				prvGraph = graph;
			}
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

- (void)processAddedGraph:(YapDatabaseCloudCoreGraph *)addedGraph
		 insertedOperations:(NSDictionary<NSNumber *, NSArray<YapDatabaseCloudCoreOperation *> *> *)insertedOperations
       modifiedOperations:(NSDictionary<NSUUID *, YapDatabaseCloudCoreOperation *> *)modifiedOperations
{
	YDBLogAutoTrace();
	
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSMutableSet<NSUUID*> *addedOpUUIDs    = nil;
		NSMutableSet<NSUUID*> *modifiedOpUUIDs = nil;
		NSMutableSet<NSUUID*> *insertedOpUUIDs = nil;
		NSMutableSet<NSUUID*> *removedOpUUIDs  = nil;
		
		if (addedGraph)
		{
			addedGraph.pipeline = self;
			if (algorithm == YDBCloudCorePipelineAlgorithm_FlatGraph) {
				addedGraph.previousGraph = [graphs lastObject];
			}
			
			[graphs addObject:addedGraph];
			
			addedOpUUIDs = [NSMutableSet setWithCapacity:addedGraph.operations.count];
			for (YapDatabaseCloudCoreOperation *operation in addedGraph.operations)
			{
				[operation clearTransactionVariables];
				[addedOpUUIDs addObject:operation.uuid];
			}
		}
		
		if ((insertedOperations.count > 0) || (modifiedOperations.count > 0))
		{
			// The modifiedOperations dictionary contains a list of every pre-existing operation
			// that was modified/replaced in the read-write transaction,
			// across EVERY PIPELINE.
			//
			// Thus, each operation may or may not belong to this pipeline.
			// So we need to identify the ones that do.
			
			NSMutableArray<YapDatabaseCloudCoreOperation *> *insertedOpsInThisPipeline = [NSMutableArray array];
			NSMutableArray<YapDatabaseCloudCoreOperation *> *modifiedOpsInThisPipeline = [NSMutableArray array];
			
			NSUInteger graphIdx = 0;
			for (YapDatabaseCloudCoreGraph *graph in graphs)
			{
				NSArray<YapDatabaseCloudCoreOperation *> *insertedInGraph = insertedOperations[@(graphIdx)];
				
				if (insertedInGraph)
				{
					[insertedOpsInThisPipeline addObjectsFromArray:insertedInGraph];
				}
				
				[graph insertOperations:insertedInGraph
				       modifyOperations:modifiedOperations
				               modified:modifiedOpsInThisPipeline];
				
				graphIdx++;
			}
			
			if (insertedOpsInThisPipeline.count > 0)
			{
				insertedOpUUIDs = [NSMutableSet setWithCapacity:insertedOpsInThisPipeline.count];
				
				for (YapDatabaseCloudCoreOperation *operation in insertedOpsInThisPipeline)
				{
					NSNumber *pendingStatus = operation.pendingStatus;
					if (pendingStatus != nil)
					{
						YDBCloudCoreOperationStatus status = (YDBCloudCoreOperationStatus)[pendingStatus integerValue];
						[self _setStatus:status forOperationUUID:operation.uuid];
					}
				
					[operation clearTransactionVariables];
					[insertedOpUUIDs addObject:operation.uuid];
				}
			}
			
			if (modifiedOpsInThisPipeline.count > 0)
			{
				modifiedOpUUIDs = [NSMutableSet setWithCapacity:modifiedOpsInThisPipeline.count];
				
				for (YapDatabaseCloudCoreOperation *operation in modifiedOpsInThisPipeline)
				{
					NSNumber *pendingStatus = operation.pendingStatus;
					if (pendingStatus != nil)
					{
						YDBCloudCoreOperationStatus status = (YDBCloudCoreOperationStatus)[pendingStatus integerValue];
						[self _setStatus:status forOperationUUID:operation.uuid];
					}
			
					[operation clearTransactionVariables];
					[modifiedOpUUIDs addObject:operation.uuid];
				}
			}
		}
		
		NSUInteger graphIdx = 0;
		while (graphIdx < graphs.count)
		{
			YapDatabaseCloudCoreGraph *graph = graphs[graphIdx];
			
			NSArray *removedOperations = [graph removeCompletedAndSkippedOperations];
			if (removedOperations.count > 0)
			{
				if (removedOpUUIDs == nil) {
					removedOpUUIDs = [NSMutableSet setWithCapacity:removedOperations.count];
				}
				
				for (YapDatabaseCloudCoreOperation *operation in removedOperations)
				{
					[startedOpUUIDs removeObject:operation.uuid];
					[ephemeralInfo removeObjectForKey:operation.uuid];
					
					[removedOpUUIDs addObject:operation.uuid];
					[modifiedOpUUIDs removeObject:operation.uuid];
				}
			}
			
			if (graph.operations.count == 0)
			{
				[graphs removeObjectAtIndex:graphIdx];
				
				if (algorithm == YDBCloudCorePipelineAlgorithm_FlatGraph)
				{
					// Careful: Graphs (in FlatCommit mode) are setup in a linked-list,
					// where each graph has a (weak) pointer to the previous graph.
					// So we need to fixup the link.
					if (graphIdx < graphs.count)
					{
						YapDatabaseCloudCoreGraph *nextGraph = graphs[graphIdx];
						if (graphIdx == 0) {
							nextGraph.previousGraph = nil;
						}
						else {
							nextGraph.previousGraph = graphs[graphIdx - 1];
						}
					}
				}
			}
			else
			{
				graphIdx++;
			}
		}
		
		if (addedOpUUIDs.count    > 0 ||
		    insertedOpUUIDs.count > 0 ||
		    modifiedOpUUIDs.count > 0 ||
		    removedOpUUIDs.count  > 0  )
		{
			// We may have transitioned from active to inactive.
			[self _checkForActiveStatusChange];
			
			// Although we could do this synchronously here (since we're inside the queue),
			// it may be better to perform this task async so we don't delay
			// the readWriteTransaction (which invoked this method).
			//
			[self queueStartNextOperationIfPossible];
			
			// Notify listeners that the operation list in the queue changed.
			[self postQueueChangedNotificationWithAdded: addedOpUUIDs
			                                   modified: modifiedOpUUIDs
			                                   inserted: insertedOpUUIDs
			                                    removed: removedOpUUIDs];
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
	BOOL flagWasAlreadySet = atomic_flag_test_and_set(&needsStartNextOperationFlag);
	if (!flagWasAlreadySet)
	{
		__weak YapDatabaseCloudCorePipeline *weakSelf = self;
		
		dispatch_async(queue, ^{ @autoreleasepool {
			
			__strong YapDatabaseCloudCorePipeline *strongSelf = weakSelf;
			if (strongSelf)
			{
				atomic_flag_clear(&strongSelf->needsStartNextOperationFlag);
				
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
	
	if (graphs.count == 0)
	{
		// Waiting for another graph to be added
		return;
	}
	
	if ([self isSuspended])
	{
		// Waiting to be resumed
		return;
	}
	
	NSUInteger maxConcurrentOperationCount = self.maxConcurrentOperationCount;
	if (maxConcurrentOperationCount == 0) {
		maxConcurrentOperationCount = NSUIntegerMax;
	}
	
	if (startedOpUUIDs.count >= maxConcurrentOperationCount)
	{
		// Waiting for one or more operations to complete
		return;
	}
	
	if (algorithm == YDBCloudCorePipelineAlgorithm_CommitGraph)
		[self _dequeueOperations_CommitGraph:maxConcurrentOperationCount];
	else
		[self _dequeueOperations_FlatGraph:maxConcurrentOperationCount];
}

- (void)_dequeueOperations_CommitGraph:(NSUInteger)maxConcurrentOperationCount
{
	YDBLogAutoTrace();
	NSAssert(dispatch_get_specific(IsOnQueueKey), @"Must be executed within queue");
	
	// Start as many operations as we can (from the current graph).
	
	YapDatabaseCloudCoreGraph *currentGraph = [graphs firstObject];
	
	YapDatabaseCloudCoreOperation *nextOp = [currentGraph nextReadyOperation:nil];
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
			
			nextOp = [currentGraph nextReadyOperation:nil];
			
		} while (nextOp);
		
		[self _checkForActiveStatusChange]; // may have transitioned from inactive to active
	}
}

- (void)_dequeueOperations_FlatGraph:(NSUInteger)maxConcurrentOperationCount
{
	YDBLogAutoTrace();
	NSAssert(dispatch_get_specific(IsOnQueueKey), @"Must be executed within queue");
	
	// Start as many operations as we can (across all graphs).
	//
	// Notes:
	//
	// In a FlatGraph configuration:
	// - Operations in commit C may have dependencies on other operations from commit C, or
	//   from earlier commits (such as B or A).
	// - We are allowed to start operations from any commit (so long as dependecies are fullfilled).
	// - If priorities are equal, we implicitly prefer operations that were queued earlier.
	//   i.e. operations from earlier graphs.
	
	NSMutableIndexSet *spentGraphs = [NSMutableIndexSet indexSet];
	
	YapDatabaseCloudCoreOperation* (^dequeueNextOperation)(void) = ^ YapDatabaseCloudCoreOperation*(){
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		YapDatabaseCloudCoreOperation *result = nil;
		
		NSUInteger graphIdx = 0;
		for (YapDatabaseCloudCoreGraph *graph in graphs)
		{
			if (![spentGraphs containsIndex:graphIdx])
			{
				YapDatabaseCloudCoreOperation *next = nil;
				if (result) {
					next = [graph nextReadyOperation:@(result.priority)];
				} else {
					next = [graph nextReadyOperation:nil];
				}
				
				if (next)
				{
					if ((result == nil) || (result.priority < next.priority))
					{
						result = next;
					}
				}
				else
				{
					[spentGraphs addIndex:graphIdx];
				}
			}
			
			graphIdx++;
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

- (void)postQueueChangedNotificationWithAdded:(NSSet<NSUUID*> *)added
                                     modified:(NSSet<NSUUID*> *)modified
                                     inserted:(NSSet<NSUUID*> *)inserted
                                      removed:(NSSet<NSUUID*> *)removed
{
	dispatch_block_t block = ^{
		
		NSMutableDictionary *userInfo = [NSMutableDictionary dictionaryWithCapacity:3];
		if (added.count > 0) {
			userInfo[YDBCloudCorePipelineQueueChangedKey_addedOperationUUIDs] = [added copy];
		}
		if (modified.count > 0) {
			userInfo[YDBCloudCorePipelineQueueChangedKey_modifiedOperationUUIDs] = [modified copy];
		}
		if (inserted.count > 0) {
			userInfo[YDBCloudCorePipelineQueueChangedKey_insertedOperationUUIDs] = [inserted copy];
		}
		if (removed.count > 0) {
			userInfo[YDBCloudCorePipelineQueueChangedKey_removedOperationUUIDs] = [removed copy];
		}
		
		[[NSNotificationCenter defaultCenter] postNotificationName: YDBCloudCorePipelineQueueChangedNotification
		                                                    object: self
		                                                  userInfo: userInfo];
	};
	
	if ([NSThread isMainThread])
		block();
	else
		dispatch_async(dispatch_get_main_queue(), block);
}

@end
