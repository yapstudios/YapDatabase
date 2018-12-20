/**
 * Copyright Deusty LLC.
**/

#import "YapDatabaseCloudCoreOperation.h"
#import "YapDatabaseCloudCoreOperationPrivate.h"
#import "YapDatabaseCloudCorePipeline.h"
#import "YapDatabaseCloudCore.h"
#import "YapDatabaseLogging.h"

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
 **/
#if DEBUG
  static const int ydbLogLevel = YDB_LOG_LEVEL_VERBOSE | YDB_LOG_FLAG_TRACE;
#else
  static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif
#pragma unused(ydbLogLevel)

static int const kYapDatabaseCloudCoreOperation_CurrentVersion = 1;
#pragma unused(kYapDatabaseCloudCoreOperation_CurrentVersion)

static NSString *const k_version            = @"version_base"; // subclasses should use version_XXX
static NSString *const k_uuid               = @"uuid";
static NSString *const k_priority           = @"priority";
static NSString *const k_dependencies       = @"dependencies";
static NSString *const k_persistentUserInfo = @"persistentUserInfo";


NSString *const YDBCloudCoreOperationIsReadyToStartNotification = @"YDBCloudCoreOperationIsReadyToStart";


@implementation YapDatabaseCloudCoreOperation

// Private properties

@synthesize operationRowid = operationRowid;

@synthesize needsDeleteDatabaseRow = needsDeleteDatabaseRow;
@synthesize needsModifyDatabaseRow = needsModifyDatabaseRow;

@synthesize pendingStatus = pendingStatus;

@dynamic pendingStatusIsCompletedOrSkipped;
@dynamic pendingStatusIsCompleted;
@dynamic pendingStatusIsSkipped;

// Public properties

@synthesize uuid = uuid;
@synthesize snapshot = snapshot;
@synthesize pipeline = pipeline;
@synthesize priority = priority;
@synthesize dependencies = dependencies;
@synthesize persistentUserInfo = persistentUserInfo;

/**
 * Make sure all your subclasses call this method ([super init]).
**/
- (instancetype)init
{
	if ((self = [super init]))
	{
		uuid = [NSUUID UUID];
	}
	return self;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark NSCoding
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (instancetype)initWithCoder:(NSCoder *)decoder
{
	if ((self = [super init]))
	{
		int version = [decoder decodeIntForKey:k_version];
		
		// The pipeline property is NOT encoded.
		// It's stored via the pipelineID column automatically,
		// and is explicitly set when the operations are restored.
		
		uuid = [decoder decodeObjectForKey:k_uuid];
		priority = [decoder decodeInt32ForKey:k_priority];
		
		if (version == 0)
		{
			// In versions < 1, dependencies was stored as an array.
			
			NSArray *dependenciesArray = [decoder decodeObjectForKey:k_dependencies];
			if (dependenciesArray) {
				dependencies = [NSSet setWithArray:dependenciesArray];
			}
		}
		else
		{
			dependencies = [decoder decodeObjectForKey:k_dependencies];
		}
		
		persistentUserInfo = [decoder decodeObjectForKey:k_persistentUserInfo];
	}
	return self;
}

- (void)encodeWithCoder:(NSCoder *)coder
{
	if (kYapDatabaseCloudCoreOperation_CurrentVersion != 0) {
		[coder encodeInt:kYapDatabaseCloudCoreOperation_CurrentVersion forKey:k_version];
	}
	
	// Notes about persistence:
	//
	// The pipeline property is NOT encoded.
	// It's stored via the `pipelineID` column automatically,
	// and is explicitly set when the operations are restored.
	//
	// The `snapshot` property is NOT encoded.
	// It's stored via the `graphID` column automatically,
	// and is explicitly set when the operations are restored.
	
	[coder encodeObject:uuid forKey:k_uuid];
	[coder encodeInt32:priority forKey:k_priority];
	[coder encodeObject:dependencies forKey:k_dependencies];
	
	[coder encodeObject:persistentUserInfo forKey:k_persistentUserInfo];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark NSCopying
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (instancetype)copyWithZone:(NSZone *)zone
{
	YapDatabaseCloudCoreOperation *copy = [[[self class] alloc] init];
	
	copy->uuid = uuid;
	copy->snapshot = snapshot;
	copy->pipeline = pipeline;
	copy->dependencies = dependencies;
	copy->priority = priority;
	copy->persistentUserInfo = persistentUserInfo;
	
	copy->operationRowid = operationRowid;
	copy->needsDeleteDatabaseRow = needsDeleteDatabaseRow;
	copy->needsModifyDatabaseRow = needsModifyDatabaseRow;
	copy->pendingStatus = pendingStatus;
	
	return copy;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Public API
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)setDependencies:(NSSet<NSUUID *> *)inDependencies
{
	NSMutableSet<NSUUID *> *newDependencies = [NSMutableSet setWithCapacity:inDependencies.count];
	for (id dependency in inDependencies)
	{
		NSUUID *dependencyUUID = nil;
		
		if ([dependency isKindOfClass:[NSUUID class]])
		{
			dependencyUUID = (NSUUID *)dependency;
		}
		else if ([dependency isKindOfClass:[YapDatabaseCloudCoreOperation class]])
		{
			dependencyUUID = [(YapDatabaseCloudCoreOperation *)dependency uuid];
		}
		
		if ([dependencyUUID isEqual:uuid]) {
			// Ignore - op cannot depend on itself
			continue;
		}
		
		if (dependencyUUID)
		{
			[newDependencies addObject:dependencyUUID];
		}
		else
		{
		#ifndef NS_BLOCK_ASSERTIONS
			NSAssert(NO, @"Bad dependecy object !");
		#endif
		}
	}
	
	NSString *const propKey = NSStringFromSelector(@selector(dependencies));
	
	[self willChangeValueForKey:propKey];
	{
		dependencies = [newDependencies copy];
	}
	[self didChangeValueForKey:propKey];
}

- (void)addDependency:(id)dependency
{
	if (dependency == nil) return;
		
	NSUUID *dependencyUUID = nil;
	
	if ([dependency isKindOfClass:[NSUUID class]])
	{
		dependencyUUID = (NSUUID *)dependency;
	}
	else if ([dependency isKindOfClass:[YapDatabaseCloudCoreOperation class]])
	{
		dependencyUUID = [(YapDatabaseCloudCoreOperation *)dependency uuid];
	}
	
	if ([dependencyUUID isEqual:uuid]) {
		// Ignore - op cannot depend on itself
		return;
	}
	
#ifndef NS_BLOCK_ASSERTIONS
	NSAssert(dependencyUUID != nil, @"Bad dependecy object !");
#else
	if (dependencyUUID == nil) {
		// Ignore - non-supported dependency type
		return;
	}
#endif
	
	NSString *const propKey = NSStringFromSelector(@selector(dependencies));
	
	[self willChangeValueForKey:propKey];
	{
		if (dependencies == nil)
			dependencies = [NSSet setWithObject:dependencyUUID];
		else
			dependencies = [dependencies setByAddingObject:dependencyUUID];
	}
	[self didChangeValueForKey:propKey];
}

/**
 * Convenience method for modifying the persistentUserInfo dictionary.
**/
- (void)setPersistentUserInfoObject:(id)userInfoObject forKey:(NSString *)userInfoKey
{
	if (userInfoKey == nil) return;
	
	NSString *const propKey = NSStringFromSelector(@selector(persistentUserInfo));
	
	[self willChangeValueForKey:propKey];
	{
		if (persistentUserInfo == nil)
		{
			if (userInfoObject)
				persistentUserInfo = @{ userInfoKey : userInfoObject };
		}
		else
		{
			NSMutableDictionary *newPersistentUserInfo = [persistentUserInfo mutableCopy];
			
			if (userInfoObject)
				[newPersistentUserInfo setObject:userInfoObject forKey:userInfoKey];
			else
				[newPersistentUserInfo removeObjectForKey:userInfoKey];
			
			persistentUserInfo = [newPersistentUserInfo copy];
		}
	}
	[self didChangeValueForKey:propKey];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Protected API
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (BOOL)pendingStatusIsCompletedOrSkipped
{
	if (pendingStatus != nil)
	{
		YDBCloudCoreOperationStatus status = (YDBCloudCoreOperationStatus)[pendingStatus integerValue];
		
		return (status == YDBCloudOperationStatus_Completed || status == YDBCloudOperationStatus_Skipped);
	}
	else
	{
		return NO;
	}
}

- (BOOL)pendingStatusIsCompleted
{
	if (pendingStatus != nil)
		return ([pendingStatus integerValue] == YDBCloudOperationStatus_Completed);
	else
		return NO;
}

- (BOOL)pendingStatusIsSkipped
{
	if (pendingStatus != nil)
		return ([pendingStatus integerValue] == YDBCloudOperationStatus_Skipped);
	else
		return NO;
}

/**
 * Subclasses can override me if they add custom transaction specific variables.
**/
- (void)clearTransactionVariables
{
	needsDeleteDatabaseRow = NO;
	needsModifyDatabaseRow = NO;
	pendingStatus = nil;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Equality
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (BOOL)isEqual:(id)object
{
	if ([object isKindOfClass:[YapDatabaseCloudCoreOperation class]])
		return [self isEqualToOperation:(YapDatabaseCloudCoreOperation *)object];
	else
		return NO;
}

/**
 * Subclasses should override this method, and add their own comparisons.
**/
- (BOOL)isEqualToOperation:(YapDatabaseCloudCoreOperation *)op
{
	if (operationRowid != op->operationRowid) return NO;
	
	if (needsDeleteDatabaseRow != op->needsDeleteDatabaseRow) return NO;
	if (needsModifyDatabaseRow != op->needsModifyDatabaseRow) return NO;
	
	if (!YDB_IsEqualOrBothNil(pendingStatus, op->pendingStatus)) return NO;
	
	if (!YDB_IsEqualOrBothNil(uuid, op->uuid)) return NO;
	if (snapshot != op->snapshot) return NO;
	if (!YDB_IsEqualOrBothNil(pipeline, op->pipeline)) return NO;
	
	if (priority != op->priority) return NO;
	
	if (!YDB_IsEqualOrBothNil(dependencies, op->dependencies)) return NO;
	if (!YDB_IsEqualOrBothNil(persistentUserInfo, op->persistentUserInfo)) return NO;
	
	return YES;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark General
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSString *)debugDescription
{
	if (!pipeline || [pipeline isEqualToString:YapDatabaseCloudCoreDefaultPipelineName])
	{
		return [NSString stringWithFormat:
			@"<YapDatabaseCloudCoreOperation[%p]: uuid=\"%@\", priority=%d>",
			self, uuid, priority];
	}
	else
	{
		return [NSString stringWithFormat:
			@"<YapDatabaseCloudCoreOperation[%p]: pipeline=\"%@\" uuid=\"%@\", priority=%d>",
			self, pipeline, uuid, priority];
	}
}

@end
