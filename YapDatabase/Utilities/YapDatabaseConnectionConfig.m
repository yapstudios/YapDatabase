#import "YapDatabaseConnectionConfig.h"

static NSUInteger const DEFAULT_OBJECT_CACHE_LIMIT   = 250;
static NSUInteger const DEFAULT_METADATA_CACHE_LIMIT = 250;


@implementation YapDatabaseConnectionConfig

@synthesize objectCacheEnabled = objectCacheEnabled;
@synthesize objectCacheLimit = objectCacheLimit;

@synthesize metadataCacheEnabled = metadataCacheEnabled;
@synthesize metadataCacheLimit = metadataCacheLimit;

@synthesize objectPolicy = objectPolicy;
@synthesize metadataPolicy = metadataPolicy;

#if TARGET_OS_IOS || TARGET_OS_TV
@synthesize autoFlushMemoryFlags = autoFlushMemoryFlags;
#endif

- (id)init
{
	if ((self = [super init]))
	{
		objectCacheEnabled = YES;
		objectCacheLimit = DEFAULT_OBJECT_CACHE_LIMIT;
		
		metadataCacheEnabled = YES;
		metadataCacheLimit = DEFAULT_METADATA_CACHE_LIMIT;
		
		objectPolicy = YapDatabasePolicyContainment;
		metadataPolicy = YapDatabasePolicyContainment;
		
		#if TARGET_OS_IOS || TARGET_OS_TV
		autoFlushMemoryFlags = YapDatabaseConnectionFlushMemoryFlags_All;
		#endif
	}
	return self;
}

- (id)copyWithZone:(NSZone __unused *)zone
{
	YapDatabaseConnectionConfig *copy = [[[self class] alloc] init];
	
	copy->objectCacheEnabled = self.objectCacheEnabled;
	copy->objectCacheLimit = self.objectCacheLimit;
	
	copy->metadataCacheEnabled = self.metadataCacheEnabled;
	copy->metadataCacheLimit = self.metadataCacheLimit;
	
	copy->objectPolicy = self.objectPolicy;
	copy->metadataPolicy = self.metadataPolicy;
	
	#if TARGET_OS_IOS || TARGET_OS_TV
	copy->autoFlushMemoryFlags = self.autoFlushMemoryFlags;
	#endif
	
	return copy;
}

- (BOOL)validateObjectPolicy:(id *)ioValue error:(NSError * __autoreleasing *)outError
{
	const YapDatabasePolicy defaultPolicy = YapDatabasePolicyContainment;
	
	if (*ioValue == nil)
	{
		*ioValue = @(defaultPolicy);
	}
	else
	{
		YapDatabasePolicy policy = (YapDatabasePolicy)[*ioValue integerValue];
		switch (policy)
		{
			case YapDatabasePolicyContainment :
			case YapDatabasePolicyShare       :
			case YapDatabasePolicyCopy        : break;
			default                           : *ioValue = @(defaultPolicy);
		}
	}
	
	return YES;
}

- (BOOL)validateMetadataPolicy:(id *)ioValue error:(NSError * __autoreleasing *)outError
{
	const YapDatabasePolicy defaultPolicy = YapDatabasePolicyContainment;
	
	if (*ioValue == nil)
	{
		*ioValue = @(defaultPolicy);
	}
	else
	{
		YapDatabasePolicy policy = (YapDatabasePolicy)[*ioValue integerValue];
		switch (policy)
		{
			case YapDatabasePolicyContainment :
			case YapDatabasePolicyShare       :
			case YapDatabasePolicyCopy        : break;
			default                           : *ioValue = @(defaultPolicy);
		}
	}
	
	return YES;
}

@end
