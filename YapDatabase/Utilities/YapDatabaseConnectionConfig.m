#import "YapDatabaseConnectionConfig.h"

static NSUInteger const DEFAULT_OBJECT_CACHE_LIMIT   = 250;
static NSUInteger const DEFAULT_METADATA_CACHE_LIMIT = 250;


@implementation YapDatabaseConnectionConfig

@synthesize objectCacheEnabled = objectCacheEnabled;
@synthesize objectCacheLimit = objectCacheLimit;

@synthesize metadataCacheEnabled = metadataCacheEnabled;
@synthesize metadataCacheLimit = metadataCacheLimit;

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
	
	#if TARGET_OS_IOS || TARGET_OS_TV
	copy->autoFlushMemoryFlags = self.autoFlushMemoryFlags;
	#endif
	
	return copy;
}

@end
