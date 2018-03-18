#import <Foundation/Foundation.h>

#import "YapDatabaseConnection.h"

/**
 * When a connection is created via [database newConnection] is will be handed one of these objects.
 * Thus the connection will inherit its initial configuration via the defaults configured for the parent database.
 *
 * Of course, the connection may then override these default configuration values, and configure itself as needed.
 *
 * For more detailed documentation on these properties, see the YapDatabaseConnection header file.
 *
 * @see YapDatabaseConnection objectCacheEnabled
 * @see YapDatabaseConnection objectCacheLimit
 * 
 * @see YapDatabaseConnection metadataCacheEnabled
 * @see YapDatabaseConnection metadataCacheLimit
 * 
 * @see YapDatabaseConnection objectPolicy
 * @see YapDatabaseConnection metadataPolicy
 * 
 * @see YapDatabaseConnection autoFlushMemoryLevel
**/
@interface YapDatabaseConnectionConfig : NSObject <NSCopying>

@property (atomic, assign, readwrite) BOOL objectCacheEnabled;
@property (atomic, assign, readwrite) NSUInteger objectCacheLimit;

@property (atomic, assign, readwrite) BOOL metadataCacheEnabled;
@property (atomic, assign, readwrite) NSUInteger metadataCacheLimit;

@property (atomic, assign, readwrite) YapDatabasePolicy objectPolicy;
@property (atomic, assign, readwrite) YapDatabasePolicy metadataPolicy;

#if TARGET_OS_IOS || TARGET_OS_TV
@property (atomic, assign, readwrite) YapDatabaseConnectionFlushMemoryFlags autoFlushMemoryFlags;
#endif

@end
