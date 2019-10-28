#import <Foundation/Foundation.h>

#import "YapDatabaseConnection.h"

/**
 * Allows you to configure the default values for new connections.
 *
 * When you create a connection via `[YapDatabase newConnection]`, that new connection will inherit
 * its initial configuration via these connectionDefaults. Of course, the connection may then override
 * these default configuration values, and configure itself as needed.
 *
 * Changing the connectionDefault values only affects future connections that will be created.
 * It does not affect connections that have already been created.
 */
@interface YapDatabaseConnectionConfig : NSObject <NSCopying>

/**
 * If YES, then future connections will be created with their objectCache enabled.
 *
 * The default value is YES.
 */
@property (atomic, assign, readwrite) BOOL objectCacheEnabled;

/**
 * Allows you to configure the default size of the objectCache for future connections.
 * A value of **zero == unlimited**
 * 
 * The default value is 250.
 */
@property (atomic, assign, readwrite) NSUInteger objectCacheLimit;

/**
 * If YES, then future connections will be created with their metadataCache enabled.
 *
 * The default value is YES.
 */
@property (atomic, assign, readwrite) BOOL metadataCacheEnabled;

/**
 * Allows you to configure the default size of the metadataCache for future connections.
 * A value of **zero == unlimited**
 *
 * The default value is 250.
 */
@property (atomic, assign, readwrite) NSUInteger metadataCacheLimit;

#if TARGET_OS_IOS || TARGET_OS_TV

/**
 * Allows you to configure how the YapDatabaseConnection should flush memory,
 * when the OS broadcasts a low-memory warning.
 *
 * The default value is YapDatabaseConnectionFlushMemoryFlags_All
 */
@property (atomic, assign, readwrite) YapDatabaseConnectionFlushMemoryFlags autoFlushMemoryFlags;

#endif

@end
