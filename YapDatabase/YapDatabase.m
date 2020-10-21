#import "YapDatabase.h"
#import "YapDatabaseAtomic.h"
#import "YapDatabasePrivate.h"
#import "YapDatabaseExtensionPrivate.h"
#import "YapCollectionKey.h"
#import "YapDatabaseManager.h"
#import "YapDatabaseConnectionState.h"
#import "YapDatabaseLogging.h"
#import "YapDatabaseString.h"
#import "YapDatabaseCryptoUtils.h"

#ifdef SQLITE_HAS_CODEC
  #import <SQLCipher/sqlite3.h>
#else
  #import "sqlite3.h"
#endif

#import <mach/mach_time.h>
#import <os/log.h>
#import <stdatomic.h>

#if ! __has_feature(objc_arc)
#warning This file must be compiled with ARC. Use -fobjc-arc flag (or convert project to ARC).
#endif

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
**/
#if robbie_hanson
  static const int ydbLogLevel = YDBLogLevelInfo;
#elif DEBUG
  static const int ydbLogLevel = YDBLogLevelInfo;
#else
  static const int ydbLogLevel = YDBLogLevelWarning;
#endif
#pragma unused(ydbLogLevel)

/**
 * YapDatabaseClosedNotification & corresponding keys.
**/

NSString *const YapDatabaseClosedNotification = @"YapDatabaseClosedNotification";

NSString *const YapDatabaseUrlKey    = @"databaseURL";
NSString *const YapDatabaseUrlWalKey = @"databaseURL_wal";
NSString *const YapDatabaseUrlShmKey = @"databaseURL_shm";

/**
 * YapDatabaseModifiedNotification & corresponding keys.
**/

NSString *const YapDatabaseModifiedNotification = @"YapDatabaseModifiedNotification";
NSString *const YapDatabaseModifiedExternallyNotification = @"YapDatabaseModifiedExternallyNotification";

NSString *const YapDatabaseSnapshotKey   = @"snapshot";
NSString *const YapDatabaseConnectionKey = @"connection";
NSString *const YapDatabaseExtensionsKey = @"extensions";
NSString *const YapDatabaseCustomKey     = @"custom";

NSString *const YapDatabaseObjectChangesKey      = @"objectChanges";
NSString *const YapDatabaseMetadataChangesKey    = @"metadataChanges";
NSString *const YapDatabaseInsertedKeysKey       = @"insertedKeys";
NSString *const YapDatabaseRemovedKeysKey        = @"removedKeys";
NSString *const YapDatabaseRemovedCollectionsKey = @"removedCollections";
NSString *const YapDatabaseRemovedRowidsKey      = @"removedRowids";
NSString *const YapDatabaseAllKeysRemovedKey     = @"allKeysRemoved";
NSString *const YapDatabaseModifiedExternallyKey = @"modifiedExternally";

NSString *const YapDatabaseRegisteredExtensionsKey   = @"registeredExtensions";
NSString *const YapDatabaseRegisteredMemoryTablesKey = @"registeredMemoryTables";
NSString *const YapDatabaseExtensionsOrderKey        = @"extensionsOrder";
NSString *const YapDatabaseExtensionDependenciesKey  = @"extensionDependencies";
NSString *const YapDatabaseNotificationKey           = @"notification";

/**
 * ConnectionPool value dictionary keys.
**/

static NSString *const YDBConnectionPoolValueKey_db        = @"db";
static NSString *const YDBConnectionPoolValueKey_main_file = @"main_file";
static NSString *const YDBConnectionPoolValueKey_wal_file  = @"wal_file";

/**
 * The database version is stored (via pragma user_version) to sqlite.
 * It is used to represent the version of the userlying architecture of YapDatabase.
 * In the event of future changes to the sqlite underpinnings of YapDatabase,
 * the version can be consulted to allow for proper on-the-fly upgrades.
 * For more information, see the upgradeTable method.
**/
#define YAP_DATABASE_CURRENT_VERION 3

/**
 * Default values
**/
#define DEFAULT_MAX_CONNECTION_POOL_COUNT 5    // connections
#define DEFAULT_CONNECTION_POOL_LIFETIME  90.0 // seconds


static int connectionBusyHandler(void *ptr, int count) {
    YapDatabase* currentDatabase = (__bridge YapDatabase*)ptr;
    
    usleep(50*1000); // sleep 50ms
    
    if (count % 4 == 1) { // log every 4th attempt but not the first one
        YDBLogWarn(@"Cannot obtain busy lock on SQLite from database (%p), is another process locking the database? Retrying in 50ms...", currentDatabase);
    }
    
    return 1;
}

typedef void (^YDBLogHandler)(YDBLogMessage *);

static YDBLogHandler logHandler = nil;

@implementation YapDatabase {
@private
	
	YapDatabaseOptions *options;
	
	sqlite3 *db; // Used for setup & checkpoints
	
	NSMutableArray *changesets;
	uint64_t snapshot;
	
	dispatch_queue_t internalQueue;
	dispatch_queue_t checkpointQueue;
	
	YapDatabaseConnectionConfig *connectionDefaults;
	
	YAPUnfairLock configLock;
	
	NSMutableDictionary<id, YapDatabaseSerializer> *objectSerializers;         // only accessible within configLock
	NSMutableDictionary<id, YapDatabaseDeserializer> *objectDeserializers;     // only accessible within configLock
	
	NSMutableDictionary<id, YapDatabasePreSanitizer> *objectPreSanitizers;     // only accessible within configLock
	NSMutableDictionary<id, YapDatabasePostSanitizer> *objectPostSanitizers;   // only accessible within configLock
	
	NSMutableDictionary<id, YapDatabaseSerializer> *metadataSerializers;       // only accessible within configLock
	NSMutableDictionary<id, YapDatabaseDeserializer> *metadataDeserializers;   // only accessible within configLock
	
	NSMutableDictionary<id, YapDatabasePreSanitizer> *metadataPreSanitizers;   // only accessible within configLock
	NSMutableDictionary<id, YapDatabasePostSanitizer> *metadataPostSanitizers; // only accessible within configLock

  NSNumber *_defaultObjectPolicy; // only accessible within configLock
	NSDictionary<NSString*, NSNumber*> *objectPolicies;   // only accessible within configLock
  NSNumber *_defaultMetadataPolicy; // only accessible within configLock
	NSDictionary<NSString*, NSNumber*> *metadataPolicies; // only accessible within configLock
	
	NSDictionary *registeredExtensions;
	NSDictionary *registeredMemoryTables;
	
	NSArray *extensionsOrder;
	NSDictionary *extensionDependencies;
	
	YapDatabaseConnection *registrationConnection;
	
	NSUInteger maxConnectionPoolCount;
	NSTimeInterval connectionPoolLifetime;
	dispatch_source_t connectionPoolTimer;
	NSMutableArray *connectionPoolValues;
	NSMutableArray *connectionPoolDates;
	
	NSString *sqliteVersion;
	uint64_t pageSize;
	
	atomic_flag pendingPassiveCheckpoint;
	atomic_flag pendingAggressiveCheckpoint;
	atomic_bool aggressiveCheckpointEnabled;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (NSURL *)defaultDatabaseURL
{
	NSError *error = nil;
	NSURL *appSupportDir =
	  [[NSFileManager defaultManager] URLForDirectory: NSApplicationSupportDirectory
	                                         inDomain: NSUserDomainMask
	                                appropriateForURL: nil
	                                           create: YES
	                                            error: &error];
		
#if !TARGET_OS_IPHONE // macOS
	if (!error)
	{
		NSString *bundleIdentifier =
		  [[NSBundle mainBundle] objectForInfoDictionaryKey:(NSString *)kCFBundleIdentifierKey];
		
		appSupportDir = [appSupportDir URLByAppendingPathComponent:bundleIdentifier isDirectory:YES];
		
		[[NSFileManager defaultManager] createDirectoryAtURL: appSupportDir
		                         withIntermediateDirectories: YES
		                                          attributes: nil
		                                               error: &error];
	}
#endif
	
	return [appSupportDir URLByAppendingPathComponent:@"yapdb.sqlite" isDirectory:NO];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (YapDatabaseSerializer)defaultSerializer
{
	return ^ NSData* (NSString __unused *collection, NSString __unused *key, id object){
		return [NSKeyedArchiver archivedDataWithRootObject:object];
	};
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (YapDatabaseDeserializer)defaultDeserializer
{
	return ^ id (NSString __unused *collection, NSString __unused *key, NSData *data){
		return data && data.length > 0 ? [NSKeyedUnarchiver unarchiveObjectWithData:data] : nil;
	};
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (YapDatabaseSerializer)propertyListSerializer
{
	return ^ NSData* (NSString __unused *collection, NSString __unused *key, id object){
		return [NSPropertyListSerialization dataWithPropertyList: object
		                                                  format: NSPropertyListBinaryFormat_v1_0
		                                                 options: NSPropertyListImmutable
		                                                   error: NULL];
	};
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (YapDatabaseDeserializer)propertyListDeserializer
{
	return ^ id (NSString __unused *collection, NSString __unused *key, NSData *data){
		return [NSPropertyListSerialization propertyListWithData:data options:0 format:NULL error:NULL];
	};
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (YapDatabaseSerializer)timestampSerializer
{
	return ^ NSData* (NSString __unused *collection, NSString __unused *key, id object) {
		
		if ([object isKindOfClass:[NSDate class]])
		{
			NSTimeInterval timestamp = [(NSDate *)object timeIntervalSinceReferenceDate];
			
			return [[NSData alloc] initWithBytes:(void *)&timestamp length:sizeof(NSTimeInterval)];
		}
		else
		{
			return [NSKeyedArchiver archivedDataWithRootObject:object];
		}
	};
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (YapDatabaseDeserializer)timestampDeserializer
{
	return ^ id (NSString __unused *collection, NSString __unused *key, NSData *data) {
		
		if ([data length] == sizeof(NSTimeInterval))
		{
			NSTimeInterval timestamp;
			memcpy((void *)&timestamp, [data bytes], sizeof(NSTimeInterval));
			
			return [[NSDate alloc] initWithTimeIntervalSinceReferenceDate:timestamp];
		}
		else
		{
			return [NSKeyedUnarchiver unarchiveObjectWithData:data];
		}
	};
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Logging
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Used by the macros defined in YapDatabaseLogging.h
 */
+ (void)log:(YDBLogLevel)level
       flag:(YDBLogFlag)flag
       file:(const char *)file
   function:(const char *)function
       line:(NSUInteger)line
     format:(NSString *)format, ...
{
	va_list args;
	if (format)
	{
		va_start(args, format);
		NSString *message = [[NSString alloc] initWithFormat:format arguments:args];
      va_end(args);
		
		YDBLogMessage *logMessage =
		  [[YDBLogMessage alloc] initWithMessage: message
		                                   level: level
		                                    flag: flag
		                                    file: [NSString stringWithFormat:@"%s", file]
		                                function: [NSString stringWithFormat:@"%s", function]
		                                    line: line];
		
		logHandler(logMessage);
	}
}

+ (YDBLogHandler)defaultLogHandler
{
	NSString *subsystem = @"yapdb";
	NSString *category = @"yapdb";
	
	os_log_t logger = os_log_create([subsystem UTF8String], [category UTF8String]);
	
	YDBLogHandler handler = ^void (YDBLogMessage *log){ @autoreleasepool {
		
		if (log.flag & YDBLogFlagError) {
			os_log_error(logger, "%{public}@ %{public}@", log.function, log.message);
		}
		else if (log.flag & YDBLogFlagWarning) {
			os_log_info(logger, "%{public}@ %{public}@", log.function, log.message);
		}
	}};
	return handler;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
+ (void)setLogHandler:(void (^)(YDBLogMessage *))inLogHandler
{
	logHandler = inLogHandler ?: [self defaultLogHandler];
}

+ (void)initialize
{
	static BOOL initialized = NO;
	if (!initialized) {
		initialized = YES;
		logHandler = [self defaultLogHandler];
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Properties
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

@synthesize databaseURL = databaseURL;
@dynamic databaseURL_wal;
@dynamic databaseURL_shm;

@dynamic options;
@dynamic sqliteVersion;

- (NSURL *)databaseURL_wal
{
	NSString *path = [[databaseURL path] stringByAppendingString:@"-wal"];
	return [NSURL fileURLWithPath:path isDirectory:NO];
}

- (NSURL *)databaseURL_shm
{
	NSString *path = [[databaseURL path] stringByAppendingString:@"-shm"];
	return [NSURL fileURLWithPath:path isDirectory:NO];
}

- (YapDatabaseOptions *)options
{
	return [options copy];
}

- (NSString *)sqliteVersion
{
	__block NSString *result = nil;
	
	dispatch_sync(snapshotQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		result = sqliteVersion;
		
	#pragma clang diagnostic pop
	});
	
	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Init
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (instancetype)init
{
	return [self initWithURL:[[self class] defaultDatabaseURL] options:nil];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (id)initWithURL:(NSURL *)inURL
{
	return [self initWithURL:inURL options:nil];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (id)initWithURL:(NSURL *)inURL options:(nullable YapDatabaseOptions *)inOptions
{
	// Standardize the path.
	// This allows for fileReferenceURL's, and non-standard paths to be passed without issue.
	NSString *databasePath = [[[inURL filePathURL] path] stringByStandardizingPath];
	
	// Ensure there is only a single database instance per file.
	// However, clients may create as many connections as desired.
	if (![YapDatabaseManager registerDatabaseForPath:databasePath])
	{
		YDBLogError(@"Only a single database instance is allowed per file. "
		            @"For concurrency you create multiple connections from a single database instance.");
		return nil;
	}
	
	if ((self = [super init]))
	{
		databaseURL = [NSURL fileURLWithPath:databasePath isDirectory:NO];
		options = inOptions ? [inOptions copy] : [[YapDatabaseOptions alloc] init];
		
		__block BOOL isNewDatabaseFile = ![[NSFileManager defaultManager] fileExistsAtPath:databasePath];
		
		BOOL(^openConfigCreate)(void) = ^BOOL (void) { @autoreleasepool {
		#pragma clang diagnostic push
		#pragma clang diagnostic ignored "-Wimplicit-retain-self"
			
			BOOL result = YES;
			
			if (result) result = [self openDatabase];
		#ifdef SQLITE_HAS_CODEC
			if (result) result = [self configureEncryptionForDatabase:db];
		#endif
			if (result) result = [self configureDatabase:isNewDatabaseFile];
			if (result) result = [self createTables];
			
			if (!result && db)
			{
				sqlite3_close(db);
				db = NULL;
			}
			
			return result;
			
		#pragma clang diagnostic pop
		}};
		
		BOOL result = openConfigCreate();
		if (!result)
		{
			// There are a few reasons why the database might not open.
			// One possibility is if the database file has become corrupt.
			
			if (options.corruptAction == YapDatabaseCorruptAction_Fail)
			{
				// Fail - do not try to resolve
			}
			else if (options.corruptAction == YapDatabaseCorruptAction_Rename)
			{
				// Try to rename the corrupt database file.
				
				BOOL renamed = NO;
				BOOL failed = NO;
				
				NSString *newDatabasePath = nil;
				int i = 0;
				
				do
				{
					NSString *extension = [NSString stringWithFormat:@"%d.corrupt", i];
					newDatabasePath = [databasePath stringByAppendingPathExtension:extension];
					
					if ([[NSFileManager defaultManager] fileExistsAtPath:newDatabasePath])
					{
						i++;
					}
					else
					{
						NSError *error = nil;
						renamed = [[NSFileManager defaultManager] moveItemAtPath: databasePath
						                                                  toPath: newDatabasePath
						                                                   error: &error];
						if (!renamed)
						{
							failed = YES;
							YDBLogError(@"Error renaming corrupt database file: (%@ -> %@) %@",
							            [databasePath lastPathComponent], [newDatabasePath lastPathComponent], error);
						}
					}
					
				} while (i < INT_MAX && !renamed && !failed);
				
				if (renamed)
				{
					isNewDatabaseFile = YES;
					result = openConfigCreate();
					if (result) {
						YDBLogInfo(@"Database corruption resolved. Renamed corrupt file. (newDB=%@) (corruptDB=%@)",
						           [databasePath lastPathComponent], [newDatabasePath lastPathComponent]);
					}
					else {
						YDBLogError(@"Database corruption unresolved. (name=%@)", [databasePath lastPathComponent]);
					}
				}
				
			}
			else // if (options.corruptAction == YapDatabaseCorruptAction_Delete)
			{
				// Try to delete the corrupt database file.
				
				NSError *error = nil;
				BOOL deleted = [[NSFileManager defaultManager] removeItemAtPath:databasePath error:&error];
				
				if (deleted)
				{
					isNewDatabaseFile = YES;
					result = openConfigCreate();
					if (result) {
						YDBLogInfo(@"Database corruption resolved. Deleted corrupt file. (name=%@)",
						                                                          [databasePath lastPathComponent]);
					}
					else {
						YDBLogError(@"Database corruption unresolved. (name=%@)", [databasePath lastPathComponent]);
					}
				}
				else
				{
					YDBLogError(@"Error deleting corrupt database file: %@", error);
				}
			}
		}
		if (!result)
		{
			return nil;
		}
		
		// Configure VFS shim (for database connections).
		
		yap_vfs_shim_name = [NSString stringWithFormat:@"yap_vfs_shim_%@", [[NSUUID UUID] UUIDString]];
		yap_vfs_shim_register([yap_vfs_shim_name UTF8String], NULL, &yap_vfs_shim);
		
		// Initialize variables
		
		internalQueue   = dispatch_queue_create("YapDatabase-Internal", NULL);
		checkpointQueue = dispatch_queue_create("YapDatabase-Checkpoint", NULL);
		snapshotQueue   = dispatch_queue_create("YapDatabase-Snapshot", NULL);
		writeQueue      = dispatch_queue_create("YapDatabase-Write", NULL);
		
		changesets = [[NSMutableArray alloc] init];
		connectionStates = [[NSMutableArray alloc] init];
		
		connectionDefaults = [[YapDatabaseConnectionConfig alloc] init];
		
		configLock = YAP_UNFAIR_LOCK_INIT;
		
		objectSerializers = [[NSMutableDictionary alloc] init];
		objectDeserializers = [[NSMutableDictionary alloc] init];
		
		objectPreSanitizers = [[NSMutableDictionary alloc] init];
		objectPostSanitizers = [[NSMutableDictionary alloc] init];
		
		metadataSerializers = [[NSMutableDictionary alloc] init];
		metadataDeserializers = [[NSMutableDictionary alloc] init];
		
		metadataPreSanitizers = [[NSMutableDictionary alloc] init];
		metadataPostSanitizers = [[NSMutableDictionary alloc] init];
		
		id defaultKey = [NSNull null];
		YapDatabaseSerializer defaultSerializer = [[self class] defaultSerializer];
		YapDatabaseDeserializer defaultDeserializer = [[self class] defaultDeserializer];
		
		objectSerializers[defaultKey] = defaultSerializer;
		objectDeserializers[defaultKey] = defaultDeserializer;
		
		metadataSerializers[defaultKey] = defaultSerializer;
		metadataDeserializers[defaultKey] = defaultDeserializer;
		
		objectPolicies = [[NSDictionary alloc] init];
		metadataPolicies = [[NSDictionary alloc] init];
		
		registeredExtensions = [[NSDictionary alloc] init];
		registeredMemoryTables = [[NSDictionary alloc] init];
		
		extensionDependencies = [[NSDictionary alloc] init];
		extensionsOrder = [[NSArray alloc] init];
		
		maxConnectionPoolCount = DEFAULT_MAX_CONNECTION_POOL_COUNT;
		connectionPoolLifetime = DEFAULT_CONNECTION_POOL_LIFETIME;
		
		// Mark the queues so we can identify them.
		// There are several methods whose use is restricted to within a certain queue.
		
		IsOnSnapshotQueueKey = &IsOnSnapshotQueueKey;
		dispatch_queue_set_specific(snapshotQueue, IsOnSnapshotQueueKey, IsOnSnapshotQueueKey, NULL);
		
		IsOnWriteQueueKey = &IsOnWriteQueueKey;
		dispatch_queue_set_specific(writeQueue, IsOnWriteQueueKey, IsOnWriteQueueKey, NULL);
		
		// Complete database setup in the background
		
		dispatch_async(snapshotQueue, ^{ @autoreleasepool {
	
			[self upgradeTable];
			[self prepare];
		}});
	}
	return self;
}

- (void)dealloc
{
	YDBLogVerbose(@"Dealloc <%@ %p: databaseName=%@>", [self class], self, [databaseURL lastPathComponent]);
	
	NSMutableDictionary *userInfo = [NSMutableDictionary dictionaryWithCapacity:3];
	userInfo[YapDatabaseUrlKey]    = self.databaseURL;
	userInfo[YapDatabaseUrlWalKey] = self.databaseURL_wal;
	userInfo[YapDatabaseUrlShmKey] = self.databaseURL_shm;
	
	NSNotification *notification =
	  [NSNotification notificationWithName:YapDatabaseClosedNotification
	                                object:nil // Cannot retain self within dealloc method
	                              userInfo:userInfo];
	
	while ([connectionPoolValues count] > 0)
	{
		NSDictionary *value = [connectionPoolValues objectAtIndex:0];
		
		sqlite3 *aDb = (sqlite3 *)[[value objectForKey:YDBConnectionPoolValueKey_db] pointerValue];
		
		int status = sqlite3_close(aDb);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error in sqlite_close: %d %s", status, sqlite3_errmsg(aDb));
		}
		
		[connectionPoolValues removeObjectAtIndex:0];
		[connectionPoolDates removeObjectAtIndex:0];
	}
	
	if (connectionPoolTimer)
		dispatch_source_cancel(connectionPoolTimer);
	
	if (db) {
		sqlite3_close(db);
		db = NULL;
	}
	if (yap_vfs_shim) {
		yap_vfs_shim_unregister(&yap_vfs_shim);
	}
	
	[YapDatabaseManager deregisterDatabaseForPath:[databaseURL path]];
	
#if !OS_OBJECT_USE_OBJC
	if (internalQueue)
		dispatch_release(internalQueue);
	if (snapshotQueue)
		dispatch_release(snapshotQueue);
	if (writeQueue)
		dispatch_release(writeQueue);
	if (checkpointQueue)
		dispatch_release(checkpointQueue);
#endif
	
	dispatch_async(dispatch_get_main_queue(), ^{
		
		[[NSNotificationCenter defaultCenter] postNotification:notification];
	});
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Setup
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Attempts to open (or create & open) the database connection.
**/
- (BOOL)openDatabase
{
	// Open the database connection.
	//
	// We use SQLITE_OPEN_NOMUTEX to use the multi-thread threading mode,
	// as we will be serializing access to the connection externally.
	
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE;
    
	int status = sqlite3_open_v2([[databaseURL path] UTF8String], &db, flags, NULL);
	if (status != SQLITE_OK)
	{
		// There are a few reasons why the database might not open.
		// One possibility is if the database file has become corrupt.
		
		// Sometimes the open function returns a db to allow us to query it for the error message.
		// The openConfigCreate block will close it for us.
		if (db) {
			YDBLogError(@"Error opening database: %d %s", status, sqlite3_errmsg(db));
		}
		else {
			YDBLogError(@"Error opening database: %d", status);
		}
		
		return NO;
	}
    // Add a busy handler if we are in multiprocess mode
    if (options.enableMultiProcessSupport) {
        sqlite3_busy_handler(db, connectionBusyHandler, (__bridge void *)(self));
    }
	
	return YES;
}

/**
 * Configures the database connection.
 * This mainly means enabling WAL mode, and configuring the auto-checkpoint.
**/
- (BOOL)configureDatabase:(BOOL)isNewDatabaseFile
{
	int status;
	
	// Set mandatory pragmas
	
	if (isNewDatabaseFile && (options.pragmaPageSize > 0))
	{
		NSString *pragma_page_size =
		  [NSString stringWithFormat:@"PRAGMA page_size = %ld;", (long)options.pragmaPageSize];
		
		status = sqlite3_exec(db, [pragma_page_size UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error setting PRAGMA page_size: %d %s", status, sqlite3_errmsg(db));
		}
	}
	
	status = sqlite3_exec(db, "PRAGMA journal_mode = WAL;", NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error setting PRAGMA journal_mode: %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	if (isNewDatabaseFile)
	{
		status = sqlite3_exec(db, "PRAGMA auto_vacuum = FULL; VACUUM;", NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error setting PRAGMA auto_vacuum: %d %s", status, sqlite3_errmsg(db));
		}
	}
	
	// Set synchronous to normal for THIS sqlite instance.
	//
	// This does NOT affect normal connections.
	// That is, this does NOT affect YapDatabaseConnection instances.
	// The sqlite connections of normal YapDatabaseConnection instances will follow the set pragmaSynchronous value.
	//
	// The reason we hardcode normal for this sqlite instance is because
	// it's only used to write the initial snapshot value.
	// And this doesn't need to be durable, as it is initialized to zero everytime.
	//
	// (This sqlite db is also used to perform checkpoints.
	//  But a normal value won't affect these operations,
	//  as they will perform sync operations whether the connection is normal or full.)
	
	status = sqlite3_exec(db, "PRAGMA synchronous = NORMAL;", NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error setting PRAGMA synchronous: %d %s", status, sqlite3_errmsg(db));
		// This isn't critical, so we can continue.
	}
	
	// Set journal_size_imit.
	//
	// We only need to do set this pragma for THIS connection,
	// because it is the only connection that performs checkpoints.
	
	NSString *pragma_journal_size_limit =
	  [NSString stringWithFormat:@"PRAGMA journal_size_limit = %ld;", (long)options.pragmaJournalSizeLimit];
	
	status = sqlite3_exec(db, [pragma_journal_size_limit UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error setting PRAGMA journal_size_limit: %d %s", status, sqlite3_errmsg(db));
		// This isn't critical, so we can continue.
	}
	
	// Set mmap_size (if needed).
	//
	// This configures memory mapped I/O.
	
	if (options.pragmaMMapSize > 0)
	{
		NSString *pragma_mmap_size =
		  [NSString stringWithFormat:@"PRAGMA mmap_size = %ld;", (long)options.pragmaMMapSize];
		
		status = sqlite3_exec(db, [pragma_mmap_size UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error setting PRAGMA mmap_size: %d %s", status, sqlite3_errmsg(db));
			// This isn't critical, so we can continue.
		}
	}
	
	// Disable autocheckpointing.
	//
	// YapDatabase has its own optimized checkpointing algorithm built-in.
	// It knows the state of every active connection for the database,
	// so it can invoke the checkpoint methods at the precise time in which a checkpoint can be most effective.
	
	sqlite3_wal_autocheckpoint(db, 0);
	
	return YES;
}


#ifdef SQLITE_HAS_CODEC
/**
 * Configures database encryption via SQLCipher.
**/
- (BOOL)configureEncryptionForDatabase:(sqlite3 *)sqlite
{
    if (options.cipherUnencryptedHeaderLength > 0) {
        if (options.cipherKeySpecBlock)
        {
            // Do nothing.
        } else if (!(options.cipherKeyBlock && options.cipherSaltBlock)) {
            NSAssert(NO, @"If you're using YapDatabaseOptions.cipherUnencryptedHeaderLength, you need to set either cipherKeySpecBlock or both cipherKeyBlock and cipherSaltBlock.");
            return NO;
        }
    }        

    if (options.cipherKeyBlock ||
        options.cipherKeySpecBlock)
	{
        NSData *_Nullable keyData = nil;
        if (options.cipherKeySpecBlock)
        {
            if (options.cipherKeyBlock) {
                NSAssert(NO, @"If you're using YapDatabaseOptions.cipherKeySpecBlock, you don't need to set a cipherKeySpecBlock.");
                return NO;
            }
            if (options.cipherSaltBlock) {
                NSAssert(NO, @"If you're using YapDatabaseOptions.cipherKeySpecBlock, you don't need to set a cipherSaltBlock.");
                return NO;
            }

            NSData *_Nullable keySpecData = options.cipherKeySpecBlock();
            if (!keySpecData)
            {
                NSAssert(NO, @"YapDatabaseOptions.cipherKeySpecBlock cannot return nil!");
                return NO;
            }
            if (keySpecData.length != kSQLCipherKeySpecLength) {
                NSAssert(NO, @"YapDatabaseOptions.cipherKeySpecBlock returned a key spec of unexpected length: %zd.", keySpecData.length);
                return NO;
            }

            // Use a raw key spec, where the 96 hexadecimal digits are provided
            // (i.e. 64 hex for the 256 bit key, followed by 32 hex for the 128 bit salt)
            // using explicit BLOB syntax, e.g.:
            //
            // x'98483C6EB40B6C31A448C22A66DED3B5E5E8D5119CAC8327B655C8B5C483648101010101010101010101010101010101'
            NSString *keySpecString = [NSString stringWithFormat:@"x'%@'", [self hexadecimalStringForData:keySpecData]];
            keyData = [keySpecString dataUsingEncoding:NSUTF8StringEncoding];
        } else {
            keyData = options.cipherKeyBlock();
            if (!keyData)
            {
                NSAssert(NO, @"YapDatabaseOptions.cipherKeyBlock cannot return nil!");
                return NO;
            }
        }
        
        //Setting the PBKDF2 default iteration number (this will have effect next time database is opened)
        if (options.cipherDefaultkdfIterNumber > 0) {
            char *errorMsg;
            NSString *pragmaCommand = [NSString stringWithFormat:@"PRAGMA cipher_default_kdf_iter = %lu", (unsigned long)options.cipherDefaultkdfIterNumber];
            if (sqlite3_exec(sqlite, [pragmaCommand UTF8String], NULL, NULL, &errorMsg) != SQLITE_OK)
            {
                YDBLogError(@"failed to set database cipher_default_kdf_iter: %s", errorMsg);
                return NO;
            }
        }
        
        //Setting the PBKDF2 iteration number
        if (options.kdfIterNumber > 0) {
            char *errorMsg;
            NSString *pragmaCommand = [NSString stringWithFormat:@"PRAGMA kdf_iter = %lu", (unsigned long)options.kdfIterNumber];
            if (sqlite3_exec(sqlite, [pragmaCommand UTF8String], NULL, NULL, &errorMsg) != SQLITE_OK)
            {
                YDBLogError(@"failed to set database kdf_iter: %s", errorMsg);
                return NO;
            }
        }
        
        //Setting the encrypted database page size
        if (options.cipherPageSize > 0) {
            char *errorMsg;
            NSString *pragmaCommand = [NSString stringWithFormat:@"PRAGMA cipher_page_size = %lu", (unsigned long)options.cipherPageSize];
            if (sqlite3_exec(sqlite, [pragmaCommand UTF8String], NULL, NULL, &errorMsg) != SQLITE_OK)
            {
                YDBLogError(@"failed to set database cipher_page_size: %s", errorMsg);
                return NO;
            }
        }
        
        int status = sqlite3_key(sqlite, [keyData bytes], (int)[keyData length]);
        if (status != SQLITE_OK)
        {
            YDBLogError(@"Error setting SQLCipher key: %d %s", status, sqlite3_errmsg(sqlite));
            return NO;
        }
        
        if (options.cipherCompatability != YapDatabaseCipherCompatability_Default) {
            char *errorMsg;
            NSString *pragmaCommand = [NSString stringWithFormat:@"PRAGMA cipher_compatibility = %lu", (unsigned long)options.cipherCompatability];
            if (sqlite3_exec(sqlite, [pragmaCommand UTF8String], NULL, NULL, &errorMsg) != SQLITE_OK)
            {
                YDBLogError(@"failed to set database cipher_compatibility: %s", errorMsg);
                return NO;
            }
        }
        
        if (options.cipherUnencryptedHeaderLength > 0 &&
            (options.cipherKeySpecBlock ||
             options.cipherSaltBlock)) {
             
            if (options.cipherKeySpecBlock) {
                // YapDatabase using cipher key spec and unencrypted header.
            } else {
                // YapDatabase using cipher salt and unencrypted header.
                
                NSData *_Nullable saltData = options.cipherSaltBlock();
                
                if (saltData == nil)
                {
                    NSAssert(NO, @"YapDatabaseOptions.cipherSaltBlock cannot return nil!");
                    return NO;
                }
                if (saltData.length != kSQLCipherSaltLength) {
                    NSAssert(NO, @"YapDatabaseOptions.cipherSaltBlock returned a salt of unexpected length: %zd.", saltData.length);
                    return NO;
                }

                {
                    char *errorMsg;
                    // Example: PRAGMA cipher_salt = "x'01010101010101010101010101010101';";
                    NSString *pragmaSql = [NSString stringWithFormat:@"PRAGMA cipher_salt = \"x'%@'\";", [self hexadecimalStringForData:saltData]];
                    if (sqlite3_exec(sqlite, [pragmaSql UTF8String], NULL, NULL, &errorMsg) != SQLITE_OK)
                    {
                        YDBLogError(@"failed to set database cipher_default_kdf_iter: %s", errorMsg);
                        return NO;
                    }
                }
            }
            
            {
                // We use cipher_plaintext_header_size NOT cipher_default_plaintext_header_size,
                // since the _default_ pragma affects a static variable.
                NSString *pragmaSql =
                [NSString stringWithFormat:@"PRAGMA cipher_plaintext_header_size = %zd;", options.cipherUnencryptedHeaderLength];
                int status = sqlite3_exec(sqlite, [pragmaSql UTF8String], NULL, NULL, NULL);
                if (status != SQLITE_OK) {
                    YDBLogError(@"Error setting PRAGMA cipher_plaintext_header_size = %zd: status: %d, error: %s",
                                options.cipherUnencryptedHeaderLength,
                                status,
                                sqlite3_errmsg(sqlite));
                    return NO;
                }
            }
        } else {
            if (options.cipherUnencryptedHeaderLength > 0) {
                NSAssert(NO, @"YapDatabaseOptions.cipherUnencryptedHeaderLength should not be used without cipherKeySpecBlock or cipherSaltBlock!");
                return NO;
            }
            if (options.cipherKeySpecBlock) {
                NSAssert(NO, @"YapDatabaseOptions.cipherKeySpecBlock should not be used without setting cipherUnencryptedHeaderLength!");
                return NO;
            }
            if (options.cipherSaltBlock) {
                NSAssert(NO, @"YapDatabaseOptions.cipherSaltBlock should not be used without setting cipherUnencryptedHeaderLength!");
                return NO;
            }
        }
	}
	
	return YES;
}

- (NSString *)hexadecimalStringForData:(NSData *)data {
    /* Returns hexadecimal string of NSData. Empty string if data is empty. */
    const unsigned char *dataBuffer = (const unsigned char *)[data bytes];
    if (!dataBuffer) {
        return @"";
    }
        
    NSUInteger dataLength = [data length];
    NSMutableString *hexString = [NSMutableString stringWithCapacity:(dataLength * 2)];
    
    for (NSUInteger i = 0; i < dataLength; ++i) {
        [hexString appendFormat:@"%02x", dataBuffer[i]];
    }
    return [hexString copy];
}

#endif

/**
 * Creates the database tables we need:
 * 
 * - yap2      : stores snapshot and metadata for extensions
 * - database2 : stores collection/key/value/metadata rows
**/
- (BOOL)createTables
{
	int status;
	
	char *createYapTableStatement =
	    "CREATE TABLE IF NOT EXISTS \"yap2\""
	    " (\"extension\" CHAR NOT NULL, "
	    "  \"key\" CHAR NOT NULL, "
	    "  \"data\" BLOB, "
	    "  PRIMARY KEY (\"extension\", \"key\")"
	    " );";
	
	status = sqlite3_exec(db, createYapTableStatement, NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Failed creating 'yap2' table: %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	char *createDatabaseTableStatement =
	    "CREATE TABLE IF NOT EXISTS \"database2\""
	    " (\"rowid\" INTEGER PRIMARY KEY,"
	    "  \"collection\" CHAR NOT NULL,"
	    "  \"key\" CHAR NOT NULL,"
	    "  \"data\" BLOB,"
	    "  \"metadata\" BLOB"
	    " );";
	
	status = sqlite3_exec(db, createDatabaseTableStatement, NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Failed creating 'database2' table: %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	char *createIndexStatement =
	    "CREATE UNIQUE INDEX IF NOT EXISTS \"true_primary_key\" ON \"database2\" ( \"collection\", \"key\" );";
	
	status = sqlite3_exec(db, createIndexStatement, NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Failed creating index on 'database2' table: %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	return YES;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Utilities
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

+ (NSString *)sqliteVersionUsing:(sqlite3 *)aDb
{
	sqlite3_stmt *statement;
	
	int status = sqlite3_prepare_v2(aDb, "SELECT sqlite_version();", -1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement! %d %s", status, sqlite3_errmsg(aDb));
		return nil;
	}
	
	NSString *version = nil;
	
	status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		const unsigned char *text = sqlite3_column_text(statement, SQLITE_COLUMN_START);
		int textSize = sqlite3_column_bytes(statement, SQLITE_COLUMN_START);
		
		version = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
	}
	else
	{
		YDBLogError(@"Error executing statement! %d %s", status, sqlite3_errmsg(aDb));
	}
	
	sqlite3_finalize(statement);
	statement = NULL;
	
	return version;
}

+ (int64_t)pragma:(NSString *)pragmaSetting using:(sqlite3 *)aDb
{
	if (pragmaSetting == nil) return -1;
	
	sqlite3_stmt *statement;
	NSString *pragma = [NSString stringWithFormat:@"PRAGMA %@;", pragmaSetting];
	
	int status = sqlite3_prepare_v2(aDb, [pragma UTF8String], -1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement! %d %s", status, sqlite3_errmsg(aDb));
		return NO;
	}
	
	int64_t result = -1;
	
	status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		result = sqlite3_column_int64(statement, SQLITE_COLUMN_START);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing statement! %d %s", status, sqlite3_errmsg(aDb));
	}
	
	sqlite3_finalize(statement);
	statement = NULL;
	
	return result;
}

+ (NSString *)pragmaValueForSynchronous:(int64_t)synchronous
{
	switch(synchronous)
	{
		case 0 : return @"OFF";
		case 1 : return @"NORMAL";
		case 2 : return @"FULL";
		default: return @"UNKNOWN";
	}
}

+ (NSString *)pragmaValueForAutoVacuum:(int64_t)auto_vacuum
{
	switch(auto_vacuum)
	{
		case 0 : return @"NONE";
		case 1 : return @"FULL";
		case 2 : return @"INCREMENTAL";
		default: return @"UNKNOWN";
	}
}

/**
 * Returns whether or not the given table exists.
**/
+ (BOOL)tableExists:(NSString *)tableName using:(sqlite3 *)aDb
{
	if (tableName == nil) return NO;
	
	sqlite3_stmt *statement;
	char *stmt = "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?";
	
	int status = sqlite3_prepare_v2(aDb, stmt, (int)strlen(stmt)+1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement! %d %s", status, sqlite3_errmsg(aDb));
		return NO;
	}
	
	BOOL result = NO;
	
	sqlite3_bind_text(statement, SQLITE_BIND_START, [tableName UTF8String], -1, SQLITE_TRANSIENT);
	
	status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		int count = sqlite3_column_int(statement, SQLITE_COLUMN_START);
		
		result = (count > 0);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing statement! %d %s", status, sqlite3_errmsg(aDb));
	}
	
	sqlite3_finalize(statement);
	statement = NULL;
	
	return result;
}

+ (NSArray *)tableNamesUsing:(sqlite3 *)aDb
{
	sqlite3_stmt *statement;
	char *stmt = "SELECT name FROM sqlite_master WHERE type = 'table';";
	
	int status = sqlite3_prepare_v2(aDb, stmt, (int)strlen(stmt)+1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement! %d %s", status, sqlite3_errmsg(aDb));
		return nil;
	}
	
	NSMutableArray *tableNames = [NSMutableArray array];
	
	while ((status = sqlite3_step(statement)) == SQLITE_ROW)
	{
		const unsigned char *text = sqlite3_column_text(statement, SQLITE_COLUMN_START);
		int textSize = sqlite3_column_bytes(statement, SQLITE_COLUMN_START);
		
		NSString *tableName = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
		
		if (tableName) {
			[tableNames addObject:tableName];
		}
		
	}
	
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing statement! %d %s", status, sqlite3_errmsg(aDb));
	}
	
	sqlite3_finalize(statement);
	statement = NULL;
	
	return tableNames;
}

/**
 * Extracts and returns column names from the given table in the database.
**/
+ (NSArray *)columnNamesForTable:(NSString *)tableName using:(sqlite3 *)aDb
{
	if (tableName == nil) return nil;
	
	sqlite3_stmt *statement;
	NSString *pragma = [NSString stringWithFormat:@"PRAGMA table_info('%@');", tableName];
	
	int status = sqlite3_prepare_v2(aDb, [pragma UTF8String], -1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement! %d %s", status, sqlite3_errmsg(aDb));
		return nil;
	}
	
	NSMutableArray *tableColumnNames = [NSMutableArray array];
	
	while ((status = sqlite3_step(statement)) == SQLITE_ROW)
	{
		// cid|name|type|notnull|dflt|value|pk
		// 0  |1   |2   |3      |4   |5    |6
		
		const unsigned char *text = sqlite3_column_text(statement, 1);
		int textSize = sqlite3_column_bytes(statement, 1);
		
		NSString *columnName = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
		if (columnName)
		{
			[tableColumnNames addObject:columnName];
		}
	}
	
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing statement! %d %s", status, sqlite3_errmsg(aDb));
	}
	
	sqlite3_finalize(statement);
	statement = NULL;
	
	return tableColumnNames;
}

/**
 * Extracts and returns column names & affinity for the given table in the database.
 * The dictionary format is:
 *
 * key:(NSString *)columnName -> value:(NSString *)affinity
**/
+ (NSDictionary *)columnNamesAndAffinityForTable:(NSString *)tableName using:(sqlite3 *)aDb
{
	if (tableName == nil) return nil;
	
	sqlite3_stmt *statement;
	NSString *pragma = [NSString stringWithFormat:@"PRAGMA table_info('%@');", tableName];
	
	int status = sqlite3_prepare_v2(aDb, [pragma UTF8String], -1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement! %d %s", status, sqlite3_errmsg(aDb));
		return nil;
	}
	
	NSMutableDictionary *columns = [NSMutableDictionary dictionary];
	
	while ((status = sqlite3_step(statement)) == SQLITE_ROW)
	{
		// cid|name|type|notnull|dflt|value|pk
		// 0  |1   |2   |3      |4   |5    |6
		
		const unsigned char *_name = sqlite3_column_text(statement, 1);
		int _nameSize = sqlite3_column_bytes(statement, 1);
		
		const unsigned char *_type = sqlite3_column_text(statement, 2);
		int _typeSize = sqlite3_column_bytes(statement, 2);
		
		NSString *name     = [[NSString alloc] initWithBytes:_name length:_nameSize encoding:NSUTF8StringEncoding];
		NSString *affinity = [[NSString alloc] initWithBytes:_type length:_typeSize encoding:NSUTF8StringEncoding];
		
		if (name && affinity)
		{
			[columns setObject:affinity forKey:name];
		}
	}
	
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing statement! %d %s", status, sqlite3_errmsg(aDb));
	}
	
	sqlite3_finalize(statement);
	statement = NULL;
	
	return columns;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Upgrade
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Gets the version of the table.
 * This is used to perform the various upgrade paths.
**/
- (BOOL)get_user_version:(int *)user_version_ptr
{
	sqlite3_stmt *pragmaStatement;
	int status;
	int user_version;
	
	char *stmt = "PRAGMA user_version;";
	
	status = sqlite3_prepare_v2(db, stmt, (int)strlen(stmt)+1, &pragmaStatement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating pragma user_version statement! %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	status = sqlite3_step(pragmaStatement);
	if (status == SQLITE_ROW)
	{
		user_version = sqlite3_column_int(pragmaStatement, SQLITE_COLUMN_START);
	}
	else
	{
		YDBLogError(@"Error fetching user_version: %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	sqlite3_finalize(pragmaStatement);
	pragmaStatement = NULL;
	
	// If user_version is zero, then this is a new database
	
	if (user_version == 0)
	{
		user_version = YAP_DATABASE_CURRENT_VERION;
		[self set_user_version:user_version];
	}
	
	if (user_version_ptr)
		*user_version_ptr = user_version;
	return YES;
}

/**
 * Sets the version of the table.
 * The version is used to check and perform upgrade logic if needed.
**/
- (BOOL)set_user_version:(int)user_version
{
	NSString *query = [NSString stringWithFormat:@"PRAGMA user_version = %d;", user_version];
	
	int status = sqlite3_exec(db, [query UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error setting user_version: %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	return YES;
}

- (BOOL)upgradeTable_1_2
{
	// In version 1, we used a table named "yap" which had {key, data}.
	// In version 2, we use a table named "yap2" which has {extension, key, data}
	
	int status = sqlite3_exec(db, "DROP TABLE IF EXISTS \"yap\"", NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Failed dropping 'yap' table: %d %s", status, sqlite3_errmsg(db));
	}
	
	return YES;
}

/**
 * In version 3 (more commonly known as version 2.1),
 * we altered the tables to use INTEGER PRIMARY KEY's so we could pass rowid's to extensions.
 * 
 * This method migrates 'database' to 'database2'.
**/
- (BOOL)upgradeTable_2_3
{
	int status;
	
	char *stmt = "INSERT INTO \"database2\" (\"collection\", \"key\", \"data\", \"metadata\")"
	             " SELECT \"collection\", \"key\", \"data\", \"metadata\" FROM \"database\";";
	
	status = sqlite3_exec(db, stmt, NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error migrating 'database' to 'database2': %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	status = sqlite3_exec(db, "DROP TABLE IF EXISTS \"database\"", NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Failed dropping 'database' table: %d %s", status, sqlite3_errmsg(db));
		return NO;
	}
	
	return YES;
}

/**
 * Performs upgrade checks, and implements the upgrade "plumbing" by invoking the appropriate upgrade methods.
 * 
 * To add custom upgrade logic, implement a method named "upgradeTable_X_Y",
 * where X is the previous version, and Y is the new version.
 * For example:
 * 
 * - (BOOL)upgradeTable_1_2 {
 *     // Upgrades from version 1 to version 2 of YapDatabase.
 *     // Return YES if successful.
 * }
 * 
 * IMPORTANT:
 * This is for upgrades of the database schema, and low-level operations of YapDatabase.
 * This is NOT for upgrading data within the database (i.e. objects, metadata, or keys).
 * Such data upgrades should be performed client side.
 *
 * This method is run asynchronously on the queue.
**/
- (void)upgradeTable
{
	int user_version = 0;
	if (![self get_user_version:&user_version]) return;
	
	while (user_version < YAP_DATABASE_CURRENT_VERION)
	{
		// Invoke method upgradeTable_X_Y
		// where X == current_version, and Y == current_version+1.
		//
		// Do this until we're up-to-date.
		
		int new_user_version = user_version + 1;
		
		NSString *selName = [NSString stringWithFormat:@"upgradeTable_%d_%d", user_version, new_user_version];
		SEL sel = NSSelectorFromString(selName);
		
		if ([self respondsToSelector:sel])
		{
			YDBLogInfo(@"Upgrading database (%@) from version %d to %d...",
			          [databaseURL lastPathComponent], user_version, new_user_version);
			
			#pragma clang diagnostic push
			#pragma clang diagnostic ignored "-Warc-performSelector-leaks"
			if ([self performSelector:sel])
			#pragma clang diagnostic pop
			{
				[self set_user_version:new_user_version];
			}
			else
			{
				YDBLogError(@"Error upgrading database (%@)", [databaseURL lastPathComponent]);
				break;
			}
		}
		else
		{
			YDBLogWarn(@"Missing upgrade method: %@", selName);
		}
		
		user_version = new_user_version;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Prepare
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method is run asynchronously on the snapshotQueue.
**/
- (void)prepare
{
	// Write it to disk (replacing any previous value from last app run)
	
	[self beginTransaction];
	{
		snapshot = [self readSnapshot];
        
		sqliteVersion = [YapDatabase sqliteVersionUsing:db];
		YDBLogVerbose(@"sqlite version = %@", sqliteVersion);
		
		pageSize = (uint64_t)[YapDatabase pragma:@"page_size" using:db];
		
		[self fetchPreviouslyRegisteredExtensionNames];
	}
	[self commitTransaction];
	[self asyncCheckpoint:snapshot];
}

- (void)beginTransaction
{
	int status = status = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error in 'BEGIN TRANSACTION' %d %s", status, sqlite3_errmsg(db));
	}
}

- (void)commitTransaction
{
	int status = status = sqlite3_exec(db, "COMMIT TRANSACTION;", NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error in 'COMMIT TRANSACTION': %d %s", status, sqlite3_errmsg(db));
	}
}

- (uint64_t)readSnapshot
{
	int status;
	sqlite3_stmt *statement;
	
	const char *stmt = "SELECT \"data\" FROM \"yap2\" WHERE \"extension\" = ? AND \"key\" = ?;";
	
	int const column_idx_data    = SQLITE_COLUMN_START;
	int const bind_idx_extension = SQLITE_BIND_START + 0;
	int const bind_idx_key       = SQLITE_BIND_START + 1;
	
	uint64_t result = 0;
	
	status = sqlite3_prepare_v2(db, stmt, (int)strlen(stmt)+1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement: %d %s", status, sqlite3_errmsg(db));
	}
	else
	{
		const char *extension = "";
		sqlite3_bind_text(statement, bind_idx_extension, extension, (int)strlen(extension), SQLITE_STATIC);
		
		const char *key = "snapshot";
		sqlite3_bind_text(statement, bind_idx_key, key, (int)strlen(key), SQLITE_STATIC);
		
		status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			result = (uint64_t)sqlite3_column_int64(statement, column_idx_data);
		}
		else if (status == SQLITE_ERROR)
		{
			YDBLogError(@"Error executing 'readSnapshot': %d %s", status, sqlite3_errmsg(db));
		}
		
		sqlite3_finalize(statement);
	}
	
	return result;
}

- (void)fetchPreviouslyRegisteredExtensionNames
{
	int status;
	sqlite3_stmt *statement;
	
	char *stmt = "SELECT DISTINCT \"extension\" FROM \"yap2\";";
	
	NSMutableArray *extensionNames = [NSMutableArray array];
	
	status = sqlite3_prepare_v2(db, stmt, (int)strlen(stmt)+1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating statement: %d %s", status, sqlite3_errmsg(db));
	}
	else
	{
		while ((status = sqlite3_step(statement)) == SQLITE_ROW)
		{
			const unsigned char *text = sqlite3_column_text(statement, SQLITE_COLUMN_START);
			int textSize = sqlite3_column_bytes(statement, SQLITE_COLUMN_START);
			
			NSString *extensionName =
			    [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			if ([extensionName length] > 0)
			{
				[extensionNames addObject:extensionName];
			}
		}
		
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"Error in statement: %d %s", status, sqlite3_errmsg(db));
		}
		
		sqlite3_finalize(statement);
	}
	
	previouslyRegisteredExtensionNames = extensionNames;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Default Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (YapDatabaseConnectionConfig *)connectionDefaults
{
	return connectionDefaults;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerDefaultSerializer:(YapDatabaseSerializer)serializer
{
	YAPUnfairLockLock(&configLock);
	{
		id key = [NSNull null];
		objectSerializers[key] = [serializer copy];
		metadataSerializers[key] = [serializer copy];
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerDefaultDeserializer:(YapDatabaseDeserializer)deserializer
{
	YAPUnfairLockLock(&configLock);
	{
		id key = [NSNull null];
		objectDeserializers[key] = [deserializer copy];
		metadataDeserializers[key] = [deserializer copy];
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerDefaultPreSanitizer:(nullable YapDatabasePreSanitizer)preSanitizer
{
	YAPUnfairLockLock(&configLock);
	{
		id key = [NSNull null];
		objectPreSanitizers[key] = [preSanitizer copy];
		metadataPreSanitizers[key] = [preSanitizer copy];
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerDefaultPostSanitizer:(nullable YapDatabasePostSanitizer)postSanitizer
{
	YAPUnfairLockLock(&configLock);
	{
		id key = [NSNull null];
		objectPostSanitizers[key] = [postSanitizer copy];
		metadataPostSanitizers[key] = [postSanitizer copy];
	}
	YAPUnfairLockUnlock(&configLock);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Per-Collection Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerSerializer:(YapDatabaseSerializer)serializer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [serializer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectSerializers[key] = value;
		metadataSerializers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerDeserializer:(YapDatabaseDeserializer)deserializer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [deserializer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectDeserializers[key] = value;
		metadataDeserializers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerPreSanitizer:(YapDatabasePreSanitizer)preSanitizer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [preSanitizer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectPreSanitizers[key] = value;
		metadataPreSanitizers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerPostSanitizer:(YapDatabasePostSanitizer)postSanitizer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [postSanitizer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectPostSanitizers[key] = value;
		metadataPostSanitizers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerSerializer:(nullable YapDatabaseSerializer)serializer
              deserializer:(nullable YapDatabaseDeserializer)deserializer
              preSanitizer:(nullable YapDatabasePreSanitizer)preSanitizer
             postSanitizer:(nullable YapDatabasePostSanitizer)postSanitizer
            forCollections:(NSArray<NSString*> *)collections
{
	YAPUnfairLockLock(&configLock);
	{
		for (NSString *collection in collections)
		{
			objectSerializers[collection] = [serializer copy];
			metadataSerializers[collection] = [serializer copy];
			
			objectDeserializers[collection] = [deserializer copy];
			metadataDeserializers[collection] = [deserializer copy];
			
			objectPreSanitizers[collection] = [preSanitizer copy];
			metadataPreSanitizers[collection] = [preSanitizer copy];
			
			objectPostSanitizers[collection] = [postSanitizer copy];
			metadataPostSanitizers[collection] = [postSanitizer copy];
		}
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerObjectSerializer:(YapDatabaseSerializer)serializer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [serializer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectSerializers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerObjectDeserializer:(YapDatabaseDeserializer)deserializer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [deserializer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectDeserializers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerObjectPreSanitizer:(YapDatabasePreSanitizer)preSanitizer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [preSanitizer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectPreSanitizers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerObjectPostSanitizer:(YapDatabasePostSanitizer)postSanitizer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [postSanitizer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		objectPostSanitizers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerMetadataSerializer:(YapDatabaseSerializer)serializer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [serializer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		metadataSerializers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerMetadataDeserializer:(YapDatabaseDeserializer)deserializer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [deserializer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		metadataDeserializers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerMetadataPreSanitizer:(YapDatabasePreSanitizer)preSanitizer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [preSanitizer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		metadataPreSanitizers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)registerMetadataPostSanitizer:(YapDatabasePostSanitizer)postSanitizer forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	id value = [postSanitizer copy];
	
	YAPUnfairLockLock(&configLock);
	{
		metadataPostSanitizers[key] = value;
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)setObjectPolicy:(YapDatabasePolicy)policy forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	
	// Sanity check: ensure policy is valid enum
	switch (policy)
	{
		case YapDatabasePolicyContainment : break;
		case YapDatabasePolicyShare       : break;
		case YapDatabasePolicyCopy        : break;
		default                           : policy = YapDatabasePolicyContainment;
	}
	
	YAPUnfairLockLock(&configLock);
	{
		NSMutableDictionary *newObjectPolicies = [objectPolicies mutableCopy];
		newObjectPolicies[key] = @(policy);
		
		objectPolicies = [newObjectPolicies copy];
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
* See header file for description.
* Or view the api's online (for both Swift & Objective-C):
* https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
*/
- (void)setDefaultObjectPolicy:(YapDatabasePolicy)policy
{
  // Sanity check: ensure policy is valid enum
  switch (policy)
  {
    case YapDatabasePolicyContainment : break;
    case YapDatabasePolicyShare       : break;
    case YapDatabasePolicyCopy        : break;
    default                           : policy = YapDatabasePolicyContainment;
  }

  YAPUnfairLockLock(&configLock);
  {
    _defaultObjectPolicy = @(policy);
  }
  YAPUnfairLockUnlock(&configLock);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)setMetadataPolicy:(YapDatabasePolicy)policy forCollection:(nullable NSString *)collection
{
	id key = collection ?: @"";
	
	// Sanity check: ensure policy is valid enum
	switch (policy)
	{
		case YapDatabasePolicyContainment : break;
		case YapDatabasePolicyShare       : break;
		case YapDatabasePolicyCopy        : break;
		default                           : policy = YapDatabasePolicyContainment;
	}
	
	YAPUnfairLockLock(&configLock);
	{
		NSMutableDictionary *newMetadataPolicies = [metadataPolicies mutableCopy];
		newMetadataPolicies[key] = @(policy);
		
		metadataPolicies = [newMetadataPolicies copy];
	}
	YAPUnfairLockUnlock(&configLock);
}

/**
* See header file for description.
* Or view the api's online (for both Swift & Objective-C):
* https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
*/
- (void)setDefaultMetadataPolicy:(YapDatabasePolicy)policy
{
  // Sanity check: ensure policy is valid enum
  switch (policy)
  {
    case YapDatabasePolicyContainment : break;
    case YapDatabasePolicyShare       : break;
    case YapDatabasePolicyCopy        : break;
    default                           : policy = YapDatabasePolicyContainment;
  }

  YAPUnfairLockLock(&configLock);
  {
    _defaultMetadataPolicy = @(policy);
  }
  YAPUnfairLockUnlock(&configLock);
}

- (YapDatabaseDeserializer)objectDeserializerForCollection:(nullable NSString *)collection
{
	id const key = collection ?: @"";
	id const defaultKey = [NSNull null];
	
	YapDatabaseDeserializer result = nil;
	YAPUnfairLockLock(&configLock);
	{
		result = objectDeserializers[key] ?: objectDeserializers[defaultKey];
	}
	YAPUnfairLockUnlock(&configLock);
	return result;
}

- (YapDatabaseDeserializer)metadataDeserializerForCollection:(nullable NSString *)collection
{
	id const key = collection ?: @"";
	id const defaultKey = [NSNull null];
	
	YapDatabaseDeserializer result = nil;
	YAPUnfairLockLock(&configLock);
	{
		result = metadataDeserializers[key] ?: metadataDeserializers[defaultKey];
	}
	YAPUnfairLockUnlock(&configLock);
	return result;
}


- (YapDatabaseCollectionConfig *)configForCollection:(nullable NSString *)collection
{
	YapDatabaseSerializer objectSerializer = nil;
	YapDatabaseSerializer metadataSerializer = nil;
	
	YapDatabasePreSanitizer objectPreSanitizer = nil;
	YapDatabasePreSanitizer metadataPreSanitizer = nil;
	
	YapDatabasePostSanitizer objectPostSanitizer = nil;
	YapDatabasePostSanitizer metadataPostSanitizer = nil;
	
	YapDatabasePolicy objectPolicy = YapDatabasePolicyContainment;
	YapDatabasePolicy metadataPolicy = YapDatabasePolicyContainment;
	
	id const key = collection ?: @"";
	id const defaultKey = [NSNull null];
	
	YAPUnfairLockLock(&configLock);
	{
		objectSerializer   =   objectSerializers[key] ?:   objectSerializers[defaultKey];
		metadataSerializer = metadataSerializers[key] ?: metadataSerializers[defaultKey];
		
		objectPreSanitizer   =   objectPreSanitizers[key] ?:   objectPreSanitizers[defaultKey];
		metadataPreSanitizer = metadataPreSanitizers[key] ?: metadataPreSanitizers[defaultKey];
		
		objectPostSanitizer   =   objectPostSanitizers[key] ?:   objectPostSanitizers[defaultKey];
		metadataPostSanitizer = metadataPostSanitizers[key] ?: metadataPostSanitizers[defaultKey];
		
		NSNumber *policy = nil;
		
    policy = objectPolicies[key] ?: _defaultObjectPolicy;
		if (policy) {
			objectPolicy = (YapDatabasePolicy)[policy integerValue];
		}
		
    policy = metadataPolicies[key] ?: _defaultMetadataPolicy;
		if (policy) {
			metadataPolicy = (YapDatabasePolicy)[policy integerValue];
		}
	}
	YAPUnfairLockUnlock(&configLock);
	
	YapDatabaseCollectionConfig *config =
	  [[YapDatabaseCollectionConfig alloc] initWithObjectSerializer: objectSerializer
	                                             metadataSerializer: metadataSerializer
	                                             objectPreSanitizer: objectPreSanitizer
	                                           metadataPreSanitizer: metadataPreSanitizer
	                                            objectPostSanitizer: objectPostSanitizer
	                                          metadataPostSanitizer: metadataPostSanitizer
	                                                   objectPolicy: objectPolicy
	                                                 metadataPolicy: metadataPolicy];
	return config;
}

- (NSNumber *)getDefaultObjectPolicy
{
  NSNumber *result = nil;
  YAPUnfairLockLock(&configLock);
  {
    result = _defaultObjectPolicy;
  }
  YAPUnfairLockUnlock(&configLock);
  return result;
}

- (NSNumber *)getDefaultMetadataPolicy
{
  NSNumber *result = nil;
  YAPUnfairLockLock(&configLock);
  {
    result = _defaultMetadataPolicy;
  }
  YAPUnfairLockUnlock(&configLock);
  return result;
}

- (void)getObjectPolicies:(NSDictionary<NSString*, NSNumber*> **)objectPoliciesPtr
         metadataPolicies:(NSDictionary<NSString*, NSNumber*> **)metadataPoliciesPtr
{
	NSDictionary *_objectPolicies = nil;
	NSDictionary *_metadataPolicies = nil;
	
	YAPUnfairLockLock(&configLock);
	{
		_objectPolicies = objectPolicies;
		_metadataPolicies = metadataPolicies;
	}
	YAPUnfairLockUnlock(&configLock);
	
	if (objectPoliciesPtr) *objectPoliciesPtr = _objectPolicies;
	if (metadataPoliciesPtr) *metadataPoliciesPtr = _metadataPolicies;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Connections
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method is called from [self newConnection].
**/
- (void)addConnection:(YapDatabaseConnection *)connection
{
	// We can asynchronously add the connection to the state table.
	// This is safe as the connection itself must go through the same queue in order to do anything.
	//
	// The primary motivation in adding the asynchronous functionality is due to the following common use case:
	//
	// YapDatabase *database = [[YapDatabase alloc] initWithPath:path];
	// YapDatabaseConnection *databaseConnection = [database newConnection];
	//
	// The YapDatabase init method is asynchronously preparing itself through the snapshot queue.
	// We'd like to avoid blocking the very next line of code and allow the asynchronous prepare to continue.
	
	dispatch_async(connection->connectionQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		dispatch_sync(snapshotQueue, ^{ @autoreleasepool {
			
			// Add the connection to the state table
			
			YapDatabaseConnectionState *state = [[YapDatabaseConnectionState alloc] initWithConnection:connection];
			[connectionStates addObject:state];
			
			YDBLogVerbose(@"Created new connection(%p) for <%@ %p: databaseName=%@, connectionCount=%lu>",
			              connection, [self class], self, [databaseURL lastPathComponent],
			              (unsigned long)[connectionStates count]);
			
			// Invoke the one-time prepare method, so the connection can perform any needed initialization.
			// Be sure to do this within the snapshotQueue, as the prepare method depends on this.
			
			[connection prepare];
		}});
		
	#pragma clang diagnostic pop
	});
}

/**
 * This method is called from YapDatabaseConnection's dealloc method.
**/
- (void)removeConnection:(YapDatabaseConnection *)connection
{
	dispatch_block_t block = ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		NSUInteger index = 0;
		for (YapDatabaseConnectionState *state in connectionStates)
		{
			if (state->connection == connection)
			{
				[connectionStates removeObjectAtIndex:index];
				break;
			}
			
			index++;
		}
		
		YDBLogVerbose(@"Removed connection(%p) from <%@ %p: databaseName=%@, connectionCount=%lu>",
		              connection, [self class], self, [databaseURL lastPathComponent],
		              (unsigned long)[connectionStates count]);
		
	#pragma clang diagnostic pop
	}};
	
	// We prefer to invoke this method synchronously.
	//
	// The connection may be the last object retaining the database.
	// It's easier to trace object deallocations when they happen in a predictable order.
	
	if (dispatch_get_specific(IsOnSnapshotQueueKey))
		block();
	else
		dispatch_sync(snapshotQueue, block);
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (YapDatabaseConnection *)newConnection
{
	YapDatabaseConnection *connection = [[YapDatabaseConnection alloc] initWithDatabase:self];
	
	[self addConnection:connection];
	return connection;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (YapDatabaseConnection *)newConnection:(YapDatabaseConnectionConfig *)config
{
	YapDatabaseConnection *connection = [[YapDatabaseConnection alloc] initWithDatabase:self config:config];
	
	[self addConnection:connection];
	return connection;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (BOOL)registerExtension:(YapDatabaseExtension *)extension withName:(NSString *)extensionName
{
	return [self registerExtension:extension withName:extensionName config:nil];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (BOOL)registerExtension:(YapDatabaseExtension *)extension
                 withName:(NSString *)extensionName
                   config:(YapDatabaseConnectionConfig *)config
{
	__block BOOL ready = NO;
	dispatch_sync(writeQueue, ^{ @autoreleasepool {
		
		ready = [self _registerExtension:extension withName:extensionName config:config];
	}});
	
	return ready;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
               completionBlock:(void(^)(BOOL ready))completionBlock
{
	[self asyncRegisterExtension:extension
	                    withName:extensionName
	                      config:nil
	             completionQueue:NULL
	             completionBlock:completionBlock];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
               completionQueue:(dispatch_queue_t)completionQueue
               completionBlock:(void(^)(BOOL ready))completionBlock
{
	[self asyncRegisterExtension:extension
	                    withName:extensionName
	                      config:nil
	             completionQueue:completionQueue
	             completionBlock:completionBlock];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
                        config:(YapDatabaseConnectionConfig *)config
               completionBlock:(void(^)(BOOL ready))completionBlock
{
	[self asyncRegisterExtension:extension
	                    withName:extensionName
	                      config:config
	             completionQueue:NULL
	             completionBlock:completionBlock];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
                        config:(YapDatabaseConnectionConfig *)config
               completionQueue:(dispatch_queue_t)completionQueue
               completionBlock:(void(^)(BOOL ready))completionBlock
{
	if (completionQueue == NULL && completionBlock != NULL)
		completionQueue = dispatch_get_main_queue();
	
	if (config)
		config = [config copy];
	
	dispatch_async(writeQueue, ^{ @autoreleasepool {
		
		BOOL ready = [self _registerExtension:extension withName:extensionName config:config];
		
		if (completionBlock)
		{
			dispatch_async(completionQueue, ^{ @autoreleasepool {
				
				completionBlock(ready);
			}});
		}
	}});
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)unregisterExtensionWithName:(NSString *)extensionName
{
	dispatch_sync(writeQueue, ^{ @autoreleasepool {
		
		[self _unregisterExtensionWithName:extensionName];
	}});
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)asyncUnregisterExtensionWithName:(NSString *)extensionName
                         completionBlock:(dispatch_block_t)completionBlock
{
	[self asyncUnregisterExtensionWithName:extensionName
	                       completionQueue:NULL
	                       completionBlock:completionBlock];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)asyncUnregisterExtensionWithName:(NSString *)extensionName
                         completionQueue:(dispatch_queue_t)completionQueue
                         completionBlock:(dispatch_block_t)completionBlock
{
	if (completionQueue == NULL && completionBlock != NULL)
		completionQueue = dispatch_get_main_queue();
	
	dispatch_async(writeQueue, ^{ @autoreleasepool {
		
		[self _unregisterExtensionWithName:extensionName];
		
		if (completionBlock)
		{
			dispatch_async(completionQueue, ^{ @autoreleasepool {
				
				completionBlock();
			}});
		}
	}});
}

/**
 * Internal utility method.
 * Handles lazy creation and destruction of short-lived registrationConnection instance.
 * 
 * @see _registerExtension:withName:
 * @see _unregisterExtensionWithName:
**/
- (YapDatabaseConnection *)registrationConnection
{
	if (registrationConnection == nil)
	{
		registrationConnection = [self newConnection];
		registrationConnection.name = @"YapDatabase_extensionRegistrationConnection";
		
		// These are the rules (regarding instance retainCount):
		// - a YapDatabaseConnection instance cannot be deallocated if there are existing/pending transactions
		// - a YapDatabase instance cannot be deallocated if there are existing connections
		//
		// Thus, as long as registrationConnection is non-nil,
		// 'self' (this YapDatabase instance) cannot be deallocated.
		//
		
		__weak YapDatabase *weakSelf = self;
		
		NSTimeInterval delayInSeconds = 5.0;
		dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(delayInSeconds * NSEC_PER_SEC));
		dispatch_after(popTime, writeQueue, ^(void){
			
			__strong YapDatabase *strongSelf = weakSelf;
			if (strongSelf)
			{
				strongSelf->registrationConnection = nil;
			}
		});
	}
	
	return registrationConnection;
}

/**
 * Internal method that handles extension registration.
 * This method must be invoked on the writeQueue.
**/
- (BOOL)_registerExtension:(YapDatabaseExtension *)extension
                  withName:(NSString *)extensionName
                    config:(YapDatabaseConnectionConfig *)config
{
	NSAssert(dispatch_get_specific(IsOnWriteQueueKey), @"Must go through writeQueue.");
	
	// Validate parameters
	
	if (extension == nil)
	{
		YDBLogError(@"Error registering extension: extension parameter is nil");
		return NO;
	}
	if ([extensionName length] == 0)
	{
		YDBLogError(@"Error registering extension: extensionName parameter is nil or empty string");
		return NO;
	}
	
	// Check to ensure extension isn't already registered,
	// or that the extensionName isn't already taken.
	
	NSDictionary *_registeredExtensions = [self registeredExtensions];
	
	if (extension.registeredName != nil)
	{
		YDBLogError(@"Error registering extension: extension is already registered");
		return NO;
	}
	if ([_registeredExtensions objectForKey:extensionName] != nil)
	{
		YDBLogError(@"Error registering extension: extensionName(%@) already registered", extensionName);
		return NO;
	}
	
	// Attempt registration
	
	extension.registeredName = extensionName;
	extension.registeredDatabase = self;
	
	BOOL result = [extension supportsDatabaseWithRegisteredExtensions:_registeredExtensions];
	if (!result)
	{
		YDBLogError(@"Error registering extension: extension doesn't support database configuration");
	}
	else
	{
		YapDatabaseConnection *connection = [self registrationConnection];
		
		YapDatabaseConnectionConfig *originalConfig = nil;
		if (config)
		{
			originalConfig = [connection copyConfig];
			[connection applyConfig:config];
		}
		
		result = [connection registerExtension:extension withName:extensionName];
		
		if (config)
		{
			[connection applyConfig:originalConfig];
		}
	}
	
	if (result)
	{
		[extension didRegisterExtension];
	}
	else
	{
		extension.registeredName = nil;
		extension.registeredDatabase = nil;
	}
	
	
	return result;
}

/**
 * Internal method that handles extension unregistration.
 * This method must be invoked on the writeQueue.
**/
- (void)_unregisterExtensionWithName:(NSString *)extensionName
{
	NSAssert(dispatch_get_specific(IsOnWriteQueueKey), @"Must go through writeQueue.");
	
	// Validate parameters
	
	if ([extensionName length] == 0)
	{
		YDBLogError(@"Error unregistering extension: extensionName parameter is nil or empty string");
		return;
	}
	
	// Perform unregistration
	
	YapDatabaseConnection *connection = [self registrationConnection];
	
	[connection unregisterExtensionWithName:extensionName];
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (id)registeredExtension:(NSString *)extensionName
{
	// This method is public
	
	__block YapDatabaseExtension *result = nil;
	
	dispatch_block_t block = ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		result = [registeredExtensions objectForKey:extensionName];
		
	#pragma clang diagnostic pop
	};
	
	if (dispatch_get_specific(IsOnSnapshotQueueKey))
		block();
	else
		dispatch_sync(snapshotQueue, block);
	
	return result;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (NSDictionary *)registeredExtensions
{
	// This method is public
	
	__block NSDictionary *extensionsCopy = nil;
	
	dispatch_block_t block = ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		extensionsCopy = registeredExtensions;
		
	#pragma clang diagnostic pop
	};
	
	if (dispatch_get_specific(IsOnSnapshotQueueKey))
		block();
	else
		dispatch_sync(snapshotQueue, block);
	
	return extensionsCopy;
}

/**
 * This method is only accessible from within the snapshotQueue.
 * Used by [YapDatabaseConnection prepare].
**/
- (NSArray *)extensionsOrder
{
	NSAssert(dispatch_get_specific(IsOnSnapshotQueueKey), @"Must go through snapshotQueue for atomic access.");
	
	return extensionsOrder;
}

/**
 * This method is only accessible from within the snapshotQueue.
 * Used by [YapDatabaseConnection prepare].
**/
- (NSDictionary *)extensionDependencies
{
	NSAssert(dispatch_get_specific(IsOnSnapshotQueueKey), @"Must go through snapshotQueue for atomic access.");
	
	return extensionDependencies;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (NSArray *)previouslyRegisteredExtensionNames
{
	__block NSArray *result = nil;
	
	dispatch_block_t block = ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		result = [previouslyRegisteredExtensionNames copy];
		
	#pragma clang diagnostic pop
	};
	
	if (dispatch_get_specific(IsOnSnapshotQueueKey))
		block();
	else
		dispatch_sync(snapshotQueue, block);
	
	return result;
}

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (void)flushExtensionRequestsWithCompletionQueue:(nullable dispatch_queue_t)completionQueue
									       completionBlock:(nullable dispatch_block_t)completionBlock
{
	if (completionQueue == NULL && completionBlock != NULL)
		completionQueue = dispatch_get_main_queue();
	
	dispatch_async(writeQueue, ^{ @autoreleasepool {
		
		if (completionBlock)
		{
			dispatch_async(completionQueue, ^{ @autoreleasepool {
				
				completionBlock();
			}});
		}
	}});
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Pooling
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSUInteger)maxConnectionPoolCount
{
	__block NSUInteger count = 0;
	
	dispatch_sync(internalQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		count = maxConnectionPoolCount;
		
	#pragma clang diagnostic pop
	});
	
	return count;
}

- (void)setMaxConnectionPoolCount:(NSUInteger)count
{
	dispatch_sync(internalQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		// Update ivar
		maxConnectionPoolCount = count;
		
		// Immediately drop any excess connections
		if ([connectionPoolValues count] > maxConnectionPoolCount)
		{
			do
			{
				sqlite3 *aDb = (sqlite3 *)[[connectionPoolValues objectAtIndex:0] pointerValue];
				
				int status = sqlite3_close(aDb);
				if (status != SQLITE_OK)
				{
					YDBLogError(@"Error in sqlite_close: %d %s", status, sqlite3_errmsg(aDb));
				}
				
				[connectionPoolValues removeObjectAtIndex:0];
				[connectionPoolDates removeObjectAtIndex:0];
				
			} while ([connectionPoolValues count] > maxConnectionPoolCount);
			
			[self resetConnectionPoolTimer];
		}
		
	#pragma clang diagnostic pop
	});
}

- (NSTimeInterval)connectionPoolLifetime
{
	__block NSTimeInterval lifetime = 0;
	
	dispatch_sync(internalQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		lifetime = connectionPoolLifetime;
		
	#pragma clang diagnostic pop
	});
	
	return lifetime;
}

- (void)setConnectionPoolLifetime:(NSTimeInterval)lifetime
{
	dispatch_sync(internalQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		// Update ivar
		connectionPoolLifetime = lifetime;
		
		// Update timer (if needed)
		[self resetConnectionPoolTimer];
		
	#pragma clang diagnostic pop
	});
}

/**
 * Adds the given connection to the connection pool if possible.
 * 
 * Returns YES if the instance was added to the pool.
 * If so, the YapDatabaseConnection must not close the instance.
 * 
 * Returns NO if the instance was not added to the pool.
 * If so, the YapDatabaseConnection must close the instance.
**/
- (BOOL)connectionPoolEnqueue:(sqlite3 *)aDb main_file:(yap_file *)main_file wal_file:(yap_file *)wal_file
{
	__block BOOL result = NO;
	
	dispatch_sync(internalQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		if ([connectionPoolValues count] < maxConnectionPoolCount)
		{
			if (connectionPoolValues == nil)
			{
				connectionPoolValues = [[NSMutableArray alloc] init];
				connectionPoolDates = [[NSMutableArray alloc] init];
			}
			
			YDBLogVerbose(@"Enqueuing connection to pool: %p", aDb);
			
			NSDictionary *value = @{
			  YDBConnectionPoolValueKey_db        : [NSValue valueWithPointer:(const void *)aDb],
			  YDBConnectionPoolValueKey_main_file : [NSValue valueWithPointer:(const void *)main_file],
			  YDBConnectionPoolValueKey_wal_file  : [NSValue valueWithPointer:(const void *)wal_file],
			};
			
			[connectionPoolValues addObject:value];
			[connectionPoolDates addObject:[NSDate date]];
			
			result = YES;
			
			if ([connectionPoolValues count] == 1)
			{
				[self resetConnectionPoolTimer];
			}
		}
		
	#pragma clang diagnostic pop
	});
	
	return result;
}

/**
 * Retrieves a connection from the connection pool if available.
 * Returns NULL if no connections are available.
**/
- (BOOL)connectionPoolDequeue:(sqlite3 **)pDb main_file:(yap_file **)pMainFile wal_file:(yap_file **)pWalFile
{
	NSParameterAssert(pDb != NULL);
	NSParameterAssert(pMainFile != NULL);
	NSParameterAssert(pWalFile != NULL);
	
	__block sqlite3 *aDb = NULL;
	__block yap_file *main_file = NULL;
	__block yap_file *wal_file = NULL;
	
	dispatch_sync(internalQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		if ([connectionPoolValues count] > 0)
		{
			NSDictionary *value = [connectionPoolValues objectAtIndex:0];
			
			aDb       = (sqlite3 *)[[value objectForKey:YDBConnectionPoolValueKey_db] pointerValue];
			main_file = (yap_file *)[[value objectForKey:YDBConnectionPoolValueKey_main_file] pointerValue];
			wal_file  = (yap_file *)[[value objectForKey:YDBConnectionPoolValueKey_wal_file] pointerValue];
			
			YDBLogVerbose(@"Dequeuing connection from pool: %p", aDb);
			
			[connectionPoolValues removeObjectAtIndex:0];
			[connectionPoolDates removeObjectAtIndex:0];
			
			[self resetConnectionPoolTimer];
		}
		
	#pragma clang diagnostic pop
	});
	
	*pDb = aDb;
	*pMainFile = main_file;
	*pWalFile = wal_file;
	
	return (aDb != NULL);
}

/**
 * Internal utility method to handle setting/resetting the timer.
**/
- (void)resetConnectionPoolTimer
{
	YDBLogAutoTrace();
	
	if (connectionPoolLifetime <= 0.0 || [connectionPoolValues count] == 0)
	{
		if (connectionPoolTimer)
		{
			dispatch_source_cancel(connectionPoolTimer);
			connectionPoolTimer = NULL;
		}
		
		return;
	}
	
	BOOL isNewTimer = NO;
	
	if (connectionPoolTimer == NULL)
	{
		connectionPoolTimer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, internalQueue);
		
		__weak YapDatabase *weakSelf = self;
		dispatch_source_set_event_handler(connectionPoolTimer, ^{ @autoreleasepool {
			
			__strong YapDatabase *strongSelf = weakSelf;
			if (strongSelf)
			{
				[strongSelf handleConnectionPoolTimerFire];
			}
		}});
		
		#if !OS_OBJECT_USE_OBJC
		dispatch_source_t timer = connectionPoolTimer;
		dispatch_source_set_cancel_handler(connectionPoolTimer, ^{
			dispatch_release(timer);
		});
		#endif
		
		isNewTimer = YES;
	}
	
	NSDate *date = [connectionPoolDates objectAtIndex:0];
	NSTimeInterval interval = [date timeIntervalSinceNow] + connectionPoolLifetime;
	
	dispatch_time_t tt = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(interval * NSEC_PER_SEC));
	dispatch_source_set_timer(connectionPoolTimer, tt, DISPATCH_TIME_FOREVER, 0);
	
	if (isNewTimer) {
		dispatch_resume(connectionPoolTimer);
	}
}

/**
 * Internal method to handle removing stale connections from the connection pool.
**/
- (void)handleConnectionPoolTimerFire
{
	YDBLogAutoTrace();
	
	NSDate *now = [NSDate date];
	
	BOOL done = NO;
	while ([connectionPoolValues count] > 0 && !done)
	{
		NSTimeInterval interval = [[connectionPoolDates objectAtIndex:0] timeIntervalSinceDate:now] * -1.0;
		
		if ((interval >= connectionPoolLifetime) || (interval < 0))
		{
			NSDictionary *value = [connectionPoolValues objectAtIndex:0];
			
			sqlite3 *aDb = (sqlite3 *)[[value objectForKey:YDBConnectionPoolValueKey_db] pointerValue];
			
			YDBLogVerbose(@"Closing connection from pool: %p", aDb);
			
			int status = sqlite3_close(aDb);
			if (status != SQLITE_OK)
			{
				YDBLogError(@"Error in sqlite_close: %d %s", status, sqlite3_errmsg(aDb));
			}
			
			[connectionPoolValues removeObjectAtIndex:0];
			[connectionPoolDates removeObjectAtIndex:0];
		}
		else
		{
			done = YES;
		}
	}
	
	[self resetConnectionPoolTimer];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Memory Tables
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method is only accessible from within the snapshotQueue.
 * Used by [YapDatabaseConnection prepare].
**/
- (NSDictionary *)registeredMemoryTables
{
	NSAssert(dispatch_get_specific(IsOnSnapshotQueueKey), @"Must go through snapshotQueue for atomic access.");
	
	return registeredMemoryTables;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Snapshot Architecture
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * See header file for description.
 * Or view the api's online (for both Swift & Objective-C):
 * https://yapstudios.github.io/YapDatabase/Classes/YapDatabase.html
 */
- (uint64_t)snapshot
{
	if (dispatch_get_specific(IsOnSnapshotQueueKey))
	{
		// Very common case.
		// This method is called on just about every transaction.
		return snapshot;
	}
	else
	{
		// Non-common case.
		// Public access implementation.
		__block uint64_t result = 0;
		
		dispatch_sync(snapshotQueue, ^{
		#pragma clang diagnostic push
		#pragma clang diagnostic ignored "-Wimplicit-retain-self"
			
			result = snapshot;
			
		#pragma clang diagnostic pop
		});
		
		return result;
	}
}

/**
 * This method is only accessible from within the snapshotQueue.
 * 
 * Prior to starting the sqlite commit, the connection must report its changeset to the database.
 * The database will store the changeset, and provide it to other connections if needed (due to a race condition).
 * 
 * The following MUST be in the dictionary:
 *
 * - snapshot : NSNumber with the changeset's snapshot
**/
- (void)notePendingChangeset:(NSDictionary *)pendingChangeset fromConnection:(YapDatabaseConnection __unused *)sender
{
	NSAssert(dispatch_get_specific(IsOnSnapshotQueueKey), @"Must go through snapshotQueue for atomic access.");
	NSAssert([pendingChangeset objectForKey:YapDatabaseSnapshotKey], @"Missing required change key: snapshot");
	
	// The sender is preparing to start the sqlite commit.
	// We save the changeset in advance to handle possible edge cases.
	
	[changesets addObject:pendingChangeset];
	
	YDBLogVerbose(@"Adding pending changeset %@ for database: %@",
	              [[changesets lastObject] objectForKey:YapDatabaseSnapshotKey], self);
}

/**
 * This method is only accessible from within the snapshotQueue.
 *
 * This method is used if a transaction finds itself in a race condition.
 * That is, the transaction started before it was able to process changesets from sibling connections.
 *
 * It should fetch the changesets needed and then process them via [connection noteCommittedChangeset:].
 *
 * Returns `nil` if the number of changesets found is not the expected one, that is, one for each snapshot increase from `connectionSnapshot` to `maxSnapshot`.
 * This can only happen in multiprocess mode, if another process has updated the database.
 * In this case the changesets are invalid, and we need to clear connection and extension caches.
**/
- (NSArray *)pendingAndCommittedChangesetsSince:(uint64_t)connectionSnapshot until:(uint64_t)maxSnapshot
{
	NSAssert(dispatch_get_specific(IsOnSnapshotQueueKey), @"Must go through snapshotQueue for atomic access.");
	
	NSUInteger capacity = (NSUInteger)(maxSnapshot - connectionSnapshot);
	NSMutableArray *relevantChangesets = [NSMutableArray arrayWithCapacity:capacity];
    
	for (NSDictionary *changeset in changesets)
	{
		uint64_t changesetSnapshot = [[changeset objectForKey:YapDatabaseSnapshotKey] unsignedLongLongValue];
		
		if ((changesetSnapshot > connectionSnapshot) && (changesetSnapshot <= maxSnapshot))
		{
			[relevantChangesets addObject:changeset];
		}
	}
    
	if (options.enableMultiProcessSupport)
	{
		const uint64_t expectedSnapshotsCount = maxSnapshot - connectionSnapshot;
		if (expectedSnapshotsCount != relevantChangesets.count)
		{
			YDBLogVerbose(@"Expected snapshot count not found: expected(%llu) != found(%llu)."
			              @" Database seems to have been modified from another process. Discarding changeset.",
			              expectedSnapshotsCount, (uint64_t)relevantChangesets.count);
			return nil;
		}
	}
	
	return relevantChangesets;
}

/**
 * This method is only accessible from within the snapshotQueue.
 *
 * Upon completion of a readwrite transaction, the connection should report it's changeset to the database.
 * The database will then forward the changes to all other connection's.
 *
 * The following MUST be in the dictionary:
 *
 * - snapshot : NSNumber with the changeset's snapshot
**/
- (void)noteCommittedChangeset:(NSDictionary *)changeset fromConnection:(YapDatabaseConnection *)sender
{
	NSAssert(dispatch_get_specific(IsOnSnapshotQueueKey), @"Must go through snapshotQueue for atomic access.");
	NSAssert([changeset objectForKey:YapDatabaseSnapshotKey], @"Missing required change key: snapshot");
    
	// The sender has finished the sqlite commit, and all data is now written to disk.
	
	// Update the in-memory snapshot,
	// which represents the most recent snapshot of the last committed readwrite transaction.
	
	snapshot = [[changeset objectForKey:YapDatabaseSnapshotKey] unsignedLongLongValue];

	// Update registeredExtensions, if changed.
	
	NSDictionary *newRegisteredExtensions = [changeset objectForKey:YapDatabaseRegisteredExtensionsKey];
	if (newRegisteredExtensions)
	{
		registeredExtensions = newRegisteredExtensions;
		extensionsOrder = [changeset objectForKey:YapDatabaseExtensionsOrderKey];
		extensionDependencies = [changeset objectForKey:YapDatabaseExtensionDependenciesKey];
	}
	
	// Update registeredMemoryTables, if changed.
	
	NSDictionary *newRegisteredMemoryTables = [changeset objectForKey:YapDatabaseRegisteredMemoryTablesKey];
	if (newRegisteredMemoryTables)
	{
		registeredMemoryTables = newRegisteredMemoryTables;
	}
	
	// Forward the changeset to all extensions.
	
	[registeredExtensions enumerateKeysAndObjectsUsingBlock:
	    ^(NSString *extName, YapDatabaseExtension *ext, BOOL __unused *stop)
	{
		[ext noteCommittedChangeset:changeset registeredName:extName];
	}];
	
	// Forward the changeset to all other connections so they can perform any needed updates.
	// Generally this means updating the in-memory components such as the cache.
	
	NSMutableArray<YapDatabaseConnection *> *strongConnections = nil;
	dispatch_group_t group = NULL;
	
	for (YapDatabaseConnectionState *state in connectionStates)
	{
		if (state->connection != sender)
		{
			// Create strong reference (state->connection is weak)
			__strong YapDatabaseConnection *connection = state->connection;
			
			if (connection)
			{
				if (strongConnections == nil)
					strongConnections = [NSMutableArray array];
				
				[strongConnections addObject:connection];
				
				if (group == NULL)
					group = dispatch_group_create();
				
				dispatch_group_async(group, connection->connectionQueue, ^{ @autoreleasepool {
					
					[connection noteCommittedChangeset:changeset];
				}});
			}
		}
	}
	
	// Schedule block to be executed once all connections have processed the changes.
	
	BOOL isInternalChangeset = (sender == nil);
	__weak YapDatabase *weakSelf = self;
	
	dispatch_block_t block = ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic warning "-Wimplicit-retain-self" // Turning warnings *** ON ***
		
		__strong YapDatabase *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		// All connections have now processed the changes.
		// So we no longer need to retain the changeset in memory.
		
		if (isInternalChangeset)
		{
			YDBLogVerbose(@"Completed internal changeset %@ for database: %@",
			              [changeset objectForKey:YapDatabaseSnapshotKey], self);
		}
		else
		{
			YDBLogVerbose(@"Dropping processed changeset %@ for database: %@",
			              [changeset objectForKey:YapDatabaseSnapshotKey], self);
			
			[strongSelf->changesets removeObjectAtIndex:0];
		}
		
		#if !OS_OBJECT_USE_OBJC
		if (group)
			dispatch_release(group);
		#endif
		
	#pragma clang diagnostic pop
	};
	
	if (group)
		dispatch_group_notify(group, snapshotQueue, block);
	else
		block();
	
	if (strongConnections)
	{
		// Edge case protection:
		// Bug fix for issues: #437, #441
		//
		// Deadlock crash if:
		// - YapDatabase is the last one holding a strong reference to a YapDatabaseConnection instance
		// - The [connection dealloc] call occurs within the snapshotQueue
		//
		// This is a workaround to ensure that the dealloc occurs outside the snapshotQueue.
		//
		dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{ @autoreleasepool {
			[strongConnections removeAllObjects];
		}});
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Manual Checkpointing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method should be called whenever the maximum checkpointable snapshot is incremented.
 * That is, the state of every connection is known to the system.
 * And a snaphot cannot be checkpointed until every connection is at or past that snapshot.
 * Thus, we can know the point at which a snapshot becomes checkpointable,
 * and we can thus optimize the checkpoint invocations such that
 * each invocation is able to checkpoint one or more commits.
**/
- (void)asyncCheckpoint:(uint64_t)maxCheckpointableSnapshot
{
	if (maxCheckpointableSnapshot > 0) {
		YDBLogVerbose(@"Checkpoint possible up to snapshot %llu", maxCheckpointableSnapshot);
	}
	
	bool aggressive = atomic_load(&aggressiveCheckpointEnabled);
	if (aggressive)
	{
		[self asyncAggressiveCheckpoint];
	}
	else
	{
		[self asyncPassiveCheckpoint];
	}
}

- (void)asyncPassiveCheckpoint
{
	bool hasPendingCheckpoint = atomic_flag_test_and_set(&pendingPassiveCheckpoint);
	if (hasPendingCheckpoint) {
		return;
	}
	
	__weak YapDatabase *weakSelf = self;
	
	dispatch_async(checkpointQueue, ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic warning "-Wimplicit-retain-self" // Turning warnings *** ON ***
		
		__strong YapDatabase *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		atomic_flag_clear(&strongSelf->pendingPassiveCheckpoint);
		
		if (atomic_load(&strongSelf->aggressiveCheckpointEnabled)) {
			return;
		}
		
		[strongSelf passiveCheckpoint];
		
	#pragma clang diagnostic pop
	}});
}

- (void)asyncAggressiveCheckpoint
{
	bool hasPendingCheckpoint = atomic_flag_test_and_set(&pendingAggressiveCheckpoint);
	if (hasPendingCheckpoint) {
		return;
	}
	
	__weak YapDatabase *weakSelf = self;
	
	dispatch_async(writeQueue, ^{
	#pragma clang diagnostic push
	#pragma clang diagnostic warning "-Wimplicit-retain-self" // Turning warnings *** ON ***
		
		__strong YapDatabase *strongSelf = weakSelf;
		if (strongSelf == nil) return;
		
		atomic_flag_clear(&strongSelf->pendingAggressiveCheckpoint);
		
		if (!atomic_load(&strongSelf->aggressiveCheckpointEnabled)) {
			return;
		}
		
		[strongSelf aggressiveCheckpoint];
		
	#pragma clang diagnostic pop
	});
}

- (void)passiveCheckpoint
{
	int checkpointResult = 0;
	int totalFrameCount = 0;
	int checkpointedFrameCount = 0;
	
	// We're going to execute a passive checkpoint.
	// That is, without disrupting any connections, we're going to write pages from the WAL into the database.
	// The checkpoint can only write pages from snapshots if all connections are at or beyond the snapshot.
	// Thus, this method is only called by a connection that moves the min snapshot forward.
	
	checkpointResult = sqlite3_wal_checkpoint_v2(db, "main", SQLITE_CHECKPOINT_PASSIVE,
	                                             &totalFrameCount, &checkpointedFrameCount);
	
	// totalFrameCount        = total number of frames in the WAL file
	// checkpointedFrameCount = total number of checkpointed frames (those copied into db file)
	//                          (including any that were already checkpointed before the function was called)
	
	YDBLogVerbose(@"Post-checkpoint: src(a) mode(passive) result(%d) frames(%d) checkpointed(%d)",
	              checkpointResult, totalFrameCount, checkpointedFrameCount);
	
	if (checkpointResult != SQLITE_OK)
	{
		if (checkpointResult == SQLITE_BUSY) {
			YDBLogVerbose(@"sqlite3_wal_checkpoint_v2 returned SQLITE_BUSY");
		}
		else {
			YDBLogWarn(@"sqlite3_wal_checkpoint_v2 returned error code: %d", checkpointResult);
		}
		
		return;// from_block
	}
	
	// Did we checkpoint the entire WAL file ?
	
	BOOL didCheckpointEntireWAL = (totalFrameCount == checkpointedFrameCount);
	
	if (didCheckpointEntireWAL)
	{
		// We've checkpointed every single frame in the WAL.
		// This means the next read-write transaction may be able to reset the WAL (instead of appending to it).
		//
		// However, the WAL reset will get spoiled if there are active read-only transactions that
		// were started before our checkpoint finished, and continue to exist during the next read-write.
		// It's not a big deal if the occasional read-only transaction happens to spoil the WAL reset.
		// In those cases, the WAL generally gets reset shortly thereafter (on a subsequent write).
		// Long-lived read transactions are a different case entirely.
		// These transactions spoil it every single time, and could potentially cause the WAL to grow indefinitely.
		//
		// The solution is to notify active long-lived connections, and tell them to re-begin their transaction
		// on the same snapshot. But this time the sqlite machinery will read directly from the database,
		// and thus unlock the WAL so it can be reset.
		
		__weak YapDatabase *weakSelf = self;
		
		dispatch_async(writeQueue, ^{ @autoreleasepool {
		#pragma clang diagnostic push
		#pragma clang diagnostic warning "-Wimplicit-retain-self" // Turning warnings *** ON ***
			
			__strong YapDatabase *strongSelf = weakSelf;
			if (strongSelf == nil) return;
			
			[strongSelf tryResetLongLivedReadTransactions];
			
		#pragma clang diagnostic pop
		}});
	}
	
	// Is the WAL file getting too big ?
	
	uint64_t walApproximateFileSize = totalFrameCount * pageSize;
	BOOL needsAggressiveCheckpoint = (walApproximateFileSize >= options.aggressiveWALTruncationSize);
	
	if (needsAggressiveCheckpoint)
	{
		atomic_store(&aggressiveCheckpointEnabled, true);
		
		[self asyncAggressiveCheckpoint];
	}
}

- (void)aggressiveCheckpoint
{
	int checkpointResult = 0;
	int totalFrameCount = 0;
	int checkpointedFrameCount = 0;
	
	// First we set an adequate busy timeout on our database connection.
	// We're going to run a non-passive checkpoint.
	// Which may cause it to busy-wait while waiting on read transactions to complete.
	
	sqlite3_busy_timeout(db, 50); // milliseconds
	
	// Step 1 of 3:
	//
	// Perform FULL checkpoint.
	//
	// This will checkpoint as many frames as possible,
	// and busy-wait until all readers are on the latest commit.
	
	checkpointResult = sqlite3_wal_checkpoint_v2(db, "main", SQLITE_CHECKPOINT_FULL,
	                                             &totalFrameCount, &checkpointedFrameCount);
	
	YDBLogInfo(@"Post-checkpoint: src(b) mode(full) result(%d) frames(%d) checkpointed(%d)",
	           checkpointResult, totalFrameCount, checkpointedFrameCount);
	
	if (totalFrameCount != checkpointedFrameCount)
	{
		return;
	}
	
	// STEP 2 of 3:
	//
	// Check for longLivedReadTransactions, and attempt to silently move them to reading directly from the database.
	// (As oppossed to reading from the latest commit in the WAL.)
	
	if (![self tryResetLongLivedReadTransactions])
	{
		YDBLogInfo(@"Aggressive checkpoint spoiled: longLivedReadTransaction is blocking");
		return;
	}
	
	// STEP 3 of 3:
	//
	// Perform TRUNCATE checkpoint.
	//
	// At this point, we've checkpointed every single frame.
	// And every connection should be reading directly from the database.
	// So we should be able to truncate the WAL file now.
	
	// Can we use SQLITE_CHECKPOINT_TRUNCATE ?
	//
	// This feature was added in sqlite v3.8.8.
	// But it was buggy until v3.8.8.2 when the following fix was added:
	//
	//   "Enhance sqlite3_wal_checkpoint_v2(TRUNCATE) interface so that it truncates the
	//    WAL file even if there is no checkpoint work to be done."
	//
	//   http://www.sqlite.org/changes.html
	//
	// It is often the case, when we call checkpoint here, that there is no checkpoint work to be done.
	// So we really can't depend on it until 3.8.8.2
	
	int checkpointMode = SQLITE_CHECKPOINT_RESTART;
	
	// Remember: The compiler defines (SQLITE_VERSION, SQLITE_VERSION_NUMBER) only tell us
	// what version we're compiling against. But we may encounter an earlier sqlite version at runtime.
	
#ifndef SQLITE_VERSION_NUMBER_3_8_8
#define SQLITE_VERSION_NUMBER_3_8_8 3008008
#endif
	
#if SQLITE_VERSION_NUMBER > SQLITE_VERSION_NUMBER_3_8_8
	
	checkpointMode = SQLITE_CHECKPOINT_TRUNCATE;
	
#elif SQLITE_VERSION_NUMBER == SQLITE_VERSION_NUMBER_3_8_8
	
	NSComparisonResult cmp = [strongSelf->sqliteVersion compare:@"3.8.8.2" options:NSNumericSearch];
	if (cmp != NSOrderedAscending)
	{
		checkpointMode = SQLITE_CHECKPOINT_TRUNCATE;
	}
	
#endif
	
	checkpointResult = sqlite3_wal_checkpoint_v2(db, "main", checkpointMode,
	                                             &totalFrameCount, &checkpointedFrameCount);
	
	YDBLogInfo(@"Post-checkpoint: src(c) mode(%@) result(%d) frames(%d) checkpointed(%d)",
	           (checkpointMode == SQLITE_CHECKPOINT_RESTART ? @"restart" : @"truncate"),
	           checkpointResult, totalFrameCount, checkpointedFrameCount);
	
	if (checkpointResult == SQLITE_OK)
	{
		if (checkpointMode == SQLITE_CHECKPOINT_RESTART)
		{
			// Write something to the database to force restart the WAL.
			// We're just going to set a random value in the yap2 table.
			
			NSString *uuid = [[NSUUID UUID] UUIDString];
			
			[self beginTransaction];
			
			int status;
			sqlite3_stmt *statement;
			
			char *stmt = "INSERT OR REPLACE INTO \"yap2\" (\"extension\", \"key\", \"data\") VALUES (?, ?, ?);";
			
			int const bind_extension = SQLITE_BIND_START + 0;
			int const bind_key       = SQLITE_BIND_START + 1;
			int const bind_data      = SQLITE_BIND_START + 2;
			
			status = sqlite3_prepare_v2(db, stmt, (int)strlen(stmt)+1, &statement, NULL);
			if (status != SQLITE_OK)
			{
				YDBLogError(@"Error creating statement: %d %s", status, sqlite3_errmsg(db));
			}
			else
			{
				char *extension = "";
				sqlite3_bind_text(statement, bind_extension, extension, (int)strlen(extension), SQLITE_STATIC);
				
				char *key = "random";
				sqlite3_bind_text(statement, bind_key, key, (int)strlen(key), SQLITE_STATIC);
				
				YapDatabaseString _uuid; MakeYapDatabaseString(&_uuid, uuid);
				sqlite3_bind_text(statement, bind_data, _uuid.str, _uuid.length, SQLITE_STATIC);
				
				status = sqlite3_step(statement);
				if (status != SQLITE_DONE)
				{
					YDBLogError(@"Error in statement: %d %s", status, sqlite3_errmsg(db));
				}
				
				sqlite3_finalize(statement);
				FreeYapDatabaseString(&_uuid);
			}
			
			[self commitTransaction];
		}
		
		atomic_store(&aggressiveCheckpointEnabled, false);
	}
}

- (BOOL)tryResetLongLivedReadTransactions
{
	NSAssert(dispatch_get_specific(IsOnWriteQueueKey), @"Must go through writeQueue.");
	
	__block NSMutableArray<YapDatabaseConnection *> *strongConnections = nil;
	__block dispatch_group_t group = NULL;
	
	__block YAPUnfairLock spinLock = YAP_UNFAIR_LOCK_INIT;
	__block atomic_bool hasWriteQueue = true;
	
	dispatch_sync(snapshotQueue, ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		for (YapDatabaseConnectionState *state in connectionStates)
		{
			if (state->activeReadTransaction && state->longLivedReadTransaction)
			{
				// Create strong reference (state->connection is weak)
				__strong YapDatabaseConnection *connection = state->connection;
				
				if (connection)
				{
					if (strongConnections == nil)
						strongConnections = [NSMutableArray array];
					
					[strongConnections addObject:connection];
					
					if (group == NULL)
						group = dispatch_group_create();
					
					dispatch_group_async(group, connection->connectionQueue, ^{
						
						YAPUnfairLockLock(&spinLock);
						{
							if (atomic_load(&hasWriteQueue))
							{
								[connection resetLongLivedReadTransaction];
							}
						}
						YAPUnfairLockUnlock(&spinLock);
					});
				}
			}
		}
		
	#pragma clang diagnostic pop
	}});
	
	if (strongConnections)
	{
		// Edge case protection:
		// Bug fix for issues: #437, #441
		//
		// Deadlock crash if:
		// - YapDatabase is the last one holding a strong reference to a YapDatabaseConnection instance
		// - The [connection dealloc] call occurs within the snapshotQueue
		//
		// This is a workaround to ensure that the dealloc occurs outside the snapshotQueue.
		//
		[strongConnections removeAllObjects];
	}
	
	// dispatch_group_wait():
	// Returns zero on success (all blocks associated with the group completed before the specified timeout)
	// or non-zero on error (timeout occurred).
	//
	long ready = 0;
	if (group) {
		ready = dispatch_group_wait(group, dispatch_time(DISPATCH_TIME_NOW, (int64_t)(50 * NSEC_PER_MSEC)));
	}
	
	if (ready != 0)
	{
		YAPUnfairLockLock(&spinLock);
		{
			atomic_store(&hasWriteQueue, false);
		}
		YAPUnfairLockUnlock(&spinLock);
		
		return NO;
	}
	else
	{
		return YES;
	}
}

/**
 * Consulted by YapDatabaseConnection after performing a read-write transaction.
 *
 * When aggressive checkpointing is triggered,
 * the connections will perform a checkpoint after every read-write transaction.
**/
- (BOOL)aggressiveCheckpointEnabled
{
	return atomic_load(&aggressiveCheckpointEnabled);
}

- (void)noteCheckpointWithTotalFrames:(int)totalFrameCount checkpointedFrames:(int)checkpointedFrameCount
{
	uint64_t walApproximateFileSize = totalFrameCount * pageSize;
	
	if (walApproximateFileSize < options.aggressiveWALTruncationSize)
	{
		atomic_store(&aggressiveCheckpointEnabled, false);
	}
}

#ifdef DEBUG

// This method is only used by tests.
- (void)flushInternalQueue
{
    dispatch_sync(internalQueue, ^{ });
}

// This method is only used by tests.
- (void)flushCheckpointQueue
{
    dispatch_sync(checkpointQueue, ^{ });
}

#endif

@end
