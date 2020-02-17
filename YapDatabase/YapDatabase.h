#import <Foundation/Foundation.h>

#import "YapDatabaseTypes.h"
#import "YapDatabaseOptions.h"
#import "YapDatabaseConnection.h"
#import "YapDatabaseTransaction.h"
#import "YapDatabaseExtension.h"
#import "YapDatabaseConnectionConfig.h"

#import "YDBLogMessage.h"

NS_ASSUME_NONNULL_BEGIN

#if defined(SQLITE_HAS_CODEC) && defined(YAP_STANDARD_SQLITE)

	#error It seems you're using CocoaPods and you included YapDatabase and YapDatabase/Cipher pods. You just need to use "pod YapDatabase/SQLCipher"

#endif

/**
 * This notification is posted when a YapDatabase instance is deallocated,
 * and has thus closed all references to the underlying sqlite files.
 * 
 * If you intend to delete the sqlite file(s) from disk,
 * it's recommended you use this notification as a hook to do so.
 * 
 * More info:
 * The YapDatabase class itself is just a retainer for the filepath, blocks, config, etc.
 * And YapDatabaseConnection(s) open a sqlite connection to the database file,
 * and rely on the blocks & config in the parent YapDatabase class.
 * Thus a YapDatabaseConnection instance purposely retains the YapDatabase instance.
 * This means that in order to fully close all references to the underlying sqlite file(s),
 * you need to deallocate YapDatabase and all associated YapDatabaseConnections.
 * While this may be simple in concept, it's generally difficult to know exactly when all
 * the instances have been deallocated. Especially when there may be a bunch of asynchronous operations going.
 * 
 * Therefore the best approach is to do the following:
 * - destroy your YapDatabase instance (set it to nil)
 * - destroy all YapDatabaseConnection instances
 * - wait for YapDatabaseClosedNotification
 * - use notification as hook to delete all associated sqlite files from disk
 *
 * The userInfo dictionary will look like this:
 * @{
 *     YapDatabasePathKey    : <NSString of full filePath to db.sqlite file>,
 *     YapDatabasePathWalKey : <NSString of full filePath to db.sqlite-wal file>,
 *     YapDatabasePathShmKey : <NSString of full filePath to db.sqlite-shm file>,
 * }
 *
 * This notification is always posted to the main thread.
 */
extern NSString *const YapDatabaseClosedNotification;

extern NSString *const YapDatabaseUrlKey;
extern NSString *const YapDatabaseUrlWalKey;
extern NSString *const YapDatabaseUrlShmKey;

/**
 * This notification is posted following a readwrite transaction where the database was modified.
 * 
 * It is documented in more detail in the wiki article "YapDatabaseModifiedNotification":
 * https://github.com/yapstudios/YapDatabase/wiki/YapDatabaseModifiedNotification
 *
 * The notification object will be the database instance itself.
 * That is, it will be an instance of YapDatabase.
 *
 * This notification is only posted for internal modifications.
 * When the `enableMultiprocessSupport` option is set, external modification notifications are made
 * available by adding a `CrossProcessNotifier` extension to the database, and listening to the
 * `YapDatabaseModifiedExternallyNotification`.
 *
 * The userInfo dictionary will look something like this:
 * @{
 *     YapDatabaseSnapshotKey   : <NSNumber of snapshot, incremented per read-write transaction w/modification>,
 *     YapDatabaseConnectionKey : <YapDatabaseConnection instance that made the modification(s)>,
 *     YapDatabaseExtensionsKey : <NSDictionary with individual changeset info per extension>,
 *     YapDatabaseCustomKey     : <Optional object associated with this change, set by you>,
 * }
 *
 * This notification is always posted to the main thread.
 */
extern NSString *const YapDatabaseModifiedNotification;

/**
 * When the `enableMultiprocessSupport` option is set and a `CrossProcessNotifier` extension has been
 * added to the database, this notification is posted following a readwrite transaction where the
 * database was modified in another process.
 *
 * This notification is always posted to the main thread.
  */
extern NSString *const YapDatabaseModifiedExternallyNotification;

extern NSString *const YapDatabaseSnapshotKey;
extern NSString *const YapDatabaseConnectionKey;
extern NSString *const YapDatabaseExtensionsKey;
extern NSString *const YapDatabaseCustomKey;

extern NSString *const YapDatabaseObjectChangesKey;
extern NSString *const YapDatabaseMetadataChangesKey;
extern NSString *const YapDatabaseInsertedKeysKey;
extern NSString *const YapDatabaseRemovedKeysKey;
extern NSString *const YapDatabaseRemovedCollectionsKey;
extern NSString *const YapDatabaseAllKeysRemovedKey;
extern NSString *const YapDatabaseModifiedExternallyKey;

/**
 * Welcome to YapDatabase!
 *
 * The project page has a wealth of documentation if you have any questions.
 * https://github.com/yapstudios/YapDatabase
 *
 * If you're new to the project you may want to visit the wiki.
 * https://github.com/yapstudios/YapDatabase/wiki
 *
 * There are 3 primary classes you'll deal with:
 * - YapDatabase
 * - YapDatabaseConnection
 * - YapDatabaseTransaction
 *
 * YapDatabase represents the top-level class, and is used to initialize the database and customize default settings.
 *
 * To access or modify the database you create one or more connections to it. (YapDatabaseConnection)
 * Connections are thread-safe, and you can spawn multiple connections in order to achieve
 * concurrent access to the database from multiple threads. For example, you can read from the database
 * concurrently from multiple connections. And you can even read from the database while writing to it
 * from another connection.
 *
 * The process of reading or writing from the database happens via a transaction. (YapDatabaseTransaction)
 * You create a read-only or read-write transaction from a connection.
 * A transaction represents an atomic action within the database.
 */
@interface YapDatabase : NSObject

/**
 * The default database file URL.
 *
 * - macOS : ~/Library/Application Support/{Bundle Identifier}/yapdb.sqlite
 * - iOS   : {App Sandbox}/Application Support/yapdb.sqlite
 */
+ (NSURL *)defaultDatabaseURL;

/**
 * The default serializer & deserializer use NSCoding (NSKeyedArchiver & NSKeyedUnarchiver).
 * This is suitable for Objective-C, but not for Swift.
 *
 * **For Swift**:
 * It's likely you'll prefer to use the Codable protocol.
 * To do so, you simply register your Codable class for the corresponding collection:
 * ```
 * database.registerCodableSerialization(String.self, forCollection: "foo")
 * database.registerCodableSerialization(MyCodableClass.self, forCollection: "bar")
 * ```
 *
 * **For Objective-C**:
 * Any objects that support the NSCoding protocol can be used.
 * Most of Apple's primary data types support NSCoding out of the box.
 * And it's easy to add NSCoding support to your own custom objects.
 */
+ (YapDatabaseSerializer)defaultSerializer;

/**
 * The default serializer & deserializer use NSCoding (NSKeyedArchiver & NSKeyedUnarchiver).
 * This is suitable for Objective-C, but not for Swift.
 *
 * **For Swift**:
 * It's likely you'll prefer to use the Codable protocol.
 * To do so, you simply register your Codable class for the corresponding collection:
 * ```
 * database.registerCodableSerialization(String.self, forCollection: "foo")
 * database.registerCodableSerialization(MyCodableClass.self, forCollection: "bar")
 * ```
 *
 * **For Objective-C**:
 * Any objects that support the NSCoding protocol can be used.
 * Most of Apple's primary data types support NSCoding out of the box.
 * And it's easy to add NSCoding support to your own custom objects.
 */
+ (YapDatabaseDeserializer)defaultDeserializer;

/**
 * **Objective-C only**:
 *
 * Property lists ONLY support the following types:
 * - NSData
 * - NSString
 * - NSArray
 * - NSDictionary
 * - NSDate
 * - NSNumber
 *
 * Although limited in functionality, property lists are highly optimized and are used extensively by Apple.
 *
 * Property lists may make a good fit when your existing code already uses them,
 * such as replacing NSUserDefaults with a database.
 *
 * **For Swift**: @see `-[YapDatabase registerSerializer:forCollection:]`
 */
+ (YapDatabaseSerializer)propertyListSerializer;

/**
 * **Objective-C only**:
 *
 * Property lists ONLY support the following types:
 * - NSData
 * - NSString
 * - NSArray
 * - NSDictionary
 * - NSDate
 * - NSNumber
 *
 * Although limited in functionality, property lists are highly optimized and are used extensively by Apple.
 *
 * Property lists may make a good fit when your existing code already uses them,
 * such as replacing NSUserDefaults with a database.
 *
 * **For Swift**: @see `-[YapDatabase registerDeserializer:forCollection:]`
 */
+ (YapDatabaseDeserializer)propertyListDeserializer;

/**
 * **Objective-C only**:
 *
 * A FASTER serializer & deserializer than the default, if serializing ONLY a NSDate object.
 * You may want to use timestampSerializer & timestampDeserializer if your metadata is simply an NSDate.
 *
 * **For Swift**: @see `-[YapDatabase registerSerializer:forCollection:]`
 */
+ (YapDatabaseSerializer)timestampSerializer;

/**
 * **Objective-C only**:
 *
 * A FASTER serializer & deserializer than the default, if serializing ONLY a NSDate object.
 * You may want to use timestampSerializer & timestampDeserializer if your metadata is simply an NSDate.
 *
 * **For Swift**: @see `-[YapDatabase registerDeserializer:forCollection:]`
 */
+ (YapDatabaseDeserializer)timestampDeserializer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Logging
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Allows you to configure a handler for log messages emitted from the framework.
 *
 * A custom log handler allows you to integrate framework-emitted log messages into your desired logging system.
 *
 * If you don't configure your own log handler, then a default handler is used, which:
 * - only logs errors & warnings
 * - uses os_log
 */
+ (void)setLogHandler:(void (^)(YDBLogMessage *))logHandler;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Init
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Opens or creates a sqlite database with the default file URL.
 *
 * @see [YapDatabase defaultDatabaseURL]
 */
- (instancetype)init;

/**
 * Opens or creates a sqlite database with the given file URL. The defaults options are used.
 *
 * **Swift example**:
 * ```
 * let documenstDir = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
 * let databaseURL = baseDir.appendingPathComponent("yapdb.sqlite")
 * let database = YapDatabase(url: databaseURL)
 * ```
 *
 * @param path
 *   A fileURL that specifies where the database file should be stored.
 */
- (nullable instancetype)initWithURL:(NSURL *)path;

/**
 * Opens or creates a sqlite database with the given URL and options.
 *
 * This is typically used to configure encryption options for the databse.
 */
- (nullable instancetype)initWithURL:(NSURL *)path options:(nullable YapDatabaseOptions *)options;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Properties
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Returns that location of the database file.
 *
 * Keep in mind that sqlite actually creates 3 different files on disk:
 * - databaseName
 * - databaseName-wal
 * - databaseName-shm
 */
@property (nonatomic, strong, readonly) NSURL *databaseURL;

/**
 * Returns that location of the database file.
 *
 * Keep in mind that sqlite actually creates 3 different files on disk:
 * - databaseName
 * - databaseName-wal
 * - databaseName-shm
 */
@property (nonatomic, strong, readonly) NSURL *databaseURL_wal;

/**
 * Returns that location of the database file.
 *
 * Keep in mind that sqlite actually creates 3 different files on disk:
 * - databaseName
 * - databaseName-wal
 * - databaseName-shm
 */
@property (nonatomic, strong, readonly) NSURL *databaseURL_shm;

/**
 * The options that were specified when the database was created.
 *
 * @note Modifying these values AFTER that database has been initialized has no effect.
 */
@property (nonatomic, copy, readonly) YapDatabaseOptions *options;

/**
 * The snapshot number is the internal synchronization state primitive for the database.
 *
 * It's generally only useful for database internals,
 * but it can sometimes come in handy for general debugging of your app.
 *
 * The snapshot is a simple 64-bit number that gets incremented upon every read-write transaction
 * that makes modifications to the database. Thanks to the concurrent architecture of YapDatabase,
 * there may be multiple concurrent connections that are inspecting the database at similar times,
 * yet they are looking at slightly different "snapshots" of the database.
 *
 * The snapshot number may thus be inspected to determine (in a general fashion) what state the connection
 * is in compared with other connections.
 *
 * - `YapDatabase.snapshot` => most up-to-date snapshot among all connections
 * - `YapDatabaseConnection.snapshot` => snapshot of individual connection
 *
 * Example:
 * ```
 * let database = YapDatabase(url: url)
 * let _ = database.snapshot // returns zero
 *
 * let connection1 = database.newConnection()
 * let connection2 = database.newConnection()
 *
 * let _ = connection1.snapshot // returns zero
 * let _ = connection2.snapshot // returns zero
 *
 * connection1.readWrite {(transaction) in
 *    transaction.setObject(objectA, forKey:keyA, inCollection:nil)
 * }
 *
 * let _ = database.snapshot;    // returns 1
 * let _ = connection1.snapshot; // returns 1
 * let _ = connection2.snapshot; // returns 1
 *
 * connection1.asyncReadWrite({ (transaction) in
 *    transaction.setObject(objectB, forKey:keyB, inCollection:nil)
 *    Thread.sleep(forTimeInterval: 1.0) // sleep for 1 second
 *
 *    let _ = connection1.snapshot // returns 1 (we know it will turn into 2 once the transaction completes)
 *
 * }, completionBlock: {
 *
 *     connection1.snapshot; // returns 2
 * })
 *
 * connection2.asyncRead {(transaction) in
 *    Thread.sleep(forTimeInterval: 5.0) // sleep for 5 seconds
 *
 *    let _ = connection2.snapshot // returns 1. Understand why? See below.
 * }
 * ```
 * 
 * It's because when connection2 started its transaction, the database was in snapshot 1.
 * (Both connection1 & connection2 started an ASYNC transaction at the same time.)
 * Thus, for the duration of its transaction, the database remains in that state for connection2.
 *
 * However, once connection2 completes its transaction, it will automatically update itself to snapshot 2.
 *
 * In general, the snapshot is primarily for internal use.
 * However, it may come in handy for some tricky edge-case bugs.
 * (i.e. why doesn't my connection see that other commit ?)
 */
@property (atomic, assign, readonly) uint64_t snapshot;

/**
 * Returns the version of sqlite being used.
 *
 * E.g.: SELECT sqlite_version();
 */
@property (atomic, readonly) NSString *sqliteVersion;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Default Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
@property (atomic, readonly) YapDatabaseConnectionConfig *connectionDefaults;

/**
 * Registers a default serializer (object => data),
 * which will be used in cases where another serializer isn't configured for the collection.
 */
- (void)registerDefaultSerializer:(YapDatabaseSerializer)serializer;

/**
 * Registers a default deserializer (data => object),
 * which will be used in cases where another deserializer isn't configured for the collection.
 */
- (void)registerDefaultDeserializer:(YapDatabaseDeserializer)deserializer;

/**
 * Registers a default PreSanitizer,
 * which will be used in cases where another PreSanitizer isn't configured for the collection.
 */
- (void)registerDefaultPreSanitizer:(nullable YapDatabasePreSanitizer)preSanitizer;

/**
 * Registeres a default PostSanitizer,
 * which will be used in cases where another PostSanitizer isn't configured for the collection.
 */
- (void)registerDefaultPostSanitizer:(nullable YapDatabasePostSanitizer)postSanitizer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Per-Collection Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Registers a serializer (object => data) to be used for all **objects & metadata** in the given collection.
 */
- (void)registerSerializer:(YapDatabaseSerializer)serializer forCollection:(nullable NSString *)collection;

/**
 * Registers a deserializer (data => object) to be used for all **objects & metadata** in the given collection.
 */
- (void)registerDeserializer:(YapDatabaseDeserializer)deserializer forCollection:(nullable NSString *)collection;

/**
 * Registers a PreSanitizer to be used for all **objects & metadata** in the given collection.
 */
- (void)registerPreSanitizer:(YapDatabasePreSanitizer)preSanitizer forCollection:(nullable NSString *)collection;

/**
 * Registers a PostSanitizer to be used for all **objects & metadata** in the given collection.
 */
- (void)registerPostSanitizer:(YapDatabasePostSanitizer)postSanitizer forCollection:(nullable NSString *)collection;

/**
 * Registers a configuration group (for both objects & metadata) for an array of collections.
 *
 * This is equivalent to looping over the array, and invoking each individual registration method. (But faster.)
 */
- (void)registerSerializer:(nullable YapDatabaseSerializer)serializer
              deserializer:(nullable YapDatabaseDeserializer)deserializer
              preSanitizer:(nullable YapDatabasePreSanitizer)preSanitizer
             postSanitizer:(nullable YapDatabasePostSanitizer)postSanitizer
            forCollections:(NSArray<NSString*> *)collections;

/**
 * Registers a serializer (object => data) to be used for all objects in the given collection.
 *
 * @note: Passing nil for the collection is the equivalent of passing the empty string.
 */
- (void)registerObjectSerializer:(YapDatabaseSerializer)serializer forCollection:(nullable NSString *)collection;

/**
 * Registers a deserializer (data => object) to be used for all objects in the given collection.
 *
 * @note: Passing nil for the collection is the equivalent of passing the empty string.
 */
- (void)registerObjectDeserializer:(YapDatabaseDeserializer)deserializer forCollection:(nullable NSString *)collection;

/**
 * Registers a PreSanitizer to be used for all objects in the given collection.
 */
- (void)registerObjectPreSanitizer:(YapDatabasePreSanitizer)preSanitizer forCollection:(nullable NSString *)collection;

/**
 * Registers a PostSanitizer to be used for all objects in the given collection.s
 */
- (void)registerObjectPostSanitizer:(YapDatabasePostSanitizer)postSanitizer forCollection:(nullable NSString *)collection;

/**
 * Registers a serializer (object => data) to be used for all metadata in the given collection.
 */
- (void)registerMetadataSerializer:(YapDatabaseSerializer)serializer forCollection:(nullable NSString *)collection;

/**
 * Registers a deserializer (data => object) to be used for all metadata in the given collection.
 */
- (void)registerMetadataDeserializer:(YapDatabaseDeserializer)deserializer forCollection:(nullable NSString *)collection;

/**
 * Registers a PreSanitizer to be used for all metadata in the given collection.
 */
- (void)registerMetadataPreSanitizer:(YapDatabasePreSanitizer)preSanitizer forCollection:(nullable NSString *)collection;

/**
 * Registers a PostSanitizer to be used for all metadata in the given collection.
 */
- (void)registerMetadataPostSanitizer:(YapDatabasePostSanitizer)postSanitizer forCollection:(nullable NSString *)collection;

/**
 * Allows you to opt-in to various performance improvements,
 * which is generally dependent on the object types you're storing in each collection.
 *
 * The Object-Policy is documented on the wiki here:
 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
 */
- (void)setObjectPolicy:(YapDatabasePolicy)policy forCollection:(nullable NSString *)collection;

/**
 * Allows you to opt-in to various performance improvements,
 * which is generally dependent on the object types you're storing in each collection.
 *
 * This object policy will be used for all collections for which an explicit object
 * policy has not been set.
 *
 * The Object-Policy is documented on the wiki here:
 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
 */
- (void)setDefaultObjectPolicy:(YapDatabasePolicy)policy;

/**
 * Allows you to opt-in to various performance improvements,
 * which is generally dependent on the object types you're storing in each collection.
 *
 * The Object-Policy is documented on the wiki here:
 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
 */
- (void)setMetadataPolicy:(YapDatabasePolicy)policy forCollection:(nullable NSString *)collection;

/**
 * Allows you to opt-in to various performance improvements,
 * which is generally dependent on the object types you're storing in each collection.
 *
 * This metadata policy will be used for all collections for which an explicit metadata
 * policy has not been set.
 *
 * The Object-Policy is documented on the wiki here:
 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
*/
- (void)setDefaultMetadataPolicy:(YapDatabasePolicy)policy;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Connections
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates and returns a new connection to the database.
 * It is through this connection that you will access the database.
 * 
 * You can create multiple connections to the database.
 * Each invocation of this method creates and returns a new connection.
 * 
 * Multiple connections can simultaneously read from the database.
 * Multiple connections can simultaneously read from the database while another connection is modifying the database.
 * For example, the main thread could be reading from the database via connection A,
 * while a background thread is writing to the database via connection B.
 * 
 * However, only a single connection may be writing to the database at any one time.
 *
 * A connection is thread-safe, and operates by serializing access to itself.
 * Thus you can share a single connection between multiple threads.
 * But for conncurrent access between multiple threads you must use multiple connections.
 *
 * You should avoid creating more connections than you need.
 * Creating a new connection everytime you need to access the database is a recipe for foolishness.
 */
- (YapDatabaseConnection *)newConnection;

/**
 * Creates and returns a new connection to the database.
 * It is through this connection that you will access the database.
 *
 * You can create multiple connections to the database.
 * Each invocation of this method creates and returns a new connection.
 *
 * Multiple connections can simultaneously read from the database.
 * Multiple connections can simultaneously read from the database while another connection is modifying the database.
 * For example, the main thread could be reading from the database via connection A,
 * while a background thread is writing to the database via connection B.
 *
 * However, only a single connection may be writing to the database at any one time.
 *
 * A connection is thread-safe, and operates by serializing access to itself.
 * Thus you can share a single connection between multiple threads.
 * But for conncurrent access between multiple threads you must use multiple connections.
 *
 * You should avoid creating more connections than you need.
 * Creating a new connection everytime you need to access the database is a recipe for foolishness.
 *
 * @param config
 *   Allows you to specify the default configuration for the connection.
 *   If nil, then `-[YapDatabase connectionDefaults]` will be used instead.
 */
- (YapDatabaseConnection *)newConnection:(nullable YapDatabaseConnectionConfig *)config;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Registers the extension with the database using the given name.
 * After registration everything works automatically using just the extension name.
 * 
 * The registration process is equivalent to a (synchronous) readwrite transaction.
 * It involves persisting various information about the extension to the database,
 * as well as possibly populating the extension by enumerating existing rows in the database.
 *
 * @param extension
 *     The YapDatabaseExtension subclass instance you wish to register.
 *     For example, this might be a YapDatabaseView instance.
 * 
 * @param extensionName
 *     This is an arbitrary string you assign to the extension.
 *     Once registered, you will generally access the extension instance via this name.
 *     For example: [[transaction ext:@"myView"] numberOfGroups];
 *
 * @return
 *     YES if the extension was properly registered.
 *     NO if an error occurred, such as the extensionName is already registered.
 * 
 * @see asyncRegisterExtension:withName:completionBlock:
 * @see asyncRegisterExtension:withName:completionQueue:completionBlock:
 */
- (BOOL)registerExtension:(YapDatabaseExtension *)extension withName:(NSString *)extensionName;

/**
 * Registers the extension with the database using the given name.
 * After registration everything works automatically using just the extension name.
 *
 * The registration process is equivalent to a (synchronous) readwrite transaction.
 * It involves persisting various information about the extension to the database,
 * as well as possibly populating the extension by enumerating existing rows in the database.
 * 
 * @param extension (required)
 *     The YapDatabaseExtension subclass instance you wish to register.
 *     For example, this might be a YapDatabaseView instance.
 *
 * @param extensionName (required)
 *     This is an arbitrary string you assign to the extension.
 *     Once registered, you will generally access the extension instance via this name.
 *     For example: [[transaction ext:@"myView"] numberOfGroups];
 * 
 * @param config (optional)
 *     You may optionally pass a config for the internal databaseConnection used to perform
 *     the extension registration process. This allows you to control things such as the
 *     cache size, which is sometimes important for performance tuning.
 * 
 * @see asyncRegisterExtension:withName:completionBlock:
 * @see asyncRegisterExtension:withName:completionQueue:completionBlock:
 */
- (BOOL)registerExtension:(YapDatabaseExtension *)extension
                 withName:(NSString *)extensionName
                   config:(nullable YapDatabaseConnectionConfig *)config;

/**
 * Asynchronoulsy starts the extension registration process.
 * After registration everything works automatically using just the extension name.
 * 
 * The registration process is equivalent to an asyncReadwrite transaction.
 * It involves persisting various information about the extension to the database,
 * as well as possibly populating the extension by enumerating existing rows in the database.
 * 
 * @param extension (required)
 *     The YapDatabaseExtension subclass instance you wish to register.
 *     For example, this might be a YapDatabaseView instance.
 *
 * @param extensionName (required)
 *     This is an arbitrary string you assign to the extension.
 *     Once registered, you will generally access the extension instance via this name.
 *     For example: [[transaction ext:@"myView"] numberOfGroups];
 *
 * @param completionBlock (optional)
 *     An optional completion block may be used.
 *     If the extension registration was successful then the ready parameter will be YES.
 *     The completionBlock will be invoked on the main thread (dispatch_get_main_queue()).
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
               completionBlock:(nullable void(^)(BOOL ready))completionBlock;

/**
 * Asynchronoulsy starts the extension registration process.
 * After registration everything works automatically using just the extension name.
 *
 * The registration process is equivalent to an asyncReadwrite transaction.
 * It involves persisting various information about the extension to the database,
 * as well as possibly populating the extension by enumerating existing rows in the database.
 * 
 * @param extension (required)
 *     The YapDatabaseExtension subclass instance you wish to register.
 *     For example, this might be a YapDatabaseView instance.
 *
 * @param extensionName (required)
 *     This is an arbitrary string you assign to the extension.
 *     Once registered, you will generally access the extension instance via this name.
 *     For example: [[transaction ext:@"myView"] numberOfGroups];
 *
 * @param completionQueue (optional)
 *     The dispatch_queue to invoke the completion block may optionally be specified.
 *     If NULL, dispatch_get_main_queue() is automatically used.
 *
 * @param completionBlock (optional)
 *     An optional completion block may be used.
 *     If the extension registration was successful then the ready parameter will be YES.
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
               completionQueue:(nullable dispatch_queue_t)completionQueue
               completionBlock:(nullable void(^)(BOOL ready))completionBlock;

/**
 * Asynchronoulsy starts the extension registration process.
 * After registration everything works automatically using just the extension name.
 *
 * The registration process is equivalent to an asyncReadwrite transaction.
 * It involves persisting various information about the extension to the database,
 * as well as possibly populating the extension by enumerating existing rows in the database.
 * 
 * @param extension (required)
 *     The YapDatabaseExtension subclass instance you wish to register.
 *     For example, this might be a YapDatabaseView instance.
 *
 * @param extensionName (required)
 *     This is an arbitrary string you assign to the extension.
 *     Once registered, you will generally access the extension instance via this name.
 *     For example: [[transaction ext:@"myView"] numberOfGroups];
 * 
 * @param config (optional)
 *     You may optionally pass a config for the internal databaseConnection used to perform
 *     the extension registration process. This allows you to control things such as the
 *     cache size, which is sometimes important for performance tuning.
 *
 * @param completionBlock (optional)
 *     An optional completion block may be used.
 *     If the extension registration was successful then the ready parameter will be YES.
 *     The completionBlock will be invoked on the main thread (dispatch_get_main_queue()).
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
                        config:(nullable YapDatabaseConnectionConfig *)config
               completionBlock:(nullable void(^)(BOOL ready))completionBlock;

/**
 * Asynchronoulsy starts the extension registration process.
 * After registration everything works automatically using just the extension name.
 *
 * The registration process is equivalent to an asyncReadwrite transaction.
 * It involves persisting various information about the extension to the database,
 * as well as possibly populating the extension by enumerating existing rows in the database.
 * 
 * @param extension (required)
 *     The YapDatabaseExtension subclass instance you wish to register.
 *     For example, this might be a YapDatabaseView instance.
 *
 * @param extensionName (required)
 *     This is an arbitrary string you assign to the extension.
 *     Once registered, you will generally access the extension instance via this name.
 *     For example: [[transaction ext:@"myView"] numberOfGroups];
 * 
 * @param config (optional)
 *     You may optionally pass a config for the internal databaseConnection used to perform
 *     the extension registration process. This allows you to control things such as the
 *     cache size, which is sometimes important for performance tuning.
 *
 * @param completionQueue (optional)
 *     The dispatch_queue to invoke the completion block may optionally be specified.
 *     If NULL, dispatch_get_main_queue() is automatically used.
 *
 * @param completionBlock (optional)
 *     An optional completion block may be used.
 *     If the extension registration was successful then the ready parameter will be YES.
 */
- (void)asyncRegisterExtension:(YapDatabaseExtension *)extension
                      withName:(NSString *)extensionName
                        config:(nullable YapDatabaseConnectionConfig *)config
               completionQueue:(nullable dispatch_queue_t)completionQueue
               completionBlock:(nullable void(^)(BOOL ready))completionBlock;

/**
 * This method unregisters an extension with the given name.
 * The associated underlying tables will be dropped from the database.
 * 
 * The unregistration process is equivalent to a (synchronous) readwrite transaction.
 * It involves deleting various information about the extension from the database,
 * as well as possibly dropping related tables the extension may have been using.
 *
 * @param extensionName (required)
 *     This is the arbitrary string you assigned to the extension when you registered it.
 *
 * Note 1:
 *   You don't need to re-register an extension in order to unregister it. For example,
 *   you've previously registered an extension (in previous app launches), but you no longer need the extension.
 *   You don't have to bother creating and registering the unneeded extension,
 *   just so you can unregister it and have the associated tables dropped.
 *   The database persists information about registered extensions, including the associated class of an extension.
 *   So you can simply pass the name of the extension, and the database system will use the associated class to
 *   drop the appropriate tables.
 *
 * Note 2:
 *   In fact, you don't even have to worry about unregistering extensions that you no longer need.
 *   That database system will automatically handle it for you.
 *   That is, upon completion of the first readWrite transaction (that makes changes), the database system will
 *   check to see if there are any "orphaned" extensions. That is, previously registered extensions that are
 *   no longer in use (and are now out-of-date because they didn't process the recent change(s) to the db).
 *   And it will automatically unregister these orhpaned extensions for you.
 *       
 * @see asyncUnregisterExtensionWithName:completionBlock:
 * @see asyncUnregisterExtensionWithName:completionQueue:completionBlock:
 */
- (void)unregisterExtensionWithName:(NSString *)extensionName;

/**
 * Asynchronoulsy starts the extension unregistration process.
 *
 * The unregistration process is equivalent to an asyncReadwrite transaction.
 * It involves deleting various information about the extension from the database,
 * as well as possibly dropping related tables the extension may have been using.
 *
 * @param extensionName (required)
 *     This is the arbitrary string you assigned to the extension when you registered it.
 * 
 * @param completionBlock (optional)
 *     An optional completion block may be used.
 *     The completionBlock will be invoked on the main thread (dispatch_get_main_queue()).
 */
- (void)asyncUnregisterExtensionWithName:(NSString *)extensionName
                         completionBlock:(nullable dispatch_block_t)completionBlock;

/**
 * Asynchronoulsy starts the extension unregistration process.
 *
 * The unregistration process is equivalent to an asyncReadwrite transaction.
 * It involves deleting various information about the extension from the database,
 * as well as possibly dropping related tables the extension may have been using.
 *
 * @param extensionName (required)
 *     This is the arbitrary string you assigned to the extension when you registered it.
 * 
 * @param completionQueue (optional)
 *     The dispatch_queue to invoke the completion block may optionally be specified.
 *     If NULL, dispatch_get_main_queue() is automatically used.
 *
 * @param completionBlock (optional)
 *     An optional completion block may be used.
 */
- (void)asyncUnregisterExtensionWithName:(NSString *)extensionName
                         completionQueue:(nullable dispatch_queue_t)completionQueue
                         completionBlock:(nullable dispatch_block_t)completionBlock;

/**
 * Returns the registered extension with the given name.
 * The returned object will be a subclass of YapDatabaseExtension.
 */
- (nullable id)registeredExtension:(NSString *)extensionName;

/**
 * Returns all currently registered extensions as a dictionary.
 * The key is the registed name (NSString), and the value is the extension (YapDatabaseExtension subclass).
 */
- (nullable NSDictionary *)registeredExtensions;

/**
 * Allows you to fetch the registered extension names from the last time the database was run.
 * Typically this means from the last time the app was run.
 * 
 * This may be used to assist in various tasks, such as cleanup or upgrade tasks.
 * 
 * If you need this information, you should fetch it early on because YapDatabase only maintains this information
 * until it sees you are done registering all your initial extensions. That is, after one initializes the database
 * they then immediately register any needed initial extensions before they begin to use the database. Once a 
 * readWriteTransaction modifies the database, YapDatabase will take this opportunity to look for orphaned extensions.
 * These are extensions that were registered at the end of the last database session,
 * but which are no longer registered. YapDatabase will automatically cleanup these orphaned extensions,
 * and also clear the previouslyRegisteredExtensionNames information at this point.
 */
- (nullable NSArray<NSString *> *)previouslyRegisteredExtensionNames;

/**
 * It's sometimes useful to find out when all async registerExtension/unregisterExtension requests have completed.
 *
 * One way to accomplish this is simply to queue an asyncReadWriteTransaction on any databaseConnection.
 * Since all async register/unregister extension requests are immediately dispatch_async'd through the
 * internal serial writeQueue, you'll know that once your asyncReadWriteTransaction is running,
 * all previously scheduled register/unregister requests have completed.
 *
 * Although the above technique works, the 'flushExtensionRequestsWithCompletionQueue::'
 * is a more efficient way to accomplish this task. (And a more elegant & readable way too.)
 *
 * @param completionQueue
 *   The dispatch_queue to invoke the completionBlock on.
 *   If NULL, dispatch_get_main_queue() is automatically used.
 *
 * @param completionBlock
 *   The block to invoke once all previously scheduled register/unregister extension requests have completed.
  */
- (void)flushExtensionRequestsWithCompletionQueue:(nullable dispatch_queue_t)completionQueue
									       completionBlock:(nullable dispatch_block_t)completionBlock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Connection Pooling
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * As recommended in the "Performance Primer" ( https://github.com/yapstudios/YapDatabase/wiki/Performance-Primer )
 * 
 * > You should consider connections to be relatively heavy weight objects.
 * >
 * > OK, truth be told they're not really that heavy weight. I'm just trying to scare you.
 * > Because in terms of performance, you get a lot of bang for your buck if you recycle your connections.
 *
 * However, experience has shown how easy it is to neglect this information.
 * Perhaps because it's just so darn easy to create a connection that it becomes easy to forgot
 * that connections aren't free.
 * 
 * Whatever the reason, the connection pool was designed to alleviate some of the overhead.
 * The most expensive component of a connection is the internal sqlite database connection.
 * The connection pool keeps these internal sqlite database connections around in a pool to help recycle them.
 *
 * So when a connection gets deallocated, it returns the sqlite database connection to the pool.
 * And when a new connection gets created, it can recycle a sqlite database connection from the pool.
 * 
 * This property sets a maximum limit on the number of items that will get stored in the pool at any one time.
 * 
 * The default value is 5.
 * 
 * See also connectionPoolLifetime,
 * which allows you to set a maximum lifetime of connections sitting around in the pool.
 */
@property (atomic, assign, readwrite) NSUInteger maxConnectionPoolCount;

/**
 * The connection pool can automatically drop "stale" connections.
 * That is, if an item stays in the pool for too long (without another connection coming along and
 * removing it from the pool to be recycled) then the connection can optionally be removed and dropped.
 *
 * This is called the connection "lifetime".
 * 
 * That is, after an item is added to the connection pool to be recycled, a timer will be started.
 * If the connection is still in the pool when the timer goes off,
 * then the connection will automatically be removed and dropped.
 *
 * The default value is 90 seconds.
 * 
 * To disable the timer, set the lifetime to zero (or any non-positive value).
 * When disabled, open connections will remain in the pool indefinitely.
 */
@property (atomic, assign, readwrite) NSTimeInterval connectionPoolLifetime;

@end

NS_ASSUME_NONNULL_END
