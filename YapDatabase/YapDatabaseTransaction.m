#import "YapDatabaseTransaction.h"
#import "YapDatabasePrivate.h"
#import "YapDatabaseExtensionPrivate.h"
#import "YapDatabaseString.h"
#import "YapDatabaseLogging.h"
#import "YapCache.h"
#import "YapCollectionKey.h"
#import "YapTouch.h"
#import "YapNull.h"

#import <objc/runtime.h>

#if ! __has_feature(objc_arc)
#warning This file must be compiled with ARC. Use -fobjc-arc flag (or convert project to ARC).
#endif

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
**/
#if DEBUG
  static const int ydbLogLevel = YDB_LOG_LEVEL_INFO;
#else
  static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif


@implementation YapDatabaseReadTransaction

+ (void)load
{
	static BOOL loaded = NO;
	if (!loaded)
	{
		// Method swizzle:
		// Both extension: and ext: are designed to be the same method (with ext: shorthand for extension:).
		// So swap out the ext: method to point to extension:.
		
		Method extMethod = class_getInstanceMethod([self class], @selector(ext:));
		IMP extensionIMP = class_getMethodImplementation([self class], @selector(extension:));
		
		method_setImplementation(extMethod, extensionIMP);
		loaded = YES;
	}
}

- (id)initWithConnection:(YapDatabaseConnection *)aConnection isReadWriteTransaction:(BOOL)flag
{
	if ((self = [super init]))
	{
		connection = aConnection;
		isReadWriteTransaction = flag;
	}
	return self;
}

@synthesize connection = connection;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Transaction States
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)beginTransaction
{
	sqlite3_stmt *statement = [connection beginTransactionStatement];
	if (statement == NULL) return;
	
	// BEGIN TRANSACTION;
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Couldn't begin transaction: %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_reset(statement);
}

- (void)preCommitReadWriteTransaction
{
	// Step 1:
	//
	// Allow extensions to flush changes to the main database table.
	// This is different from flushing changes to their own private tables.
	// We're referring here to the main collection/key/value table that's public.
	
	__block BOOL restart;
	__block BOOL prevExtModifiesMainDatabaseTable;
	do
	{
		isMutated = NO;
		
		restart = NO;
		prevExtModifiesMainDatabaseTable = NO;
		
		[extensions enumerateKeysAndObjectsUsingBlock:^(id extNameObj, id extTransactionObj, BOOL *stop) {
			
			BOOL extModifiesMainDatabaseTable =
			  [(YapDatabaseExtensionTransaction *)extTransactionObj flushPendingChangesToMainDatabaseTable];
			
			if (extModifiesMainDatabaseTable)
			{
				if (!isMutated)
				{
					prevExtModifiesMainDatabaseTable = YES;
				}
				else
				{
					if (prevExtModifiesMainDatabaseTable)
					{
						restart = YES;
						*stop = YES;
					}
					else
					{
						prevExtModifiesMainDatabaseTable = YES;
					}
				}
			}
		}];
	
	} while (restart);
	
	// Step 2:
	//
	// Allow extensions to perform any "cleanup" code needed before the changesets are requested,
	// and before the commit is executed.
	
	[extensions enumerateKeysAndObjectsUsingBlock:^(id extNameObj, id extTransactionObj, BOOL *stop) {
		
		[(YapDatabaseExtensionTransaction *)extTransactionObj prepareChangeset];
	}];
}

- (void)commitTransaction
{
	if (isReadWriteTransaction)
	{
		[extensions enumerateKeysAndObjectsUsingBlock:^(id extNameObj, id extTransactionObj, BOOL *stop) {
			
			[(YapDatabaseExtensionTransaction *)extTransactionObj commitTransaction];
		}];
		
		[yapMemoryTableTransaction commit];
	}
	
	sqlite3_stmt *statement = [connection commitTransactionStatement];
	if (statement)
	{
		// COMMIT TRANSACTION;
		
		int status = sqlite3_step(statement);
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"Couldn't commit transaction: %d %s", status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_reset(statement);
	}
}

- (void)rollbackTransaction
{
	[extensions enumerateKeysAndObjectsUsingBlock:^(id extNameObj, id extTransactionObj, BOOL *stop) {
		
		[(YapDatabaseExtensionTransaction *)extTransactionObj rollbackTransaction];
	}];
	
	[yapMemoryTableTransaction rollback];
	
	sqlite3_stmt *statement = [connection rollbackTransactionStatement];
	if (statement)
	{
		// ROLLBACK TRANSACTION;
		
		int status = sqlite3_step(statement);
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"Couldn't rollback transaction: %d %s", status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_reset(statement);
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Count
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSUInteger)numberOfCollections
{
	sqlite3_stmt *statement = [connection getCollectionCountStatement];
	if (statement == NULL) return 0;
	
	// SELECT COUNT(DISTINCT collection) AS NumberOfRows FROM "database2";
	
	NSUInteger result = 0;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		result = (NSUInteger)sqlite3_column_int64(statement, 0);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getCollectionCountStatement': %d %s",
		            status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_reset(statement);
	
	return result;
}

- (NSUInteger)numberOfKeysInCollection:(NSString *)collection
{
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection getKeyCountForCollectionStatement];
	if (statement == NULL) return 0;
	
	// SELECT COUNT(*) AS NumberOfRows FROM "database2" WHERE "collection" = ?;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	NSUInteger result = 0;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		result = (NSUInteger)sqlite3_column_int64(statement, 0);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getKeyCountForCollectionStatement': %d %s",
		            status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	
	return result;
}

- (NSUInteger)numberOfKeysInAllCollections
{
	sqlite3_stmt *statement = [connection getKeyCountForAllStatement];
	if (statement == NULL) return 0;
	
	// SELECT COUNT(*) AS NumberOfRows FROM "database2";
	
	NSUInteger result = 0;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		result = (NSUInteger)sqlite3_column_int64(statement, 0);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getKeyCountForAllStatement': %d %s",
		            status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_reset(statement);
	
	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark List
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSArray *)allCollections
{
	sqlite3_stmt *statement = [connection enumerateCollectionsStatement];
	if (statement == NULL) return nil;
	
	// SELECT DISTINCT "collection" FROM "database2";";
	
	NSMutableArray *result = [NSMutableArray array];
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			const unsigned char *text = sqlite3_column_text(statement, 0);
			int textSize = sqlite3_column_bytes(statement, 0);
			
			NSString *collection = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			[result addObject:collection];
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	sqlite3_reset(statement);
	
	return result;
}

- (NSArray *)allKeysInCollection:(NSString *)collection
{
	NSUInteger count = [self numberOfKeysInCollection:collection];
	NSMutableArray *result = [NSMutableArray arrayWithCapacity:count];
	
	[self _enumerateKeysInCollection:collection usingBlock:^(int64_t rowid, NSString *key, BOOL *stop) {
		
		[result addObject:key];
	}];
	
	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Internal (using rowid)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (BOOL)getRowid:(int64_t *)rowidPtr forKey:(NSString *)key inCollection:(NSString *)collection
{
	if (key == nil) {
		if (rowidPtr) *rowidPtr = 0;
		return NO;
	}
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection getRowidForKeyStatement];
	if (statement == NULL) {
		if (rowidPtr) *rowidPtr = 0;
		return NO;
	}
	
	// SELECT "rowid" FROM "database2" WHERE "collection" = ? AND "key" = ?;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length,  SQLITE_STATIC);
	
	int64_t rowid = 0;
	BOOL result = NO;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		rowid = sqlite3_column_int64(statement, 0);
		result = YES;
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getRowidForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);
	
	if (rowidPtr) *rowidPtr = rowid;
	return result;
}

- (YapCollectionKey *)collectionKeyForRowid:(int64_t)rowid
{
	NSNumber *rowidNumber = @(rowid);
	
	YapCollectionKey *collectionKey = [connection->keyCache objectForKey:rowidNumber];
	if (collectionKey)
	{
		return collectionKey;
	}
	
	sqlite3_stmt *statement = [connection getKeyForRowidStatement];
	if (statement == NULL) {
		return nil;
	}
	
	// SELECT "collection", "key" FROM "database2" WHERE "rowid" = ?;
	
	sqlite3_bind_int64(statement, 1, rowid);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const unsigned char *text0 = sqlite3_column_text(statement, 0);
		int textSize0 = sqlite3_column_bytes(statement, 0);
		
		const unsigned char *text1 = sqlite3_column_text(statement, 1);
		int textSize1 = sqlite3_column_bytes(statement, 1);
		
		NSString *collection = [[NSString alloc] initWithBytes:text0 length:textSize0 encoding:NSUTF8StringEncoding];
		NSString *key        = [[NSString alloc] initWithBytes:text1 length:textSize1 encoding:NSUTF8StringEncoding];
		
		collectionKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
		
		[connection->keyCache setObject:collectionKey forKey:rowidNumber];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getKeyForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	return collectionKey;
}

- (BOOL)getCollectionKey:(YapCollectionKey **)collectionKeyPtr object:(id *)objectPtr forRowid:(int64_t)rowid
{
	YapCollectionKey *collectionKey = [self collectionKeyForRowid:rowid];
	if (collectionKey)
	{
		id object = [self objectForCollectionKey:collectionKey withRowid:rowid];
	
		if (collectionKeyPtr) *collectionKeyPtr = collectionKey;
		if (objectPtr) *objectPtr = object;
		return YES;
	}
	else
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		if (objectPtr) *objectPtr = nil;
		return NO;
	}
}

- (BOOL)getCollectionKey:(YapCollectionKey **)collectionKeyPtr metadata:(id *)metadataPtr forRowid:(int64_t)rowid
{
	YapCollectionKey *collectionKey = [self collectionKeyForRowid:rowid];
	if (collectionKey)
	{
		id metadata = [self metadataForCollectionKey:collectionKey withRowid:rowid];
		
		if (collectionKeyPtr) *collectionKeyPtr = collectionKey;
		if (metadataPtr) *metadataPtr = metadata;
		return YES;
	}
	else
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		return NO;
	}
}

- (BOOL)getCollectionKey:(YapCollectionKey **)collectionKeyPtr
                  object:(id *)objectPtr
                metadata:(id *)metadataPtr
                forRowid:(int64_t)rowid
{
	YapCollectionKey *collectionKey = [self collectionKeyForRowid:rowid];
	if (collectionKey == nil)
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		if (objectPtr) *objectPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		return NO;
	}
	
	if ([self getObject:objectPtr metadata:metadataPtr forCollectionKey:collectionKey withRowid:rowid])
	{
		if (collectionKeyPtr) *collectionKeyPtr = collectionKey;
		return YES;
	}
	else
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		return NO;
	}
}

- (BOOL)hasRowid:(int64_t)rowid
{
	sqlite3_stmt *statement = [connection getCountForRowidStatement];
	if (statement == NULL) return NO;
	
	// SELECT COUNT(*) AS NumberOfRows FROM "database2" WHERE "rowid" = ?;
	
	sqlite3_bind_int64(statement, 1, rowid);
	
	BOOL result = NO;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		result = (sqlite3_column_int64(statement, 0) > 0);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getCountForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	return result;
}

- (id)objectForKey:(NSString *)key inCollection:(NSString *)collection withRowid:(int64_t)rowid
{
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	return [self objectForCollectionKey:cacheKey withRowid:rowid];
}

- (id)objectForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid
{
	if (cacheKey == nil) return nil;
	
	id object = [connection->objectCache objectForKey:cacheKey];
	if (object)
		return object;
	
	sqlite3_stmt *statement = [connection getDataForRowidStatement];
	if (statement == NULL) return nil;
	
	// SELECT "data" FROM "database2" WHERE "rowid" = ?;
	
	sqlite3_bind_int64(statement, 1, rowid);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
		
		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		object = connection->database->objectDeserializer(cacheKey.collection, cacheKey.key, data);
		
		if (object)
			[connection->objectCache setObject:object forKey:cacheKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getDataForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	return object;
}

- (id)metadataForKey:(NSString *)key inCollection:(NSString *)collection withRowid:(int64_t)rowid
{
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	return [self metadataForCollectionKey:cacheKey withRowid:rowid];
}

- (id)metadataForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid
{
	if (cacheKey == nil) return nil;
	
	id metadata = [connection->metadataCache objectForKey:cacheKey];
	if (metadata)
		return metadata;
	
	sqlite3_stmt *statement = [connection getMetadataForRowidStatement];
	if (statement == NULL) return nil;
	
	// SELECT "metadata" FROM "database2" WHERE "rowid" = ?;
	
	sqlite3_bind_int64(statement, 1, rowid);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
		
		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		metadata = connection->database->metadataDeserializer(cacheKey.collection, cacheKey.key, data);
		
		if (metadata)
			[connection->metadataCache setObject:metadata forKey:cacheKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getMetadataForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	return metadata;
}

- (BOOL)getObject:(id *)objectPtr
		 metadata:(id *)metadataPtr
 forCollectionKey:(YapCollectionKey *)collectionKey
		withRowid:(int64_t)rowid
{
	id object = [connection->objectCache objectForKey:collectionKey];
	id metadata = [connection->metadataCache objectForKey:collectionKey];
	
	if (object || metadata)
	{
		if (object == nil)
		{
			object = [self objectForCollectionKey:collectionKey withRowid:rowid];
		}
		else if (metadata == nil)
		{
			metadata = [self metadataForCollectionKey:collectionKey withRowid:rowid];
		}
		
		if (objectPtr) *objectPtr = object;
		if (metadataPtr) *metadataPtr = metadata;
		return YES;
	}
	
	sqlite3_stmt *statement = [connection getAllForRowidStatement];
	if (statement == NULL) {
		if (objectPtr) *objectPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		return NO;
	}
	
	// SELECT "data", "metadata" FROM "database2" WHERE "rowid" = ?;
	
	sqlite3_bind_int64(statement, 1, rowid);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
		
		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		object = connection->database->objectDeserializer(collectionKey.collection, collectionKey.key, data);
		
		if (object)
			[connection->objectCache setObject:object forKey:collectionKey];
		
		const void *mBlob = sqlite3_column_blob(statement, 1);
		int mBlobSize = sqlite3_column_bytes(statement, 1);
		
		if (mBlobSize > 0)
		{
			// Performance tuning:
			// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
			
			NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
			metadata = connection->database->metadataDeserializer(collectionKey.collection, collectionKey.key, mData);
		}
		
		if (metadata)
			[connection->metadataCache setObject:metadata forKey:collectionKey];
		else
			[connection->metadataCache setObject:[YapNull null] forKey:collectionKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getKeyForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	if (objectPtr) *objectPtr = object;
	if (metadataPtr) *metadataPtr = metadata;
	return YES;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Object & Metadata
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (id)objectForKey:(NSString *)key inCollection:(NSString *)collection
{
	if (key == nil) return nil;
	if (collection == nil) collection = @"";
	
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	id object = [connection->objectCache objectForKey:cacheKey];
	if (object)
		return object;
	
	sqlite3_stmt *statement = [connection getDataForKeyStatement];
	if (statement == NULL) return nil;
	
	// SELECT "data" FROM "database2" WHERE "collection" = ? AND "key" = ?;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
		// Be sure not to call sqlite3_reset until we're done with the data.
		
		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		object = connection->database->objectDeserializer(collection, key, data);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getDataForKeyStatement': %d %s, key(%@)",
		                                                    status, sqlite3_errmsg(connection->db), key);
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);
	
	if (object)
		[connection->objectCache setObject:object forKey:cacheKey];
	
	return object;
}

- (BOOL)hasObjectForKey:(NSString *)key inCollection:(NSString *)collection
{
	if (key == nil) return NO;
	if (collection == nil) collection = @"";
	
	// Shortcut:
	// We may not need to query the database if we have the key in any of our caches.
	
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	if ([connection->objectCache objectForKey:cacheKey]) return YES;
	if ([connection->metadataCache objectForKey:cacheKey]) return YES;
	
	// The normal SQL way
	
	return [self getRowid:NULL forKey:key inCollection:collection];
}

- (BOOL)getObject:(id *)objectPtr metadata:(id *)metadataPtr forKey:(NSString *)key inCollection:(NSString *)collection
{
	if (key == nil)
	{
		if (objectPtr) *objectPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		
		return NO;
	}
	if (collection == nil) collection = @"";
	
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	id object = [connection->objectCache objectForKey:cacheKey];
	id metadata = [connection->metadataCache objectForKey:cacheKey];
	
	BOOL found = NO;
	
	if (object && metadata)
	{
		// Both object and metadata were in cache.
		found = YES;
		
		// Need to check for empty metadata placeholder from cache.
		if (metadata == [YapNull null])
			metadata = nil;
	}
	else if (!object && metadata)
	{
		// Metadata was in cache.
		found = YES;
		
		// Need to check for empty metadata placeholder from cache.
		if (metadata == [YapNull null])
			metadata = nil;
		
		// Missing object. Fetch individually if requested.
		if (objectPtr)
			object = [self objectForKey:key inCollection:collection];
	}
	else if (object && !metadata)
	{
		// Object was in cache.
		found = YES;
		
		// Missing metadata. Fetch individually if requested.
		if (metadataPtr)
			metadata = [self metadataForKey:key inCollection:collection];
	}
	else // (!object && !metadata)
	{
		// Both object and metadata are missing.
		// Fetch via query.
		
		sqlite3_stmt *statement = [connection getAllForKeyStatement];
		if (statement)
		{
			// SELECT "data", "metadata" FROM "database2" WHERE "collection" = ? AND "key" = ? ;
			
			YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
			sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
			
			YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
			sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
			
			int status = sqlite3_step(statement);
			if (status == SQLITE_ROW)
			{
				if (connection->needsMarkSqlLevelSharedReadLock)
					[connection markSqlLevelSharedReadLockAcquired];
				
				const void *oBlob = sqlite3_column_blob(statement, 0);
				int oBlobSize = sqlite3_column_bytes(statement, 0);
				
				const void *mBlob = sqlite3_column_blob(statement, 1);
				int mBlobSize = sqlite3_column_bytes(statement, 1);
				
				if (objectPtr)
				{
					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);
				}
				
				if (metadataPtr && mBlobSize > 0)
				{
					NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
					metadata = connection->database->metadataDeserializer(collection, key, mData);
				}
				
				found = YES;
			}
			else if (status == SQLITE_ERROR)
			{
				YDBLogError(@"Error executing 'getAllForKeyStatement': %d %s",
				                                                   status, sqlite3_errmsg(connection->db));
			}
			
			if (object)
			{
				[connection->objectCache setObject:object forKey:cacheKey];
				
				if (metadata)
					[connection->metadataCache setObject:metadata forKey:cacheKey];
				else
					[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
			}
			
			sqlite3_clear_bindings(statement);
			sqlite3_reset(statement);
			FreeYapDatabaseString(&_collection);
			FreeYapDatabaseString(&_key);
		}
	}
	
	if (objectPtr) *objectPtr = object;
	if (metadataPtr) *metadataPtr = metadata;
	
	return found;
}

- (id)metadataForKey:(NSString *)key inCollection:(NSString *)collection
{
	if (key == nil) return nil;
	if (collection == nil) collection = @"";
	
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	id metadata = [connection->metadataCache objectForKey:cacheKey];
	if (metadata)
	{
		if (metadata == [YapNull null])
			return nil;
		else
			return metadata;
	}
	
	sqlite3_stmt *statement = [connection getMetadataForKeyStatement];
	if (statement == NULL) return nil;
	
	// SELECT "metadata" FROM "database2" WHERE "collection" = ? AND "key" = ? ;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	BOOL found = NO;
	NSData *metadataData = nil;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		found = YES;
		
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		// Performance tuning:
		//
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
		// But be sure not to call sqlite3_reset until we're done with the data.
		
		if (blobSize > 0)
			metadataData = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getMetadataForKeyStatement': %d %s",
		                                                        status, sqlite3_errmsg(connection->db));
	}
	
	if (found)
	{
		if (metadataData)
			metadata = connection->database->metadataDeserializer(collection, key, metadataData);
		
		if (metadata)
			[connection->metadataCache setObject:metadata forKey:cacheKey];
		else
			[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);
	
	return metadata;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Primitive
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Primitive access.
 * This method is available in-case you have a need to fetch the raw serializedObject from the database.
 *
 * This method is slower than objectForKey:inCollection:, since that method makes use of the objectCache.
 * In contrast, this method always fetches the raw data from disk.
 *
 * @see objectForKey:inCollection:
**/
- (NSData *)serializedObjectForKey:(NSString *)key inCollection:(NSString *)collection;
{
	if (key == nil) return nil;
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection getDataForKeyStatement];
	if (statement == NULL) return nil;
	
	NSData *result = nil;
	
	// SELECT "data" FROM "database2" WHERE "collection" = ? AND "key" = ?;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length,  SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		result = [[NSData alloc] initWithBytes:blob length:blobSize];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getDataForKeyStatement': %d %s, key(%@)",
		                                                    status, sqlite3_errmsg(connection->db), key);
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);

	return result;
}

/**
 * Primitive access.
 * This method is available in-case you have a need to fetch the raw serializedMetadata from the database.
 *
 * This method is slower than metadataForKey:inCollection:, since that method makes use of the metadataCache.
 * In contrast, this method always fetches the raw data from disk.
 *
 * @see metadataForKey:inCollection:
**/
- (NSData *)serializedMetadataForKey:(NSString *)key inCollection:(NSString *)collection
{
	if (key == nil) return nil;
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection getMetadataForKeyStatement];
	if (statement == NULL) return nil;
	
	// SELECT "metadata" FROM "database2" WHERE "collection" = ? AND "key" = ? ;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	NSData *result = nil;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		result = [[NSData alloc] initWithBytes:blob length:blobSize];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getDataForKeyStatement': %d %s, key(%@)",
					status, sqlite3_errmsg(connection->db), key);
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);
	
	return result;
}

/**
 * Primitive access.
 * This method is available in-case you have a need to fetch the raw serialized forms from the database.
 *
 * This method is slower than getObject:metadata:forKey:inCollection:, since that method makes use of the caches.
 * In contrast, this method always fetches the raw data from disk.
 *
 * @see getObject:metadata:forKey:inCollection:
**/
- (BOOL)getSerializedObject:(NSData **)serializedObjectPtr
         serializedMetadata:(NSData **)serializedMetadataPtr
                     forKey:(NSString *)key
               inCollection:(NSString *)collection
{
	if (key == nil) {
		if (serializedObjectPtr) *serializedObjectPtr = nil;
		if (serializedMetadataPtr) *serializedMetadataPtr = nil;
		return NO;
	}
	if (collection == nil) collection = @"";
		
	sqlite3_stmt *statement = [connection getAllForKeyStatement];
	if (statement == NULL) {
		if (serializedObjectPtr) *serializedObjectPtr = nil;
		if (serializedMetadataPtr) *serializedMetadataPtr = nil;
		return NO;
	}
	
	NSData *serializedObject = nil;
	NSData *serializedMetadata = nil;
	
	BOOL found = NO;
	
	// SELECT "data", "metadata" FROM "database2" WHERE "collection" = ? AND "key" = ? ;
		
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
		
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		const void *oBlob = sqlite3_column_blob(statement, 0);
		int oBlobSize = sqlite3_column_bytes(statement, 0);
		
		const void *mBlob = sqlite3_column_blob(statement, 1);
		int mBlobSize = sqlite3_column_bytes(statement, 1);
		
		if (serializedObjectPtr) {
			serializedObject = [NSData dataWithBytes:(void *)oBlob length:oBlobSize];
		}
		
		if (serializedMetadataPtr) {
			serializedMetadata = [NSData dataWithBytes:(void *)mBlob length:mBlobSize];
		}
		
		found = YES;
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getAllForKeyStatement': %d %s",
		                                                   status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);
	
	if (serializedObjectPtr) *serializedObjectPtr = serializedObject;
	if (serializedMetadataPtr) *serializedMetadataPtr = serializedMetadata;
	
	return found;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Enumerate
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Fast enumeration over all the collections in the database.
 * 
 * This uses a "SELECT collection FROM database" operation,
 * and then steps over the results invoking the given block handler.
**/
- (void)enumerateCollectionsUsingBlock:(void (^)(NSString *collection, BOOL *stop))block
{
	if (block == NULL) return;
	
	sqlite3_stmt *statement = [connection enumerateCollectionsStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT DISTINCT "collection" FROM "database2";
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			const unsigned char *text = sqlite3_column_text(statement, 0);
			int textSize = sqlite3_column_bytes(statement, 0);
			
			NSString *collection = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			block(collection, &stop);
			
			if (stop || isMutated) break;
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_reset(statement);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * This method is rarely needed, but may be helpful in certain situations.
 * 
 * This method may be used if you have the key, but not the collection for a particular item.
 * Please note that this is not the ideal situation.
 * 
 * Since there may be numerous collections for a given key, this method enumerates all possible collections.
**/
- (void)enumerateCollectionsForKey:(NSString *)key usingBlock:(void (^)(NSString *collection, BOOL *stop))block
{
	if (key == nil) return;
	if (block == NULL) return;
	
	sqlite3_stmt *statement = [connection enumerateCollectionsForKeyStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "collection" FROM "database2" WHERE "key" = ?;
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 1, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			const unsigned char *text = sqlite3_column_text(statement, 0);
			int textSize = sqlite3_column_bytes(statement, 0);
			
			NSString *collection = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			block(collection, &stop);
			
			if (stop || isMutated) break;
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over all keys in the given collection.
 *
 * This uses a "SELECT key FROM database WHERE collection = ?" operation,
 * and then steps over the results invoking the given block handler.
**/
- (void)enumerateKeysInCollection:(NSString *)collection
                       usingBlock:(void (^)(NSString *key, BOOL *stop))block
{
	if (block == NULL) return;
	
	[self _enumerateKeysInCollection:collection usingBlock:^(int64_t rowid, NSString *key, BOOL *stop) {
		
		block(key, stop);
	}];
}

/**
 * Fast enumeration over all keys in the given collection.
 *
 * This uses a "SELECT collection, key FROM database" operation,
 * and then steps over the results invoking the given block handler.
**/
- (void)enumerateKeysInAllCollectionsUsingBlock:(void (^)(NSString *collection, NSString *key, BOOL *stop))block
{
	if (block == NULL) return;
	
	[self _enumerateKeysInAllCollectionsUsingBlock:^(int64_t rowid, NSString *collection, NSString *key, BOOL *stop) {
		
		block(collection, key, stop);
	}];
}

/**
 * Fast enumeration over all keys and associated metadata in the given collection.
 * 
 * This uses a "SELECT key, metadata FROM database WHERE collection = ?" operation and steps over the results.
 * 
 * If you only need to enumerate over certain items (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the deserialization step for those items you're not interested in.
 * 
 * Keep in mind that you cannot modify the collection mid-enumeration (just like any other kind of enumeration).
**/
- (void)enumerateKeysAndMetadataInCollection:(NSString *)collection
                                  usingBlock:(void (^)(NSString *key, id metadata, BOOL *stop))block
{
	[self enumerateKeysAndMetadataInCollection:collection usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over all keys and associated metadata in the given collection.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
 * 
 * Keep in mind that you cannot modify the collection mid-enumeration (just like any other kind of enumeration).
**/
- (void)enumerateKeysAndMetadataInCollection:(NSString *)collection
                                  usingBlock:(void (^)(NSString *key, id metadata, BOOL *stop))block
                                  withFilter:(BOOL (^)(NSString *key))filter
{
	if (block == NULL) return;
	
	if (filter)
	{
		[self _enumerateKeysAndMetadataInCollection:collection
		                                 usingBlock:^(int64_t rowid, NSString *key, id metadata, BOOL *stop) {
		
			block(key, metadata, stop);
			
		} withFilter:^BOOL(int64_t rowid, NSString *key) {
			
			return filter(key);
		}];
	}
	else
	{
		[self _enumerateKeysAndMetadataInCollection:collection
		                                 usingBlock:^(int64_t rowid, NSString *key, id metadata, BOOL *stop) {
		
			block(key, metadata, stop);
			
		} withFilter:NULL];
	}
}

/**
 * Fast enumeration over all key/metadata pairs in all collections.
 * 
 * This uses a "SELECT metadata FROM database ORDER BY collection ASC" operation, and steps over the results.
 * 
 * If you only need to enumerate over certain objects (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the deserialization step for those objects you're not interested in.
 * 
 * Keep in mind that you cannot modify the database mid-enumeration (just like any other kind of enumeration).
**/
- (void)enumerateKeysAndMetadataInAllCollectionsUsingBlock:
                                        (void (^)(NSString *collection, NSString *key, id metadata, BOOL *stop))block
{
	[self enumerateKeysAndMetadataInAllCollectionsUsingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over all key/metadata pairs in all collections.
 *
 * This uses a "SELECT metadata FROM database ORDER BY collection ASC" operation and steps over the results.
 * 
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
 *
 * Keep in mind that you cannot modify the database mid-enumeration (just like any other kind of enumeration).
 **/
- (void)enumerateKeysAndMetadataInAllCollectionsUsingBlock:
                                        (void (^)(NSString *collection, NSString *key, id metadata, BOOL *stop))block
                             withFilter:(BOOL (^)(NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	
	if (filter)
	{
		[self _enumerateKeysAndMetadataInAllCollectionsUsingBlock:
		    ^(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop) {
			
			block(collection, key, metadata, stop);
			
		} withFilter:^BOOL(int64_t rowid, NSString *collection, NSString *key) {
			
			return filter(collection, key);
		}];
	}
	else
	{
		[self _enumerateKeysAndMetadataInAllCollectionsUsingBlock:
		    ^(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop) {
			
			block(collection, key, metadata, stop);
			
		} withFilter:NULL];
	}
}

/**
 * Fast enumeration over all objects in the database.
 *
 * This uses a "SELECT key, object from database WHERE collection = ?" operation, and then steps over the results,
 * deserializing each object, and then invoking the given block handler.
 *
 * If you only need to enumerate over certain objects (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those objects you're not interested in.
**/
- (void)enumerateKeysAndObjectsInCollection:(NSString *)collection
                                 usingBlock:(void (^)(NSString *key, id object, BOOL *stop))block
{
	[self enumerateKeysAndObjectsInCollection:collection usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over objects in the database for which you're interested in.
 * The filter block allows you to decide which objects you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
**/
- (void)enumerateKeysAndObjectsInCollection:(NSString *)collection
                                 usingBlock:(void (^)(NSString *key, id object, BOOL *stop))block
                                 withFilter:(BOOL (^)(NSString *key))filter
{
	if (block == NULL) return;
	
	if (filter)
	{
		[self _enumerateKeysAndObjectsInCollection:collection
		                                usingBlock:^(int64_t rowid, NSString *key, id object, BOOL *stop) {
			
			block(key, object, stop);
			
		} withFilter:^BOOL(int64_t rowid, NSString *key) {
			
			return filter(key);
		}];
	}
	else
	{
		[self _enumerateKeysAndObjectsInCollection:collection
		                                usingBlock:^(int64_t rowid, NSString *key, id object, BOOL *stop) {
			
			block(key, object, stop);
			
		} withFilter:NULL];
	}
}

/**
 * Enumerates all key/object pairs in all collections.
 *
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 *
 * If you only need to enumerate over certain objects (e.g. subset of collections, or keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those objects you're not interested in.
**/
- (void)enumerateKeysAndObjectsInAllCollectionsUsingBlock:
                            (void (^)(NSString *collection, NSString *key, id object, BOOL *stop))block
{
	[self enumerateKeysAndObjectsInAllCollectionsUsingBlock:block withFilter:NULL];
}

/**
 * Enumerates all key/object pairs in all collections.
 * The filter block allows you to decide which objects you're interested in.
 *
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given
 * collection/key pair. If the filter block returns NO, then the block handler is skipped for the given pair,
 * which avoids the cost associated with deserializing the object.
**/
- (void)enumerateKeysAndObjectsInAllCollectionsUsingBlock:
                            (void (^)(NSString *collection, NSString *key, id object, BOOL *stop))block
                 withFilter:(BOOL (^)(NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	
	if (filter)
	{
		[self _enumerateKeysAndObjectsInAllCollectionsUsingBlock:
		    ^(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop) {
			
			block(collection, key, object, stop);
			
		} withFilter:^BOOL(int64_t rowid, NSString *collection, NSString *key) {
			
			return filter(collection, key);
		}];
	}
	else
	{
		[self _enumerateKeysAndObjectsInAllCollectionsUsingBlock:
		    ^(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop) {
			
			block(collection, key, object, stop);
			
		} withFilter:NULL];
	}
}

/**
 * Fast enumeration over all rows in the database.
 *
 * This uses a "SELECT key, data, metadata from database WHERE collection = ?" operation,
 * and then steps over the results, deserializing each object & metadata, and then invoking the given block handler.
 *
 * If you only need to enumerate over certain rows (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those rows you're not interested in.
**/
- (void)enumerateRowsInCollection:(NSString *)collection
                       usingBlock:(void (^)(NSString *key, id object, id metadata, BOOL *stop))block
{
	[self enumerateRowsInCollection:collection usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over rows in the database for which you're interested in.
 * The filter block allows you to decide which rows you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object & metadata.
**/
- (void)enumerateRowsInCollection:(NSString *)collection
                       usingBlock:(void (^)(NSString *key, id object, id metadata, BOOL *stop))block
                       withFilter:(BOOL (^)(NSString *key))filter
{
	if (block == NULL) return;
	
	if (filter)
	{
		[self _enumerateRowsInCollection:collection
		                      usingBlock:^(int64_t rowid, NSString *key, id object, id metadata, BOOL *stop) {
			
			block(key, object, metadata, stop);
			
		} withFilter:^BOOL(int64_t rowid, NSString *key) {
			
			return filter(key);
		}];
	}
	else
	{
		[self _enumerateRowsInCollection:collection
		                      usingBlock:^(int64_t rowid, NSString *key, id object, id metadata, BOOL *stop) {
			
			block(key, object, metadata, stop);
			
		} withFilter:NULL];
	}
}

/**
 * Enumerates all rows in all collections.
 * 
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 * 
 * If you only need to enumerate over certain rows (e.g. subset of collections, or keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those objects you're not interested in.
**/
- (void)enumerateRowsInAllCollectionsUsingBlock:
                            (void (^)(NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
{
	[self enumerateRowsInAllCollectionsUsingBlock:block withFilter:NULL];
}

/**
 * Enumerates all rows in all collections.
 * The filter block allows you to decide which objects you're interested in.
 *
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 * 
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given
 * collection/key pair. If the filter block returns NO, then the block handler is skipped for the given pair,
 * which avoids the cost associated with deserializing the object.
**/
- (void)enumerateRowsInAllCollectionsUsingBlock:
                            (void (^)(NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
                 withFilter:(BOOL (^)(NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	
	if (filter)
	{
		[self _enumerateRowsInAllCollectionsUsingBlock:
		    ^(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop) {
			
			block(collection, key, object, metadata, stop);
			
		} withFilter:^BOOL(int64_t rowid, NSString *collection, NSString *key) {
			
			return filter(collection, key);
		}];
	}
	else
	{
		[self _enumerateRowsInAllCollectionsUsingBlock:
		    ^(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop) {
			
			block(collection, key, object, metadata, stop);
			
		} withFilter:NULL];
	}
}

/**
 * Enumerates over the given list of keys (unordered).
 *
 * This method is faster than fetching individual items as it optimizes cache access.
 * That is, it will first enumerate over items in the cache and then fetch items from the database,
 * thus optimizing the cache and reducing query size.
 *
 * If any keys are missing from the database, the 'metadata' parameter will be nil.
 *
 * IMPORTANT:
 * Due to cache optimizations, the items may not be enumerated in the same order as the 'keys' parameter.
**/
- (void)enumerateMetadataForKeys:(NSArray *)keys
                    inCollection:(NSString *)collection
             unorderedUsingBlock:(void (^)(NSUInteger keyIndex, id metadata, BOOL *stop))block
{
	if (block == NULL) return;
	if ([keys count] == 0) return;
	if (collection == nil) collection = @"";
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// Check the cache first (to optimize cache)
	
	NSMutableArray *missingIndexes = [NSMutableArray arrayWithCapacity:[keys count]];
	NSUInteger keyIndex = 0;
	
	for (NSString *key in keys)
	{
		YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
		
		id metadata = [connection->metadataCache objectForKey:cacheKey];
		if (metadata)
		{
			if (metadata == [YapNull null])
				block(keyIndex, nil, &stop);
			else
				block(keyIndex, metadata, &stop);
			
			if (stop || isMutated) break;
		}
		else
		{
			[missingIndexes addObject:@(keyIndex)];
		}
		
		keyIndex++;
	}
	
	if (stop) {
		return;
	}
	if (isMutated) {
		@throw [self mutationDuringEnumerationException];
		return;
	}
	if ([missingIndexes count] == 0) {
		return;
	}
	
	// Go to database for any missing keys (if needed)
	
	// Sqlite has an upper bound on the number of host parameters that may be used in a single query.
	// We need to watch out for this in case a large array of keys is passed.
	
	NSUInteger maxHostParams = (NSUInteger) sqlite3_limit(connection->db, SQLITE_LIMIT_VARIABLE_NUMBER, -1);
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	
	do
	{
		// Determine how many parameters to use in the query
		
		NSUInteger numKeyParams = MIN([missingIndexes count], (maxHostParams-1)); // minus 1 for collection param
		
		// Create the SQL query:
		// SELECT "key", "metadata" FROM "database2" WHERE "collection" = ? AND key IN (?, ?, ...);
		
		NSUInteger capacity = 80 + (numKeyParams * 3);
		NSMutableString *query = [NSMutableString stringWithCapacity:capacity];
		
		[query appendString:@"SELECT \"key\", \"metadata\" FROM \"database2\""];
		[query appendString:@" WHERE \"collection\" = ? AND \"key\" IN ("];
		
		NSUInteger i;
		for (i = 0; i < numKeyParams; i++)
		{
			if (i == 0)
				[query appendFormat:@"?"];
			else
				[query appendFormat:@", ?"];
		}
		
		[query appendString:@");"];
		
		sqlite3_stmt *statement;
		
		int status = sqlite3_prepare_v2(connection->db, [query UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating 'metadataForKeys' statement: %d %s",
						status, sqlite3_errmsg(connection->db));
			break; // Break from do/while. Still need to free _collection.
		}
		
		// Bind parameters.
		// And move objects from the missingIndexes array into keyIndexDict.
		
		NSMutableDictionary *keyIndexDict = [NSMutableDictionary dictionaryWithCapacity:numKeyParams];
		
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		for (i = 0; i < numKeyParams; i++)
		{
			NSNumber *keyIndexNumber = [missingIndexes objectAtIndex:i];
			NSString *key = [keys objectAtIndex:[keyIndexNumber unsignedIntegerValue]];
			
			[keyIndexDict setObject:keyIndexNumber forKey:key];
			
			sqlite3_bind_text(statement, (int)(i + 2), [key UTF8String], -1, SQLITE_TRANSIENT);
		}
		
		[missingIndexes removeObjectsInRange:NSMakeRange(0, numKeyParams)];
		
		// Execute the query and step over the results
		
		status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (connection->needsMarkSqlLevelSharedReadLock)
				[connection markSqlLevelSharedReadLockAcquired];
			
			do
			{
				const unsigned char *text = sqlite3_column_text(statement, 0);
				int textSize = sqlite3_column_bytes(statement, 0);
				
				const void *blob = sqlite3_column_blob(statement, 1);
				int blobSize = sqlite3_column_bytes(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				keyIndex = [[keyIndexDict objectForKey:key] unsignedIntegerValue];
				
				NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
				
				id metadata = data ? connection->database->metadataDeserializer(collection, key, data) : nil;
				
				if (metadata)
				{
					YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
					
					[connection->metadataCache setObject:metadata forKey:cacheKey];
				}
				
				block(keyIndex, metadata, &stop);
				
				[keyIndexDict removeObjectForKey:key];
				
				if (stop || isMutated) break;
				
			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}
		
		if ((status != SQLITE_DONE) && !stop && !isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_finalize(statement);
		statement = NULL;
		
		if (stop) {
			FreeYapDatabaseString(&_collection);
			return;
		}
		if (isMutated) {
			FreeYapDatabaseString(&_collection);
			@throw [self mutationDuringEnumerationException];
			return;
		}
		
		// If there are any remaining items in the keyIndexDict,
		// then those items didn't exist in the database.
		
		for (NSNumber *keyIndexNumber in [keyIndexDict objectEnumerator])
		{
			block([keyIndexNumber unsignedIntegerValue], nil, &stop);
			
			// Do NOT add keys to the cache that don't exist in the database.
			
			if (stop || isMutated) break;
		}
		
		if (stop) {
			FreeYapDatabaseString(&_collection);
			return;
		}
		if (isMutated) {
			FreeYapDatabaseString(&_collection);
			@throw [self mutationDuringEnumerationException];
			return;
		}
		
		
	} while ([missingIndexes count] > 0);
	
	FreeYapDatabaseString(&_collection);
}

/**
 * Enumerates over the given list of keys (unordered).
 *
 * This method is faster than fetching individual items as it optimizes cache access.
 * That is, it will first enumerate over items in the cache and then fetch items from the database,
 * thus optimizing the cache and reducing query size.
 *
 * If any keys are missing from the database, the 'object' parameter will be nil.
 *
 * IMPORTANT:
 * Due to cache optimizations, the items may not be enumerated in the same order as the 'keys' parameter.
**/
- (void)enumerateObjectsForKeys:(NSArray *)keys
                   inCollection:(NSString *)collection
            unorderedUsingBlock:(void (^)(NSUInteger keyIndex, id object, BOOL *stop))block
{
	if (block == NULL) return;
	if ([keys count] == 0) return;
	if (collection == nil) collection = @"";
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// Check the cache first (to optimize cache)
	
	NSMutableArray *missingIndexes = [NSMutableArray arrayWithCapacity:[keys count]];
	NSUInteger keyIndex = 0;
	
	for (NSString *key in keys)
	{
		YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
		
		id object = [connection->objectCache objectForKey:cacheKey];
		if (object)
		{
			block(keyIndex, object, &stop);
			
			if (stop || isMutated) break;
		}
		else
		{
			[missingIndexes addObject:@(keyIndex)];
		}
		
		keyIndex++;
	}
	
	if (stop) {
		return;
	}
	if (isMutated) {
		@throw [self mutationDuringEnumerationException];
		return;
	}
	if ([missingIndexes count] == 0) {
		return;
	}
	
	// Go to database for any missing keys (if needed)
	
	// Sqlite has an upper bound on the number of host parameters that may be used in a single query.
	// We need to watch out for this in case a large array of keys is passed.
	
	NSUInteger maxHostParams = (NSUInteger) sqlite3_limit(connection->db, SQLITE_LIMIT_VARIABLE_NUMBER, -1);
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	
	do
	{
		// Determine how many parameters to use in the query
		
		NSUInteger numKeyParams = MIN([missingIndexes count], (maxHostParams-1)); // minus 1 for collection param
		
		// Create the SQL query:
		// SELECT "key", "data" FROM "database2" WHERE "collection" = ? AND key IN (?, ?, ...);
		
		NSUInteger capacity = 80 + (numKeyParams * 3);
		NSMutableString *query = [NSMutableString stringWithCapacity:capacity];
		
		[query appendString:@"SELECT \"key\", \"data\" FROM \"database2\""];
		[query appendString:@" WHERE \"collection\" = ? AND \"key\" IN ("];
		
		NSUInteger i;
		for (i = 0; i < numKeyParams; i++)
		{
			if (i == 0)
				[query appendFormat:@"?"];
			else
				[query appendFormat:@", ?"];
		}
		
		[query appendString:@");"];
		
		sqlite3_stmt *statement;
		
		int status = sqlite3_prepare_v2(connection->db, [query UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating 'objectsForKeys' statement: %d %s",
						status, sqlite3_errmsg(connection->db));
			break; // Break from do/while. Still need to free _collection.
		}
		
		// Bind parameters.
		// And move objects from the missingIndexes array into keyIndexDict.
		
		NSMutableDictionary *keyIndexDict = [NSMutableDictionary dictionaryWithCapacity:numKeyParams];
		
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		for (i = 0; i < numKeyParams; i++)
		{
			NSNumber *keyIndexNumber = [missingIndexes objectAtIndex:i];
			NSString *key = [keys objectAtIndex:[keyIndexNumber unsignedIntegerValue]];
			
			[keyIndexDict setObject:keyIndexNumber forKey:key];
			
			sqlite3_bind_text(statement, (int)(i + 2), [key UTF8String], -1, SQLITE_TRANSIENT);
		}
		
		[missingIndexes removeObjectsInRange:NSMakeRange(0, numKeyParams)];
		
		// Execute the query and step over the results
		
		status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (connection->needsMarkSqlLevelSharedReadLock)
				[connection markSqlLevelSharedReadLockAcquired];
			
			do
			{
				const unsigned char *text = sqlite3_column_text(statement, 0);
				int textSize = sqlite3_column_bytes(statement, 0);
				
				const void *blob = sqlite3_column_blob(statement, 1);
				int blobSize = sqlite3_column_bytes(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				keyIndex = [[keyIndexDict objectForKey:key] unsignedIntegerValue];
				
				NSData *objectData = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
				id object = connection->database->objectDeserializer(collection, key, objectData);
				
				if (object)
				{
					YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
					[connection->objectCache setObject:object forKey:cacheKey];
				}
				
				block(keyIndex, object, &stop);
				
				[keyIndexDict removeObjectForKey:key];
				
				if (stop || isMutated) break;
				
			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}
		
		if ((status != SQLITE_DONE) && !stop && !isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_finalize(statement);
		statement = NULL;
		
		if (stop) {
			FreeYapDatabaseString(&_collection);
			return;
		}
		if (isMutated) {
			FreeYapDatabaseString(&_collection);
			@throw [self mutationDuringEnumerationException];
			return;
		}
		
		// If there are any remaining items in the keyIndexDict,
		// then those items didn't exist in the database.
		
		for (NSNumber *keyIndexNumber in [keyIndexDict objectEnumerator])
		{
			block([keyIndexNumber unsignedIntegerValue], nil, &stop);
			
			// Do NOT add keys to the cache that don't exist in the database.
			
			if (stop || isMutated) break;
		}
		
		if (stop) {
			FreeYapDatabaseString(&_collection);
			return;
		}
		if (isMutated) {
			FreeYapDatabaseString(&_collection);
			@throw [self mutationDuringEnumerationException];
			return;
		}
		
		
	} while ([missingIndexes count] > 0);
	
	FreeYapDatabaseString(&_collection);
}

/**
 * Enumerates over the given list of keys (unordered).
 *
 * This method is faster than fetching individual items as it optimizes cache access.
 * That is, it will first enumerate over items in the cache and then fetch items from the database,
 * thus optimizing the cache and reducing query size.
 *
 * If any keys are missing from the database, the 'object' parameter will be nil.
 *
 * IMPORTANT:
 * Due to cache optimizations, the items may not be enumerated in the same order as the 'keys' parameter.
**/
- (void)enumerateRowsForKeys:(NSArray *)keys
                inCollection:(NSString *)collection
         unorderedUsingBlock:(void (^)(NSUInteger keyIndex, id object, id metadata, BOOL *stop))block
{
	if (block == NULL) return;
	if ([keys count] == 0) return;
	if (collection == nil) collection = @"";
	
	isMutated = NO; // mutation during enumeration protection
	__block BOOL stop = NO;
	
	// Check the cache first (to optimize cache)
	
	NSMutableArray *missingIndexes = [NSMutableArray arrayWithCapacity:[keys count]];
	NSUInteger keyIndex = 0;
	
	for (NSString *key in keys)
	{
		YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
		
		id object = [connection->objectCache objectForKey:cacheKey];
		if (object)
		{
			id metadata = [connection->metadataCache objectForKey:cacheKey];
			if (metadata)
			{
				if (metadata == [YapNull null])
					block(keyIndex, object, nil, &stop);
				else
					block(keyIndex, object, metadata, &stop);
				
				if (stop || isMutated) break;
			}
			else
			{
				[missingIndexes addObject:@(keyIndex)];
			}
		}
		else
		{
			[missingIndexes addObject:@(keyIndex)];
		}
		
		keyIndex++;
	}
	
	if (stop) {
		return;
	}
	if (isMutated) {
		@throw [self mutationDuringEnumerationException];
		return;
	}
	if ([missingIndexes count] == 0) {
		return;
	}
	
	// Go to database for any missing keys (if needed)
	
	// Sqlite has an upper bound on the number of host parameters that may be used in a single query.
	// We need to watch out for this in case a large array of keys is passed.
	
	NSUInteger maxHostParams = (NSUInteger) sqlite3_limit(connection->db, SQLITE_LIMIT_VARIABLE_NUMBER, -1);
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	
	do
	{
		// Determine how many parameters to use in the query
		
		NSUInteger numKeyParams = MIN([missingIndexes count], (maxHostParams-1)); // minus 1 for collection param
		
		// Create the SQL query:
		// SELECT "key", "data", "metadata" FROM "database2" WHERE "collection" = ? AND key IN (?, ?, ...);
		
		NSUInteger capacity = 80 + (numKeyParams * 3);
		NSMutableString *query = [NSMutableString stringWithCapacity:capacity];
		
		[query appendString:@"SELECT \"key\", \"data\", \"metadata\" FROM \"database2\""];
		[query appendString:@" WHERE \"collection\" = ? AND \"key\" IN ("];
		
		NSUInteger i;
		for (i = 0; i < numKeyParams; i++)
		{
			if (i == 0)
				[query appendFormat:@"?"];
			else
				[query appendFormat:@", ?"];
		}
		
		[query appendString:@");"];
		
		sqlite3_stmt *statement;
		
		int status = sqlite3_prepare_v2(connection->db, [query UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating 'objectsAndMetadataForKeys' statement: %d %s",
						status, sqlite3_errmsg(connection->db));
			break; // Break from do/while. Still need to free _collection.
		}
		
		// Bind parameters.
		// And move objects from the missingIndexes array into keyIndexDict.
		
		NSMutableDictionary *keyIndexDict = [NSMutableDictionary dictionaryWithCapacity:numKeyParams];
		
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		for (i = 0; i < numKeyParams; i++)
		{
			NSNumber *keyIndexNumber = [missingIndexes objectAtIndex:i];
			NSString *key = [keys objectAtIndex:[keyIndexNumber unsignedIntegerValue]];
			
			[keyIndexDict setObject:keyIndexNumber forKey:key];
			
			sqlite3_bind_text(statement, (int)(i + 2), [key UTF8String], -1, SQLITE_TRANSIENT);
		}
		
		[missingIndexes removeObjectsInRange:NSMakeRange(0, numKeyParams)];
		
		// Execute the query and step over the results
		
		status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (connection->needsMarkSqlLevelSharedReadLock)
				[connection markSqlLevelSharedReadLockAcquired];
			
			do
			{
				const unsigned char *text = sqlite3_column_text(statement, 0);
				int textSize = sqlite3_column_bytes(statement, 0);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				keyIndex = [[keyIndexDict objectForKey:key] unsignedIntegerValue];
				
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
				id object = [connection->objectCache objectForKey:cacheKey];
				if (object == nil)
				{
					const void *oBlob = sqlite3_column_blob(statement, 1);
					int oBlobSize = sqlite3_column_bytes(statement, 1);
					
					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);
					
					if (object)
						[connection->objectCache setObject:object forKey:cacheKey];
				}
				
				id metadata = [connection->metadataCache objectForKey:cacheKey];
				if (metadata)
				{
					if (metadata == [YapNull null])
						metadata = nil;
				}
				else
				{
					const void *mBlob = sqlite3_column_blob(statement, 2);
					int mBlobSize = sqlite3_column_bytes(statement, 2);
					
					if (mBlobSize > 0)
					{
						NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
						metadata = connection->database->metadataDeserializer(collection, key, mData);
					}
					
					if (metadata)
						[connection->metadataCache setObject:metadata forKey:cacheKey];
					else
						[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
				}
				
				block(keyIndex, object, metadata, &stop);
				
				[keyIndexDict removeObjectForKey:key];
				
				if (stop || isMutated) break;
				
			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}
		
		if ((status != SQLITE_DONE) && !stop && !isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_finalize(statement);
		statement = NULL;
		
		if (stop) {
			FreeYapDatabaseString(&_collection);
			return;
		}
		if (isMutated) {
			FreeYapDatabaseString(&_collection);
			@throw [self mutationDuringEnumerationException];
			return;
		}
		
		// If there are any remaining items in the keyIndexDict,
		// then those items didn't exist in the database.
		
		for (NSNumber *keyIndexNumber in [keyIndexDict objectEnumerator])
		{
			block([keyIndexNumber unsignedIntegerValue], nil, nil, &stop);
			
			// Do NOT add keys to the cache that don't exist in the database.
			
			if (stop || isMutated) break;
		}
		
		if (stop) {
			FreeYapDatabaseString(&_collection);
			return;
		}
		if (isMutated) {
			FreeYapDatabaseString(&_collection);
			@throw [self mutationDuringEnumerationException];
			return;
		}
		
		
	} while ([missingIndexes count] > 0);
	
	FreeYapDatabaseString(&_collection);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Internal Enumerate (using rowid)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Fast enumeration over all keys in the given collection.
 *
 * This uses a "SELECT key FROM database WHERE collection = ?" operation,
 * and then steps over the results invoking the given block handler.
**/
- (void)_enumerateKeysInCollection:(NSString *)collection
                        usingBlock:(void (^)(int64_t rowid, NSString *key, BOOL *stop))block
{
	if (block == NULL) return;
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection enumerateKeysInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "key" FROM "database2" WHERE collection = ?;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text = sqlite3_column_text(statement, 1);
			int textSize = sqlite3_column_bytes(statement, 1);
			
			NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			block(rowid, key, &stop);
			
			if (stop || isMutated) break;
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over all keys in select collections.
 *
 * This uses a "SELECT key FROM database WHERE collection = ?" operation,
 * and then steps over the results invoking the given block handler.
**/
- (void)_enumerateKeysInCollections:(NSArray *)collections
                         usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block
{
	if (block == NULL) return;
	if ([collections count] == 0) return;
	
	sqlite3_stmt *statement = [connection enumerateKeysInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "key" FROM "database2" WHERE collection = ?;
	
	for (NSString *collection in collections)
	{
		YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		int status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (connection->needsMarkSqlLevelSharedReadLock)
				[connection markSqlLevelSharedReadLockAcquired];
			
			do
			{
				int64_t rowid = sqlite3_column_int64(statement, 0);
				
				const unsigned char *text = sqlite3_column_text(statement, 1);
				int textSize = sqlite3_column_bytes(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				
				block(rowid, collection, key, &stop);
				
				if (stop || isMutated) break;
				
			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}
		
		if ((status != SQLITE_DONE) && !stop && !isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
		FreeYapDatabaseString(&_collection);
		
		if (isMutated && !stop)
		{
			@throw [self mutationDuringEnumerationException];
		}
			
		if (stop)
		{
			break;
		}
		
	} // end for (NSString *collection in collections)
}

/**
 * Fast enumeration over all keys in the given collection.
 *
 * This uses a "SELECT collection, key FROM database" operation,
 * and then steps over the results invoking the given block handler.
**/
- (void)_enumerateKeysInAllCollectionsUsingBlock:
                            (void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block
{
	if (block == NULL) return;
	
	sqlite3_stmt *statement = [connection enumerateKeysInAllCollectionsStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "collection", "key" FROM "database2";
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text1 = sqlite3_column_text(statement, 1);
			int textSize1 = sqlite3_column_bytes(statement, 1);
			
			const unsigned char *text2 = sqlite3_column_text(statement, 2);
			int textSize2 = sqlite3_column_bytes(statement, 2);
			
			NSString *collection, *key;
			
			collection = [[NSString alloc] initWithBytes:text1 length:textSize1 encoding:NSUTF8StringEncoding];
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];
			
			block(rowid, collection, key, &stop);
			
			if (stop || isMutated) break;
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_reset(statement);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over all keys and associated metadata in the given collection.
 * 
 * This uses a "SELECT key, metadata FROM database WHERE collection = ?" operation and steps over the results.
 * 
 * If you only need to enumerate over certain items (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the deserialization step for those items you're not interested in.
 * 
 * Keep in mind that you cannot modify the collection mid-enumeration (just like any other kind of enumeration).
**/
- (void)_enumerateKeysAndMetadataInCollection:(NSString *)collection
                                   usingBlock:(void (^)(int64_t rowid, NSString *key, id metadata, BOOL *stop))block
{
	[self _enumerateKeysAndMetadataInCollection:collection usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over all keys and associated metadata in the given collection.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
 * 
 * Keep in mind that you cannot modify the collection mid-enumeration (just like any other kind of enumeration).
**/
- (void)_enumerateKeysAndMetadataInCollection:(NSString *)collection
                                   usingBlock:(void (^)(int64_t rowid, NSString *key, id metadata, BOOL *stop))block
                                   withFilter:(BOOL (^)(int64_t rowid, NSString *key))filter;
{
	if (block == NULL) return;
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection enumerateKeysAndMetadataInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "key", "metadata" FROM "database2" WHERE "collection" = ?;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text = sqlite3_column_text(statement, 1);
			int textSize = sqlite3_column_bytes(statement, 1);
			
			NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
			
				id metadata = [connection->metadataCache objectForKey:cacheKey];
				if (metadata)
				{
					if (metadata == [YapNull null])
						metadata = nil;
				}
				else
				{
					const void *mBlob = sqlite3_column_blob(statement, 2);
					int mBlobSize = sqlite3_column_bytes(statement, 2);
					
					if (mBlobSize > 0)
					{
						NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
						metadata = connection->database->metadataDeserializer(collection, key, mData);
					}
					
					if (unlimitedMetadataCacheLimit ||
					    [connection->metadataCache count] < connection->metadataCacheLimit)
					{
						if (metadata)
							[connection->metadataCache setObject:metadata forKey:cacheKey];
						else
							[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
					}
				}
				
				block(rowid, key, metadata, &stop);
				
				if (stop || isMutated) break;
			}
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over select keys and associated metadata in the given collection.
 * 
 * This uses a "SELECT key, metadata FROM database WHERE collection = ?" operation and steps over the results.
 * 
 * If you only need to enumerate over certain items (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the deserialization step for those items you're not interested in.
 * 
 * Keep in mind that you cannot modify the collection mid-enumeration (just like any other kind of enumeration).
**/
- (void)_enumerateKeysAndMetadataInCollections:(NSArray *)collections
                usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
{
	[self _enumerateKeysAndMetadataInCollections:collections usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over selected keys and associated metadata in the given collection.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
 * 
 * Keep in mind that you cannot modify the collection mid-enumeration (just like any other kind of enumeration).
**/
- (void)_enumerateKeysAndMetadataInCollections:(NSArray *)collections
                usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
                withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	if ([collections count] == 0) return;
	
	sqlite3_stmt *statement = [connection enumerateKeysAndMetadataInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);
	
	// SELECT "rowid", "key", "metadata" FROM "database2" WHERE "collection" = ?;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.
	
	for (NSString *collection in collections)
	{
		YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		int status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (connection->needsMarkSqlLevelSharedReadLock)
				[connection markSqlLevelSharedReadLockAcquired];
			
			do
			{
				int64_t rowid = sqlite3_column_int64(statement, 0);
				
				const unsigned char *text = sqlite3_column_text(statement, 1);
				int textSize = sqlite3_column_bytes(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				
				BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
				if (invokeBlock)
				{
					YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
					id metadata = [connection->metadataCache objectForKey:cacheKey];
					if (metadata)
					{
						if (metadata == [YapNull null])
							metadata = nil;
					}
					else
					{
						const void *mBlob = sqlite3_column_blob(statement, 2);
						int mBlobSize = sqlite3_column_bytes(statement, 2);
						
						if (mBlobSize > 0)
						{
							NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
							metadata = connection->database->metadataDeserializer(collection, key, mData);
						}
						
						if (unlimitedMetadataCacheLimit ||
						    [connection->metadataCache count] < connection->metadataCacheLimit)
						{
							if (metadata)
								[connection->metadataCache setObject:metadata forKey:cacheKey];
							else
								[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
						}
					}
					
					block(rowid, collection, key, metadata, &stop);
					
					if (stop || isMutated) break;
				}
				
			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}
		
		if ((status != SQLITE_DONE) && !stop && !isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
		FreeYapDatabaseString(&_collection);
		
		if (isMutated && !stop)
		{
			@throw [self mutationDuringEnumerationException];
		}
		
		if (stop)
		{
			break;
		}
		
	} // end for (NSString *collection in collections)
}

/**
 * Fast enumeration over all key/metadata pairs in all collections.
 * 
 * This uses a "SELECT metadata FROM database ORDER BY collection ASC" operation, and steps over the results.
 * 
 * If you only need to enumerate over certain objects (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the deserialization step for those objects you're not interested in.
 * 
 * Keep in mind that you cannot modify the database mid-enumeration (just like any other kind of enumeration).
**/
- (void)_enumerateKeysAndMetadataInAllCollectionsUsingBlock:
                        (void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
{
	[self _enumerateKeysAndMetadataInAllCollectionsUsingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over all key/metadata pairs in all collections.
 *
 * This uses a "SELECT metadata FROM database ORDER BY collection ASC" operation and steps over the results.
 * 
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
 *
 * Keep in mind that you cannot modify the database mid-enumeration (just like any other kind of enumeration).
 **/
- (void)_enumerateKeysAndMetadataInAllCollectionsUsingBlock:
                        (void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
             withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	
	sqlite3_stmt *statement = [connection enumerateKeysAndMetadataInAllCollectionsStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "collection", "key", "metadata" FROM "database2" ORDER BY "collection" ASC;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.
	
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text1 = sqlite3_column_text(statement, 1);
			int textSize1 = sqlite3_column_bytes(statement, 1);
			
			const unsigned char *text2 = sqlite3_column_text(statement, 2);
			int textSize2 = sqlite3_column_bytes(statement, 2);
			
			NSString *collection, *key;
			
			collection = [[NSString alloc] initWithBytes:text1 length:textSize1 encoding:NSUTF8StringEncoding];
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];
			
			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
				id metadata = [connection->metadataCache objectForKey:cacheKey];
				if (metadata)
				{
					if (metadata == [YapNull null])
						metadata = nil;
				}
				else
				{
					const void *mBlob = sqlite3_column_blob(statement, 3);
					int mBlobSize = sqlite3_column_bytes(statement, 3);
					
					if (mBlobSize > 0)
					{
						NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
						metadata = connection->database->metadataDeserializer(collection, key, mData);
					}
					
					if (unlimitedMetadataCacheLimit ||
					    [connection->metadataCache count] < connection->metadataCacheLimit)
					{
						if (metadata)
							[connection->metadataCache setObject:metadata forKey:cacheKey];
						else
							[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
					}
				}
				
				block(rowid, collection, key, metadata, &stop);
				
				if (stop || isMutated) break;
			}
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_reset(statement);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over all objects in the database.
 *
 * This uses a "SELECT key, object from database WHERE collection = ?" operation, and then steps over the results,
 * deserializing each object, and then invoking the given block handler.
 *
 * If you only need to enumerate over certain objects (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those objects you're not interested in.
**/
- (void)_enumerateKeysAndObjectsInCollection:(NSString *)collection
                                  usingBlock:(void (^)(int64_t rowid, NSString *key, id object, BOOL *stop))block
{
	[self _enumerateKeysAndObjectsInCollection:collection usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over objects in the database for which you're interested in.
 * The filter block allows you to decide which objects you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
**/
- (void)_enumerateKeysAndObjectsInCollection:(NSString *)collection
                                  usingBlock:(void (^)(int64_t rowid, NSString *key, id object, BOOL *stop))block
                                  withFilter:(BOOL (^)(int64_t rowid, NSString *key))filter
{
	if (block == NULL) return;
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection enumerateKeysAndObjectsInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "key", "data", FROM "database2" WHERE "collection" = ?;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text = sqlite3_column_text(statement, 1);
			int textSize = sqlite3_column_bytes(statement, 1);
			
			NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
				id object = [connection->objectCache objectForKey:cacheKey];
				if (object == nil)
				{
					const void *oBlob = sqlite3_column_blob(statement, 2);
					int oBlobSize = sqlite3_column_bytes(statement, 2);
					
					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);
					
					if (unlimitedObjectCacheLimit || [connection->objectCache count] < connection->objectCacheLimit)
					{
						if (object)
							[connection->objectCache setObject:object forKey:cacheKey];
					}
				}
				
				block(rowid, key, object, &stop);
				
				if (stop || isMutated) break;
			}
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over selected objects in the database.
 *
 * This uses a "SELECT key, object from database WHERE collection = ?" operation, and then steps over the results,
 * deserializing each object, and then invoking the given block handler.
 *
 * If you only need to enumerate over certain objects (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those objects you're not interested in.
**/
- (void)_enumerateKeysAndObjectsInCollections:(NSArray *)collections usingBlock:
                            (void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
{
	[self _enumerateKeysAndObjectsInCollections:collections usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over objects in the database for which you're interested in.
 * The filter block allows you to decide which objects you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
**/
- (void)_enumerateKeysAndObjectsInCollections:(NSArray *)collections
                 usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                 withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	if ([collections count] == 0) return;
	
	sqlite3_stmt *statement = [connection enumerateKeysAndObjectsInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	
	// SELECT "rowid", "key", "data", FROM "database2" WHERE "collection" = ?;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.
	
	for (NSString *collection in collections)
	{
		YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		int status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (connection->needsMarkSqlLevelSharedReadLock)
				[connection markSqlLevelSharedReadLockAcquired];
			
			do
			{
				int64_t rowid = sqlite3_column_int64(statement, 0);
				
				const unsigned char *text = sqlite3_column_text(statement, 1);
				int textSize = sqlite3_column_bytes(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				
				BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
				if (invokeBlock)
				{
					YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
					
					id object = [connection->objectCache objectForKey:cacheKey];
					if (object == nil)
					{
						const void *oBlob = sqlite3_column_blob(statement, 2);
						int oBlobSize = sqlite3_column_bytes(statement, 2);
						
						NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
						object = connection->database->objectDeserializer(collection, key, oData);
						
						if (unlimitedObjectCacheLimit ||
						    [connection->objectCache count] < connection->objectCacheLimit)
						{
							if (object)
								[connection->objectCache setObject:object forKey:cacheKey];
						}
					}
					
					block(rowid, collection, key, object, &stop);
					
					if (stop || isMutated) break;
				}
				
			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}
		
		if ((status != SQLITE_DONE) && !stop && !isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
		FreeYapDatabaseString(&_collection);
		
		if (isMutated && !stop)
		{
			@throw [self mutationDuringEnumerationException];
		}
		
		if (stop)
		{
			break;
		}
		
	} // end for (NSString *collection in collections)
}

/**
 * Enumerates all key/object pairs in all collections.
 *
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 *
 * If you only need to enumerate over certain objects (e.g. subset of collections, or keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those objects you're not interested in.
**/
- (void)_enumerateKeysAndObjectsInAllCollectionsUsingBlock:
                            (void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
{
	[self _enumerateKeysAndObjectsInAllCollectionsUsingBlock:block withFilter:NULL];
}

/**
 * Enumerates all key/object pairs in all collections.
 * The filter block allows you to decide which objects you're interested in.
 *
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given
 * collection/key pair. If the filter block returns NO, then the block handler is skipped for the given pair,
 * which avoids the cost associated with deserializing the object.
**/
- (void)_enumerateKeysAndObjectsInAllCollectionsUsingBlock:
                            (void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                 withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	
	sqlite3_stmt *statement = [connection enumerateKeysAndObjectsInAllCollectionsStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "collection", "key", "data" FROM "database2" ORDER BY \"collection\" ASC;";
	//           0           1         2       3
	
	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text1 = sqlite3_column_text(statement, 1);
			int textSize1 = sqlite3_column_bytes(statement, 1);
			
			const unsigned char *text2 = sqlite3_column_text(statement, 2);
			int textSize2 = sqlite3_column_bytes(statement, 2);
			
			NSString *collection, *key;
			
			collection = [[NSString alloc] initWithBytes:text1 length:textSize1 encoding:NSUTF8StringEncoding];
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];
			
			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
				id object = [connection->objectCache objectForKey:cacheKey];
				if (object == nil)
				{
					const void *oBlob = sqlite3_column_blob(statement, 3);
					int oBlobSize = sqlite3_column_bytes(statement, 3);
					
					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);
					
					if (unlimitedObjectCacheLimit || [connection->objectCache count] < connection->objectCacheLimit)
					{
						if (object)
							[connection->objectCache setObject:object forKey:cacheKey];
					}
				}
				
				block(rowid, collection, key, object, &stop);
				
				if (stop || isMutated) break;
			}
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over all rows in the database.
 *
 * This uses a "SELECT key, data, metadata from database WHERE collection = ?" operation,
 * and then steps over the results, deserializing each object & metadata, and then invoking the given block handler.
 *
 * If you only need to enumerate over certain rows (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those rows you're not interested in.
**/
- (void)_enumerateRowsInCollection:(NSString *)collection
                        usingBlock:(void (^)(int64_t rowid, NSString *key, id object, id metadata, BOOL *stop))block
{
	[self _enumerateRowsInCollection:collection usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over rows in the database for which you're interested in.
 * The filter block allows you to decide which rows you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object & metadata.
**/
- (void)_enumerateRowsInCollection:(NSString *)collection
                        usingBlock:(void (^)(int64_t rowid, NSString *key, id object, id metadata, BOOL *stop))block
                        withFilter:(BOOL (^)(int64_t rowid, NSString *key))filter
{
	if (block == NULL) return;
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection enumerateRowsInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "key", "data", "metadata" FROM "database2" WHERE "collection" = ?;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
	
	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text = sqlite3_column_text(statement, 1);
			int textSize = sqlite3_column_bytes(statement, 1);
			
			NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
				id object = [connection->objectCache objectForKey:cacheKey];
				if (object == nil)
				{
					const void *oBlob = sqlite3_column_blob(statement, 2);
					int oBlobSize = sqlite3_column_bytes(statement, 2);
					
					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);
					
					if (unlimitedObjectCacheLimit || [connection->objectCache count] < connection->objectCacheLimit)
					{
						if (object)
							[connection->objectCache setObject:object forKey:cacheKey];
					}
				}
				
				id metadata = [connection->metadataCache objectForKey:cacheKey];
				if (metadata)
				{
					if (metadata == [YapNull null])
						metadata = nil;
				}
				else
				{
					const void *mBlob = sqlite3_column_blob(statement, 3);
					int mBlobSize = sqlite3_column_bytes(statement, 3);
					
					if (mBlobSize > 0)
					{
						NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
						metadata = connection->database->metadataDeserializer(collection, key, mData);
					}
					
					if (unlimitedMetadataCacheLimit ||
					    [connection->metadataCache count] < connection->metadataCacheLimit)
					{
						if (metadata)
							[connection->metadataCache setObject:metadata forKey:cacheKey];
						else
							[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
					}
				}
				
				block(rowid, key, object, metadata, &stop);
				
				if (stop || isMutated) break;
			}
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_collection);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

/**
 * Fast enumeration over select rows in the database.
 *
 * This uses a "SELECT key, data, metadata from database WHERE collection = ?" operation,
 * and then steps over the results, deserializing each object & metadata, and then invoking the given block handler.
 *
 * If you only need to enumerate over certain rows (e.g. keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those rows you're not interested in.
**/
- (void)_enumerateRowsInCollections:(NSArray *)collections usingBlock:
                (void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
{
	[self _enumerateRowsInCollections:collections usingBlock:block withFilter:NULL];
}

/**
 * Fast enumeration over rows in the database for which you're interested in.
 * The filter block allows you to decide which rows you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object & metadata.
**/
- (void)_enumerateRowsInCollections:(NSArray *)collections
     usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
     withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	if ([collections count] == 0) return;
	
	sqlite3_stmt *statement = [connection enumerateRowsInCollectionStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);
	
	// SELECT "rowid", "key", "data", "metadata" FROM "database2" WHERE "collection" = ?;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.
	
	for (NSString *collection in collections)
	{
		YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		int status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (connection->needsMarkSqlLevelSharedReadLock)
				[connection markSqlLevelSharedReadLockAcquired];
			
			do
			{
				int64_t rowid = sqlite3_column_int64(statement, 0);
				
				const unsigned char *text = sqlite3_column_text(statement, 1);
				int textSize = sqlite3_column_bytes(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				
				BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
				if (invokeBlock)
				{
					YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
					
					id object = [connection->objectCache objectForKey:cacheKey];
					if (object == nil)
					{
						const void *oBlob = sqlite3_column_blob(statement, 2);
						int oBlobSize = sqlite3_column_bytes(statement, 2);
						
						NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
						object = connection->database->objectDeserializer(collection, key, oData);
						
						if (unlimitedObjectCacheLimit ||
						    [connection->objectCache count] < connection->objectCacheLimit)
						{
							if (object)
								[connection->objectCache setObject:object forKey:cacheKey];
						}
					}
					
					id metadata = [connection->metadataCache objectForKey:cacheKey];
					if (metadata)
					{
						if (metadata == [YapNull null])
							metadata = nil;
					}
					else
					{
						const void *mBlob = sqlite3_column_blob(statement, 3);
						int mBlobSize = sqlite3_column_bytes(statement, 3);
						
						if (mBlobSize > 0)
						{
							NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
							metadata = connection->database->metadataDeserializer(collection, key, mData);
						}
						
						if (unlimitedMetadataCacheLimit ||
						    [connection->metadataCache count] < connection->metadataCacheLimit)
						{
							if (metadata)
								[connection->metadataCache setObject:metadata forKey:cacheKey];
							else
								[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
						}
					}
					
					block(rowid, collection, key, object, metadata, &stop);
					
					if (stop || isMutated) break;
				}
				
			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}
		
		if ((status != SQLITE_DONE) && !stop && !isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
		FreeYapDatabaseString(&_collection);
		
		if (isMutated && !stop)
		{
			@throw [self mutationDuringEnumerationException];
		}
		
		if (stop)
		{
			break;
		}
	
	} // end for (NSString *collection in collections)
}

/**
 * Enumerates all rows in all collections.
 * 
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 * 
 * If you only need to enumerate over certain rows (e.g. subset of collections, or keys with a particular prefix),
 * consider using the alternative version below which provides a filter,
 * allowing you to skip the serialization step for those objects you're not interested in.
**/
- (void)_enumerateRowsInAllCollectionsUsingBlock:
                (void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
{
	[self _enumerateRowsInAllCollectionsUsingBlock:block withFilter:NULL];
}

/**
 * Enumerates all rows in all collections.
 * The filter block allows you to decide which objects you're interested in.
 *
 * The enumeration is sorted by collection. That is, it will enumerate fully over a single collection
 * before moving onto another collection.
 * 
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given
 * collection/key pair. If the filter block returns NO, then the block handler is skipped for the given pair,
 * which avoids the cost associated with deserializing the object.
**/
- (void)_enumerateRowsInAllCollectionsUsingBlock:
                (void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
     withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
	if (block == NULL) return;
	
	sqlite3_stmt *statement = [connection enumerateRowsInAllCollectionsStatement];
	if (statement == NULL) return;
	
	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;
	
	// SELECT "rowid", "collection", "key", "data", "metadata" FROM "database2" ORDER BY \"collection\" ASC;";
	//           0           1         2       3         4
	
	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];
		
		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);
			
			const unsigned char *text1 = sqlite3_column_text(statement, 1);
			int textSize1 = sqlite3_column_bytes(statement, 1);
			
			const unsigned char *text2 = sqlite3_column_text(statement, 2);
			int textSize2 = sqlite3_column_bytes(statement, 2);
			
			NSString *collection, *key;
			
			collection = [[NSString alloc] initWithBytes:text1 length:textSize1 encoding:NSUTF8StringEncoding];
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];
			
			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
				id object = [connection->objectCache objectForKey:cacheKey];
				if (object == nil)
				{
					const void *oBlob = sqlite3_column_blob(statement, 3);
					int oBlobSize = sqlite3_column_bytes(statement, 3);
					
					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);
					
					if (unlimitedObjectCacheLimit || [connection->objectCache count] < connection->objectCacheLimit)
					{
						if (object)
							[connection->objectCache setObject:object forKey:cacheKey];
					}
				}
				
				id metadata = [connection->metadataCache objectForKey:cacheKey];
				if (metadata)
				{
					if (metadata == [YapNull null])
						metadata = nil;
				}
				else
				{
					const void *mBlob = sqlite3_column_blob(statement, 4);
					int mBlobSize = sqlite3_column_bytes(statement, 4);
					
					if (mBlobSize > 0)
					{
						NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
						metadata = connection->database->metadataDeserializer(collection, key, mData);
					}
					
					if (unlimitedMetadataCacheLimit ||
					    [connection->metadataCache count] < connection->metadataCacheLimit)
					{
						if (metadata)
							[connection->metadataCache setObject:metadata forKey:cacheKey];
						else
							[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
					}
				}
				
				block(rowid, collection, key, object, metadata, &stop);
				
				if (stop || isMutated) break;
			}
			
		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}
	
	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Returns an extension transaction corresponding to the extension type registered under the given name.
 * If the extension has not yet been prepared, it is done so automatically.
 *
 * @return
 *     A subclass of YapDatabaseExtensionTransaction,
 *     according to the type of extension registered under the given name.
 *
 * One must register an extension with the database before it can be accessed from within connections or transactions.
 * After registration everything works automatically using just the registered extension name.
 *
 * @see [YapDatabase registerExtension:withName:]
**/
- (id)extension:(NSString *)extensionName
{
	// This method is PUBLIC
	
	if (extensionsReady)
		return [extensions objectForKey:extensionName];
	
	if (extensions == nil)
		extensions = [[NSMutableDictionary alloc] init];
	
	YapDatabaseExtensionTransaction *extTransaction = [extensions objectForKey:extensionName];
	if (extTransaction == nil)
	{
		YapDatabaseExtensionConnection *extConnection = [connection extension:extensionName];
		if (extConnection)
		{
			if (isReadWriteTransaction)
				extTransaction = [extConnection newReadWriteTransaction:(YapDatabaseReadWriteTransaction *)self];
			else
				extTransaction = [extConnection newReadTransaction:self];
			
			if ([extTransaction prepareIfNeeded])
			{
				[extensions setObject:extTransaction forKey:extensionName];
			}
			else
			{
				extTransaction = nil;
			}
		}
	}
	
	return extTransaction;
}

- (id)ext:(NSString *)extensionName
{
	// This method is PUBLIC
	
	// The "+ (void)load" method swizzles the implementation of this class
	// to point to the implementation of the extension: method.
	//
	// So the two methods are literally the same thing.
	
	return [self extension:extensionName]; // This method is swizzled !
}

- (void)prepareExtensions
{
	if (extensions == nil)
		extensions = [[NSMutableDictionary alloc] init];
	
	NSDictionary *extConnections = [connection extensions];
	
	[extConnections enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		
		__unsafe_unretained NSString *extName = key;
		__unsafe_unretained YapDatabaseExtensionConnection *extConnection = obj;
		
		YapDatabaseExtensionTransaction *extTransaction = [extensions objectForKey:extName];
		if (extTransaction == nil)
		{
			if (isReadWriteTransaction)
				extTransaction = [extConnection newReadWriteTransaction:(YapDatabaseReadWriteTransaction *)self];
			else
				extTransaction = [extConnection newReadTransaction:self];
			
			if ([extTransaction prepareIfNeeded])
			{
				[extensions setObject:extTransaction forKey:extName];
			}
		}
	}];
	
	if (orderedExtensions == nil)
		orderedExtensions = [[NSMutableArray alloc] initWithCapacity:[extensions count]];
	
	for (NSString *extName in connection->extensionsOrder)
	{
		YapDatabaseExtensionTransaction *extTransaction = [extensions objectForKey:extName];
		if (extTransaction)
		{
			[orderedExtensions addObject:extTransaction];
		}
	}
	
	extensionsReady = YES;
}

- (NSDictionary *)extensions
{
	// This method is INTERNAL
	
	if (!extensionsReady)
	{
		[self prepareExtensions];
	}
	
	return extensions;
}

- (NSArray *)orderedExtensions
{
	// This method is INTERNAL
	
	if (!extensionsReady)
	{
		[self prepareExtensions];
	}
	
	return orderedExtensions;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Memory Tables
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (YapMemoryTableTransaction *)memoryTableTransaction:(NSString *)tableName
{
	BOOL isYap = [tableName isEqualToString:@"yap"];
	if (isYap && yapMemoryTableTransaction)
		return yapMemoryTableTransaction;
	
	YapMemoryTableTransaction *memoryTableTransaction = nil;
		
	YapMemoryTable *table = [[connection registeredMemoryTables] objectForKey:tableName];
	if (table)
	{
		uint64_t snapshot = [connection snapshot];
		
		if (isReadWriteTransaction)
			memoryTableTransaction = [table newReadWriteTransactionWithSnapshot:(snapshot + 1)];
		else
			memoryTableTransaction = [table newReadTransactionWithSnapshot:snapshot];
	}
	
	if (isYap) {
		yapMemoryTableTransaction = memoryTableTransaction;
	}
	return memoryTableTransaction;
}

/**
 * The system automatically creates a special YapMemoryTable that's available for any extension.
 * This memoryTable is registered under the reserved name @"yap".
 *
 * The yapMemoryTableTransaction uses a keyClass of [YapCollectionKey class].
 * Thus, when using it, you must pass a YapCollectionKey, where collectionKey.collection == extensionName.
 * 
 * This memory table is an in-memory alternative to using the yap sqlite table.
**/
- (YapMemoryTableTransaction *)yapMemoryTableTransaction
{
	return [self memoryTableTransaction:@"yap"];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Yap2 Table
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (BOOL)getBoolValue:(BOOL *)valuePtr forKey:(NSString *)key extension:(NSString *)extensionName
{
	int intValue = 0;
	BOOL result = [self getIntValue:&intValue forKey:key extension:extensionName];
	
	if (valuePtr) *valuePtr = (intValue == 0) ? NO : YES;
	return result;
}

- (BOOL)getIntValue:(int *)valuePtr forKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapGetDataForKeyStatement];
	if (statement == NULL) {
		if (valuePtr) *valuePtr = 0;
		return NO;
	}
	
	BOOL result = NO;
	int value = 0;
	
	// SELECT data FROM 'yap2' WHERE extension = ? AND key = ? ;
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		result = YES;
		value = sqlite3_column_int(statement, 0);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'yapGetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
	
	if (valuePtr) *valuePtr = value;
	return result;
}

- (BOOL)getDoubleValue:(double *)valuePtr forKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapGetDataForKeyStatement];
	if (statement == NULL) {
		if (valuePtr) *valuePtr = 0.0;
		return NO;
	}
	
	BOOL result = NO;
	double value = 0.0;
	
	// SELECT data FROM 'yap2' WHERE extension = ? AND key = ? ;
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		result = YES;
		value = sqlite3_column_double(statement, 0);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'yapGetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
	
	if (valuePtr) *valuePtr = value;
	return result;
}

- (NSString *)stringValueForKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapGetDataForKeyStatement];
	if (statement == NULL) return nil;
	
	NSString *value = nil;
	
	// SELECT data FROM 'yap2' WHERE extension = ? AND key = ? ;
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		const unsigned char *text = sqlite3_column_text(statement, 0);
		int textSize = sqlite3_column_bytes(statement, 0);
		
		value = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'yapGetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
	
	return value;
}

- (NSData *)dataValueForKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapGetDataForKeyStatement];
	if (statement == NULL) return nil;
	
	NSData *value = nil;
	
	// SELECT data FROM 'yap2' WHERE extension = ? AND key = ? ;
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);
		
		value = [[NSData alloc] initWithBytes:(void *)blob length:blobSize];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'yapGetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
	
	return value;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Exceptions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSException *)mutationDuringEnumerationException
{
	NSString *reason = [NSString stringWithFormat:
	    @"Database <%@: %p> was mutated while being enumerated.", NSStringFromClass([self class]), self];
	
	NSDictionary *userInfo = @{ NSLocalizedRecoverySuggestionErrorKey:
	    @"If you modify the database during enumeration"
		@" you MUST set the 'stop' parameter of the enumeration block to YES (*stop = YES;)."};
	
	return [NSException exceptionWithName:@"YapDatabaseException" reason:reason userInfo:userInfo];
}

@end

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark -
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

@implementation YapDatabaseReadWriteTransaction

#pragma mark Transaction Control

/**
 * Under normal circumstances, when a read-write transaction block completes,
 * the changes are automatically committed. If, however, something goes wrong and
 * you'd like to abort and discard all changes made within the transaction,
 * then invoke this method.
 *
 * You should generally return (exit the transaction block) after invoking this method.
 * Any changes made within the the transaction before and after invoking this method will be discarded.
 *
 * Invoking this method from within a read-only transaction does nothing.
**/
- (void)rollback
{
	rollback = YES;
}

/**
 * The YapDatabaseModifiedNotification is posted following a readwrite transaction which made changes.
 * 
 * These notifications are used in a variety of ways:
 * - They may be used as a general notification mechanism to detect changes to the database.
 * - They may be used by extensions to post change information.
 *   For example, YapDatabaseView will post the index changes, which can easily be used to animate a tableView.
 * - They are integrated into the architecture of long-lived transactions in order to maintain a steady state.
 *
 * Thus it is recommended you integrate your own notification information into this existing notification,
 * as opposed to broadcasting your own separate notification.
 * 
 * For more information, and code samples, please see the wiki article:
 * https://github.com/yaptv/YapDatabase/wiki/YapDatabaseModifiedNotification
**/
@synthesize yapDatabaseModifiedNotificationCustomObject = customObjectForNotification;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Object & Metadata
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Sets the object for the given key/collection.
 * The object is automatically serialized using the database's configured objectSerializer.
 *
 * If you pass nil for the object, then this method will remove the row from the database (if it exists).
 * This method implicitly sets the associated metadata to nil.
 *
 * @param object
 *   The object to store in the database.
 *   This object is automatically serialized using the database's configured objectSerializer.
 *
 * @param key
 *   The lookup key.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   This value should not be nil. If a nil key is passed, then this method does nothing.
 *
 * @param collection
 *   The lookup collection.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   If a nil collection is passed, then the collection is implicitly the empty string (@"").
**/
- (void)setObject:(id)object forKey:(NSString *)key inCollection:(NSString *)collection
{
	[self setObject:object forKey:key inCollection:collection withMetadata:nil
                                                          serializedObject:nil
	                                                    serializedMetadata:nil];
}

/**
 * Sets the object & metadata for the given key/collection.
 *
 * If you pass nil for the object, then this method will remove the row from the database (if it exists).
 *
 * @param object
 *   The object to store in the database.
 *   This object is automatically serialized using the database's configured objectSerializer.
 *
 * @param key
 *   The lookup key.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   This value should not be nil. If a nil key is passed, then this method does nothing.
 *
 * @param collection
 *   The lookup collection.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   If a nil collection is passed, then the collection is implicitly the empty string (@"").
 *
 * @param metadata
 *   The metadata to store in the database.
 *   This metadata is automatically serialized using the database's configured metadataSerializer.
 *   The metadata is optional. You can pass nil for the metadata is unneeded.
 *   If non-nil then the metadata is also written to the database (metadata is also persistent).
**/
- (void)setObject:(id)object forKey:(NSString *)key inCollection:(NSString *)collection withMetadata:(id)metadata
{
	[self setObject:object forKey:key inCollection:collection withMetadata:metadata
                                                          serializedObject:nil
	                                                    serializedMetadata:nil];
}

/**
 * Sets the object & metadata for the given key/collection.
 * 
 * This method allows for a bit of optimization if you happen to already have a serialized version of
 * the object and/or metadata. For example, if you downloaded an object in serialized form,
 * and you still have the raw NSData, then you can use this method to skip the serialization step
 * when storing the object to the database.
 *
 * If you pass nil for the object, then this method will remove the row from the database (if it exists).
 *
 * @param object
 *   The object to store in the database.
 *   This object is automatically serialized using the database's configured objectSerializer.
 *
 * @param key
 *   The lookup key.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   This value should not be nil. If a nil key is passed, then this method does nothing.
 *
 * @param collection
 *   The lookup collection.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   If a nil collection is passed, then the collection is implicitly the empty string (@"").
 *
 * @param metadata
 *   The metadata to store in the database.
 *   This metadata is automatically serialized using the database's configured metadataSerializer.
 *   The metadata is optional. You can pass nil for the metadata is unneeded.
 *   If non-nil then the metadata is also written to the database (metadata is also persistent).
 * 
 * @param preSerializedObject
 *   This value is optional.
 *   If non-nil then the object serialization step is skipped, and this value is used instead.
 *   It is assumed that preSerializedObject is equal to what we would get if we ran the object through
 *   the database's configured objectSerializer.
 * 
 * @param preSerializedMetadata
 *   This value is optional.
 *   If non-nil then the metadata serialization step is skipped, and this value is used instead.
 *   It is assumed that preSerializedMetadata is equal to what we would get if we ran the metadata through
 *   the database's configured metadataSerializer.
 *
 * The preSerializedObject is only used if object is non-nil.
 * The preSerializedMetadata is only used if metadata is non-nil.
**/
- (void)setObject:(id)object forKey:(NSString *)key inCollection:(NSString *)collection
                                                    withMetadata:(id)metadata
                                                serializedObject:(NSData *)preSerializedObject
                                              serializedMetadata:(NSData *)preSerializedMetadata
{
	if (object == nil)
	{
		[self removeObjectForKey:key inCollection:collection];
		return;
	}
	
	if (key == nil) return;
	if (collection == nil) collection = @"";
	
	if (connection->database->objectSanitizer)
	{
		object = connection->database->objectSanitizer(collection, key, object);
		if (object == nil)
		{
			YDBLogWarn(@"Object sanitizer returned nil for key(%@) object: %@", key, object);
			
			[self removeObjectForKey:key inCollection:collection];
			return;
		}
	}
	if (metadata && connection->database->metadataSanitizer)
	{
		metadata = connection->database->metadataSanitizer(collection, key, metadata);
		if (metadata == nil)
		{
			YDBLogWarn(@"Metadata sanitizer returned nil for key(%@) metadata: %@", key, metadata);
		}
	}
	
	// To use SQLITE_STATIC on our data, we use the objc_precise_lifetime attribute.
	// This ensures the data isn't released until it goes out of scope.
	
	__attribute__((objc_precise_lifetime)) NSData *serializedObject = nil;
	if (preSerializedObject)
		serializedObject = preSerializedObject;
	else
		serializedObject = connection->database->objectSerializer(collection, key, object);
	
	__attribute__((objc_precise_lifetime)) NSData *serializedMetadata = nil;
	if (metadata)
	{
		if (preSerializedMetadata)
			serializedMetadata = preSerializedMetadata;
		else
			serializedMetadata = connection->database->metadataSerializer(collection, key, metadata);
	}
	
	
	BOOL found = NO;
	int64_t rowid = 0;
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	
	if (YES) // fetch rowid for key
	{
		sqlite3_stmt *statement = [connection getRowidForKeyStatement];
		if (statement == NULL) {
			FreeYapDatabaseString(&_collection);
			FreeYapDatabaseString(&_key);
			return;
		}
		
		// SELECT "rowid" FROM "database2" WHERE "collection" = ? AND "key" = ?;
		
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
		
		int status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			rowid = sqlite3_column_int64(statement, 0);
			found = YES;
		}
		else if (status == SQLITE_ERROR)
		{
			YDBLogError(@"Error executing 'getRowidForKeyStatement': %d %s, key(%@)",
			            status, sqlite3_errmsg(connection->db), key);
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
	}
	
	BOOL set = YES;
    YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
    
    for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
    {
        if (found)
            [extTransaction handleWillUpdateObject:object
                                  forCollectionKey:cacheKey
                                      withMetadata:metadata
                                             rowid:rowid];
        else
            [extTransaction handleWillInsertObject:object
                                  forCollectionKey:cacheKey
                                      withMetadata:metadata];
    }
    
    
	
	if (found) // update data for key
	{
		sqlite3_stmt *statement = [connection updateAllForRowidStatement];
		if (statement == NULL) {
			FreeYapDatabaseString(&_collection);
			FreeYapDatabaseString(&_key);
			return;
		}
		
		// UPDATE "database2" SET "data" = ?, "metadata" = ? WHERE "rowid" = ?;
		
		sqlite3_bind_blob(statement, 1, serializedObject.bytes, (int)serializedObject.length, SQLITE_STATIC);
		sqlite3_bind_blob(statement, 2, serializedMetadata.bytes, (int)serializedMetadata.length, SQLITE_STATIC);
		
		sqlite3_bind_int64(statement, 3, rowid);
		
		int status = sqlite3_step(statement);
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"Error executing 'updateAllForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
			set = NO;
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
	}
	else // insert data for key
	{
		sqlite3_stmt *statement = [connection insertForRowidStatement];
		if (statement == NULL) {
			FreeYapDatabaseString(&_collection);
			FreeYapDatabaseString(&_key);
			return;
		}
		
		// INSERT INTO "database2" ("collection", "key", "data", "metadata") VALUES (?, ?, ?, ?);
		
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
		
		sqlite3_bind_blob(statement, 3, serializedObject.bytes, (int)serializedObject.length, SQLITE_STATIC);
		sqlite3_bind_blob(statement, 4, serializedMetadata.bytes, (int)serializedMetadata.length, SQLITE_STATIC);
		
		int status = sqlite3_step(statement);
		if (status == SQLITE_DONE)
		{
			rowid = sqlite3_last_insert_rowid(connection->db);
		}
		else
		{
			YDBLogError(@"Error executing 'insertForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
			set = NO;
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
	}
	
	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);
	
	if (!set) return;
	
	connection->hasDiskChanges = YES;
	isMutated = YES;  // mutation during enumeration protection
	
	[connection->keyCache setObject:cacheKey forKey:@(rowid)];
	
	id _object = nil;
	if (connection->objectPolicy == YapDatabasePolicyContainment) {
		_object = [YapNull null];
	}
	else if (connection->objectPolicy == YapDatabasePolicyShare) {
		_object = object;
	}
	else // if (connection->objectPolicy == YapDatabasePolicyCopy)
	{
		if ([object conformsToProtocol:@protocol(NSCopying)])
			_object = [object copy];
		else
			_object = [YapNull null];
	}
	
	[connection->objectCache setObject:object forKey:cacheKey];
	[connection->objectChanges setObject:_object forKey:cacheKey];
	
	if (metadata)
	{
		id _metadata = nil;
		if (connection->metadataPolicy == YapDatabasePolicyContainment) {
			_metadata = [YapNull null];
		}
		else if (connection->metadataPolicy == YapDatabasePolicyShare) {
			_metadata = metadata;
		}
		else // if (connection->metadataPolicy = YapDatabasePolicyCopy)
		{
			if ([metadata conformsToProtocol:@protocol(NSCopying)])
				_metadata = [metadata copy];
			else
				_metadata = [YapNull null];
		}
		
		[connection->metadataCache setObject:metadata forKey:cacheKey];
		[connection->metadataChanges setObject:_metadata forKey:cacheKey];
	}
	else
	{
		[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
		[connection->metadataChanges setObject:[YapNull null] forKey:cacheKey];
	}
	
	for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
	{
		if (found)
			[extTransaction handleUpdateObject:object
			                  forCollectionKey:cacheKey
			                      withMetadata:metadata
			                             rowid:rowid];
		else
			[extTransaction handleInsertObject:object
			                  forCollectionKey:cacheKey
			                      withMetadata:metadata
			                             rowid:rowid];
	}
}

/**
 * If a row with the given key/collection exists, then replaces the object for that row with the new value.
 *
 * It only replaces the object. The metadata for the row doesn't change.
 * If there is no row in the database for the given key/collection then this method does nothing.
 *
 * If you pass nil for the object, then this method will remove the row from the database (if it exists).
 *
 * @param object
 *   The object to store in the database.
 *   This object is automatically serialized using the database's configured objectSerializer.
 *
 * @param key
 *   The lookup key.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   This value should not be nil. If a nil key is passed, then this method does nothing.
 *
 * @param collection
 *   The lookup collection.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   If a nil collection is passed, then the collection is implicitly the empty string (@"").
**/
- (void)replaceObject:(id)object forKey:(NSString *)key inCollection:(NSString *)collection
{
	int64_t rowid = 0;
	if ([self getRowid:&rowid forKey:key inCollection:collection])
	{
		[self replaceObject:object forKey:key inCollection:collection withRowid:rowid serializedObject:nil];
	}
}

/**
 * If a row with the given key/collection exists, then replaces the object for that row with the new value.
 *
 * It only replaces the object. The metadata for the row doesn't change.
 * If there is no row in the database for the given key/collection then this method does nothing.
 *
 * If you pass nil for the object, then this method will remove the row from the database (if it exists).
 * 
 * This method allows for a bit of optimization if you happen to already have a serialized version of
 * the object and/or metadata. For example, if you downloaded an object in serialized form,
 * and you still have the raw serialized NSData, then you can use this method to skip the serialization step
 * when storing the object to the database.
 *
 * @param object
 *   The object to store in the database.
 *   This object is automatically serialized using the database's configured objectSerializer.
 *
 * @param key
 *   The lookup key.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   This value should not be nil. If a nil key is passed, then this method does nothing.
 *
 * @param collection
 *   The lookup collection.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   If a nil collection is passed, then the collection is implicitly the empty string (@"").
 *
 * @param preSerializedObject
 *   This value is optional.
 *   If non-nil then the object serialization step is skipped, and this value is used instead.
 *   It is assumed that preSerializedObject is equal to what we would get if we ran the object through
 *   the database's configured objectSerializer.
**/
- (void)replaceObject:(id)object forKey:(NSString *)key inCollection:(NSString *)collection
                                                withSerializedObject:(NSData *)preSerializedObject
{
	int64_t rowid = 0;
	if ([self getRowid:&rowid forKey:key inCollection:collection])
	{
		[self replaceObject:object
		             forKey:key
		       inCollection:collection
		          withRowid:rowid
		   serializedObject:preSerializedObject];
	}
}

/**
 * Internal replaceObject method that takes a rowid.
**/
- (void)replaceObject:(id)object forKey:(NSString *)key inCollection:(NSString *)collection
                                                           withRowid:(int64_t)rowid
                                                    serializedObject:(NSData *)preSerializedObject
{
	if (object == nil)
	{
		[self removeObjectForKey:key inCollection:collection withRowid:rowid];
		return;
	}
	
	NSAssert(key != nil, @"Internal error");
	if (collection == nil) collection = @"";
	
	if (connection->database->objectSanitizer)
	{
		object = connection->database->objectSanitizer(collection, key, object);
		if (object == nil)
		{
			YDBLogWarn(@"Object sanitizer returned nil for key(%@) object: %@", key, object);
			
			[self removeObjectForKey:key inCollection:collection withRowid:rowid];
			return;
		}
	}
	
    YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
    for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
    {
        [extTransaction handleWillReplaceObject:object forCollectionKey:cacheKey withRowid:rowid];
    }
    
	// To use SQLITE_STATIC on our data blob, we use the objc_precise_lifetime attribute.
	// This ensures the data isn't released until it goes out of scope.
	
	__attribute__((objc_precise_lifetime)) NSData *serializedObject = nil;
	if (preSerializedObject)
		serializedObject = preSerializedObject;
	else
		serializedObject = connection->database->objectSerializer(collection, key, object);
	
	sqlite3_stmt *statement = [connection updateObjectForRowidStatement];
	if (statement == NULL) return;
	
	// UPDATE "database2" SET "data" = ? WHERE "rowid" = ?;
	
	sqlite3_bind_blob(statement, 1, serializedObject.bytes, (int)serializedObject.length, SQLITE_STATIC);
	sqlite3_bind_int64(statement, 2, rowid);
	
	BOOL updated = YES;
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing 'updateObjectForRowidStatement': %d %s",
		                                                    status, sqlite3_errmsg(connection->db));
		updated = NO;
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	if (!updated) return;
	
	connection->hasDiskChanges = YES;
	isMutated = YES;  // mutation during enumeration protection
	
	id _object = nil;
	if (connection->objectPolicy == YapDatabasePolicyContainment) {
		_object = [YapNull null];
	}
	else if (connection->objectPolicy == YapDatabasePolicyShare) {
		_object = object;
	}
	else // if (connection->objectPolicy = YapDatabasePolicyCopy)
	{
		if ([object conformsToProtocol:@protocol(NSCopying)])
			_object = [object copy];
		else
			_object = [YapNull null];
	}
	
	[connection->objectCache setObject:object forKey:cacheKey];
	[connection->objectChanges setObject:_object forKey:cacheKey];
	
	for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
	{
		[extTransaction handleReplaceObject:object forCollectionKey:cacheKey withRowid:rowid];
	}
}

/**
 * If a row with the given key/collection exists, then replaces the metadata for that row with the new value.
 *
 * It only replaces the metadata. The object for the row doesn't change.
 * If there is no row in the database for the given key/collection then this method does nothing.
 *
 * If you pass nil for the metadata, any metadata previously associated with the key/collection is removed.
 *
 * @param metadata
 *   The metadata to store in the database.
 *   This metadata is automatically serialized using the database's configured metadataSerializer.
 *
 * @param key
 *   The lookup key.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   This value should not be nil. If a nil key is passed, then this method does nothing.
 *
 * @param collection
 *   The lookup collection.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   If a nil collection is passed, then the collection is implicitly the empty string (@"").
**/
- (void)replaceMetadata:(id)metadata forKey:(NSString *)key inCollection:(NSString *)collection
{
	int64_t rowid = 0;
	if ([self getRowid:&rowid forKey:key inCollection:collection])
	{
		[self replaceMetadata:metadata forKey:key inCollection:collection withRowid:rowid serializedMetadata:nil];
	}
}

/**
 * If a row with the given key/collection exists, then replaces the metadata for that row with the new value.
 *
 * It only replaces the metadata. The object for the row doesn't change.
 * If there is no row in the database for the given key/collection then this method does nothing.
 *
 * If you pass nil for the metadata, any metadata previously associated with the key/collection is removed.
 *
 * This method allows for a bit of optimization if you happen to already have a serialized version of
 * the object and/or metadata. For example, if you downloaded an object in serialized form,
 * and you still have the raw serialized NSData, then you can use this method to skip the serialization step
 * when storing the object to the database.
 * 
 * @param metadata
 *   The metadata to store in the database.
 *   This metadata is automatically serialized using the database's configured metadataSerializer.
 *
 * @param key
 *   The lookup key.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   This value should not be nil. If a nil key is passed, then this method does nothing.
 *
 * @param collection
 *   The lookup collection.
 *   The <collection, key> tuple is used to uniquely identify the row in the database.
 *   If a nil collection is passed, then the collection is implicitly the empty string (@"").
 * 
 * @param preSerializedMetadata
 *   This value is optional.
 *   If non-nil then the metadata serialization step is skipped, and this value is used instead.
 *   It is assumed that preSerializedMetadata is equal to what we would get if we ran the metadata through
 *   the database's configured metadataSerializer.
**/
- (void)replaceMetadata:(id)metadata forKey:(NSString *)key inCollection:(NSString *)collection
                                                  withSerializedMetadata:(NSData *)preSerializedMetadata
{
	int64_t rowid = 0;
	if ([self getRowid:&rowid forKey:key inCollection:collection])
	{
		[self replaceMetadata:metadata
		               forKey:key
		         inCollection:collection
		            withRowid:rowid
		   serializedMetadata:preSerializedMetadata];
	}
}

/**
 * Internal replaceMetadata method that takes a rowid.
**/
- (void)replaceMetadata:(id)metadata
                 forKey:(NSString *)key
           inCollection:(NSString *)collection
              withRowid:(int64_t)rowid
     serializedMetadata:(NSData *)preSerializedMetadata
{
	NSAssert(key != nil, @"Internal error");
	if (collection == nil) collection = @"";
	
	if (metadata && connection->database->metadataSanitizer)
	{
		metadata = connection->database->metadataSanitizer(collection, key, metadata);
		if (metadata == nil)
		{
			YDBLogWarn(@"Metadata sanitizer returned nil for key: %@", key);
		}
	}
	
	// To use SQLITE_STATIC on our data blob, we use the objc_precise_lifetime attribute.
	// This ensures the data isn't released until it goes out of scope.
	
	__attribute__((objc_precise_lifetime)) NSData *serializedMetadata = nil;
	if (metadata)
	{
		if (preSerializedMetadata)
			serializedMetadata = preSerializedMetadata;
		else
			serializedMetadata = connection->database->metadataSerializer(collection, key, metadata);
	}
	
	sqlite3_stmt *statement = [connection updateMetadataForRowidStatement];
	if (statement == NULL) return;
	
	// UPDATE "database2" SET "metadata" = ? WHERE "rowid" = ?;
	
	sqlite3_bind_blob(statement, 1, serializedMetadata.bytes, (int)serializedMetadata.length, SQLITE_STATIC);
	sqlite3_bind_int64(statement, 2, rowid);
	
	BOOL updated = YES;
	
    YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
    for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
    {
        [extTransaction handleWillReplaceMetadata:metadata forCollectionKey:cacheKey withRowid:rowid];
    }

	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing 'updateMetadataForRowidStatement': %d %s",
		                                                    status, sqlite3_errmsg(connection->db));
		updated = NO;
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	if (!updated) return;
	
	connection->hasDiskChanges = YES;
	isMutated = YES;  // mutation during enumeration protection
	
	if (metadata)
	{
		id _metadata = nil;
		if (connection->metadataPolicy == YapDatabasePolicyContainment) {
			_metadata = [YapNull null];
		}
		else if (connection->metadataPolicy == YapDatabasePolicyShare) {
			_metadata = metadata;
		}
		else // if (connection->metadataPolicy = YapDatabasePolicyCopy)
		{
			if ([metadata conformsToProtocol:@protocol(NSCopying)])
				_metadata = [metadata copy];
			else
				_metadata = [YapNull null];
		}
		
		[connection->metadataCache setObject:metadata forKey:cacheKey];
		[connection->metadataChanges setObject:_metadata forKey:cacheKey];
	}
	else
	{
		[connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
		[connection->metadataChanges setObject:[YapNull null] forKey:cacheKey];
	}
	
	for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
	{
		[extTransaction handleReplaceMetadata:metadata forCollectionKey:cacheKey withRowid:rowid];
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Touch
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)touchObjectForKey:(NSString *)key inCollection:(NSString *)collection
{
	if (collection == nil) collection = @"";
	
	int64_t rowid = 0;
	if (![self getRowid:&rowid forKey:key inCollection:collection]) return;
	
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	if ([connection->objectChanges objectForKey:cacheKey] == nil)
		[connection->objectChanges setObject:[YapTouch touch] forKey:cacheKey];
	
	if ([connection->metadataChanges objectForKey:cacheKey] == nil)
		[connection->metadataChanges setObject:[YapTouch touch] forKey:cacheKey];
	
	for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
	{
		[extTransaction handleTouchObjectForCollectionKey:cacheKey withRowid:rowid];
	}
}

- (void)touchMetadataForKey:(NSString *)key inCollection:(NSString *)collection
{
	if (collection == nil) collection = @"";
	
	int64_t rowid = 0;
	if (![self getRowid:&rowid forKey:key inCollection:collection]) return;
	
	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	if ([connection->metadataChanges objectForKey:cacheKey] == nil)
		[connection->metadataChanges setObject:[YapTouch touch] forKey:cacheKey];
	
	for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
	{
		[extTransaction handleTouchMetadataForCollectionKey:cacheKey withRowid:rowid];
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Remove
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)removeObjectForKey:(NSString *)key inCollection:(NSString *)collection withRowid:(int64_t)rowid
{
	if (key == nil) return;
	if (collection == nil) collection = @"";
	
	sqlite3_stmt *statement = [connection removeForRowidStatement];
	if (statement == NULL) return;
	
	// DELETE FROM 'database' WHERE 'rowid' = ?;
	
	sqlite3_bind_int64(statement, 1, rowid);
	
	BOOL removed = YES;
	
    YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
    for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
    {
        [extTransaction handleWillRemoveObjectForCollectionKey:cacheKey withRowid:rowid];
    }

	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing 'removeForRowidStatement': %d %s, key(%@)",
		                                                   status, sqlite3_errmsg(connection->db), key);
		removed = NO;
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	if (!removed) return;
	
	connection->hasDiskChanges = YES;
	isMutated = YES;  // mutation during enumeration protection
	NSNumber *rowidNumber = @(rowid);
	
	[connection->keyCache removeObjectForKey:rowidNumber];
	[connection->objectCache removeObjectForKey:cacheKey];
	[connection->metadataCache removeObjectForKey:cacheKey];
	
	[connection->objectChanges removeObjectForKey:cacheKey];
	[connection->metadataChanges removeObjectForKey:cacheKey];
	[connection->removedKeys addObject:cacheKey];
	[connection->removedRowids addObject:rowidNumber];
	
	for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
	{
		[extTransaction handleRemoveObjectForCollectionKey:cacheKey withRowid:rowid];
	}
}

- (void)removeObjectForKey:(NSString *)key inCollection:(NSString *)collection
{
	int64_t rowid = 0;
	if ([self getRowid:&rowid forKey:key inCollection:collection])
	{
		[self removeObjectForKey:key inCollection:collection withRowid:rowid];
	}
}

- (void)removeObjectsForKeys:(NSArray *)keys inCollection:(NSString *)collection
{
	NSUInteger keysCount = [keys count];
	
	if (keysCount == 0) return;
	if (keysCount == 1) {
		[self removeObjectForKey:[keys objectAtIndex:0] inCollection:collection];
		return;
	}
	
	if (collection == nil)
		collection = @"";
	else
		collection = [collection copy]; // mutable string protection
	
	NSMutableArray *foundKeys = nil;
	NSMutableArray *foundRowids = nil;
	
	// Sqlite has an upper bound on the number of host parameters that may be used in a single query.
	// We need to watch out for this in case a large array of keys is passed.
	
	NSUInteger maxHostParams = (NSUInteger) sqlite3_limit(connection->db, SQLITE_LIMIT_VARIABLE_NUMBER, -1);
	
	// Loop over the keys, and remove them in big batches.
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	
	NSUInteger keysIndex = 0;
	do
	{
		NSUInteger left = keysCount - keysIndex;
		NSUInteger numKeyParams = MIN(left, (maxHostParams-1)); // minus 1 for collectionParam
		
		if (foundKeys == nil)
		{
			foundKeys   = [NSMutableArray arrayWithCapacity:numKeyParams];
			foundRowids = [NSMutableArray arrayWithCapacity:numKeyParams];
		}
		else
		{
			[foundKeys removeAllObjects];
			[foundRowids removeAllObjects];
		}
		
		// Find rowids for keys
		
		if (YES)
		{
			// SELECT "key", "rowid" FROM "database2" WHERE "collection" = ? AND "key" IN (?, ?, ...);
		
			NSUInteger capacity = 100 + (numKeyParams * 3);
			NSMutableString *query = [NSMutableString stringWithCapacity:capacity];
			
			[query appendString:
			    @"SELECT \"key\", \"rowid\" FROM \"database2\" WHERE \"collection\" = ? AND \"key\" IN ("];
			
			NSUInteger i;
			for (i = 0; i < numKeyParams; i++)
			{
				if (i == 0)
					[query appendFormat:@"?"];
				else
					[query appendFormat:@", ?"];
			}
			
			[query appendString:@");"];
			
			sqlite3_stmt *statement;
			
			int status = sqlite3_prepare_v2(connection->db, [query UTF8String], -1, &statement, NULL);
			if (status != SQLITE_OK)
			{
				YDBLogError(@"Error creating 'removeKeys:inCollection:' statement (A): %d %s",
				                                                              status, sqlite3_errmsg(connection->db));
				FreeYapDatabaseString(&_collection);
				return;
			}
			
			sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
			
			for (i = 0; i < numKeyParams; i++)
			{
				NSString *key = [keys objectAtIndex:(keysIndex + i)];
				sqlite3_bind_text(statement, (int)(i + 2), [key UTF8String], -1, SQLITE_TRANSIENT);
			}
			
			while ((status = sqlite3_step(statement)) == SQLITE_ROW)
			{
				const unsigned char *text = sqlite3_column_text(statement, 0);
				int textSize = sqlite3_column_bytes(statement, 0);
				
				int64_t rowid = sqlite3_column_int64(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				
				[foundKeys addObject:key];
				[foundRowids addObject:@(rowid)];
			}
			
			if (status != SQLITE_DONE)
			{
				YDBLogError(@"Error executing 'removeKeys:inCollection:' statement (A): %d %s",
				                                                               status, sqlite3_errmsg(connection->db));
			}
			
			sqlite3_finalize(statement);
			statement = NULL;
		}
		
		// Now remove all the matching rows
		
		NSUInteger foundCount = [foundRowids count];
		
		if (foundCount > 0)
		{
			// DELETE FROM "database2" WHERE "rowid" in (?, ?, ...);
			
			NSUInteger capacity = 50 + (foundCount * 3);
			NSMutableString *query = [NSMutableString stringWithCapacity:capacity];
			
			[query appendString:@"DELETE FROM \"database2\" WHERE \"rowid\" IN ("];
			
			NSUInteger i;
			for (i = 0; i < foundCount; i++)
			{
				if (i == 0)
					[query appendFormat:@"?"];
				else
					[query appendFormat:@", ?"];
			}
			
			[query appendString:@");"];
			
			sqlite3_stmt *statement;
			
			int status = sqlite3_prepare_v2(connection->db, [query UTF8String], -1, &statement, NULL);
			if (status != SQLITE_OK)
			{
				YDBLogError(@"Error creating 'removeKeys:inCollection:' statement (B): %d %s",
							status, sqlite3_errmsg(connection->db));
				return;
			}
			
			for (i = 0; i < foundCount; i++)
			{
				int64_t rowid = [[foundRowids objectAtIndex:i] longLongValue];
				
				sqlite3_bind_int64(statement, (int)(i + 1), rowid);
			}
			
            for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
            {
                [extTransaction handleWillRemoveObjectsForKeys:foundKeys
                                                  inCollection:collection
                                                    withRowids:foundRowids];
            }
            
			status = sqlite3_step(statement);
			if (status != SQLITE_DONE)
			{
				YDBLogError(@"Error executing 'removeKeys:inCollection:' statement (B): %d %s",
							status, sqlite3_errmsg(connection->db));
			}
			
			sqlite3_finalize(statement);
			statement = NULL;
			
			connection->hasDiskChanges = YES;
			isMutated = YES;  // mutation during enumeration protection
			
			[connection->keyCache removeObjectsForKeys:foundRowids];
			[connection->removedRowids addObjectsFromArray:foundRowids];
			
			for (NSString *key in foundKeys)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
				
				[connection->objectCache removeObjectForKey:cacheKey];
				[connection->metadataCache removeObjectForKey:cacheKey];
				
				[connection->objectChanges removeObjectForKey:cacheKey];
				[connection->metadataChanges removeObjectForKey:cacheKey];
				[connection->removedKeys addObject:cacheKey];
			}
			
			for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
			{
				[extTransaction handleRemoveObjectsForKeys:foundKeys
				                              inCollection:collection
				                                withRowids:foundRowids];
			}
			
		}
		
		// Move on to the next batch (if there's more)
		
		keysIndex += numKeyParams;
		
	} while (keysIndex < keysCount);
	
	
	FreeYapDatabaseString(&_collection);
}

- (void)removeAllObjectsInCollection:(NSString *)collection
{
	if (collection == nil)
		collection  = @"";
	else
		collection = [collection copy]; // mutable string protection
	
	// Purge the caches and changesets
	
	NSMutableArray *toRemove = [NSMutableArray array];
	
	{ // keyCache
		
		[connection->keyCache enumerateKeysAndObjectsWithBlock:^(id key, id obj, BOOL *stop) {
			
			__unsafe_unretained NSNumber *rowidNumber = (NSNumber *)key;
			__unsafe_unretained YapCollectionKey *collectionKey = (YapCollectionKey *)obj;
			if ([collectionKey.collection isEqualToString:collection])
			{
				[toRemove addObject:rowidNumber];
			}
		}];
		
		[connection->keyCache removeObjectsForKeys:toRemove];
		[toRemove removeAllObjects];
	}
	
	{ // objectCache
		
		[connection->objectCache enumerateKeysWithBlock:^(id key, BOOL *stop) {
			
			__unsafe_unretained YapCollectionKey *cacheKey = (YapCollectionKey *)key;
			if ([cacheKey.collection isEqualToString:collection])
			{
				[toRemove addObject:cacheKey];
			}
		}];
		
		[connection->objectCache removeObjectsForKeys:toRemove];
		[toRemove removeAllObjects];
	}
	
	{ // objectChanges
		
		for (id key in [connection->objectChanges keyEnumerator])
		{
			__unsafe_unretained YapCollectionKey *cacheKey = (YapCollectionKey *)key;
			if ([cacheKey.collection isEqualToString:collection])
			{
				[toRemove addObject:cacheKey];
			}
		}
		
		[connection->objectChanges removeObjectsForKeys:toRemove];
		[toRemove removeAllObjects];
	}
	
	{ // metadataCache
		
		[connection->metadataCache enumerateKeysWithBlock:^(id key, BOOL *stop) {
			
			__unsafe_unretained YapCollectionKey *cacheKey = (YapCollectionKey *)key;
			if ([cacheKey.collection isEqualToString:collection])
			{
				[toRemove addObject:cacheKey];
			}
		}];
		
		[connection->metadataCache removeObjectsForKeys:toRemove];
		[toRemove removeAllObjects];
	}
	
	{ // metadataChanges
		
		for (id key in [connection->metadataChanges keyEnumerator])
		{
			__unsafe_unretained YapCollectionKey *cacheKey = (YapCollectionKey *)key;
			if ([cacheKey.collection isEqualToString:collection])
			{
				[toRemove addObject:cacheKey];
			}
		}
		
		[connection->metadataChanges removeObjectsForKeys:toRemove];
	}
	
	[connection->removedCollections addObject:collection];
	
	// If there are no active extensions we can take a shortcut
	
	if ([[self extensions] count] == 0)
	{
		sqlite3_stmt *statement = [connection removeCollectionStatement];
		if (statement == NULL) return;
	
		// DELETE FROM "database2" WHERE "collection" = ?;
		
		YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
		
		int status = sqlite3_step(statement);
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"Error executing 'removeCollectionStatement': %d %s, collection(%@)",
			                                                       status, sqlite3_errmsg(connection->db), collection);
		}
		
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
		FreeYapDatabaseString(&_collection);
		
		connection->hasDiskChanges = YES;
		isMutated = YES;  // mutation during enumeration protection
		
		return;
	} // end shortcut
	
	
	NSUInteger left = [self numberOfKeysInCollection:collection];
	
	NSMutableArray *foundKeys = nil;
	NSMutableArray *foundRowids = nil;
	
	// Sqlite has an upper bound on the number of host parameters that may be used in a single query.
	// We need to watch out for this in case a large array of keys is passed.
	
	NSUInteger maxHostParams = (NSUInteger) sqlite3_limit(connection->db, SQLITE_LIMIT_VARIABLE_NUMBER, -1);
	
	// Loop over the keys, and remove them in big batches.
	
	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
	
	do
	{
		NSUInteger numKeyParams = MIN(left, maxHostParams-1); // minus 1 for collectionParam
		
		if (foundKeys == nil)
		{
			foundKeys   = [NSMutableArray arrayWithCapacity:numKeyParams];
			foundRowids = [NSMutableArray arrayWithCapacity:numKeyParams];
		}
		else
		{
			[foundKeys removeAllObjects];
			[foundRowids removeAllObjects];
		}
		
		NSUInteger foundCount = 0;
		
		// Find rowids for keys
		
		if (YES)
		{
			sqlite3_stmt *statement = [connection enumerateKeysInCollectionStatement];
			if (statement == NULL) {
				FreeYapDatabaseString(&_collection);
				return;
			}
			
			// SELECT "rowid", "key" FROM "database2" WHERE collection = ?;
			
			sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);
			
			int status;
			while ((status = sqlite3_step(statement)) == SQLITE_ROW)
			{
				int64_t rowid = sqlite3_column_int64(statement, 0);
				
				const unsigned char *text = sqlite3_column_text(statement, 1);
				int textSize = sqlite3_column_bytes(statement, 1);
				
				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
				
				[foundKeys addObject:key];
				[foundRowids addObject:@(rowid)];
				
				if (++foundCount >= numKeyParams)
				{
					break;
				}
			}
			
			if ((foundCount < numKeyParams) && (status != SQLITE_DONE))
			{
				YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
			}
			
			sqlite3_clear_bindings(statement);
			sqlite3_reset(statement);
		}
		
		// Now remove all the matching rows
		
		if (foundCount > 0)
		{
			// DELETE FROM "database2" WHERE "rowid" in (?, ?, ...);
			
			NSUInteger capacity = 50 + (foundCount * 3);
			NSMutableString *query = [NSMutableString stringWithCapacity:capacity];
			
			[query appendString:@"DELETE FROM \"database2\" WHERE \"rowid\" IN ("];
			
			NSUInteger i;
			for (i = 0; i < foundCount; i++)
			{
				if (i == 0)
					[query appendFormat:@"?"];
				else
					[query appendFormat:@", ?"];
			}
			
			[query appendString:@");"];
			
			sqlite3_stmt *statement;
			
			int status = sqlite3_prepare_v2(connection->db, [query UTF8String], -1, &statement, NULL);
			if (status != SQLITE_OK)
			{
				YDBLogError(@"Error creating 'removeAllObjectsInCollection:' statement: %d %s",
				            status, sqlite3_errmsg(connection->db));
				
				FreeYapDatabaseString(&_collection);
				return;
			}
			
			for (i = 0; i < foundCount; i++)
			{
				int64_t rowid = [[foundRowids objectAtIndex:i] longLongValue];
				
				sqlite3_bind_int64(statement, (int)(i + 1), rowid);
			}
            
            for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
            {
                [extTransaction handleWillRemoveObjectsForKeys:foundKeys
                                                  inCollection:collection
                                                    withRowids:foundRowids];
            }
			
			status = sqlite3_step(statement);
			if (status != SQLITE_DONE)
			{
				YDBLogError(@"Error executing 'removeAllObjectsInCollection:' statement: %d %s",
				            status, sqlite3_errmsg(connection->db));
			}
			
			sqlite3_finalize(statement);
			statement = NULL;
			
			connection->hasDiskChanges = YES;
			isMutated = YES;  // mutation during enumeration protection
			
			[connection->removedRowids addObjectsFromArray:foundRowids];
			
			for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
			{
				[extTransaction handleRemoveObjectsForKeys:foundKeys
				                              inCollection:collection
				                                withRowids:foundRowids];
			}
		}
		
		// Move on to the next batch (if there's more)
		
		left -= foundCount;
		
	} while((left > 0) && ([foundKeys count] > 0));
	
	
	FreeYapDatabaseString(&_collection);
}

- (void)removeAllObjectsInAllCollections
{
	sqlite3_stmt *statement = [connection removeAllStatement];
	if (statement == NULL) return;

    for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
    {
        [extTransaction handleWillRemoveAllObjectsInAllCollections];
    }
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing 'removeAllStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_reset(statement);
	
	connection->hasDiskChanges = YES;
	isMutated = YES;  // mutation during enumeration protection
	
	[connection->keyCache removeAllObjects];
	[connection->objectCache removeAllObjects];
	[connection->metadataCache removeAllObjects];
	
	[connection->objectChanges removeAllObjects];
	[connection->metadataChanges removeAllObjects];
	[connection->removedKeys removeAllObjects];
	[connection->removedCollections removeAllObjects];
	[connection->removedRowids removeAllObjects];
	connection->allKeysRemoved = YES;
	
	for (YapDatabaseExtensionTransaction *extTransaction in [self orderedExtensions])
	{
		[extTransaction handleRemoveAllObjectsInAllCollections];
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)addRegisteredExtensionTransaction:(YapDatabaseExtensionTransaction *)extTransaction withName:(NSString *)extName
{
	// This method is INTERNAL
	
	if (extensions == nil)
		extensions = [[NSMutableDictionary alloc] init];
	
	[extensions setObject:extTransaction forKey:extName];
}

- (void)removeRegisteredExtensionTransactionWithName:(NSString *)extName
{
	// This method is INTERNAL
	
	[extensions removeObjectForKey:extName];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Yap2 Table
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)setBoolValue:(BOOL)value forKey:(NSString *)key extension:(NSString *)extensionName
{
	[self setIntValue:(value ? 1 : 0) forKey:key extension:extensionName];
}

- (void)setIntValue:(int)value forKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapSetDataForKeyStatement];
	if (statement == NULL) return;
	
	// INSERT OR REPLACE INTO "yap2" ("extension", "key", "data") VALUES (?, ?, ?);
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	sqlite3_bind_int(statement, 3, value);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_DONE)
	{
		connection->hasDiskChanges = YES;
	}
	else
	{
		YDBLogError(@"Error executing 'yapSetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
}

- (void)setDoubleValue:(double)value forKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapSetDataForKeyStatement];
	if (statement == NULL) return;
	
	// INSERT OR REPLACE INTO "yap2" ("extension", "key", "data") VALUES (?, ?, ?);
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	sqlite3_bind_double(statement, 3, value);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_DONE)
	{
		connection->hasDiskChanges = YES;
	}
	else
	{
		YDBLogError(@"Error executing 'yapSetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
}

- (void)setStringValue:(NSString *)value forKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapSetDataForKeyStatement];
	if (statement == NULL) return;
	
	// INSERT OR REPLACE INTO "yap2" ("extension", "key", "data") VALUES (?, ?, ?);
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	YapDatabaseString _value; MakeYapDatabaseString(&_value, value);
	sqlite3_bind_text(statement, 3, _value.str, _value.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_DONE)
	{
		connection->hasDiskChanges = YES;
	}
	else
	{
		YDBLogError(@"Error executing 'yapSetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
	FreeYapDatabaseString(&_value);
}

- (void)setDataValue:(NSData *)value forKey:(NSString *)key extension:(NSString *)extensionName
{
	if (extensionName == nil)
		extensionName = @"";
	
	sqlite3_stmt *statement = [connection yapSetDataForKeyStatement];
	if (statement == NULL) return;
	
	// INSERT OR REPLACE INTO "yap2" ("extension", "key", "data") VALUES (?, ?, ?);
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	__attribute__((objc_precise_lifetime)) NSData *data = value;
	sqlite3_bind_blob(statement, 3, data.bytes, (int)data.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_DONE)
	{
		connection->hasDiskChanges = YES;
	}
	else
	{
		YDBLogError(@"Error executing 'yapSetDataForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
}

- (void)removeValueForKey:(NSString *)key extension:(NSString *)extensionName
{
	// Be careful with this statement.
	//
	// The snapshot value is in the yap table, and uses an empty string for the extensionName.
	// The snapshot value is critical to the underlying architecture of the system.
	// Removing it could cripple the system.
	
	NSAssert(key != nil, @"Invalid key!");
	NSAssert(extensionName != nil, @"Invalid extensionName!");
	
	sqlite3_stmt *statement = [connection yapRemoveForKeyStatement];
	if (statement == NULL) return;
	
	// DELETE FROM "yap2" WHERE "extension" = ? AND "key" = ?;
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_DONE)
	{
		connection->hasDiskChanges = YES;
	}
	else
	{
		YDBLogError(@"Error executing 'yapRemoveForKeyStatement': %d %s, extension(%@)",
					status, sqlite3_errmsg(connection->db), extensionName);
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
	FreeYapDatabaseString(&_key);
}

- (void)removeAllValuesForExtension:(NSString *)extensionName
{
	// Be careful with this statement.
	//
	// The snapshot value is in the yap table, and uses an empty string for the extensionName.
	// The snapshot value is critical to the underlying architecture of the system.
	// Removing it could cripple the system.
	
	NSAssert(extensionName != nil, @"Invalid extensionName!");
	
	sqlite3_stmt *statement = [connection yapRemoveExtensionStatement];
	if (statement == NULL) return;
	
	// DELETE FROM "yap2" WHERE "extension" = ?;
	
	YapDatabaseString _extension; MakeYapDatabaseString(&_extension, extensionName);
	sqlite3_bind_text(statement, 1, _extension.str, _extension.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_DONE)
	{
		connection->hasDiskChanges = YES;
	}
	else
	{
		YDBLogError(@"Error executing 'yapRemoveExtensionStatement': %d %s, extension(%@)",
					status, sqlite3_errmsg(connection->db), extensionName);
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_extension);
}

@end
