/**
 * Copyright Deusty LLC.
**/

#import "YapDatabaseCloudCorePrivate.h"
#import "YapDatabasePrivate.h"
#import "YapDatabaseExtensionPrivate.h"

#import "YapDatabaseString.h"
#import "YapDatabaseLogging.h"

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
**/
#if DEBUG
  static const int ydbLogLevel = YDB_LOG_LEVEL_WARN; // YDB_LOG_LEVEL_VERBOSE; // | YDB_LOG_FLAG_TRACE;
#else
  static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif
#pragma unused(ydbLogLevel)

/**
 * Keys for yap2 extension configuration table.
**/
static NSString *const ext_key_classVersion = @"classVersion";
static NSString *const ext_key_versionTag   = @"versionTag";


@implementation YapDatabaseCloudCoreTransaction

- (id)initWithParentConnection:(YapDatabaseCloudCoreConnection *)inParentConnection
           databaseTransaction:(YapDatabaseReadTransaction *)inDatabaseTransaction
{
	if ((self = [super init]))
	{
		parentConnection = inParentConnection;
		databaseTransaction = inDatabaseTransaction;
	}
	return self;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Extension Lifecycle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * This method is called to create any necessary tables,
 * as well as populate the view by enumerating over the existing rows in the database.
 *
 * Return YES if completed successfully, or if already prepared.
 * Return NO if some kind of error occured.
**/
- (BOOL)createIfNeeded
{
	YDBLogAutoTrace();
	
	BOOL needsCreateTables = NO;
	BOOL needsMigrateTables = NO;
	BOOL needsPopulateTables = NO;
	
	// Capture NEW values
	//
	// classVersion - the internal version number of YapDatabaseView implementation
	// versionTag - user specified versionTag, used to force upgrade mechanisms
	
	int classVersion = YAPDATABASE_CLOUDCORE_CLASS_VERSION;
	
	NSString *versionTag = parentConnection->parent->versionTag;
	
	// Fetch OLD values
	//
	// - hasOldClassVersion - will be YES if the extension exists from a previous run of the app
	
	int oldClassVersion = 0;
	BOOL hasOldClassVersion = [self getIntValue:&oldClassVersion forExtensionKey:ext_key_classVersion persistent:YES];
	
	NSString *oldVersionTag = [self stringValueForExtensionKey:ext_key_versionTag persistent:YES];
	
	if (!hasOldClassVersion)
	{
		// First time registration
		
		needsCreateTables = YES;
		needsPopulateTables = YES;
	}
	else if (oldClassVersion != classVersion)
	{
		// Upgrading from older codebase
		
		needsMigrateTables = YES;
	}
	
	if (hasOldClassVersion && ![versionTag isEqualToString:oldVersionTag])
	{
		// Handle user-indicated change
		
		needsPopulateTables = YES;
	}
	else
	{
		// Restoring an up-to-date extension from a previous run.
	}
	
	// We have all the information we need.
	// Now just execute the plan.
	
	if (needsCreateTables || needsMigrateTables)
	{
		if (needsCreateTables)
		{
			if (![self createTables]) return NO;
		}
		else
		{
			if (oldClassVersion <= 1)
			{
				if (![self migrateTables_fromv1_to_v2]) return NO;
			}
			if (oldClassVersion <= 2)
			{
				if (![self migrateTables_fromv2_to_v3]) return NO;
			}
		}
		
		[self setIntValue:classVersion forExtensionKey:ext_key_classVersion persistent:YES];
	}
	
	if (![self restorePreviousOperations]) return NO;
	
	if (needsPopulateTables)
	{
		if (![self populateTables]) return NO;
		
		[self setStringValue:versionTag forExtensionKey:ext_key_versionTag persistent:YES];
	}
	
	return YES;
}

/**
 * This method is called to prepare the transaction for use.
 *
 * Remember, an extension transaction is a very short lived object.
 * Thus it stores the majority of its state within the extension connection (the parent).
 *
 * Return YES if completed successfully, or if already prepared.
 * Return NO if some kind of error occured.
**/
- (BOOL)prepareIfNeeded
{
	YDBLogAutoTrace();
	
	// Nothing to do here for this extension.
	
	return YES;
}

- (BOOL)createTables
{
	YDBLogAutoTrace();
	
	sqlite3 *db = databaseTransaction->connection->db;
	
	NSString *pipelineTableName = [self pipelineTableName];
	NSString *mappingTableName  = [self mappingTableName];
	NSString *queueTableName    = [self queueTableName];
	NSString *tagTableName      = [self tagTableName];
	
	int status;
	
	// CREATE: Pipeline Table
	//
	// | rowid | name | algorithm |
	
	YDBLogVerbose(@"Creating CloudCore table for registeredName(%@): %@", [self registeredName], pipelineTableName);
	
	NSString *createPipelineTable = [NSString stringWithFormat:
	  @"CREATE TABLE IF NOT EXISTS \"%@\""
	  @" (\"rowid\" INTEGER PRIMARY KEY,"
	  @"  \"name\" TEXT NOT NULL,"
	  @"  \"algorithm\" INTEGER NOT NULL"
	  @" );", pipelineTableName];
	
	YDBLogVerbose(@"%@", createPipelineTable);
	status = sqlite3_exec(db, [createPipelineTable UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"%@ - Failed creating table (%@): %d %s",
		            THIS_METHOD, pipelineTableName, status, sqlite3_errmsg(db));
		return NO;
	}
	
	// CREATE: Queue Table
	//
	// | rowid | pipelineID | graphID | operation |
	
	YDBLogVerbose(@"Creating CloudCore table for registeredName(%@): %@", [self registeredName], queueTableName);
	
	NSString *createQueueTable = [NSString stringWithFormat:
	  @"CREATE TABLE IF NOT EXISTS \"%@\""
	  @" (\"rowid\" INTEGER PRIMARY KEY,"
	  @"  \"pipelineID\" INTEGER,"         // Foreign key for pipeline table (may be null)
	  @"  \"graphID\" INTEGER NOT NULL,"   // Graph order (uint64_t)
	  @"  \"operation\" BLOB"              // Serialized operation
	  @" );", queueTableName];
	
	YDBLogVerbose(@"%@", createQueueTable);
	status = sqlite3_exec(db, [createQueueTable UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"%@ - Failed creating table (%@): %d %s",
		            THIS_METHOD, queueTableName, status, sqlite3_errmsg(db));
		return NO;
	}
	
	if (parentConnection->parent->options.enableTagSupport)
	{
		// CREATE: Tag Table
		//
		// | key | identifier | tag |
		
		YDBLogVerbose(@"Creating CloudCore table for registeredName(%@): %@", [self registeredName], tagTableName);
		
		NSString *createTagTable = [NSString stringWithFormat:
		  @"CREATE TABLE IF NOT EXISTS \"%@\""
		  @" (\"key\" TEXT NOT NULL,"
		  @"  \"identifier\" TEXT NOT NULL,"
		  @"  \"tag\" BLOB NOT NULL,"
		  @"  PRIMARY KEY (\"key\", \"identifier\")"
		  @" );", tagTableName];
		
		YDBLogVerbose(@"%@", createTagTable);
		status = sqlite3_exec(db, [createTagTable UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@ - Failed creating table (%@): %d %s",
			            THIS_METHOD, createTagTable, status, sqlite3_errmsg(db));
			return NO;
		}
	}
	
	if (parentConnection->parent->options.enableAttachDetachSupport)
	{
		// CREATE: Mapping Table
		//
		// | database_rowid | cloudURI |
		//
		// Many-To-Many:
		// - a single database_rowid might map to multiple identifiers
		// - a single identifier might be retained by multiple database_rowid's
		
		YDBLogVerbose(@"Creating CloudCore table for registeredName(%@): %@", [self registeredName], mappingTableName);
		
		NSString *createMappingTable = [NSString stringWithFormat:
		  @"CREATE TABLE IF NOT EXISTS \"%@\""
		  @" (\"database_rowid\" INTEGER NOT NULL,"
		  @"  \"cloudURI\" TEXT NOT NULL"
		  @" );", mappingTableName];
		
		NSString *createMappingTableIndex_rowid = [NSString stringWithFormat:
		  @"CREATE INDEX IF NOT EXISTS \"database_rowid\" ON \"%@\" (\"database_rowid\");", mappingTableName];
		
		NSString *createMappingTableIndex_cloudURI = [NSString stringWithFormat:
		  @"CREATE INDEX IF NOT EXISTS \"cloudURI\" ON \"%@\" (\"cloudURI\");", mappingTableName];
		
		YDBLogVerbose(@"%@", createMappingTable);
		status = sqlite3_exec(db, [createMappingTable UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@ - Failed creating table (%@): %d %s",
			            THIS_METHOD, mappingTableName, status, sqlite3_errmsg(db));
			return NO;
		}
		
		YDBLogVerbose(@"%@", createMappingTableIndex_rowid);
		status = sqlite3_exec(db, [createMappingTableIndex_rowid UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@ - Failed creating index (database_rowid) on table (%@): %d %s",
						THIS_METHOD, mappingTableName, status, sqlite3_errmsg(db));
			return NO;
		}
		
		YDBLogVerbose(@"%@", createMappingTableIndex_cloudURI);
		status = sqlite3_exec(db, [createMappingTableIndex_cloudURI UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@ - Failed creating index (cloudURI) on table (%@): %d %s",
						THIS_METHOD, mappingTableName, status, sqlite3_errmsg(db));
			return NO;
		}
	}
	
	return YES;
}

- (BOOL)migrateTables_fromv1_to_v2
{
	YDBLogAutoTrace();
	
	sqlite3 *db = databaseTransaction->connection->db;
	
	NSString *old_queueTableName = [parentConnection->parent queueV1TableName];
	NSString *new_queueTableName = [parentConnection->parent queueV2TableName];
	
	int status;
	
	// STEP 1 of 5
	//
	// CREATE: (New) Queue Table
	//
	// | rowid | pipelineID | graphID | operation |
	{
		YDBLogVerbose(@"Creating CloudCore table for registeredName(%@): %@", [self registeredName], new_queueTableName);
		
		NSString *createQueueTable = [NSString stringWithFormat:
		  @"CREATE TABLE IF NOT EXISTS \"%@\""
		  @" (\"rowid\" INTEGER PRIMARY KEY,"
		  @"  \"pipelineID\" INTEGER,"         // Foreign key for pipeline table (may be null)
		  @"  \"graphID\" INTEGER NOT NULL,"   // Graph order (uint64_t)
		  @"  \"operation\" BLOB"              // Serialized operation
		  @" );", new_queueTableName];
		
		YDBLogVerbose(@"%@", createQueueTable);
		status = sqlite3_exec(db, [createQueueTable UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@ - Failed creating table (%@): %d %s",
			            THIS_METHOD, new_queueTableName, status, sqlite3_errmsg(db));
			return NO;
		}
	}
	
	// STEP 2 of 5
	//
	// - Enumerate the old_queueTable
	// - Parse graphOrder information
	
	NSMutableDictionary *old_table = [NSMutableDictionary dictionary];
	
	NSString *const key_pipelineID = @"pipelineID";
	NSString *const key_graphUUID  = @"graphUUID";
	NSString *const key_operation  = @"operation";
	
	NSMutableDictionary *graphOrderPerPipeline = [NSMutableDictionary dictionary];
	NSMutableDictionary *operationCountPerGraph = [NSMutableDictionary dictionary];
	
	{
		sqlite3_stmt *statement;
		
		NSString *enumerate = [NSString stringWithFormat:
		  @"SELECT * FROM \"%@\";", old_queueTableName];
		
		int const column_idx_rowid       = SQLITE_COLUMN_START +  0;
		int const column_idx_pipelineID  = SQLITE_COLUMN_START +  1; // INTEGER
		int const column_idx_graphID     = SQLITE_COLUMN_START +  2; // BLOB NOT NULL (UUID in raw form)
		int const column_idx_prevGraphID = SQLITE_COLUMN_START +  3; // BLOB          (UUID in raw form)
		int const column_idx_operation   = SQLITE_COLUMN_START +  4; // BLOB
		
		status = sqlite3_prepare_v2(db, [enumerate UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating prepared statement (B): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
			return NO;
		}
		
		while ((status = sqlite3_step(statement)) == SQLITE_ROW)
		{
			// - Extract rowid
			
			int64_t rowid = sqlite3_column_int64(statement, column_idx_rowid);
			
			// - Extract pipeline information
			
			id pipelineID = nil;
			
			int column_type = sqlite3_column_type(statement, column_idx_pipelineID);
			if (column_type == SQLITE_NULL)
			{
				pipelineID = YapDatabaseCloudCoreDefaultPipelineName;
			}
			else
			{
				int64_t pipelineRowid = sqlite3_column_int64(statement, column_idx_pipelineID);
				pipelineID = @(pipelineRowid);
			}
			
			// - Extract graphUUID & prevGraphUUID information
			// - Add to graphOrderPerPipeline
			
			NSUUID *graphUUID = nil;
			{
				int blobSize = sqlite3_column_bytes(statement, column_idx_graphID);
				if (blobSize == sizeof(uuid_t))
				{
					const void *blob = sqlite3_column_blob(statement, column_idx_graphID);
					graphUUID = [[NSUUID alloc] initWithUUIDBytes:(const unsigned char *)blob];
				}
				else
				{
					NSAssert(NO, @"Invalid UUID blobSize: graphUUID");
				}
			}
			
			NSUUID *prevGraphUUID = nil;
			{
				int blobSize = sqlite3_column_bytes(statement, column_idx_prevGraphID);
				if (blobSize == sizeof(uuid_t))
				{
					const void *blob = sqlite3_column_blob(statement, column_idx_prevGraphID);
					prevGraphUUID = [[NSUUID alloc] initWithUUIDBytes:(const unsigned char *)blob];
				}
				else if (blobSize > 0)
				{
					NSAssert(NO, @"Invalid UUID blobSize: prevGraphUUID");
				}
			}
			
			// Extract operation info
			
			const void *blob = sqlite3_column_blob(statement, column_idx_operation);
			int blobSize = sqlite3_column_bytes(statement, column_idx_operation);
			
			NSData *operationBlob = [NSData dataWithBytes:(void *)blob length:blobSize];
			
			// Store row information for migration
			
			old_table[@(rowid)] = @{
				key_pipelineID : pipelineID,
				key_graphUUID  : graphUUID,
				key_operation  : operationBlob
			};
			
			// Store graph information for parsing
			
			NSMutableDictionary *graphOrder = graphOrderPerPipeline[pipelineID];
			if (graphOrder == nil)
			{
				graphOrder = graphOrderPerPipeline[pipelineID] = [NSMutableDictionary dictionary];
			}
			
			if (prevGraphUUID)
				graphOrder[prevGraphUUID] = graphUUID;
			else
				graphOrder[[NSNull null]] = graphUUID;
			
			NSNumber *count = operationCountPerGraph[graphUUID];
			if (count == nil)
			{
				operationCountPerGraph[graphUUID] = @(1);
			}
			else
			{
				operationCountPerGraph[graphUUID] = @(count.unsignedLongLongValue + 1);
			}
		}
		
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"%@: Error executing statement (A): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}
		
		sqlite3_finalize(statement);
		statement = NULL;
	}
	
	// STEP 3 of 5
	//
	// Order the graphs (per pipeline)
	
	NSMutableDictionary *sortedGraphsPerPipeline = [NSMutableDictionary dictionary];
	
	[graphOrderPerPipeline enumerateKeysAndObjectsUsingBlock:
	    ^(NSString *pipelineName, NSMutableDictionary *graphOrder, BOOL *stop)
	{
		__block NSUUID *oldestGraphUUID = nil;
		
		[graphOrder enumerateKeysAndObjectsUsingBlock:^(id key, id value, BOOL *stop) {
			
			// key           -> value
			// prevGraphUUID -> graphUUID
			
			__unsafe_unretained id prevGraphUUID = key;
			__unsafe_unretained NSUUID *graphUUID = (NSUUID *)value;
			
			if (prevGraphUUID == [NSNull null])
			{
				oldestGraphUUID = graphUUID;
				*stop = YES;
			}
			else
			{
				NSNumber *operationCount = operationCountPerGraph[prevGraphUUID];
				if (operationCount == nil)
				{
					// No operations for the referenced prevGraphUUID.
					// This is because we finished that graph, and thus deleted all its operations.
					
					oldestGraphUUID = graphUUID;
					*stop = YES;
				}
			}
		}];
		
		NSMutableDictionary *sortedGraphs = [NSMutableDictionary dictionaryWithCapacity:[graphOrder count]];
		NSUInteger index = 0;
		
		NSUUID *graphUUID = oldestGraphUUID;
		while (graphUUID)
		{
			sortedGraphs[graphUUID] = @(index);
			
			graphUUID = graphOrder[graphUUID];
			index++;
		}
		
		sortedGraphsPerPipeline[pipelineName] = sortedGraphs;
	}];
	
	// Step 4 of 5
	//
	// Populate the new_queueTable
	
	{
		sqlite3_stmt *statement = [parentConnection queueTable_insertStatement];
		
		int const bind_idx_pipelineID    = SQLITE_BIND_START + 0;  // INTEGER
		int const bind_idx_graphID       = SQLITE_BIND_START + 1;  // INTEGER NOT NULL
		int const bind_idx_operation     = SQLITE_BIND_START + 2;  // BLOB
		
		for (NSDictionary *old_row in [old_table objectEnumerator])
		{
			id pipelineID         = old_row[key_pipelineID];
			NSUUID *graphUUID     = old_row[key_graphUUID];
			NSData *operationBlob = old_row[key_operation];
			
			NSNumber *graphID = sortedGraphsPerPipeline[pipelineID][graphUUID];
			
			if ([pipelineID isKindOfClass:[NSNumber class]])
			{
				sqlite3_bind_int64(statement, bind_idx_pipelineID, (int64_t)[pipelineID longLongValue]);
			}
			
			sqlite3_bind_int64(statement, bind_idx_graphID, (int64_t)[graphID unsignedLongLongValue]);
			
			sqlite3_bind_blob(statement, bind_idx_operation,
			                  operationBlob.bytes, (int)operationBlob.length, SQLITE_STATIC);
	
			status = sqlite3_step(statement);
			if (status != SQLITE_DONE)
			{
				YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
								status, sqlite3_errmsg(databaseTransaction->connection->db));
			}
			
			sqlite3_clear_bindings(statement);
			sqlite3_reset(statement);
		}
	}
	
	// Step 5 of 5
	//
	// Delete the old_queueTable
	
	NSString *dropTable = [NSString stringWithFormat:@"DROP TABLE IF EXISTS \"%@\";", old_queueTableName];
	
	status = sqlite3_exec(db, [dropTable UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"%@ - Failed dropping table (%@): %d %s",
		            THIS_METHOD, old_queueTableName, status, sqlite3_errmsg(db));
	}
	
	return YES;
}

- (BOOL)migrateTables_fromv2_to_v3
{
	YDBLogAutoTrace();
	
	sqlite3 *db = databaseTransaction->connection->db;
	
	NSString *old_pipelineTableName = [parentConnection->parent pipelineV2TableName];
	NSString *new_pipelineTableName = [parentConnection->parent pipelineV3TableName];
	
	int status;
	
	// STEP 1 of 4
	//
	// CREATE: (New) Pipeline Table
	//
	// | rowid | name | algorithm |
	{
		YDBLogVerbose(@"Creating CloudCore table for registeredName(%@): %@",
		  [self registeredName], new_pipelineTableName);
		
		NSString *createPipelineTable = [NSString stringWithFormat:
			@"CREATE TABLE IF NOT EXISTS \"%@\""
			@" (\"rowid\" INTEGER PRIMARY KEY,"
			@"  \"name\" TEXT NOT NULL,"
			@"  \"algorithm\" INTEGER NOT NULL"
			@" );", new_pipelineTableName];
		
		YDBLogVerbose(@"%@", createPipelineTable);
		status = sqlite3_exec(db, [createPipelineTable UTF8String], NULL, NULL, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@ - Failed creating table (%@): %d %s",
							THIS_METHOD, new_pipelineTableName, status, sqlite3_errmsg(db));
			return NO;
		}
	}
	
	// STEP 2 of 4
	//
	// - Enumerate the OLD pipeline table
	// - Read all the existing names
	
	NSMutableArray<NSString *> *old_table = [NSMutableArray array];
	
	{
		sqlite3_stmt *statement;
		
		NSString *enumerate = [NSString stringWithFormat:
			@"SELECT * FROM \"%@\";", old_pipelineTableName];
		
	//	int const column_idx_rowid = SQLITE_COLUMN_START + 0;
		int const column_idx_name  = SQLITE_COLUMN_START + 1; // TEXT NOT NULL
		
		status = sqlite3_prepare_v2(db, [enumerate UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating prepared statement (B): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
			return NO;
		}
		
		while ((status = sqlite3_step(statement)) == SQLITE_ROW)
		{
			// - Extract name
			
			const unsigned char *text = sqlite3_column_text(statement, column_idx_name);
			int textSize = sqlite3_column_bytes(statement, column_idx_name);
			
			NSString *name = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			if (name) {
				[old_table addObject:name];
			}
		}
		
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"%@: Error executing statement (A): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}
		
		sqlite3_finalize(statement);
		statement = NULL;
	}
	
	// STEP 3 of 4
	//
	// Populate the NEW pipeline table
	
	{
		sqlite3_stmt *statement = [parentConnection pipelineTable_insertStatement];
		
		int const bind_idx_name      = SQLITE_BIND_START + 0;  // TEXT NOT NULL
		int const bind_idx_algorithm = SQLITE_BIND_START + 1;  // INTEGER NOT NULL
		
		for (NSString *name in old_table)
		{
			sqlite3_bind_text(statement, bind_idx_name, [name UTF8String], -1, SQLITE_TRANSIENT);
			sqlite3_bind_int(statement, bind_idx_algorithm, YDBCloudCorePipelineAlgorithm_CommitGraph);
			
			status = sqlite3_step(statement);
			if (status != SQLITE_DONE)
			{
				YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
								status, sqlite3_errmsg(databaseTransaction->connection->db));
			}
			
			sqlite3_clear_bindings(statement);
			sqlite3_reset(statement);
		}
	}
	
	// STEP 4 of 4
	//
	// Delete the old_queueTable
	
	NSString *dropTable = [NSString stringWithFormat:@"DROP TABLE IF EXISTS \"%@\";", old_pipelineTableName];
	
	status = sqlite3_exec(db, [dropTable UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"%@ - Failed dropping table (%@): %d %s",
						THIS_METHOD, old_pipelineTableName, status, sqlite3_errmsg(db));
	}
	
	return YES;
}

/**
 * Restores all operations by loading them into memory, and sending to the associated pipeline(s).
**/
- (BOOL)restorePreviousOperations
{
	sqlite3 *db = databaseTransaction->connection->db;
	
	NSMutableDictionary *rowidToPipelineName = [NSMutableDictionary dictionary];
	NSMutableDictionary *prvPipelineInfo = [NSMutableDictionary dictionary];
	
	// Step 1 of 4:
	//
	// Read pipeline table
	{
		sqlite3_stmt *statement;
		int status;
		
		NSString *enumerate = [NSString stringWithFormat:
		  @"SELECT * FROM \"%@\";", [self pipelineTableName]];
		
		int const column_idx_rowid     = SQLITE_COLUMN_START + 0;
		int const column_idx_name      = SQLITE_COLUMN_START + 1;
		int const column_idx_algorithm = SQLITE_COLUMN_START + 2;
		
		status = sqlite3_prepare_v2(db, [enumerate UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating prepared statement (A): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
			return NO;
		}
		
		while ((status = sqlite3_step(statement)) == SQLITE_ROW)
		{
			int64_t rowid = sqlite3_column_int64(statement, column_idx_rowid);
			
			const unsigned char *text = sqlite3_column_text(statement, column_idx_name);
			int textSize = sqlite3_column_bytes(statement, column_idx_name);
			
			NSString *name = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
			
			int algorithm = sqlite3_column_int(statement, column_idx_algorithm);
			
			if (name)
			{
				prvPipelineInfo[name] = @[@(rowid), @(algorithm)];
			}
		}
		
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"%@: Error executing statement (A): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}
		
		sqlite3_finalize(statement);
		statement = NULL;
	}
	
	// In version 2, we didn't store the default pipeline in the database.
	// Because it was always required, and because this was before we supported the FlatGraph algorithm.
	// So we need to fake it if it's not there.
	//
	if (prvPipelineInfo[YapDatabaseCloudCoreDefaultPipelineName] == nil)
	{
		prvPipelineInfo[YapDatabaseCloudCoreDefaultPipelineName] = [NSNull null];
	}
	
	// Step 2 of 4:
	//
	// Update pipeline table
	{
		NSMutableSet<NSNumber *> *pipelineRowidsToDelete = [NSMutableSet set];
		
		NSMutableArray<YapDatabaseCloudCorePipeline *> *pipelinesToUpdate = [NSMutableArray array];
		NSMutableArray<YapDatabaseCloudCorePipeline *> *pipelinesToInsert =
		  [[parentConnection->parent registeredPipelines] mutableCopy];
		
		for (NSString *prvName in prvPipelineInfo)
		{
			NSArray *info = prvPipelineInfo[prvName];
			
			// Remember: YDBCloudCorePipeline has a `previousNames` attribute.
			// So the previous name may not match the current name.
			// However, the [YDBCloudCore pipelineWithName:] method will handle this for us.
			
			YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:prvName];
			if (pipeline)
			{
				if ([info isKindOfClass:[NSArray class]])
				{
					NSNumber *prvRowid = info[0];
					NSNumber *prvAlgo  = info[1];
					
					[pipelinesToInsert removeObjectIdenticalTo:pipeline];
					
					pipeline.rowid = [prvRowid longLongValue];
					rowidToPipelineName[prvRowid] = pipeline.name;
				
					if (![prvName isEqualToString:pipeline.name] ||           // name change
						 [prvAlgo unsignedIntegerValue] != pipeline.algorithm) // algorithm change
					{
						[pipelinesToUpdate addObject:pipeline];
					}
				}
				else // info == NSNull (this is for YapDatabaseCloudCoreDefaultPipelineName)
				{
					// Pipeline remains in `pipelinesToInsert` array
				}
			}
			else
			{
				// This pipeline no longer exists.
				// So we'll need to delete it from the table.
				
				if ([info isKindOfClass:[NSArray class]])
				{
					NSNumber *prvRowid = info[0];
					
					[pipelineRowidsToDelete addObject:prvRowid];
				}
			}
		}
		
		if (pipelinesToUpdate.count > 0)
		{
			sqlite3_stmt *statement = [parentConnection pipelineTable_updateStatement];
			if (statement == NULL){
				return NO;
			}
			
			// UPDATE <tableName> SET "name" = ?, "algorithm" = ? WHERE "rowid" = ?;
			
			int const bind_idx_name      = SQLITE_BIND_START + 0;
			int const bind_idx_algorithm = SQLITE_BIND_START + 1;
			int const bind_idx_rowid     = SQLITE_BIND_START + 2;
			
			BOOL foundError = NO;
			
			for (YapDatabaseCloudCorePipeline *pipeline in pipelinesToUpdate)
			{
				sqlite3_bind_text(statement, bind_idx_name, [pipeline.name UTF8String], -1, SQLITE_TRANSIENT);
				sqlite3_bind_int(statement, bind_idx_algorithm, pipeline.algorithm);
				sqlite3_bind_int64(statement, bind_idx_rowid, pipeline.rowid);
				
				int status = sqlite3_step(statement);
				if (status != SQLITE_DONE)
				{
					YDBLogError(@"%@: Error executing statement (B1): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
					foundError = YES;
				}
				
				sqlite3_reset(statement);
				sqlite3_clear_bindings(statement);
				
				if (foundError) {
					return NO;
				}
			}
		}
		
		if (pipelinesToInsert.count > 0)
		{
			sqlite3_stmt *statement = [parentConnection pipelineTable_insertStatement];
			if (statement == NULL) {
				return NO;
			}
			
			// INSERT INTO "pipelineTableName" ("name", "algorithm") VALUES (?, ?);
			
			int const bind_idx_name      = SQLITE_BIND_START + 0;
			int const bind_idx_algorithm = SQLITE_BIND_START + 1;
			
			BOOL foundError = NO;
			
			for (YapDatabaseCloudCorePipeline *pipeline in pipelinesToInsert)
			{
				sqlite3_bind_text(statement, bind_idx_name, [pipeline.name UTF8String], -1, SQLITE_TRANSIENT);
				sqlite3_bind_int(statement, bind_idx_algorithm, pipeline.algorithm);
				
				int status = sqlite3_step(statement);
				if (status == SQLITE_DONE)
				{
					int64_t newRowid = sqlite3_last_insert_rowid(db);
					
					pipeline.rowid = newRowid;
					rowidToPipelineName[@(newRowid)] = pipeline.name;
				}
				else
				{
					YDBLogError(@"%@: Error executing statement (B2): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
					foundError = YES;
				}
				
				sqlite3_reset(statement);
				sqlite3_clear_bindings(statement);
				
				if (foundError) {
					return NO;
				}
			}
		}
		
		if (pipelineRowidsToDelete.count > 0)
		{
			sqlite3_stmt *statement = [parentConnection pipelineTable_removeStatement];
			if (statement == NULL){
				return NO;
			}
			
			// DELETE FROM "pipelineTableName" WHERE "rowid" = ?;
			
			BOOL foundError = NO;
			
			for (NSNumber *rowid in pipelineRowidsToDelete)
			{
				sqlite3_bind_int64(statement, SQLITE_BIND_START, [rowid longLongValue]);
				
				int status = sqlite3_step(statement);
				if (status != SQLITE_DONE)
				{
					YDBLogError(@"%@: Error executing statement (B3): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
					foundError = YES;
				}
				
				sqlite3_reset(statement);
				sqlite3_clear_bindings(statement);
				
				if (foundError) {
					return NO;
				}
			}
		}
	}
	
	// Step 3 of 4:
	//
	// Read queue table
	
	NSMutableDictionary *operations = [NSMutableDictionary dictionary];
	
	{
		sqlite3_stmt *statement;
		int status;
		
		NSString *enumerate = [NSString stringWithFormat:@"SELECT * FROM \"%@\";", [self queueTableName]];
		
		int const column_idx_rowid       = SQLITE_COLUMN_START +  0; // INTEGER PRIMARY KEY
		int const column_idx_pipelineID  = SQLITE_COLUMN_START +  1; // INTEGER
		int const column_idx_graphID     = SQLITE_COLUMN_START +  2; // INTEGER NOT NULL
		int const column_idx_operation   = SQLITE_COLUMN_START +  3; // BLOB
		
		status = sqlite3_prepare_v2(db, [enumerate UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating prepared statement (B): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
			return NO;
		}
		
		while ((status = sqlite3_step(statement)) == SQLITE_ROW)
		{
			// - Extract pipeline information
			
			NSString *pipelineName = nil;
			
			int column_type = sqlite3_column_type(statement, column_idx_pipelineID);
			if (column_type != SQLITE_NULL)
			{
				int64_t pipelineRowid = sqlite3_column_int64(statement, column_idx_pipelineID);
				
				pipelineName = rowidToPipelineName[@(pipelineRowid)];
			}
			
			// ensure pipelineName is valid (and convert from alias if needed)
			
			if (pipelineName == nil) {
				pipelineName = YapDatabaseCloudCoreDefaultPipelineName;
			}
			else {
				NSString *standardizedPipelineName =
					[[parentConnection->parent pipelineWithName:pipelineName] name];
				if (standardizedPipelineName) {
					pipelineName = standardizedPipelineName;
				}
			}
			
			// - Extract graph order information
			
			uint64_t snapshot = (uint64_t)sqlite3_column_int64(statement, column_idx_graphID);
			
			// - Extract operation information
			// - Create operation instance
			
			const void *blob = sqlite3_column_blob(statement, column_idx_operation);
			int blobSize = sqlite3_column_bytes(statement, column_idx_operation);
			
			NSData *operationBlob = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
			
			YapDatabaseCloudCoreOperation *operation = [self deserializeOperation:operationBlob];
			
			operation.operationRowid = sqlite3_column_int64(statement, column_idx_rowid);
			operation.pipeline = pipelineName;
			operation.snapshot = snapshot;
			
			// - Add to operationsPerPipeline
			
			NSMutableDictionary *operationsPerPipeline = operations[pipelineName];
			if (operationsPerPipeline == nil)
			{
				operationsPerPipeline = operations[pipelineName] = [NSMutableDictionary dictionary];
			}
			
			NSMutableArray *operationsPerGraph = operationsPerPipeline[@(snapshot)];
			if (operationsPerGraph == nil)
			{
				operationsPerGraph = operationsPerPipeline[@(snapshot)] = [NSMutableArray array];
			}
			
			[operationsPerGraph addObject:operation];
		}
		
		if (status != SQLITE_DONE)
		{
			YDBLogError(@"%@: Error executing statement (A): %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}
		
		sqlite3_finalize(statement);
		statement = NULL;
	}
	
	// Step 4 of 4:
	//
	// Create the graphs (per pipeline)
	
	for (NSString *pipelineName in operations)
	{
		NSDictionary *operationsPerPipeline = operations[pipelineName];
		
		// key   : @(snapshot) (uint64_t)
		// value : @[operation, ...]
		
		NSArray<NSNumber *> *unsortedGraphIDs = [operationsPerPipeline allKeys];
		NSArray<NSNumber *> *sortedGraphIDs = [unsortedGraphIDs sortedArrayUsingSelector:@selector(compare:)];
		
		NSMutableArray<YapDatabaseCloudCoreGraph *> *sortedGraphs =
		  [NSMutableArray arrayWithCapacity:[sortedGraphIDs count]];
		
		for (NSNumber *snapshot in sortedGraphIDs)
		{
			NSArray<YapDatabaseCloudCoreOperation *> *operationsPerGraph = operationsPerPipeline[snapshot];
			
			YapDatabaseCloudCoreGraph *graph =
			  [[YapDatabaseCloudCoreGraph alloc] initWithSnapshot:[snapshot unsignedLongLongValue]
			                                           operations:operationsPerGraph];
			
			[sortedGraphs addObject:graph];
		}
		
		YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:pipelineName];
		
		NSArray *prvInfo = prvPipelineInfo[pipelineName];
		NSNumber *prvAlgorithm = [prvInfo isKindOfClass:[NSArray class]] ? prvInfo[1] : nil;
		
		[pipeline restoreGraphs:sortedGraphs previousAlgorithm:prvAlgorithm];
	}
	
	return YES;
}

- (BOOL)populateTables
{
	// Subclasses may wish to override me.
	
	return YES;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Accessors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Required override method from YapDatabaseExtensionTransaction.
**/
- (YapDatabaseReadTransaction *)databaseTransaction
{
	return databaseTransaction;
}

/**
 * Required override method from YapDatabaseExtensionTransaction.
**/
- (YapDatabaseExtensionConnection *)extensionConnection
{
	return parentConnection;
}

- (NSString *)registeredName
{
	return [parentConnection->parent registeredName];
}

- (NSString *)pipelineTableName
{
	return [parentConnection->parent pipelineTableName];
}

- (NSString *)mappingTableName
{
	return [parentConnection->parent mappingTableName];
}

- (NSString *)queueTableName
{
	return [parentConnection->parent queueTableName];
}

- (NSString *)tagTableName
{
	return [parentConnection->parent tagTableName];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Utilities - Operations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSData *)serializeOperation:(YapDatabaseCloudCoreOperation *)operation
{
	if (operation == nil) return nil;
	
	return parentConnection->parent->operationSerializer(operation);
}

- (YapDatabaseCloudCoreOperation *)deserializeOperation:(NSData *)operationBlob
{
	if (operationBlob.length == 0) return nil;
	
	return parentConnection->parent->operationDeserializer(operationBlob);
}

- (YapDatabaseCloudCorePipeline *)standardizeOperationPipeline:(YapDatabaseCloudCoreOperation *)operation
{
	// Check to make sure the given pipeline name actually corresponds to a registered pipeline.
	// If not, we need to fallback to the default pipeline.
	//
	// Also we should make sure the pipelineName is standardized.
	// That is, it shouldn't be an alias.
	
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:operation.pipeline];
	if (pipeline == nil)
	{
		YDBLogWarn(@"No registered pipeline for name: %@. "
					  @"The operation will be scheduled in the default pipeline.", operation.pipeline);
		
		pipeline = [parentConnection->parent defaultPipeline];
	}
	
	operation.pipeline = pipeline.name; // enforce standardized name (not nil, not alias)
	return pipeline;
}

/**
 * Helper method to add a modified operation to the list.
**/
- (void)addModifiedOperation:(YapDatabaseCloudCoreOperation *)modifiedOp
{
	NSParameterAssert(modifiedOp != nil);
	
	// Find the originalOp & replace it.
	
	NSUUID *uuid = modifiedOp.uuid;
	
	__block BOOL found = NO;
	__block NSUInteger foundIdx = 0;
	
	[parentConnection->operations_added enumerateKeysAndObjectsUsingBlock:
	  ^(NSString *pipelineName, NSMutableArray<YapDatabaseCloudCoreOperation *> *operations, BOOL *stop)
	{
		NSUInteger idx = 0;
		for (YapDatabaseCloudCoreOperation *op in operations)
		{
			if ([op.uuid isEqual:uuid])
			{
				found = YES;
				foundIdx = idx;
				
				*stop = YES;
				break;
			}
			
			idx++;
		}
		
		if (found)
		{
			[operations replaceObjectAtIndex:foundIdx withObject:modifiedOp];
		}
	}];
	
	if (found) return;
	
	[parentConnection->operations_inserted enumerateKeysAndObjectsUsingBlock:
	  ^(NSString *pipelineName, NSMutableDictionary *graphs, BOOL *outerStop)
	{
		[graphs enumerateKeysAndObjectsUsingBlock:
		  ^(NSNumber *graphIdx, NSMutableArray<YapDatabaseCloudCoreOperation *> *operations, BOOL *innerStop)
		{
			NSUInteger idx = 0;
			for (YapDatabaseCloudCoreOperation *op in operations)
			{
				if ([op.uuid isEqual:uuid])
				{
					found = YES;
					foundIdx = idx;
					
					*innerStop = YES;
					*outerStop = YES;
					break;
				}
				
				idx++;
			}
			
			if (found)
			{
				[operations replaceObjectAtIndex:foundIdx withObject:modifiedOp];
			}
		}];
	}];
	
	if (found) return;
	
	parentConnection->operations_modified[modifiedOp.uuid] = modifiedOp;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Utilities - queue
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)queueTable_insertOperations:(NSArray *)operations
                       withSnapshot:(uint64_t)snapshot
                           pipeline:(YapDatabaseCloudCorePipeline *)pipeline
{
	YDBLogAutoTrace();
	
	if (operations.count == 0) return;
	
	sqlite3_stmt *statement = [parentConnection queueTable_insertStatement];
	if (statement == NULL) {
		return;
	}
	
	// INSERT INTO "queueTableName"
	//   ("pipelineID",
	//    "graphID",    <-- Historical name
	//    "operation")
	//   VALUES (?, ?, ?, ?);
	
	int const bind_idx_pipelineID     = SQLITE_BIND_START + 0; // INTEGER
	int const bind_idx_graphID        = SQLITE_BIND_START + 1; // INTEGER NOT NULL
	int const bind_idx_operation      = SQLITE_BIND_START + 2; // BLOB
	
	
	BOOL needsBindPipelineRowid = ![pipeline.name isEqualToString:YapDatabaseCloudCoreDefaultPipelineName];
	
	for (YapDatabaseCloudCoreOperation *operation in operations)
	{
		// pipelineID
		
		if (needsBindPipelineRowid)
		{
			sqlite3_bind_int64(statement, bind_idx_pipelineID, pipeline.rowid);
		}
		
		// graphID / snapshot
		
		NSAssert(operation.snapshot == snapshot, @"Maybe forgot to set operation.snapshot somewhere ?");
		sqlite3_bind_int64(statement, bind_idx_graphID, snapshot);
		
		// operation
		
		__attribute__((objc_precise_lifetime)) NSData *operationBlob = [self serializeOperation:operation];
		
		sqlite3_bind_blob(statement, bind_idx_operation, operationBlob.bytes, (int)operationBlob.length, SQLITE_STATIC);
	
		int status = sqlite3_step(statement);
		if (status == SQLITE_DONE)
		{
			int64_t opRowid = sqlite3_last_insert_rowid(databaseTransaction->connection->db);
			operation.operationRowid = opRowid;
		}
		else
		{
			YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
			            status, sqlite3_errmsg(databaseTransaction->connection->db));
		}
	
		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
	}
}

- (void)queueTable_modifyOperation:(YapDatabaseCloudCoreOperation *)operation
{
	YDBLogAutoTrace();
	
	sqlite3_stmt *statement = [parentConnection queueTable_modifyStatement];
	if (statement == NULL) {
		return;
	}
	
	// UPDATE "queueTableName" SET "operation" = ? WHERE "rowid" = ?;

	int const bind_idx_operation = SQLITE_BIND_START + 0;
	int const bind_idx_rowid     = SQLITE_BIND_START + 1;
	
	__attribute__((objc_precise_lifetime)) NSData *operationBlob = [self serializeOperation:operation];
	sqlite3_bind_blob(statement, bind_idx_operation, operationBlob.bytes, (int)operationBlob.length, SQLITE_STATIC);
	
	sqlite3_bind_int64(statement, bind_idx_rowid, operation.operationRowid);
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
					status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
}

- (void)queueTable_removeRowWithRowid:(int64_t)operationRowid
{
	YDBLogAutoTrace();
	
	sqlite3_stmt *statement = [parentConnection queueTable_removeStatement];
	if (statement == NULL) {
		return;
	}
	
	// DELETE FROM "queueTableName" WHERE "rowid" = ?;
	
	sqlite3_bind_int64(statement, SQLITE_BIND_START, operationRowid);
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
}

- (void)queueTable_removeAllRows
{
	YDBLogAutoTrace();
	
	sqlite3_stmt *statement = [parentConnection queueTable_removeAllStatement];
	if (statement == NULL) {
		return;
	}
	
	// DELETE FROM "queueTableName";
	
	YDBLogVerbose(@"Deleting all rows from queue table...");
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_reset(statement);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Utilities - mappings
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSSet<NSString *> *)allAttachedCloudURIsForRowid:(int64_t)rowid
{
	YDBLogAutoTrace();
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection mappingTable_fetchForRowidStatement];
	if (statement == NULL) {
		return nil;
	}
	
	NSMutableSet *attachedCloudURIs = [NSMutableSet setWithCapacity:1];
	
	// SELECT "cloudURI" FROM "mappingTableName" WHERE "database_rowid" = ?;
	
	const int column_idx_clouduri = SQLITE_COLUMN_START;
	const int bind_idx_rowid = SQLITE_BIND_START;
	
	sqlite3_bind_int64(statement, bind_idx_rowid, rowid);
	
	int status;
	while ((status = sqlite3_step(statement)) == SQLITE_ROW)
	{
		const unsigned char *text = sqlite3_column_text(statement, column_idx_clouduri);
		int textSize = sqlite3_column_bytes(statement, column_idx_clouduri);
		
		NSString *cloudURI = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
		if (cloudURI)
		{
			[attachedCloudURIs addObject:cloudURI];
		}
	}
	
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	
	[parentConnection->dirtyMappingInfo enumerateValuesForKey:@(rowid) withBlock:
	    ^(NSString *cloudURI, id metadata, BOOL *stop)
	{
		if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsInsert)
		{
			[attachedCloudURIs addObject:cloudURI];
		}
		else if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsRemove)
		{
			[attachedCloudURIs removeObject:cloudURI];
		}
	}];
	
	return attachedCloudURIs;
}

- (NSSet<NSNumber *> *)allAttachedRowidsForCloudURI:(NSString *)cloudURI
{
	YDBLogAutoTrace();
	NSParameterAssert(cloudURI != nil);
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection mappingTable_fetchForCloudURIStatement];
	if (statement == NULL) {
		return nil;
	}
	
	NSMutableSet *attachedRowids = [NSMutableSet setWithCapacity:1];
	
	// SELECT "database_rowid" FROM "mappingTableName" WHERE "cloudURI" = ?;
	
	int const column_idx_rowid = SQLITE_COLUMN_START;
	int const bind_idx_identifier = SQLITE_BIND_START;
	
	YapDatabaseString _cloudURI; MakeYapDatabaseString(&_cloudURI, cloudURI);
	sqlite3_bind_text(statement, bind_idx_identifier, _cloudURI.str, _cloudURI.length, SQLITE_STATIC);
	
	int status;
	while ((status = sqlite3_step(statement)) == SQLITE_ROW)
	{
		int64_t databaseRowid = sqlite3_column_int64(statement, column_idx_rowid);
		
		[attachedRowids addObject:@(databaseRowid)];
	}
	
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_cloudURI);
	
	[parentConnection->dirtyMappingInfo enumerateKeysForValue:cloudURI withBlock:
	    ^(NSNumber *rowid, id metadata, BOOL *stop)
	{
		if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsInsert)
		{
			[attachedRowids addObject:rowid];
		}
		else if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsRemove)
		{
			[attachedRowids removeObject:rowid];
		}
	}];
	
	return attachedRowids;
}

- (BOOL)containsMappingWithRowid:(int64_t)rowid cloudURI:(NSString *)cloudURI
{
	YDBLogAutoTrace();
	NSParameterAssert(cloudURI != nil);
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	// Check dirtyMappingInfo
	
	NSString *metadata = [parentConnection->dirtyMappingInfo metadataForKey:@(rowid) value:cloudURI];
	if (metadata)
	{
		if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsInsert)
			return YES;
		
		if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsRemove)
			return NO;
	}
	
	// Check cleanMappingCache
	
	if ([parentConnection->cleanMappingCache containsKey:@(rowid) value:cloudURI])
	{
		return YES;
	}
	
	// Query database
	
	sqlite3_stmt *statement = [parentConnection mappingTable_fetchStatement];
	if (statement == NULL) {
		return NO;
	}
	
	// SELECT COUNT(*) AS NumberOfRows FROM "mappingTableName" WHERE "database_rowid" = ? AND "cloudURI" = ?;
	
	const int column_idx_count = SQLITE_COLUMN_START;
	
	const int bind_idx_rowid    = SQLITE_BIND_START + 0;
	const int bind_idx_clouduri = SQLITE_BIND_START + 1;
	
	sqlite3_bind_int64(statement, bind_idx_rowid, rowid);
	
	YapDatabaseString _cloudURI; MakeYapDatabaseString(&_cloudURI, cloudURI);
	sqlite3_bind_text(statement, bind_idx_clouduri, _cloudURI.str, _cloudURI.length, SQLITE_STATIC);
	
	int64_t count = 0;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		count = sqlite3_column_int64(statement, column_idx_count);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
					status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_cloudURI);
	
	if (count > 0)
	{
		// Add to cache
		[parentConnection->cleanMappingCache insertKey:@(rowid) value:cloudURI];
		
		return YES;
	}
	else
	{
		return NO;
	}
}

- (void)attachCloudURI:(NSString *)cloudURI forRowid:(int64_t)rowid
{
	YDBLogAutoTrace();
	NSParameterAssert(cloudURI != nil);
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	if (![self containsMappingWithRowid:rowid cloudURI:cloudURI])
	{
		[parentConnection->dirtyMappingInfo insertKey:@(rowid)
		                                        value:cloudURI
		                                     metadata:YDBCloudCore_DiryMappingMetadata_NeedsInsert];
	}
}

- (void)detachCloudURI:(NSString *)cloudURI forRowid:(int64_t)rowid
{
	YDBLogAutoTrace();
	NSParameterAssert(cloudURI != nil);
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	if ([self containsMappingWithRowid:rowid cloudURI:cloudURI])
	{
		[parentConnection->cleanMappingCache removeItemWithKey:@(rowid) value:cloudURI];
		
		[parentConnection->dirtyMappingInfo insertKey:@(rowid)
		                                        value:cloudURI
		                                     metadata:YDBCloudCore_DiryMappingMetadata_NeedsRemove];
	}
}

- (void)mappingTable_insertRowWithRowid:(int64_t)rowid cloudURI:(NSString *)cloudURI
{
	YDBLogAutoTrace();
	NSParameterAssert(cloudURI != nil);
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection mappingTable_insertStatement];
	if (statement == NULL) {
		return; // from_block
	}
	
	// INSERT OR REPLACE INTO "mappingTableName" ("database_rowid", "cloudURI") VALUES (?, ?);
	
	const int bind_idx_rowid    = SQLITE_BIND_START + 0;
	const int bind_idx_clouduri = SQLITE_BIND_START + 1;
	
	sqlite3_bind_int64(statement, bind_idx_rowid, rowid);
	
	YapDatabaseString _cloudURI; MakeYapDatabaseString(&_cloudURI, cloudURI);
	sqlite3_bind_text(statement, bind_idx_clouduri, _cloudURI.str, _cloudURI.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_cloudURI);
}

- (void)mappingTable_removeRowWithRowid:(int64_t)rowid cloudURI:(NSString *)cloudURI
{
	YDBLogAutoTrace();
	NSParameterAssert(cloudURI != nil);
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection mappingTable_removeStatement];
	if (statement == NULL) {
		return; // from_block
	}
	
	// DELETE FROM "mappingTableName" WHERE "database_rowid" = ? AND "cloudURI" = ?;
	
	const int bind_idx_rowid    = SQLITE_BIND_START + 0;
	const int bind_idx_clouduri = SQLITE_BIND_START + 1;
	
	sqlite3_bind_int64(statement, bind_idx_rowid, rowid);
	
	YapDatabaseString _cloudURI; MakeYapDatabaseString(&_cloudURI, cloudURI);
	sqlite3_bind_text(statement, bind_idx_clouduri, _cloudURI.str, _cloudURI.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
					status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_cloudURI);
}

- (void)mappingTable_removeAllRows
{
	YDBLogAutoTrace();
	
	NSAssert(parentConnection->parent->options.enableAttachDetachSupport,
	         @"YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection mappingTable_removeAllStatement];
	if (statement == NULL) {
		return;
	}
	
	// DELETE FROM "mappingTableName";
	
	YDBLogVerbose(@"Deleting all rows from mapping table...");
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
					status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_reset(statement);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Utilities - tag
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (id)tagForStatement:(sqlite3_stmt *)statement column:(int)column_idx_tag
{
	id tag = nil;
	const int column_type = sqlite3_column_type(statement, column_idx_tag);
	
	if (column_type == SQLITE_INTEGER)
	{
		int64_t value = sqlite3_column_int64(statement, column_idx_tag);
		
		tag = @(value);
	}
	else if (column_type == SQLITE_FLOAT)
	{
		double value = sqlite3_column_double(statement, column_idx_tag);
		
		tag = @(value);
	}
	else if (column_type == SQLITE_TEXT)
	{
		const unsigned char *text = sqlite3_column_text(statement, column_idx_tag);
		int textLen = sqlite3_column_bytes(statement, column_idx_tag);
		
		tag = [[NSString alloc] initWithBytes:text length:textLen encoding:NSUTF8StringEncoding];
	}
	else if (column_type == SQLITE_BLOB)
	{
		const void *blob = sqlite3_column_blob(statement, column_idx_tag);
		int blobSize = sqlite3_column_bytes(statement, column_idx_tag);
		
		tag = [NSData dataWithBytes:(void *)blob length:blobSize];
	}
	
	return tag;
}

- (NSDictionary<NSString*, id> *)allTagsForKey:(NSString *)key
{
	if (key == nil) return nil;
	
	NSAssert(parentConnection->parent->options.enableTagSupport, @"YapDatabaseCloudCoreOptions.enableTagSupport == NO");
	
	NSMutableDictionary *results = [NSMutableDictionary dictionary];
	
	sqlite3_stmt *statement = [parentConnection tagTable_enumerateForKeyStatement];
	if (statement == NULL) {
		return nil;
	}
	
	// SELECT "identifier", "tag" FROM "<tagTableName>" WHERE "key" = ?;
	
	const int bind_idx_key = SQLITE_BIND_START + 0;
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, bind_idx_key, _key.str, _key.length, SQLITE_STATIC);
	
	const int column_idx_identifier = SQLITE_COLUMN_START + 0;
	const int column_idx_tag        = SQLITE_COLUMN_START + 1;
	
	int status;
	while ((status = sqlite3_step(statement)) == SQLITE_ROW)
	{
		const unsigned char *text = sqlite3_column_text(statement, column_idx_identifier);
		int textSize = sqlite3_column_bytes(statement, column_idx_identifier);
		
		NSString *identifier = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];
		if (identifier)
		{
			id tag = [self tagForStatement:statement column:column_idx_tag];
			
			results[identifier] = tag;
		}
	}
	
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);
	
	[parentConnection->dirtyTags enumerateKeysAndObjectsUsingBlock:^(YapCollectionKey *tuple, id tag, BOOL *stop) {
		
		NSString *tuple_key        = tuple.collection;
		NSString *tuple_identifier = tuple.key;
		
		if ([tuple_key isEqualToString:key])
		{
			if (tag == [NSNull null])
				results[tuple_identifier] = nil;
			else
				results[tuple_identifier] = tag;
		}
	}];
	
	return results;
}

- (void)tagTable_insertOrUpdateRowWithKey:(NSString *)key
                               identifier:(NSString *)identifier
                                      tag:(id)tag
{
	NSParameterAssert(key != nil);
	NSParameterAssert(identifier != nil);
	NSParameterAssert(tag != nil);
	
	NSAssert(parentConnection->parent->options.enableTagSupport, @"YapDatabaseCloudCoreOptions.enableTagSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection tagTable_setStatement];
	if (statement == NULL) {
		return;
	}
	
	// INSERT OR REPLACE INTO "changeTagTableName" ("key", "identifier", "changeTag") VALUES (?, ?, ?);
	
	int const bind_idx_key        = SQLITE_BIND_START + 0;
	int const bind_idx_identifier = SQLITE_BIND_START + 1;
	int const bind_idx_changeTag  = SQLITE_BIND_START + 2;
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, bind_idx_key, _key.str, _key.length, SQLITE_STATIC);
	
	YapDatabaseString _identifier; MakeYapDatabaseString(&_identifier, identifier);
	sqlite3_bind_text(statement, bind_idx_identifier, _identifier.str, _identifier.length, SQLITE_STATIC);
	
	if ([tag isKindOfClass:[NSNumber class]])
	{
		__unsafe_unretained NSNumber *number = (NSNumber *)tag;
		
		CFNumberType numberType = CFNumberGetType((CFNumberRef)number);
		
		if (numberType == kCFNumberFloat32Type ||
		    numberType == kCFNumberFloat64Type ||
		    numberType == kCFNumberFloatType   ||
		    numberType == kCFNumberDoubleType  ||
		    numberType == kCFNumberCGFloatType  )
		{
			double value = [number doubleValue];
			sqlite3_bind_double(statement, bind_idx_changeTag, value);
		}
		else
		{
			int64_t value = [number longLongValue];
			sqlite3_bind_int64(statement, bind_idx_changeTag, value);
		}
	}
	else if ([tag isKindOfClass:[NSString class]])
	{
		__unsafe_unretained NSString *string = (NSString *)tag;
		
		sqlite3_bind_text(statement, bind_idx_changeTag, [string UTF8String], -1, SQLITE_TRANSIENT);
	}
	else if ([tag isKindOfClass:[NSData class]])
	{
		__unsafe_unretained NSData *data = (NSData *)tag;
		
		sqlite3_bind_blob(statement, bind_idx_changeTag, [data bytes], (int)data.length, SQLITE_STATIC);
	}
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);
	FreeYapDatabaseString(&_identifier);
}

- (void)tagTable_removeRowWithKey:(NSString *)key identifier:(NSString *)identifier
{
	NSParameterAssert(key != nil);
	NSParameterAssert(identifier != nil);
	
	NSAssert(parentConnection->parent->options.enableTagSupport, @"YapDatabaseCloudCoreOptions.enableTagSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection tagTable_removeForKeyIdentifierStatement];
	if (statement == NULL) {
		return;
	}
	
	// DELETE FROM "tagTableName" WHERE "key" = ? AND "identifier" = ?;
	
	int const bind_idx_key        = SQLITE_BIND_START + 0;
	int const bind_idx_identifier = SQLITE_BIND_START + 1;
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, bind_idx_key, _key.str, _key.length, SQLITE_STATIC);
	
	YapDatabaseString _identifier; MakeYapDatabaseString(&_identifier, identifier);
	sqlite3_bind_text(statement, bind_idx_identifier, _identifier.str, _identifier.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);
	FreeYapDatabaseString(&_identifier);
}

- (void)tagTable_removeRowsWithKey:(NSString *)key
{
	NSParameterAssert(key != nil);
	
	NSAssert(parentConnection->parent->options.enableTagSupport, @"YapDatabaseCloudCoreOptions.enableTagSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection tagTable_removeForKeyStatement];
	if (statement == NULL) {
		return;
	}
	
	// DELETE FROM "tagTableName" WHERE "key" = ?;
	
	int const bind_idx_key = SQLITE_BIND_START;

	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, bind_idx_key, _key.str, _key.length, SQLITE_STATIC);
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
					status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);
}

- (void)tagTable_removeAllRows
{
	YDBLogAutoTrace();
	
	NSAssert(parentConnection->parent->options.enableTagSupport, @"YapDatabaseCloudCoreOptions.enableTagSupport == NO");
	
	sqlite3_stmt *statement = [parentConnection tagTable_removeAllStatement];
	if (statement == NULL) {
		return;
	}
	
	// DELETE FROM "tagTableName";
	
	YDBLogVerbose(@"Deleting all rows from tag table...");
	
	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ - Error executing statement: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_reset(statement);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Transaction Hooks
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * Corresponds to the following method(s) in YapDatabaseReadWriteTransaction:
 * - setObject:forKey:inCollection:
 * - setObject:forKey:inCollection:withMetadata:
 * - setObject:forKey:inCollection:withMetadata:serializedObject:serializedMetadata:
 *
 * The row is being inserted, meaning there is not currently an entry for the collection/key tuple.
**/
- (void)didInsertObject:(id)object
       forCollectionKey:(YapCollectionKey *)collectionKey
           withMetadata:(id)metadata
                  rowid:(int64_t)rowid
{
	YDBLogAutoTrace();
	
	// Check for pending attach request
	
	if ([parentConnection->pendingAttachRequests containsKey:collectionKey])
	{
		[parentConnection->pendingAttachRequests enumerateValuesForKey:collectionKey withBlock:
		    ^(NSString *cloudURI, id metadata, BOOL *stop)
		{
			[self attachCloudURI:cloudURI forRowid:rowid];
		}];
		
		[parentConnection->pendingAttachRequests removeAllItemsWithKey:collectionKey];
	}
}

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * Corresponds to the following method(s) in YapDatabaseReadWriteTransaction:
 * - setObject:forKey:inCollection:
 * - setObject:forKey:inCollection:withMetadata:
 * - setObject:forKey:inCollection:withMetadata:serializedObject:serializedMetadata:
 *
 * The row is being modified, meaning there is already an entry for the collection/key tuple which is being modified.
**/
- (void)didUpdateObject:(id)object
       forCollectionKey:(YapCollectionKey *)collectionKey
           withMetadata:(id)metadata
                  rowid:(int64_t)rowid
{
	// Nothing to do here
}

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * Corresponds to the following method(s) in YapDatabaseReadWriteTransaction:
 * - replaceObject:forKey:inCollection:
 * - replaceObject:forKey:inCollection:withSerializedObject:
 *
 * There is already a row for the collection/key tuple, and only the object is being modified (metadata untouched).
**/
- (void)didReplaceObject:(id)object forCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	// Nothing to do here
}

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * Corresponds to the following method(s) in YapDatabaseReadWriteTransaction:
 * - replaceMetadata:forKey:inCollection:
 * - replaceMetadata:forKey:inCollection:withSerializedMetadata:
 *
 * There is already a row for the collection/key tuple, and only the metadata is being modified (object untouched).
**/
- (void)didReplaceMetadata:(id)metadata forCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	// Nothing to do here
}

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * Corresponds to the following method(s) in YapDatabaseReadWriteTransaction:
 * - touchObjectForKey:inCollection:collection:
**/
- (void)didTouchObjectForCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	// Nothing to do here
}

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * Corresponds to the following method(s) in YapDatabaseReadWriteTransaction:
 * - touchMetadataForKey:inCollection:
**/
- (void)didTouchMetadataForCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	// Nothing to do here
}

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * Corresponds to the following method(s) in YapDatabaseReadWriteTransaction:
 * - touchRowForKey:inCollection:
**/
- (void)didTouchRowForCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	// Nothing to do here
}

/**
 * YapDatabaseReadWriteTransaction Hook, invoked post-op.
 *
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
**/
- (void)didRemoveObjectForCollectionKey:(YapCollectionKey *)ck withRowid:(int64_t)rowid
{
	// Nothing to do here
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
**/
- (void)didRemoveObjectsForKeys:(NSArray *)keys inCollection:(NSString *)collection withRowids:(NSArray *)rowids
{
	// Nothing to do here
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
**/
- (void)didRemoveAllObjectsInAllCollections
{
	YDBLogAutoTrace();
	
	[self queueTable_removeAllRows];
	
	if (parentConnection->parent->options.enableTagSupport) {
		[self tagTable_removeAllRows];
	}
	
	if (parentConnection->parent->options.enableAttachDetachSupport) {
		[self mappingTable_removeAllRows];
	}
	
	[parentConnection->pendingAttachRequests removeAllItems];
	
	[parentConnection->operations_added removeAllObjects];
	[parentConnection->operations_inserted removeAllObjects];
	[parentConnection->operations_modified removeAllObjects];
	
	[parentConnection->cleanMappingCache removeAllItems];
	[parentConnection->dirtyMappingInfo removeAllItems];
	
	[parentConnection->tagCache removeAllObjects];
	[parentConnection->dirtyTags removeAllObjects];
	
	parentConnection->reset = YES;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Subclass Hooks
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Subclasses may override this method to perform additional validation.
**/
- (void)validateOperation:(YapDatabaseCloudCoreOperation *)operation
{
	NSSet *allowedOperationClasses = parentConnection->parent->options.allowedOperationClasses;
	if (allowedOperationClasses)
	{
		BOOL allowed = NO;
		for (Class class in allowedOperationClasses)
		{
			if ([operation isKindOfClass:class])
			{
				allowed = YES;
				break;
			}
		}
		
		if (!allowed)
		{
			@throw [self disallowedOperationClass:operation];
		}
	}
}

- (void)willAddOperation:(YapDatabaseCloudCoreOperation *)operation
              inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
            withGraphIdx:(NSUInteger)opGraphIdx
{
	// Available as subclass hook.
	// This is a good place to modify the operation (e.g. by adding implicit dependencies).
}

- (void)didAddOperation:(YapDatabaseCloudCoreOperation *)operation
             inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
           withGraphIdx:(NSUInteger)opGraphIdx
{
	// Available as subclass hook.
}

- (void)willInsertOperation:(YapDatabaseCloudCoreOperation *)operation
                 inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
               withGraphIdx:(NSUInteger)opGraphIdx
{
	// Available as subclass hook
}

- (void)didInsertOperation:(YapDatabaseCloudCoreOperation *)operation
                inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
              withGraphIdx:(NSUInteger)opGraphIdx
{
	// Available as subclass hook
}

- (void)willModifyOperation:(YapDatabaseCloudCoreOperation *)operation
                 inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
               withGraphIdx:(NSUInteger)opGraphIdx
{
	// Available as subclass hook
}

- (void)didModifyOperation:(YapDatabaseCloudCoreOperation *)operation
                 inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
               withGraphIdx:(NSUInteger)opGraphIdx
{
	// Available as subclass hook
}

- (void)didCompleteOperation:(YapDatabaseCloudCoreOperation *)operation
{
	// Available as subclass hook
}

- (void)didSkipOperation:(YapDatabaseCloudCoreOperation *)operation
{
	// Available as subclass hook
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Operation Handling
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Allows you to queue an operation to be executed automatically by the appropriate pipeline.
 * This may be used as an alternative to creating an operation from within the YapDatabaseCloudCoreHandler.
 *
 * @param operation
 *   The operation to be added to the pipeline's queue.
 *   The operation.pipeline property specifies which pipeline to use.
 *   The operation will be added to a new graph for the current commit.
 *
 * @return
 *   NO if the operation isn't properly configured for use.
**/
- (BOOL)addOperation:(YapDatabaseCloudCoreOperation *)operation
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return NO;
	}
	
	// Sanity checks
	
	if (operation == nil) return NO;
	
	if ([self _operationWithUUID:operation.uuid inPipeline:operation.pipeline] != nil)
	{
		// The operation already exists.
		// Did you mean to use the 'modifyOperation' method?
		return NO;
	}
	
	// Public API safety:
	// - prevent user from modifying the operation after import
	// - ensure operation.pipeline is valid
	
	operation = [operation copy];
	YapDatabaseCloudCorePipeline *pipeline = [self standardizeOperationPipeline:operation];
	NSUInteger graphIdx = pipeline.graphCount;
	
	uint64_t nextSnapshot = [databaseTransaction->connection snapshot] + 1;
	operation.snapshot = nextSnapshot;
	
	// Import logic
	
	[self validateOperation:operation];
	[self willAddOperation:operation inPipeline:pipeline withGraphIdx:graphIdx];
	
	NSMutableArray<YapDatabaseCloudCoreOperation *> *addedOps = parentConnection->operations_added[pipeline.name];
	if (addedOps == nil)
	{
		addedOps = [NSMutableArray arrayWithCapacity:1];
		parentConnection->operations_added[pipeline.name] = addedOps;
	}
	
	[addedOps addObject:operation];
	
	[self didAddOperation:operation inPipeline:pipeline withGraphIdx:graphIdx];
	return YES;
}

/**
 * Allows you to insert an operation into an existing graph.
 *
 * For example, say an operation in the currently executing graph (graphIdx = 0) fails due to some conflict.
 * And to resolve the conflict you need to:
 * - execute a different (new operation)
 * - and then re-try the failed operation
 *
 * What you can do is create & insert the new operation (into graphIdx zero).
 * And modify the old operation to depend on the new operation (@see 'modifyOperation').
 *
 * The dependency graph will automatically be recalculated using the inserted operation.
 *
 * @param operation
 *   The operation to be inserted into the pipeline's queue.
 *   The operation.pipeline property specifies which pipeline to use.
 *   The operation will be inserted into the graph corresponding to the graphIdx parameter.
 *
 * @param graphIdx
 *   The graph index for the corresponding pipeline.
 *   The currently executing graph index is always zero, which is the most common value.
 *
 * @return
 *   NO if the operation isn't properly configured for use.
**/
- (BOOL)insertOperation:(YapDatabaseCloudCoreOperation *)operation inGraph:(NSInteger)graphIdx
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return NO;
	}
	
	// Sanity checks
	
	if (operation == nil) return NO;
	
	if ([self _operationWithUUID:operation.uuid inPipeline:operation.pipeline] != nil)
	{
		// The operation already exists.
		// Did you mean to use the 'modifyOperation' method?
		return NO;
	}
	
	// Public API safety:
	// - prevent user from modifying the operation after import
	// - ensure operation.pipeline is valid
	
	operation = [operation copy];
	YapDatabaseCloudCorePipeline *pipeline = [self standardizeOperationPipeline:operation];
	
	// Is this a valid graphIdx ?
	//
	if (graphIdx >= pipeline.graphCount)
	{
		return [self addOperation:operation];
	}
	
	uint64_t snapshot = 0;
	[pipeline getSnapshot:&snapshot forGraphIndex:graphIdx];
	operation.snapshot = snapshot;
	
	// Import logic
	
	[self validateOperation:operation];
	[self willInsertOperation:operation inPipeline:pipeline withGraphIdx:graphIdx];
	
	NSMutableDictionary *graphs = parentConnection->operations_inserted[pipeline.name];
	if (graphs == nil)
	{
		graphs = [NSMutableDictionary dictionaryWithCapacity:1];
		parentConnection->operations_inserted[pipeline.name] = graphs;
	}
	
	NSMutableArray<YapDatabaseCloudCoreOperation *> *insertedOps = graphs[@(graphIdx)];
	if (insertedOps == nil)
	{
		insertedOps = [NSMutableArray arrayWithCapacity:1];
		graphs[@(graphIdx)] = insertedOps;
	}
	
	[insertedOps addObject:operation];
	
	[self didInsertOperation:operation inPipeline:pipeline withGraphIdx:graphIdx];
	return YES;
}

/**
 * Replaces the existing operation with the new version.
 *
 * The dependency graph will automatically be recalculated using the new operation version.
**/
- (BOOL)modifyOperation:(YapDatabaseCloudCoreOperation *)operation
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return NO;
	}
	
	// Sanity checks
	
	if (operation == nil) return NO;
	
	YapDatabaseCloudCoreOperation *previous = [self _operationWithUUID:operation.uuid inPipeline:operation.pipeline];
	if (previous == nil)
	{
		// The operation doesn't appear to exist.
		// It either never existed, or it's already been completed or skipped.
		return NO;
	}
	
	// Public API safety:
	// Prevent the user from modifying the operation after import.
	
	operation = [operation copy];
	operation.pipeline = previous.pipeline; // changing this not supported; delete old & create new.
	operation.snapshot = previous.snapshot; // changing this not supported; delete old & create new.
	
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:operation.pipeline];
	NSUInteger graphIdx = [self graphForOperation:operation];
	
	// Modify logic
	
	[self validateOperation:operation];
	[self willModifyOperation:operation inPipeline:pipeline withGraphIdx:graphIdx];
	
	[self addModifiedOperation:operation];
	
	[self didModifyOperation:operation inPipeline:pipeline withGraphIdx:graphIdx];
	return YES;
}

/**
 * This method MUST be invoked in order to mark an operation as complete.
 * 
 * Until an operation is marked as completed or skipped,
 * the pipeline will act as if the operation is still in progress.
 * And the only way to mark an operation as complete or skipped,
 * is to use either the completeOperation: or one of the skipOperation methods.
 * These methods allow the system to remove the operation from its internal sqlite table.
**/
- (void)completeOperationWithUUID:(NSUUID *)uuid
{
	[self completeOperationWithUUID:uuid inPipeline:nil];
}

- (void)completeOperationWithUUID:(NSUUID *)uuid inPipeline:(NSString *)pipelineName
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	
	YapDatabaseCloudCoreOperation *op = nil;
	if (pipelineName)
		op = [self _operationWithUUID:uuid inPipeline:pipelineName];
	else
		op = [self _operationWithUUID:uuid];
	
	if (op && !op.pendingStatusIsCompleted)
	{
		op = [op copy];
		
		op.needsDeleteDatabaseRow = YES;
		op.pendingStatus = @(YDBCloudOperationStatus_Completed);
		
		[self addModifiedOperation:op];
		[self didCompleteOperation:op];
	}
}

/**
 * Use this method to skip/abort operations.
 *
 * Until an operation is marked as completed or skipped,
 * the pipeline will act as if the operation is still in progress.
 * And the only way to mark an operation as complete or skipped,
 * is to use either the completeOperation: or one of the skipOperation methods.
 * These methods allow the system to remove the operation from its internal sqlite table.
**/
- (void)skipOperationWithUUID:(NSUUID *)uuid
{
	[self skipOperationWithUUID:uuid inPipeline:nil];
}

- (void)skipOperationWithUUID:(NSUUID *)uuid inPipeline:(NSString *)pipelineName
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	
	YapDatabaseCloudCoreOperation *op = nil;
	if (pipelineName)
		op = [self _operationWithUUID:uuid inPipeline:pipelineName];
	else
		op = [self _operationWithUUID:uuid];
	
	if (op && !op.pendingStatusIsCompletedOrSkipped)
	{
		op = [op copy];
			
		op.needsDeleteDatabaseRow = YES;
		op.pendingStatus = @(YDBCloudOperationStatus_Skipped);
		
		[self addModifiedOperation:op];
		[self didSkipOperation:op];
	}
}

/**
 * Use this method to skip/abort operations (across all registered pipelines).
**/
- (void)skipOperationsPassingTest:(BOOL (NS_NOESCAPE^)(YapDatabaseCloudCorePipeline *pipeline,
                                                       YapDatabaseCloudCoreOperation *operation,
                                                       NSUInteger graphIdx, BOOL *stop))testBlock
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	
	__block NSMutableArray *skippedOps = nil;
	
	[self _enumerateAndModifyOperations:YDBCloudCore_EnumOps_All
	                         usingBlock:
	  ^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCorePipeline *pipeline,
	                                   YapDatabaseCloudCoreOperation *operation,
	                                   NSUInteger graphIdx, BOOL *stop)
	{
		operation = [operation copy];
		BOOL shouldSkip = testBlock(pipeline, operation, graphIdx, stop);
		
		if (shouldSkip)
		{
			operation.needsDeleteDatabaseRow = YES;
			operation.pendingStatus = @(YDBCloudOperationStatus_Skipped);
			
			if (skippedOps == nil)
				skippedOps = [NSMutableArray array];
			
			[skippedOps addObject:operation];
			return operation;
		}
		else
		{
			return nil;
		}
	}];
	
	for (YapDatabaseCloudCoreOperation *op in skippedOps)
	{
		[self didSkipOperation:op];
	}
}

/**
 * Use this method to skip/abort operations in a specific pipeline.
**/
- (void)skipOperationsInPipeline:(NSString *)pipelineName
                     passingTest:(BOOL (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation,
                                                      NSUInteger graphIdx, BOOL *stop))testBlock;
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:pipelineName];
	
	__block NSMutableArray *skippedOps = nil;
	
	[self _enumerateAndModifyOperations:YDBCloudCore_EnumOps_All
	                         inPipeline:pipeline
	                         usingBlock:
	  ^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		operation = [operation copy];
		BOOL shouldSkip = testBlock(operation, graphIdx, stop);
		
		if (shouldSkip)
		{
			operation.needsDeleteDatabaseRow = YES;
			operation.pendingStatus = @(YDBCloudOperationStatus_Skipped);
			
			if (skippedOps == nil)
				skippedOps = [NSMutableArray array];
			
			[skippedOps addObject:operation];
			return operation;
		}
		else
		{
			return nil;
		}
	}];
	
	for (YapDatabaseCloudCoreOperation *op in skippedOps)
	{
		[self didSkipOperation:op];
	}
}

/**
 * Returns ALL dependencies for the given operation,
 * calculated by recursively visiting dependencies of dependecies.
 */
- (NSSet<NSUUID*> *)recursiveDependenciesForOperation:(YapDatabaseCloudCoreOperation *)operation
{
	NSMutableSet<NSUUID*> *visited = [NSMutableSet set];
	[self recursiveDependencies:operation visited:visited];
	
	return visited;
}

- (void)recursiveDependencies:(YapDatabaseCloudCoreOperation *)operation visited:(NSMutableSet<NSUUID*> *)visited
{
	for (NSUUID *depUUID in operation.dependencies)
	{
		if (![visited containsObject:depUUID])
		{
			YapDatabaseCloudCoreOperation *depOp = [self _operationWithUUID:depUUID inPipeline:operation.pipeline];
			if (depOp)
			{
				[visited addObject:depUUID];
				[self recursiveDependencies:depOp visited:visited];
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Operation Searching
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Searches for an operation with the given UUID.
 *
 * @return The corresponding operation, if found. Otherwise nil.
**/
- (YapDatabaseCloudCoreOperation *)operationWithUUID:(NSUUID *)uuid
{
	// Public API safety:
	// Prevent the user from modifying operations in the pipeline by returning a copy.
	
	return [[self _operationWithUUID:uuid] copy];
}

/**
 * Searches for an operation with the given UUID and pipeline.
 * If you know the pipeline, this method is a bit more efficient than 'operationWithUUID'.
 *
 * @return The corresponding operation, if found. Otherwise nil.
**/
- (YapDatabaseCloudCoreOperation *)operationWithUUID:(NSUUID *)uuid inPipeline:(NSString *)pipelineName
{
	// Public API safety:
	// Prevent the user from modifying operations in the pipeline by returning a copy.
	
	return [[self _operationWithUUID:uuid inPipeline:pipelineName] copy];
}

/**
 * Internal version of 'operationWithUUID:'.
 * 
 * The public version returns a copy of the operation (for safety).
 * The internal version returns the operation sans copy (only safe for internal components).
**/
- (YapDatabaseCloudCoreOperation *)_operationWithUUID:(NSUUID *)uuid
{
	// Search operations from previous commits.
	
	NSArray *allPipelines = [parentConnection->parent registeredPipelines];
	
	for (YapDatabaseCloudCorePipeline *pipeline in allPipelines)
	{
		YapDatabaseCloudCoreOperation *originalOp = [pipeline _operationWithUUID:uuid];
		if (originalOp)
		{
			YapDatabaseCloudCoreOperation *modifiedOp = parentConnection->operations_modified[uuid];
			
			if (modifiedOp)
				return modifiedOp;
			else
				return originalOp;
		}
	}
	
	// Search operations that have been added (to a new graph) during this transaction.
	
	__block YapDatabaseCloudCoreOperation *matchedOp = nil;
	
	[parentConnection->operations_added enumerateKeysAndObjectsUsingBlock:
	  ^(NSString *pipelineName, NSArray<YapDatabaseCloudCoreOperation *> *ops, BOOL *stop)
	{
		for (YapDatabaseCloudCoreOperation *op in ops)
		{
			if ([op.uuid isEqual:uuid])
			{
				matchedOp = op;
				
				*stop = YES;
				break;
			}
		}
	}];
	
	if (matchedOp) return matchedOp;
	
	// Search operations that have been inserted (into a previous graph) during this transaction.
	
	[parentConnection->operations_inserted enumerateKeysAndObjectsUsingBlock:
	  ^(NSString *pipelineName, NSMutableDictionary *graphs, BOOL *outerStop)
	{
		[graphs enumerateKeysAndObjectsUsingBlock:
		  ^(NSNumber *graphIdx, NSArray<YapDatabaseCloudCoreOperation *> *ops, BOOL *innerStop)
		{
			for (YapDatabaseCloudCoreOperation *op in ops)
			{
				if ([op.uuid isEqual:uuid])
				{
					matchedOp = op;
					
					*innerStop = YES;
					*outerStop = YES;
					break;
				}
			}
		}];
	}];
	
	return matchedOp;
}

/**
 * Internal version of 'operationWithUUID:inPipeline:'.
 *
 * The public version returns a copy of the operation (for safety).
 * The internal version returns the operation sans copy (only safe for internal components).
**/
- (YapDatabaseCloudCoreOperation *)_operationWithUUID:(NSUUID *)uuid inPipeline:(NSString *)pipelineName
{
	// Search operations from previous commits.
	
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:pipelineName];
	
	YapDatabaseCloudCoreOperation *originalOp = [pipeline _operationWithUUID:uuid];
	if (originalOp)
	{
		YapDatabaseCloudCoreOperation *modifiedOp = parentConnection->operations_modified[uuid];
		
		if (modifiedOp)
			return modifiedOp;
		else
			return originalOp;
	}
	
	// Search operations that have been added (to a new graph) during this transaction.
	
	NSArray<YapDatabaseCloudCoreOperation *> *ops = parentConnection->operations_added[pipeline.name];
	for (YapDatabaseCloudCoreOperation *op in ops)
	{
		if ([op.uuid isEqual:uuid])
		{
			return op;
		}
	}
	
	// Search operations that have been inserted (into a previous graph) during this transaction.
	
	__block YapDatabaseCloudCoreOperation *matchedOp = nil;
	
	NSDictionary *graphs = parentConnection->operations_inserted[pipeline.name];
	
	[graphs enumerateKeysAndObjectsUsingBlock:
	  ^(NSNumber *graphIdx, NSArray<YapDatabaseCloudCoreOperation *> *ops, BOOL *stop)
	{
		for (YapDatabaseCloudCoreOperation *op in ops)
		{
			if ([op.uuid isEqual:uuid])
			{
				matchedOp = op;
				
				*stop = YES;
				break;
			}
		}
	}];
	
	return matchedOp;
}

/**
 * Fetches the graph index that corresponds to newly added operations.
 * That is, operations that are added during this commit (read-write transaction).
 *
 * This may be useful if you need to find and modify operations added during the current read/write transaction.
 *
 * @return
 *   The index of the graph that will contain newly added operations from this commit.
 *   Or NSNotFound if the pipeline isn't found.
**/
- (NSUInteger)graphForAddedOperationsInPipeline:(NSString *)pipelineName
{
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:pipelineName];
	
	if (pipeline)
		return pipeline.graphCount;
	else
		return NSNotFound;
}

/**
 * @param operation
 *   The operation to search for.
 *   The operation.pipeline property specifies which pipeline to use.
 *
 * @return
 *   The index of the graph that contains the given operation.
 *   Or NSNotFound if a graph isn't found.
**/
- (NSUInteger)graphForOperation:(YapDatabaseCloudCoreOperation *)operation
{
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:operation.pipeline];
	if (pipeline == nil) {
		return NSNotFound;
	}
	
	NSUUID *uuid = operation.uuid;
	
	// Search operations from previous commits.
	
	__block BOOL found = NO;
	__block NSUInteger foundGraphIdx = NSNotFound;
	
	[pipeline _enumerateOperationsUsingBlock:
	  ^(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		if ([operation.uuid isEqual:uuid])
		{
			found = YES;
			foundGraphIdx = graphIdx;
			*stop = YES;
		}
	}];
	
	if (found) {
		return foundGraphIdx;
	}
	
	// Search operations that have been added (to a new graph) during this transaction.
	
	NSArray<YapDatabaseCloudCoreOperation *> *ops = parentConnection->operations_added[pipeline.name];
	for (YapDatabaseCloudCoreOperation *op in ops)
	{
		if ([op.uuid isEqual:uuid])
		{
			// This is an added operation for a new graph.
			// So the graphIdx is going to be the next available idx (i.e. currentGraphs.count)
			
			return [pipeline graphCount];
		}
	}
	
	// Search operations that have been inserted (into a previous graph) during this transaction.
	
	NSDictionary *graphs = parentConnection->operations_inserted[pipeline.name];
	
	[graphs enumerateKeysAndObjectsUsingBlock:
	  ^(NSNumber *graphIdx, NSArray<YapDatabaseCloudCoreOperation *> *ops, BOOL *stop)
	{
		for (YapDatabaseCloudCoreOperation *op in ops)
		{
			if ([op.uuid isEqual:uuid])
			{
				// This is an inserted operaration for a previous graph.
				
				foundGraphIdx = graphIdx.unsignedIntegerValue;
				*stop = YES;
			}
		}
	}];
	
	return foundGraphIdx;
}

/**
 * Public API
**/
- (void)enumerateOperationsUsingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCorePipeline *pipeline,
                                                           YapDatabaseCloudCoreOperation *operation,
                                                           NSUInteger graphIdx, BOOL *stop))enumBlock
{
	[self _enumerateOperationsUsingBlock:
	    ^(YapDatabaseCloudCorePipeline *pipeline,
	      YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		enumBlock(pipeline, [operation copy], graphIdx, stop);
	}];
}

/**
 * Public API
**/
- (void)enumerateOperationsInPipeline:(NSString *)pipelineName
                           usingBlock:
    (void (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop))enumBlock
{
	[self _enumerateOperationsInPipeline:pipelineName
									  usingBlock:^(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		enumBlock([operation copy], graphIdx, stop);
	}];
}

/**
 * Public API
**/
- (void)enumerateAddedOperationsUsingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCorePipeline *pipeline,
                                                                YapDatabaseCloudCoreOperation *operation,
                                                                NSUInteger graphIdx, BOOL *stop))enumBlock
{
	if (enumBlock == nil) return;
	if (databaseTransaction->isReadWriteTransaction == NO) return;
	
	[self _enumerateAndModifyOperations:YDBCloudCore_EnumOps_Added
	                         usingBlock:
	  ^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCorePipeline *pipeline,
	                                   YapDatabaseCloudCoreOperation *operation,
	                                   NSUInteger graphIdx, BOOL *stop)
	{
		enumBlock(pipeline, [operation copy], graphIdx, stop);
		return nil;
	}];
}

/**
 * Public API
**/
- (void)enumerateAddedOperationsInPipeline:(NSString *)pipelineName
                                usingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation,
                                                                NSUInteger graphIdx, BOOL *stop))enumBlock
{
	if (enumBlock == nil) return;
	if (databaseTransaction->isReadWriteTransaction == NO) return;
	
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:pipelineName];
	
	[self _enumerateAndModifyOperations:YDBCloudCore_EnumOps_All
	                         inPipeline:pipeline
	                         usingBlock:
	  ^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		enumBlock([operation copy], graphIdx, stop);
		return nil;
	}];
}

/**
 * Internal enumerate method.
 * 
 * The public version returns a copy of the operation (for safety).
 * The internal version returns the operation sans copy (only safe for internal components).
**/
- (void)_enumerateOperationsUsingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCorePipeline *pipeline,
                                                            YapDatabaseCloudCoreOperation *operation,
                                                            NSUInteger graphIdx, BOOL *stop))enumBlock
{
	if (enumBlock == nil) return;
	
	if (databaseTransaction->isReadWriteTransaction)
	{
		// This is a lot more complicated, as we have to take into account:
		// - operations that have been added
		// - operations that have been inserted
		// - operations that have been modified
		//
		// So we use the `_enumerateAndModifyOperations:::` method, which handle all this for us.
		
		[self _enumerateAndModifyOperations:YDBCloudCore_EnumOps_All
		                         usingBlock:
		  ^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCorePipeline *pipeline,
		                                   YapDatabaseCloudCoreOperation *operation,
		                                   NSUInteger graphIdx, BOOL *stop)
		{
			enumBlock(pipeline, operation, graphIdx, stop);
			return nil;
		}];
	}
	else
	{
		__block BOOL stop = NO;
		
		NSArray *allPipelines = [parentConnection->parent registeredPipelines];
		for (YapDatabaseCloudCorePipeline *pipeline in allPipelines)
		{
			[pipeline _enumerateOperationsUsingBlock:
			  ^(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *innerStop)
			{
				enumBlock(pipeline, operation, graphIdx, &stop);
				
				if (stop) *innerStop = YES;
			}];
			
			if (stop) break;
		}
	}
}

/**
 * Internal enumerate method.
 *
 * The public version returns a copy of the operation (for safety).
 * The internal version returns the operation sans copy (only safe for internal components).
**/
- (void)_enumerateOperationsInPipeline:(NSString *)pipelineName
                            usingBlock:
    (void (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop))enumBlock
{
	if (enumBlock == nil) return;
	
	YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:pipelineName];
	
	if (databaseTransaction->isReadWriteTransaction)
	{
		// This is a lot more complicated, as we have to take into account:
		// - operations that have been added
		// - operations that have been inserted
		// - operations that have been modified
		//
		// So we use the `_enumerateAndModifyOperations:::` method, which handle all this for us.
		
		[self _enumerateAndModifyOperations:YDBCloudCore_EnumOps_All
		                         inPipeline:pipeline
		                         usingBlock:
		  ^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
		{
			enumBlock(operation, graphIdx, stop);
			return nil;
		}];
	}
	else
	{
		[pipeline _enumerateOperationsUsingBlock:enumBlock];
	}
}

/**
 * Internal enumerate method (for readWriteTransactions only).
 *
 * Allows for enumeration of all existing, inserted & added operations (filtering as needed via parameter).
**/
- (void)_enumerateOperations:(YDBCloudCore_EnumOps)flags
                  usingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCorePipeline *pipeline,
                                                  YapDatabaseCloudCoreOperation *operation,
                                                  NSUInteger graphIdx, BOOL *stop))enumBlock
{
	[self _enumerateAndModifyOperations:flags
	                         usingBlock:
	^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCorePipeline *pipeline,
	                                 YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		enumBlock(pipeline, operation, graphIdx, stop);
		return nil;
	}];
}

/**
 * Internal enumerate method (for readWriteTransactions only).
 *
 * Allows for enumeration of all existing, inserted & added operations (filtering as needed via parameter).
**/
- (void)_enumerateOperations:(YDBCloudCore_EnumOps)flags
                  inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
                  usingBlock:(void (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation,
                                                  NSUInteger graphIdx, BOOL *stop))enumBlock
{
	[self _enumerateAndModifyOperations:flags
	                         inPipeline:pipeline
	                         usingBlock:
	^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
	{
		enumBlock(operation, graphIdx, stop);
		return nil;
	}];
}

/**
 * Internal enumerate method (for readWriteTransactions only).
 *
 * Allows for enumeration of all existing, inserted & added operations (filtering as needed via parameter),
 * and allows for the modification of any item during enumeration.
**/
- (void)_enumerateAndModifyOperations:(YDBCloudCore_EnumOps)flags
                           usingBlock:(YapDatabaseCloudCoreOperation *
                                      (NS_NOESCAPE^)(YapDatabaseCloudCorePipeline *pipeline,
                                                     YapDatabaseCloudCoreOperation *operation,
                                                     NSUInteger graphIdx, BOOL *stop))enumBlock
{
	NSAssert(databaseTransaction->isReadWriteTransaction, @"Oops");
	if (enumBlock == nil) return;
	
	NSArray *allPipelines = [parentConnection->parent registeredPipelines];
	
	for (YapDatabaseCloudCorePipeline *pipeline in allPipelines)
	{
		[self _enumerateAndModifyOperations:flags
		                         inPipeline:pipeline
		                         usingBlock:
		    ^YapDatabaseCloudCoreOperation *(YapDatabaseCloudCoreOperation *operation, NSUInteger graphIdx, BOOL *stop)
		{
			return enumBlock(pipeline, operation, graphIdx, stop);
		}];
	}
}

/**
 * Internal enumerate method (for readWriteTransactions only).
 *
 * Allows for enumeration of all existing, inserted & added operations (filtering as needed via parameter),
 * and allows for the modification of any item during enumeration.
**/
- (void)_enumerateAndModifyOperations:(YDBCloudCore_EnumOps)flags
                           inPipeline:(YapDatabaseCloudCorePipeline *)pipeline
                           usingBlock:(YapDatabaseCloudCoreOperation *
                                      (NS_NOESCAPE^)(YapDatabaseCloudCoreOperation *operation,
                                                     NSUInteger graphIdx, BOOL *stop))enumBlock
{
	NSAssert(databaseTransaction->isReadWriteTransaction, @"Oops");
	if (enumBlock == nil) return;
	
	__block BOOL stop = NO;
	
	NSArray<NSArray<YapDatabaseCloudCoreOperation *> *> *graphOperations = pipeline.graphOperations;
	
	[graphOperations enumerateObjectsUsingBlock:
		^(NSArray<YapDatabaseCloudCoreOperation *> *operations, NSUInteger idx, BOOL *innerStop)
	{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		if (flags & YDBCloudCore_EnumOps_Existing)
		{
			for (YapDatabaseCloudCoreOperation *queuedOp in operations)
			{
				YapDatabaseCloudCoreOperation *modifiedOp = parentConnection->operations_modified[queuedOp.uuid];
			
				if (modifiedOp)
					modifiedOp = enumBlock(modifiedOp, idx, &stop);
				else
					modifiedOp = enumBlock(queuedOp, idx, &stop);
			
				if (modifiedOp)
				{
					parentConnection->operations_modified[modifiedOp.uuid] = modifiedOp;
				}
			
				if (stop) {
					*innerStop = YES;
					return;
				}
			}
		}
		
		if (flags & YDBCloudCore_EnumOps_Inserted)
		{
			NSDictionary *insertedGraphs = parentConnection->operations_inserted[pipeline.name];
			NSMutableArray<YapDatabaseCloudCoreOperation *> *insertedOps = insertedGraphs[@(idx)];
			
			for (NSUInteger i = 0; i < insertedOps.count; i++)
			{
				YapDatabaseCloudCoreOperation *op = insertedOps[i];
				
				YapDatabaseCloudCoreOperation *modifiedOp = enumBlock(op, idx, &stop);
				
				if (modifiedOp)
				{
					insertedOps[i] = modifiedOp;
				}
				
				if (stop) {
					*innerStop = YES;
					return;
				}
			}
		}
		
	#pragma clang diagnostic pop
	}];
	
	if (!stop && (flags & YDBCloudCore_EnumOps_Added))
	{
		NSUInteger nextGraphIdx = graphOperations.count;
		
		NSMutableArray<YapDatabaseCloudCoreOperation *> *addedOps =
		  parentConnection->operations_added[pipeline.name];
		
		for (NSUInteger i = 0; i < addedOps.count; i++)
		{
			YapDatabaseCloudCoreOperation *op = addedOps[i];
			
			YapDatabaseCloudCoreOperation *modifiedOp = enumBlock(op, nextGraphIdx, &stop);
			
			if (modifiedOp)
			{
				addedOps[i] = modifiedOp;
			}
			
			if (stop) break;
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Tag Support
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Returns the currently set tag for the given key/identifier tuple.
 *
 * @param key
 *   A unique identifier for the resource.
 *   E.g. the cloudURI for a remote file.
 *
 * @param identifier
 *   The type of tag being stored.
 *   E.g. "eTag", "globalFileID"
 *   If nil, the identifier is automatically converted to the empty string.
 *
 * @return
 *   The most recently assigned tag.
**/
- (id)tagForKey:(NSString *)key withIdentifier:(NSString *)identifier
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!parentConnection->parent->options.enableTagSupport)
	{
		@throw [self tagSupportDisabled:NSStringFromSelector(_cmd)];
		return nil;
	}
	
	if (key == nil) return nil;
	if (identifier == nil) identifier = @"";
	
	YapCollectionKey *tuple = YapCollectionKeyCreate(key, identifier);
	
	id tag = nil;
	
	// Check dirtyTags (modified values from current transaction)
	
	tag = [parentConnection->dirtyTags objectForKey:tuple];
	if (tag)
	{
		if (tag == [NSNull null])
			return nil;
		else
			return tag;
	}
	
	// Check tagCache (cached clean values)
	
	tag = [parentConnection->tagCache objectForKey:tuple];
	if (tag)
	{
		if (tag == [NSNull null])
			return nil;
		else
			return tag;
	}
	
	// Fetch from disk
	
	sqlite3_stmt *statement = [parentConnection tagTable_fetchStatement];
	if (statement == NULL) {
		return nil;
	}
	
	// SELECT "tag" FROM "tagTableName" WHERE "key" = ? AND "identifier" = ?;
	
	const int bind_idx_key        = SQLITE_BIND_START + 0;
	const int bind_idx_identifier = SQLITE_BIND_START + 1;
	
	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, bind_idx_key, _key.str, _key.length, SQLITE_STATIC);
	
	YapDatabaseString _identifier; MakeYapDatabaseString(&_identifier, identifier);
	sqlite3_bind_text(statement, bind_idx_identifier, _identifier.str, _identifier.length, SQLITE_STATIC);
	
	const int column_idx_tag = SQLITE_COLUMN_START;
	
	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		tag = [self tagForStatement:statement column:column_idx_tag];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'recordTable_getInfoForHashStatement': %d %s",
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
	
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);
	FreeYapDatabaseString(&_identifier);
	
	if (tag)
		[parentConnection->tagCache setObject:tag forKey:tuple];
	else
		[parentConnection->tagCache setObject:[NSNull null] forKey:tuple];
	
	return tag;
}

/**
 * Allows you to update the current tag value for the given key/identifier tuple.
 *
 * @param tag
 *   The tag to store.
 *
 *   The following classes are supported:
 *   - NSString
 *   - NSNumber
 *   - NSData
 *
 * @param key
 *   A unique identifier for the resource.
 *   E.g. the cloudURI for a remote file.
 *
 * @param identifier
 *   The type of tag being stored.
 *   E.g. "eTag", "globalFileID"
 *   If nil, the identifier is automatically converted to the empty string.
 *
 * If the given tag is nil, the effect is the same as invoking removeTagForKey:withIdentifier:.
 * If the given tag is an unsupported class, throws an exception.
**/
- (void)setTag:(id)tag forKey:(NSString *)key withIdentifier:(NSString *)identifier
{
	YDBLogAutoTrace();
	
	if (tag == nil)
	{
		[self removeTagForKey:key withIdentifier:identifier];
		return;
	}
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	if (!parentConnection->parent->options.enableTagSupport)
	{
		@throw [self tagSupportDisabled:NSStringFromSelector(_cmd)];
		return;
	}
	
	if (key == nil)
	{
		YDBLogWarn(@"%@ - Ignoring: key is nil", THIS_METHOD);
		return;
	}
	if (identifier == nil)
		identifier = @"";
	
	if (![tag isKindOfClass:[NSNumber class]] &&
	    ![tag isKindOfClass:[NSString class]] &&
	    ![tag isKindOfClass:[NSData class]])
	{
		YDBLogWarn(@"%@ - Ignoring: unsupported changeTag class: %@", THIS_METHOD, [tag class]);
		return;
	}
	
	YapCollectionKey *tuple = YapCollectionKeyCreate(key, identifier);
	
	[parentConnection->dirtyTags setObject:tag forKey:tuple];
	[parentConnection->tagCache removeObjectForKey:tuple];
}

/**
 * See header file for description.
 */
- (void)enumerateTagsForKey:(NSString *)key
						withBlock:(void (^NS_NOESCAPE)(NSString *identifier, id tag, BOOL *stop))block
{
	YDBLogAutoTrace();
	
	if (key == nil) return;
	if (block == nil) return;
	
	NSDictionary<NSString*, id> *results = [self allTagsForKey:key];
	
	[results enumerateKeysAndObjectsUsingBlock:^(NSString *identifier, id tag, BOOL *stop) {
		
		block(identifier, tag, stop);
	}];
}

/**
 * Removes the tag for the given key/identifier tuple.
 *
 * Note that this method only removes the specific key+identifier value.
 * If there are other tags with the same key, but different identifier, then those values will remain.
 * To remove all such values, use removeAllTagsForKey.
 *
 * @param key
 *   A unique identifier for the resource.
 *   E.g. the cloudURI for a remote file.
 *
 * @param identifier
 *   The type of tag being stored.
 *   E.g. "eTag", "globalFileID"
 *   If nil, the identifier is automatically converted to the empty string.
 *
 * @see removeAllTagsForCloudURI
**/
- (void)removeTagForKey:(NSString *)key withIdentifier:(NSString *)identifier
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	if (!parentConnection->parent->options.enableTagSupport)
	{
		@throw [self tagSupportDisabled:NSStringFromSelector(_cmd)];
		return;
	}
	
	if (key == nil)
	{
		YDBLogWarn(@"%@ - Ignoring: key is nil", THIS_METHOD);
		return;
	}
	if (identifier == nil)
		identifier = @"";
	
	YapCollectionKey *tuple = YapCollectionKeyCreate(key, identifier);
	
	[parentConnection->dirtyTags setObject:[NSNull null] forKey:tuple];
	[parentConnection->tagCache removeObjectForKey:tuple];
}

/**
 * Removes all tags with the given key (matching any identifier).
**/
- (void)removeAllTagsForKey:(NSString *)key
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	if (!parentConnection->parent->options.enableTagSupport)
	{
		@throw [self tagSupportDisabled:NSStringFromSelector(_cmd)];
		return;
	}
	
	if (key == nil)
	{
		YDBLogWarn(@"%@ - Ignoring: key is nil", THIS_METHOD);
		return;
	}
	
	// Remove matching items from dirtyTags (modified values from current transaction)
	
	NSMutableArray<YapCollectionKey*> *keysToRemove = [NSMutableArray array];
	
	for (YapCollectionKey *tuple in parentConnection->dirtyTags)
	{
		__unsafe_unretained NSString *tuple_key        = tuple.collection;
	//	__unsafe_unretained NSString *tuple_identifier = tuple.key;
		
		if ([tuple_key isEqualToString:key])
		{
			[keysToRemove addObject:tuple];
		}
	}
	
	if (keysToRemove.count > 0)
	{
		[parentConnection->dirtyTags removeObjectsForKeys:keysToRemove];
		[keysToRemove removeAllObjects];
	}
	
	// Remove matching items from tagCache (cached clean values)
	
	[parentConnection->tagCache enumerateKeysWithBlock:^(YapCollectionKey *tuple, BOOL *stop) {
		
		__unsafe_unretained NSString *tuple_key        = tuple.collection;
	//	__unsafe_unretained NSString *tuple_identifier = tuple.key;
		
		if ([tuple_key isEqualToString:key])
		{
			[keysToRemove addObject:tuple];
		}
	}];
	
	if (keysToRemove.count > 0)
	{
		[parentConnection->tagCache removeObjectsForKeys:keysToRemove];
	}
	
	// Hit the disk
	
	[self tagTable_removeRowsWithKey:key];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Attach / Detach Support
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * See header file for method description.
**/
- (void)attachCloudURI:(NSString *)inCloudURI
                forKey:(NSString *)key
          inCollection:(NSString *)collection
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	if (!parentConnection->parent->options.enableAttachDetachSupport)
	{
		@throw [self attachDetachSupportDisabled:NSStringFromSelector(_cmd)];
		return;
	}
	
	NSString *cloudURI = [inCloudURI copy]; // mutable string protection
	
	if (cloudURI == nil) {
		YDBLogWarn(@"%@ - Ignoring: cloudURI is nil", THIS_METHOD);
		return;
	}
	
	int64_t rowid = 0;
	if ([databaseTransaction getRowid:&rowid forKey:key inCollection:collection])
	{
		[self attachCloudURI:cloudURI forRowid:rowid];
	}
	else
	{
		YapCollectionKey *collectionKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
		
		if (parentConnection->pendingAttachRequests == nil)
			parentConnection->pendingAttachRequests = [[YapManyToManyCache alloc] initWithCountLimit:0];
		
		[parentConnection->pendingAttachRequests insertKey:collectionKey value:cloudURI];
	}
}

/**
 * See header file for method description.
**/
- (void)detachCloudURI:(NSString *)inCloudURI
                forKey:(NSString *)key
          inCollection:(NSString *)collection
{
	YDBLogAutoTrace();
	
	// Proper API usage check
	if (!databaseTransaction->isReadWriteTransaction)
	{
		@throw [self requiresReadWriteTransactionException:NSStringFromSelector(_cmd)];
		return;
	}
	if (!parentConnection->parent->options.enableAttachDetachSupport)
	{
		@throw [self attachDetachSupportDisabled:NSStringFromSelector(_cmd)];
		return;
	}
	
	NSString *cloudURI = [inCloudURI copy]; // mutable string protection
	
	if (cloudURI == nil) {
		YDBLogWarn(@"%@ - Ignoring: cloudURI is nil", THIS_METHOD);
		return;
	}
	
	int64_t rowid = 0;
	if (![databaseTransaction getRowid:&rowid forKey:key inCollection:collection])
	{
		// Doesn't exist in the database.
		// Remove from pendingAttachRequests (if needed), and return.
		
		BOOL logWarning = YES;
		
		if ([parentConnection->pendingAttachRequests count] > 0)
		{
			YapCollectionKey *collectionKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
			
			if ([parentConnection->pendingAttachRequests containsKey:collectionKey value:cloudURI])
			{
				[parentConnection->pendingAttachRequests removeItemWithKey:collectionKey value:cloudURI];
				logWarning = NO;
			}
		}
		
		if (logWarning) {
			YDBLogWarn(@"%@ - No row in database with given collection/key: %@, %@", THIS_METHOD, collection, key);
		}
		
		return;
	}
	
	// Perform detach
		
	[self detachCloudURI:cloudURI forRowid:rowid];
}

- (void)enumerateAttachedForCloudURI:(NSString *)cloudURI
                          usingBlock:(void (NS_NOESCAPE^)(NSString *key, NSString *collection, BOOL pending, BOOL *stop))block
{
	BOOL stop = NO;
	
	NSSet<NSNumber *> *rowids = [self allAttachedRowidsForCloudURI:cloudURI];
	for (NSNumber *rowidNum in rowids)
	{
		YapCollectionKey *ck = [databaseTransaction collectionKeyForRowid:[rowidNum longLongValue]];
		if (ck) {
			block(ck.key, ck.collection, NO, &stop);
		}
		
		if (stop) break;
	}
	
	if (stop) return;
	
	if (parentConnection->pendingAttachRequests)
	{
		[parentConnection->pendingAttachRequests enumerateKeysForValue: cloudURI
		                                                     withBlock:^(YapCollectionKey *ck, id metadata, BOOL *stop)
		{
			block(ck.key, ck.collection, YES, stop);
		}];
	}
}

- (void)enumerateAttachedForKey:(NSString *)key
                     collection:(nullable NSString *)collection
                     usingBlock:(void (NS_NOESCAPE^)(NSString *cloudURI, BOOL *stop))block
{
	YapCollectionKey *collectionKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
	
	int64_t rowid = 0;
	if ([databaseTransaction getRowid:&rowid forCollectionKey:collectionKey])
	{
		NSSet<NSString*> *cloudURIs = [self allAttachedCloudURIsForRowid:rowid];
		
		BOOL stop = NO;
		for (NSString *cloudURI in cloudURIs)
		{
			block(cloudURI, &stop);
			
			if (stop) break;
		}
	}
	else if (parentConnection->pendingAttachRequests)
	{
		[parentConnection->pendingAttachRequests enumerateValuesForKey:collectionKey
		                                                     withBlock:^(NSString *cloudURI, id metadata, BOOL *stop)
		{
			block(cloudURI, stop);
		}];
	}
}

- (void)_enumerateAttachedForRowid:(int64_t)rowid
                        usingBlock:(void (NS_NOESCAPE^)(NSString *cloudURI, BOOL *stop))block
{
	NSSet<NSString*> *cloudURIs = [self allAttachedCloudURIsForRowid:rowid];
	
	BOOL stop = NO;
	for (NSString *cloudURI in cloudURIs)
	{
		block(cloudURI, &stop);
		
		if (stop) break;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Cleanup & Commit
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Subclasses may OPTIONALLY implement this method.
 * This method is only called if within a readwrite transaction.
 *
 * Subclasses should write any last changes to their database table(s) if needed,
 * and should perform any needed cleanup before the changeset is requested.
 *
 * Remember, the changeset is requested immediately after this method is invoked.
**/
- (void)flushPendingChangesToExtensionTables
{
	YDBLogAutoTrace();
	
	NSArray<YapDatabaseCloudCorePipeline *> *pipelines = [parentConnection->parent registeredPipelines];

	// Step 1 of 3:
	//
	// Flush changes to queue table
	
	[parentConnection->operations_added enumerateKeysAndObjectsUsingBlock:
	  ^(NSString *pipelineName, NSArray *addedOperations, BOOL *stop)
	{
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		YapDatabaseCloudCorePipeline *pipeline = [parentConnection->parent pipelineWithName:pipelineName];
		uint64_t nextSnapshot = [databaseTransaction->connection snapshot] + 1;
		
		[self queueTable_insertOperations: addedOperations
		                     withSnapshot: nextSnapshot
		                         pipeline: pipeline];
		
		YapDatabaseCloudCoreGraph *graph =
		  [[YapDatabaseCloudCoreGraph alloc] initWithSnapshot:nextSnapshot operations:addedOperations];
		
		[parentConnection->graphs_added setObject:graph forKey:pipelineName];
		
	#pragma clang diagnostic pop
	}];
	
	for (YapDatabaseCloudCorePipeline *pipeline in pipelines)
	{
		NSDictionary *graphs = parentConnection->operations_inserted[pipeline.name];
		
		[graphs enumerateKeysAndObjectsUsingBlock:
		  ^(NSNumber *graphIdxNum, NSArray<YapDatabaseCloudCoreOperation *> *insertedOps, BOOL *stop)
		{
			uint64_t snapshot = 0;
			[pipeline getSnapshot:&snapshot forGraphIndex:[graphIdxNum unsignedIntegerValue]];
			
			[self queueTable_insertOperations: insertedOps
			                     withSnapshot: snapshot
			                         pipeline: pipeline];
		}];
	}
	
	for (YapDatabaseCloudCoreOperation *modifiedOp in [parentConnection->operations_modified objectEnumerator])
	{
		if (modifiedOp.needsDeleteDatabaseRow)
		{
			[self queueTable_removeRowWithRowid:modifiedOp.operationRowid];
		}
		else if (modifiedOp.needsModifyDatabaseRow)
		{
			[self queueTable_modifyOperation:modifiedOp];
		}
	}
	
	// Step 2 of 3:
	//
	// Flush changes to mapping table
	
	if (parentConnection->dirtyMappingInfo.count > 0)
	{
		[parentConnection->dirtyMappingInfo enumerateWithBlock:
		    ^(NSNumber *rowid, NSString *cloudURI, id metadata, BOOL *stop)
		{
			if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsInsert)
			{
				[self mappingTable_insertRowWithRowid:[rowid unsignedLongLongValue] cloudURI:cloudURI];
				
				[self->parentConnection->cleanMappingCache insertKey:rowid value:cloudURI];
			}
			else if (metadata == YDBCloudCore_DiryMappingMetadata_NeedsRemove)
			{
				[self mappingTable_removeRowWithRowid:[rowid unsignedLongLongValue] cloudURI:cloudURI];
			}
		}];
	}
	
	// Step 3 of 3:
	//
	// Flush changes to tag table
	
	if (parentConnection->dirtyTags.count > 0)
	{
		NSNull *nsnull = [NSNull null];
		
		[parentConnection->dirtyTags enumerateKeysAndObjectsUsingBlock:
		    ^(YapCollectionKey *tuple, id tag, BOOL *stop)
		{
			NSString *key        = tuple.collection;
			NSString *identifier = tuple.key;
			
			if (tag == nsnull)
			{
				[self tagTable_removeRowWithKey:key identifier:identifier];
			}
			else
			{
				[self tagTable_insertOrUpdateRowWithKey:key identifier:identifier tag:tag];
				
				[self->parentConnection->tagCache setObject:tag forKey:tuple];
			}
		}];
	}
}

/**
 * Required override method from YapDatabaseExtensionTransaction.
**/
- (void)didCommitTransaction
{
	YDBLogAutoTrace();
	
	[parentConnection->parent commitAddedGraphs: parentConnection->graphs_added
	                         insertedOperations: parentConnection->operations_inserted
	                         modifiedOperations: parentConnection->operations_modified];
	
	// Forward to connection for further cleanup.
	
	[parentConnection postCommitCleanup];
	
	// An extensionTransaction is only valid within the scope of its encompassing databaseTransaction.
	// I imagine this may occasionally be misunderstood, and developers may attempt to store the extension in an ivar,
	// and then use it outside the context of the database transaction block.
	// Thus, this code is here as a safety net to ensure that such accidental misuse doesn't do any damage.
	
	parentConnection = nil;    // Do not remove !
	databaseTransaction = nil; // Do not remove !
}

/**
 * Required override method from YapDatabaseExtensionTransaction.
**/
- (void)didRollbackTransaction
{
	YDBLogAutoTrace();
	
	// Forward to connection for further cleanup.
	
	[parentConnection postRollbackCleanup];
	
	// An extensionTransaction is only valid within the scope of its encompassing databaseTransaction.
	// I imagine this may occasionally be misunderstood, and developers may attempt to store the extension in an ivar,
	// and then use it outside the context of the database transaction block.
	// Thus, this code is here as a safety net to ensure that such accidental misuse doesn't do any damage.

	parentConnection = nil;    // Do not remove !
	databaseTransaction = nil; // Do not remove !
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Exceptions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSException *)requiresReadWriteTransactionException:(NSString *)methodName
{
	NSString *extName = NSStringFromClass([[[self extensionConnection] extension] class]);
	NSString *className = NSStringFromClass([self class]);
	
	NSString *reason = [NSString stringWithFormat:
	  @"The method [%@ %@] can only be used within a readWriteTransaction.", className, methodName];
	
	return [NSException exceptionWithName:extName reason:reason userInfo:nil];
}

- (NSException *)disallowedOperationClass:(YapDatabaseCloudCoreOperation *)operation
{
	NSString *extName = NSStringFromClass([[[self extensionConnection] extension] class]);
	NSSet *allowedOperationClasses = parentConnection->parent->options.allowedOperationClasses;
	
	NSString *reason = [NSString stringWithFormat:
	  @"An operation is disallowed by configuration settings.\n"
	  @" - operation: %@\n"
	  @" - YapDatabaseCloudCoreOptions.allowedOperationClasses: %@", operation, allowedOperationClasses];
	
	return [NSException exceptionWithName:extName reason:reason userInfo:nil];
}

- (NSException *)tagSupportDisabled:(NSString *)methodName
{
	NSString *extName = NSStringFromClass([[[self extensionConnection] extension] class]);
	NSString *className = NSStringFromClass([self class]);
	
	NSString *reason = [NSString stringWithFormat:
	  @"Attempting to use tag method ([%@ %@]), but tag support has been disabled"
	  @" (YapDatabaseCloudCoreOptions.enableTagSupport == NO).", className, methodName];
	
	return [NSException exceptionWithName:extName reason:reason userInfo:nil];
}

- (NSException *)attachDetachSupportDisabled:(NSString *)methodName
{
	NSString *extName = NSStringFromClass([[[self extensionConnection] extension] class]);
	NSString *className = NSStringFromClass([self class]);
	
	NSString *reason = [NSString stringWithFormat:
	  @"Attempting to use attach/detach method ([%@ %@]), but attach/detach support has been disabled"
	  @" (YapDatabaseCloudCoreOptions.enableAttachDetachSupport == NO).", className, methodName];
	
	return [NSException exceptionWithName:extName reason:reason userInfo:nil];
}

@end
