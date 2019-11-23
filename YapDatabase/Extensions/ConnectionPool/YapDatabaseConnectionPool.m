#import "YapDatabaseConnectionPool.h"

#define DEFAULT_CONNECTION_LIMIT ((NSUInteger)3)

@implementation YapDatabaseConnectionPool {
	
	YapDatabase *database;
	
	dispatch_queue_t queue;
	NSMutableArray<YapDatabaseConnection *> *connections;
	
	NSUInteger connectionLimit;
	YapDatabaseConnectionConfig *connectionDefaults;
}

@dynamic connectionLimit;
@dynamic connectionDefaults;
@synthesize didCreateNewConnectionBlock;

- (instancetype)initWithDatabase:(YapDatabase *)inDatabase
{
	NSParameterAssert(inDatabase != nil);
	
	if ((self = [super init]))
	{
		database = inDatabase;
		
		queue = dispatch_queue_create("YapDatabaseConnectionPool", DISPATCH_QUEUE_SERIAL);
		connections = [[NSMutableArray alloc] init];
		
		connectionLimit = DEFAULT_CONNECTION_LIMIT;
	}
	return self;
}

- (NSUInteger)connectionLimit
{
	__block NSUInteger result = 0;
	dispatch_sync(queue, ^{
		result = self->connectionLimit;
	});
	
	return result;
}

- (void)setConnectionLimit:(NSUInteger)limit
{
	if (limit == 0) {
		limit = DEFAULT_CONNECTION_LIMIT;
	}
	
	dispatch_sync(queue, ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		connectionLimit = limit;
		
		while (connections.count > connectionLimit)
		{
			[connections removeLastObject];
		}
		
	#pragma clang diagnostic pop
	}});
}

- (YapDatabaseConnectionConfig *)connectionDefaults
{
	__block YapDatabaseConnectionConfig *result = nil;
	dispatch_sync(queue, ^{
		result = self->connectionDefaults;
	});
	
	return result;
}

- (void)setConnectionDefaults:(YapDatabaseConnectionConfig *)config
{
	dispatch_sync(queue, ^{
		self->connectionDefaults = config;
	});
}

- (YapDatabaseConnection *)connection
{
	__block YapDatabaseConnection *result = nil;
	__block BOOL isNewConnection = NO;
	
	dispatch_sync(queue, ^{ @autoreleasepool {
	#pragma clang diagnostic push
	#pragma clang diagnostic ignored "-Wimplicit-retain-self"
		
		uint64_t minLoad = 0;
		
		for (YapDatabaseConnection *connection in connections)
		{
			uint64_t load = connection.pendingTransactionCount;
			
			if (!result || load < minLoad)
			{
				result = connection;
				minLoad = load;
			}
			
			if (minLoad == 0) {
				// Found what we needed.
				// We can stop looking now.
				break;
			}
		}
		
		BOOL createNewConnection = NO;
		
		if (result == nil)
		{
			createNewConnection = YES;
		}
		else if (minLoad > 0)
		{
			if (connections.count < connectionLimit) {
				createNewConnection = YES;
			}
		}
		
		if (createNewConnection)
		{
			result = [database newConnection:connectionDefaults];
			[connections addObject:result];
			
			isNewConnection = YES;
		}
		
	#pragma clang diagnostic pop
	}});
	
	if (isNewConnection)
	{
		void (^block)(YapDatabaseConnection*) = self.didCreateNewConnectionBlock;
		if (block) { @autoreleasepool {
			block(result);
		}}
	}
	
	return result;
}

@end
