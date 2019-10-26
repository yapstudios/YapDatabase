#import <XCTest/XCTest.h>

#import "YapDatabase.h"
#import "YapDatabaseFullTextSearch.h"


@interface TestYapDatabaseFullTextSearch : XCTestCase

@end

#pragma mark -

@implementation TestYapDatabaseFullTextSearch

- (NSString *)fileName
{
	NSString *filePath = [NSString stringWithFormat:@"%s", __FILE__];
	NSString *fileName = [filePath lastPathComponent];
	
	NSUInteger dotLocation = [fileName rangeOfString:@"." options:NSBackwardsSearch].location;
	if (dotLocation != NSNotFound) {
		 fileName = [fileName substringToIndex:dotLocation];
	}
	
	return fileName;
}

- (NSURL *)databaseURL:(NSString *)suffix
{
	NSString *databaseName = [NSString stringWithFormat:@"%@-%@.sqlite", [self fileName], suffix];
	
	NSArray<NSURL*> *urls = [[NSFileManager defaultManager] URLsForDirectory:NSCachesDirectory inDomains:NSUserDomainMask];
	NSURL *baseDir = [urls firstObject];
	
	return [baseDir URLByAppendingPathComponent:databaseName isDirectory:NO];
}

- (void)setUp
{
	[super setUp];
}

- (void)tearDown
{
	[super tearDown];
}

- (void)test
{
	NSURL *databaseURL = [self databaseURL:NSStringFromSelector(_cmd)];
	
	[[NSFileManager defaultManager] removeItemAtURL:databaseURL error:NULL];
	YapDatabase *database = [[YapDatabase alloc] initWithURL:databaseURL];
	
	XCTAssertNotNil(database, @"Oops");
	
	YapDatabaseConnection *connection = [database newConnection];
	
	YapDatabaseFullTextSearchHandler *handler = [YapDatabaseFullTextSearchHandler withObjectBlock:
	    ^(YapDatabaseReadTransaction *transaction, NSMutableDictionary *dict, NSString *collection, NSString *key, id object){
		
		[dict setObject:object forKey:@"content"];
	}];
	
	YapDatabaseFullTextSearch *fts =
	  [[YapDatabaseFullTextSearch alloc] initWithColumnNames:@[@"content"]
	                                                 handler:handler];
	
	[database registerExtension:fts withName:@"fts"];
	
	[connection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
		
		[transaction setObject:@"hello world"       forKey:@"key1" inCollection:nil];
		[transaction setObject:@"hello coffee shop" forKey:@"key2" inCollection:nil];
		[transaction setObject:@"hello laptop"      forKey:@"key3" inCollection:nil];
		[transaction setObject:@"hello work"        forKey:@"key4" inCollection:nil];
	}];
	
	[connection readWithBlock:^(YapDatabaseReadTransaction *transaction) {
		
		__block NSUInteger count;
		
		// Find matches for: hello
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"hello"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 4, @"Missing search results");
		
		// Find matches for: coffee
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"coffee"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 1, @"Missing search results");
		
		// Find matches for: hello wor*
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"hello wor*"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 2, @"Missing search results");
		
		// Find matches for: quack
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"quack"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 0, @"Missing search results");
	}];
	
	[connection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
		
		[transaction setObject:@"hello distraction" forKey:@"key4" inCollection:nil];
	}];
	
	[connection readWithBlock:^(YapDatabaseReadTransaction *transaction) {
		
		__block NSUInteger count;
		
		// Find matches for: hello
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"hello"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 4, @"Missing search results");
		
		// Find matches for: coffee
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"coffee"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 1, @"Missing search results");
		
		// Find matches for: hello wor*
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"hello wor*"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 1, @"Missing search results");
		
		// Find matches for: quack
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"quack"
		                                     usingBlock:^(NSString *collection, NSString *key, BOOL *stop) {
			count++;
		}];
		XCTAssertTrue(count == 0, @"Missing search results");
	}];
}

- (void)testSnippet
{
	NSURL *databaseURL = [self databaseURL:NSStringFromSelector(_cmd)];
	
	[[NSFileManager defaultManager] removeItemAtURL:databaseURL error:NULL];
	YapDatabase *database = [[YapDatabase alloc] initWithURL:databaseURL];
	
	XCTAssertNotNil(database, @"Oops");
	
	YapDatabaseConnection *connection = [database newConnection];
	
	YapDatabaseFullTextSearchHandler *handler = [YapDatabaseFullTextSearchHandler withObjectBlock:
	    ^(YapDatabaseReadTransaction *transaction, NSMutableDictionary *dict, NSString *collection, NSString *key, id object){
		
		[dict setObject:object forKey:@"content"];
	}];
	
	YapDatabaseFullTextSearch *fts =
	  [[YapDatabaseFullTextSearch alloc] initWithColumnNames:@[@"content"]
	                                                 handler:handler];
	
	[database registerExtension:fts withName:@"fts"];
	
	[connection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
		
		[transaction setObject:@"hello world"       forKey:@"key1" inCollection:nil];
		[transaction setObject:@"hello coffee shop" forKey:@"key2" inCollection:nil];
		[transaction setObject:@"hello laptop"      forKey:@"key3" inCollection:nil];
		[transaction setObject:@"hello work"        forKey:@"key4" inCollection:nil];
	}];
	
	[connection readWithBlock:^(YapDatabaseReadTransaction *transaction) {
		
		__block NSUInteger count;
		
		// Find matches for: hello
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"hello"
		                             withSnippetOptions:nil
		                                     usingBlock:
		    ^(NSString *snippet, NSString *collection, NSString *key, BOOL *stop) {
			
		//	NSLog(@"snippet: %@", snippet);
			count++;
		}];
		XCTAssertTrue(count == 4, @"Missing search results");
		
		// Find matches for: coffee
		
		YapDatabaseFullTextSearchSnippetOptions *options = [YapDatabaseFullTextSearchSnippetOptions new];
		options.startMatchText = @"[[";
		options.endMatchText   = @"]]";
		options.ellipsesText   = @"…";
		options.columnName     = @"content";
		options.numberOfTokens = 2;
		
		count = 0;
		[[transaction ext:@"fts"] enumerateKeysMatching:@"coffee"
		                             withSnippetOptions:options
		                                     usingBlock:
		    ^(NSString *snippet, NSString *collection, NSString *key, BOOL *stop) {
			
			NSLog(@"snippet: %@", snippet);
			count++;
		}];
		XCTAssertTrue(count == 1, @"Missing search results");
	}];
}

@end
