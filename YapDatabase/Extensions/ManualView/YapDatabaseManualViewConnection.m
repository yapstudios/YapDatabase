#import "YapDatabaseManualViewConnection.h"
#import "YapDatabaseManualViewPrivate.h"
#import "YapDatabaseLogging.h"

#if ! __has_feature(objc_arc)
#warning This file must be compiled with ARC. Use -fobjc-arc flag (or convert project to ARC).
#endif

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
**/
#if DEBUG
  static const int ydbLogLevel = YDBLogLevelWarning;
#else
  static const int ydbLogLevel = YDBLogLevelWarning;
#endif
#pragma unused(ydbLogLevel)


@implementation YapDatabaseManualViewConnection

#pragma mark Properties

- (YapDatabaseManualView *)manualView
{
	return (YapDatabaseManualView *)parent;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Transactions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Required override method from YapDatabaseExtensionConnection.
**/
- (id)newReadTransaction:(YapDatabaseReadTransaction *)databaseTransaction
{
	YDBLogAutoTrace();
	
	YapDatabaseManualViewTransaction *transaction =
	  [[YapDatabaseManualViewTransaction alloc] initWithParentConnection:self
	                                                 databaseTransaction:databaseTransaction];
	
	return transaction;
}

/**
 * Required override method from YapDatabaseExtensionConnection.
**/
- (id)newReadWriteTransaction:(YapDatabaseReadWriteTransaction *)databaseTransaction
{
	YDBLogAutoTrace();
	
	YapDatabaseManualViewTransaction *transaction =
	  [[YapDatabaseManualViewTransaction alloc] initWithParentConnection:self
	                                                 databaseTransaction:databaseTransaction];
	
	[self prepareForReadWriteTransaction];
	return transaction;
}

@end
