/**
 * Copyright Deusty LLC.
**/

#import <Foundation/Foundation.h>
#import "YapDatabaseExtensionConnection.h"

@class YapDatabaseCloudCore;

NS_ASSUME_NONNULL_BEGIN

@interface YapDatabaseCloudCoreConnection : YapDatabaseExtensionConnection

/**
 * Returns the parent instance.
**/
@property (nonatomic, strong, readonly) YapDatabaseCloudCore *cloudCore;

@end

NS_ASSUME_NONNULL_END
