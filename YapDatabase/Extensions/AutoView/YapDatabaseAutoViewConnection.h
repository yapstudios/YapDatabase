#import <Foundation/Foundation.h>

#import "YapDatabaseViewConnection.h"

@class YapDatabaseView;
@class YapDatabaseAutoView;

NS_ASSUME_NONNULL_BEGIN

/**
 * Welcome to YapDatabase!
 *
 * https://github.com/yapstudios/YapDatabase
 *
 * The project wiki has a wealth of documentation if you have any questions.
 * https://github.com/yapstudios/YapDatabase/wiki
 *
 * YapDatabaseAutoView is an extension designed to work with YapDatabase.
 * It gives you a persistent sorted "view" of a configurable subset of your data.
 *
 * For the full documentation on Views, please see the related wiki article:
 * https://github.com/yapstudios/YapDatabase/wiki/Views
 *
 *
 * As an extension, YapDatabaseAutoViewConnection is automatically created by YapDatabaseConnnection.
 * You can access this object via:
 *
 * [databaseConnection extension:@"myRegisteredViewName"]
 *
 * @see YapDatabaseAutoView
 * @see YapDatabaseAutoViewTransaction
 */
@interface YapDatabaseAutoViewConnection : YapDatabaseViewConnection

// Returns properly typed parent view instance
@property (nonatomic, strong, readonly) YapDatabaseAutoView *autoView;

@end

NS_ASSUME_NONNULL_END
