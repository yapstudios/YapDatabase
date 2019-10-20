#import <Foundation/Foundation.h>

#ifdef SQLITE_HAS_CODEC
  #import <SQLCipher/sqlite3.h>
#else
  #import "sqlite3.h"
#endif

/**
 * Simple wrapper class to facilitate storing sqlite3_stmt items as objects (primarily in YapCache).
 */
@interface YapDatabaseStatement : NSObject

- (id)initWithStatement:(sqlite3_stmt *)stmt;

@property (nonatomic, assign, readonly) sqlite3_stmt *stmt;

@end
