#import <Foundation/Foundation.h>

#import "YapDatabase.h"

NS_ASSUME_NONNULL_BEGIN

/**
 * The connection pool class was designed to help you optimize background read-only transactions.
 * As a reminder:
 *
 * - You're encouraged to use a dedicated read-only connection for the main thread.
 *   https://github.com/yapstudios/YapDatabase/wiki/Performance-Primer#readonly_vs_readwrite_transactions
 *
 * - You're encouraged to share the dedicated read-only main-thread connection between your viewControllers:
 *   https://github.com/yapstudios/YapDatabase/wiki/Performance-Pro#sharing_the_ui_databaseconnection
 *
 * - You're encouraged to create a dedicated read-write connection for read-write transactions:
 *   (Because there can only be a single read-write transaction per database at any one time.)
 *   https://github.com/yapstudios/YapDatabase/wiki/Performance-Primer
 *
 * This leaves only non-main-thread read-only transactions. What's the recommendation for them?
 * You could create a single read-only connection that will be shared by all background tasks.
 * However, since all transactions are serialized via the shared connection,
 * this implies that background task A may have to wait for background task B to finish its read-only transaction
 * before background task A can execute its transaction.
 * And for background tasks, this is likely not the intended result.
 *
 * The connection pool was designed to increase the performance in these scenarios.
 * It will create connections on demand, up to (but not over) the connectionLimit.
 * And it will vend connections using a simple load balancer that's based on the number of pending
 * transactions that each connection has.
 * (So you'll be handed the connection with the smallest queue of pending "work".)
 *
 * This allows for increased parallelization amongst your background tasks.
**/
@interface YapDatabaseConnectionPool : NSObject

/**
 * Initializes a new connction pool with default configuration values.
 * All database connections are created on demand, so you can configure the pool after initialization.
**/
- (instancetype)initWithDatabase:(YapDatabase *)database;

/**
 * Specifies the maximum number of connections the pool is allowed to create.
 * Connections are created on demand, so the limit may never be reached.
 *
 * You can update this property at anytime.
 *
 * The default value is 3.
 * Zero is not a valid number, and will be treated as the default value.
**/
@property (atomic, assign, readwrite) NSUInteger connectionLimit;

/**
 * By default, new database connections inherit their default configuration settings via
 * YapDatabase.connectionDefaults, the same way that all connections do when one invokes [database newConnection].
 * You may optionally configure an alternative set of defaults specifically for connections created via this pool.
 *
 * The default value for this property is nil,
 * which means new database connections will inherit their configuration from YapDatabase.connectionDefaults.
**/
@property (atomic, strong, readwrite) YapDatabaseConnectionConfig *connectionDefaults;

/**
 * Allows you to perform additional configuration on a newly created connection.
 * This block is invoked BEFORE the connection is returned to the caller.
**/
@property (atomic, copy, readwrite) void(^didCreateNewConnectionBlock)(YapDatabaseConnection *newConnection);

/**
 * Returns an existing connection from the pool, or creates a new connection, depending upon the pool's configuration,
 * and the number of pending/active transactions for existing connections.
 *
 * - If there's an existing connection in the pool that doesn't have pending/active transactions,
 *   then that connection is returned.
 * - Otherwise, if the connection count is below connectionCount, a new connection is created & returned.
 * - Otherwise, an existing connection will be automatically chosen based on the number of pending/active transactions.
**/
- (YapDatabaseConnection *)connection;

@end

NS_ASSUME_NONNULL_END
