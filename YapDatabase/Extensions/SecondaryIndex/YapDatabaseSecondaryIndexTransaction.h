#import <Foundation/Foundation.h>

#import "YapDatabaseExtensionTransaction.h"
#import "YapDatabaseQuery.h"

NS_ASSUME_NONNULL_BEGIN

@interface YapDatabaseSecondaryIndexTransaction : YapDatabaseExtensionTransaction

/**
 * These methods allow you to enumerates matches from the secondary index(es) using a given query.
 *
 * The query that you input is an SQL style query (appropriate for SQLite semantics),
 * excluding the "SELECT ... FROM 'tableName'" component.
 * 
 * For example:
 * 
 * query = [YapDatabaseQuery queryWithFormat:@"WHERE age >= 62"];
 * [[transaction ext:@"idx"] enumerateKeysMatchingQuery:query usingBlock:^(NSString *key, BOOL *stop) {
 * 
 *     // ...
 * }];
 *
 * You can also pass parameters to the query using the standard SQLite placeholder:
 * 
 * query = [YapDatabaseQuery queryWithFormat:@"WHERE age >= ? AND state == ?", @(age), state];
 * [[transaction ext:@"idx"] enumerateKeysMatchingQuery:query usingBlock:^(NSString *key, BOOL *stop) {
 *
 *     // ...
 * }];
 *
 * For more information, and more examples, please see YapDatabaseQuery.
 * 
 * @return NO if there was a problem with the given query. YES otherwise.
 * 
 * @see YapDatabaseQuery
 */

- (BOOL)enumerateKeysMatchingQuery:(YapDatabaseQuery *)query
                        usingBlock:(void (NS_NOESCAPE^)(NSString *collection, NSString *key, BOOL *stop))block
NS_REFINED_FOR_SWIFT;


- (BOOL)enumerateKeysAndMetadataMatchingQuery:(YapDatabaseQuery *)query
                                   usingBlock:
            (void (NS_NOESCAPE^)(NSString *collection, NSString *key, __nullable id metadata, BOOL *stop))block
NS_REFINED_FOR_SWIFT;


- (BOOL)enumerateKeysAndObjectsMatchingQuery:(YapDatabaseQuery *)query
                                  usingBlock:
                         (void (NS_NOESCAPE^)(NSString *collection, NSString *key, id object, BOOL *stop))block
NS_REFINED_FOR_SWIFT;


- (BOOL)enumerateRowsMatchingQuery:(YapDatabaseQuery *)query
                        usingBlock:
       (void (NS_NOESCAPE^)(NSString *collection, NSString *key, id object, __nullable id metadata, BOOL *stop))block
NS_REFINED_FOR_SWIFT;


- (BOOL)enumerateIndexedValuesInColumn:(NSString *)column
                         matchingQuery:(YapDatabaseQuery *)query
                            usingBlock:(void (^NS_NOESCAPE)(__nullable id indexedValue, BOOL *stop))block
NS_REFINED_FOR_SWIFT;

/**
 * Skips the enumeration process, and just gives you the count of matching rows.
 */
- (BOOL)getNumberOfRows:(NSUInteger *)count matchingQuery:(YapDatabaseQuery *)query NS_REFINED_FOR_SWIFT;

/**
 * Aggregate Queries.
 * 
 * E.g.: avg, max, min, sum
 * 
 * For more inforation, see the sqlite docs on "Aggregate Functions":
 * https://www.sqlite.org/lang_aggfunc.html
 */
- (nullable id)performAggregateQuery:(YapDatabaseQuery *)query;

/**
 * This method assists in performing a query over a subset of rows,
 * where the subset is a known set of keys.
 * 
 * For example:
 * 
 * Say you have a bunch of tracks & playlist objects in the database.
 * And you've added a secondary index on track.duration.
 * Now you want to quickly figure out the duration of an entire playlist.
 * 
 * NSArray *keys = [self trackKeysInPlaylist:playlist];
 * NSArray *rowids = [[[transaction ext:@"idx"] rowidsForKeys:keys inCollection:@"tracks"] allValues];
 *
 * YapDatabaseQuery *query =
 *   [YapDatabaseQuery queryWithAggregateFunction:@"SUM(duration)" format:@"WHERE rowid IN (?)", rowids];
 */
- (NSDictionary<NSString*, NSNumber*> *)rowidsForKeys:(NSArray<NSString *> *)keys
                                         inCollection:(nullable NSString *)collection;

@end

NS_ASSUME_NONNULL_END
