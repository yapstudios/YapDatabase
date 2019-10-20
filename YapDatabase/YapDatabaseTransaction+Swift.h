#import "YapDatabaseTransaction.h"

NS_ASSUME_NONNULL_BEGIN

@interface YapDatabaseReadTransaction ()
	
/**
 * Object access.
 * Allows you to pass a custom deserializer, to be used instead of the default deserializer.
 */
- (nullable id)objectForKey:(NSString *)key
					inCollection:(nullable NSString *)collection
			  withDeserializer:(nullable YapDatabaseDeserializer)deserializer NS_REFINED_FOR_SWIFT;

/**
 * Returns the metadata associated with the {collection, key} tuple.
 * If the item is cached in memory, it's immediately returned.
 * Otherwise the item is fetched from disk and deserialized.
 */
- (nullable id)metadataForKey:(NSString *)key
                 inCollection:(nullable NSString *)collection
             withDeserializer:(nullable YapDatabaseDeserializer)deserializer NS_REFINED_FOR_SWIFT;

/**
 * Provides access to both object and metadata in a single call.
 *
 * @return YES if the key exists in the database. NO otherwise, in which case both object and metadata will be nil.
 */
- (BOOL)getObject:(__nullable id * __nullable)objectPtr
         metadata:(__nullable id * __nullable)metadataPtr
           forKey:(NSString *)key
     inCollection:(nullable NSString *)collection
     withObjectDeserializer:(nullable YapDatabaseDeserializer)objectDeserializer
       metadataDeserializer:(nullable YapDatabaseDeserializer)metadataDeserializer NS_REFINED_FOR_SWIFT;

/**
 * Fast enumeration over objects in the database for which you're interested in.
 * The filter block allows you to decide which objects you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
 */
- (void)enumerateKeysAndObjectsInCollection:(nullable NSString *)collection
                                 usingBlock:(void (NS_NOESCAPE^)(NSString *key, id object, BOOL *stop))block
                                 withFilter:(nullable BOOL (NS_NOESCAPE^)(NSString *key))filter
                               deserializer:(nullable YapDatabaseDeserializer)deserializer NS_REFINED_FOR_SWIFT;

/**
 * Fast enumeration over all keys and associated metadata in the given collection.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object.
 *
 * Keep in mind that you cannot modify the collection mid-enumeration (just like any other kind of enumeration).
 */
- (void)enumerateKeysAndMetadataInCollection:(nullable NSString *)collection
                               usingBlock:(void (NS_NOESCAPE^)(NSString *key, __nullable id metadata, BOOL *stop))block
                               withFilter:(nullable BOOL (NS_NOESCAPE^)(NSString *key))filter
                             deserializer:(nullable YapDatabaseDeserializer)deserializer NS_REFINED_FOR_SWIFT;

/**
 * Fast enumeration over rows in the database for which you're interested in.
 * The filter block allows you to decide which rows you're interested in.
 *
 * From the filter block, simply return YES if you'd like the block handler to be invoked for the given key.
 * If the filter block returns NO, then the block handler is skipped for the given key,
 * which avoids the cost associated with deserializing the object & metadata.
 */
- (void)enumerateRowsInCollection:(nullable NSString *)collection
                    usingBlock:(void (NS_NOESCAPE^)(NSString *key, id object, __nullable id metadata, BOOL *stop))block
                    withFilter:(nullable BOOL (NS_NOESCAPE^)(NSString *key))filter
            objectDeserializer:(nullable YapDatabaseDeserializer)objectDeserializer
          metadataDeserializer:(nullable YapDatabaseDeserializer)metadataDeserializer NS_REFINED_FOR_SWIFT;


@end

NS_ASSUME_NONNULL_END
