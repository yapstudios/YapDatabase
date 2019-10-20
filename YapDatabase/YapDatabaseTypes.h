#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/**
 * How does YapDatabase store my objects to disk?
 *
 * That question is answered extensively in the wiki article "Storing Objects":
 * https://github.com/yapstudios/YapDatabase/wiki/Storing-Objects
 *
 * Here's the intro from the wiki article:
 *
 * > In order to store an object to disk (via YapDatabase or any other protocol) you need some way of
 * > serializing the object. That is, convert the object into a big blob of bytes. And then, to get your
 * > object back from the disk you deserialize it (convert big blob of bytes back into object form).
 * >
 * > With YapDatabase, you can choose the default serialization/deserialization process,
 * > or you can customize it and use your own routines.
 *
 * In order to support adding objects to the database, serializers and deserializers are used.
 * The serializer and deserializer are just simple blocks that you can optionally configure.
 * The default serializer/deserializer uses NSCoding, so they are as simple and fast:
 *
 * defaultSerializer = ^(NSString *collection, NSString *key, id object){
 *     return [NSKeyedArchiver archivedDataWithRootObject:object];
 * };
 * defaultDeserializer = ^(NSString *collection, NSString *key, NSData *data) {
 *     return [NSKeyedUnarchiver unarchiveObjectWithData:data];
 * };
 *
 * If you use the initWithPath initializer, the default serializer/deserializer are used.
 * Thus to store objects in the database, the objects need only support the NSCoding protocol.
 * You may optionally use a custom serializer/deserializer for the objects and/or metadata.
 */
typedef NSData * __nonnull (^YapDatabaseSerializer)(NSString *collection, NSString *key, id object);
typedef id __nullable (^YapDatabaseDeserializer)(NSString *collection, NSString *key, NSData *data);

/**
 * The sanitizer block allows you to enforce desired behavior of the objects you put into the database.
 *
 * If set, the sanitizer block will be run on all items being input into the database via
 * the setObject:forKey:inCollection: (and other setObject:XXX: methods).
 *
 * You have 2 different hooks for running a sanitizer block:
 *
 * The PreSanitizer is run:
 * - Before the object is serialized
 * - Before the object is stored in the cache
 * - Before the object is passed to extensions
 *
 * The PostSanitizer is run:
 * - After the object has been serialized
 * - After the object has been stored in the cache
 * - After the object has been passed to extensions
 *
 * The PreSanitizer is generally used validate the objects going into the database,
 * and/or to enforce immutability of those objects.
 *
 * Enforcing immutability is a topic covered in the "Object Policy" wiki article:
 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
 *
 * The PostSanitizer is generally used to "clear flags" that are used by extensions.
 * For example, your objects might have a "changedProperties" property that tells extensions exactly
 * what properties where changed on a modified object. And the extension uses that information
 * in order to automatically sync the changes to the cloud. Thus the PostSanitizer would be used
 * to clear the "changedProperties" after the extension has processed the modified object.
 *
 * An example of such a use for the PostSanitizer is discussed in the YapDatabaseCloudKit wiki article:
 * https://github.com/yapstudios/YapDatabase/wiki/YapDatabaseCloudKit
 */
typedef id __nonnull (^YapDatabasePreSanitizer)(NSString *collection, NSString *key, id obj);
typedef void (^YapDatabasePostSanitizer)(NSString *collection, NSString *key, id obj);

NS_ASSUME_NONNULL_END
