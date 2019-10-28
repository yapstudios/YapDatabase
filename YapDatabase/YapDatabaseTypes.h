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
 * > With YapDatabase, you can choose your preferred serialization/deserialization process.
 *
 * In order to support adding objects to the database, serializers and deserializers are used.
 * The serializer and deserializer are just simple blocks that you can optionally configure.
 *
 * The default serializer/deserializer uses NSCoding.
 * This is suitable for Objective-C, but not for Swift.
 *
 * **For Swift**:
 * It's likely you'll prefer to use the Codable protocol.
 * To do so, you simply register your Codable class for the corresponding collection:
 * ```
 * database.registerCodableSerialization(String.self, forCollection: "foo")
 * database.registerCodableSerialization(MyCodableClass.self, forCollection: "bar")
 * ```
 *
 * **For Objective-C**:
 * The default serializer & deserializer use NSCoding (NSKeyedArchiver & NSKeyedUnarchiver).
 * Most of Apple's primary data types support NSCoding out of the box.
 * And it's easy to add NSCoding support to your own custom objects.
 *
 * ```
 * defaultSerializer = ^(NSString *collection, NSString *key, id object){
 *     return [NSKeyedArchiver archivedDataWithRootObject:object];
 * };
 * defaultDeserializer = ^(NSString *collection, NSString *key, NSData *data) {
 *     return [NSKeyedUnarchiver unarchiveObjectWithData:data];
 * };
 * ```
 */
typedef NSData * __nonnull (^YapDatabaseSerializer)(NSString *collection, NSString *key, id object);

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
 * > With YapDatabase, you can choose your preferred serialization/deserialization process.
 *
 * In order to support adding objects to the database, serializers and deserializers are used.
 * The serializer and deserializer are just simple blocks that you can optionally configure.
 *
 * The default serializer/deserializer uses NSCoding.
 * This is suitable for Objective-C, but not for Swift.
 *
 * **For Swift**:
 * It's likely you'll prefer to use the Codable protocol.
 * To do so, you simply register your Codable class for the corresponding collection:
 * ```
 * database.registerCodableSerialization(String.self, forCollection: "foo")
 * database.registerCodableSerialization(MyCodableClass.self, forCollection: "bar")
 * ```
 *
 * **For Objective-C**:
 * The default serializer & deserializer use NSCoding (NSKeyedArchiver & NSKeyedUnarchiver).
 * Most of Apple's primary data types support NSCoding out of the box.
 * And it's easy to add NSCoding support to your own custom objects.
 *
 * ```
 * defaultSerializer = ^(NSString *collection, NSString *key, id object){
 *     return [NSKeyedArchiver archivedDataWithRootObject:object];
 * };
 * defaultDeserializer = ^(NSString *collection, NSString *key, NSData *data) {
 *     return [NSKeyedUnarchiver unarchiveObjectWithData:data];
 * };
 * ```
 */
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

/**
 * The sanitizer block allows you to enforce desired behavior of the objects you put into the database.
 *
 * If set, the sanitizer block will be run on all items being input into the database via
 * the `setObject:forKey:inCollection:` (and other setObject:XXX: methods).
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
typedef void (^YapDatabasePostSanitizer)(NSString *collection, NSString *key, id obj);

/**
 * YapDatabase allows you to opt-in to advanced performance optimizations.
 *
 * The Object-Policy is documented on the wiki here:
 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
 */
typedef NS_ENUM(NSInteger, YapDatabasePolicy) {
	
	/**
	 * This is the default policy, unless configured otherwise.
	 */
	YapDatabasePolicyContainment = 0,
	
	/**
	 * An advanced technique that allows you to share the same instance of an object between databaseConnection's.
	 * This is VERY dangerous, unless the object is immutable.
	 * In which case it becomes safe, and you get a performance benefit from it.
	 *
	 * The Object-Policy is documented on the wiki here:
	 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
	 */
	YapDatabasePolicyShare       = 1,
	
	/**
	 * An advanced technique that copies objects from one databaseConnection to another.
	 * This only works if the object supports NSCopying.
	 *
	 * This can be dangerous, if you don't perform copying correctly.
	 * That is, you need to ensure that changes to an original object cannot affect copies of the object.
	 * This is generally what one would expect to happen, but its also easy to get wrong.
	 *
	 * The Object-Policy is documented on the wiki here:
	 * https://github.com/yapstudios/YapDatabase/wiki/Object-Policy
	 */
	YapDatabasePolicyCopy        = 2,
};

NS_ASSUME_NONNULL_END
