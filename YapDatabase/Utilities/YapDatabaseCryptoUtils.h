//
//  Copyright (c) 2018 Open Whisper Systems. All rights reserved.
//

#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

#ifdef SQLITE_HAS_CODEC

extern const NSUInteger kSqliteHeaderLength;
extern const NSUInteger kSQLCipherSaltLength;
extern const NSUInteger kSQLCipherDerivedKeyLength;
extern const NSUInteger kSQLCipherKeySpecLength;

typedef void (^YapDatabaseSaltBlock)(NSData *saltData);
typedef void (^YapDatabaseKeySpecBlock)(NSData *keySpecData);

// This class contains utility methods for use with SQLCipher encrypted
// databases, specifically to address an issue around database files that
// reside in the "shared data container" used to share files between
// iOS main apps and their app extensions.
//
//
// The Issue
//
// iOS will terminate suspended apps which hold a file lock on files in the shared
// container.  An exception is made for certain kinds of Sqlite files, so that iOS apps
// can share databases with their app extensions.  Unfortunately, this exception does
// not apply for SQLCipher databases which have encrypted the Sqlite file header,
// which is the default behavior of SQLCipher. Therefore apps which try to share an
// SQLCipher database with their app extensions and use WAL (write-ahead logging) will
// be terminated whenever they are sent to the background (0x10deadcc terminations).
//
// * YapDatabase always uses WAL.
// * This issue seems to affect all versions of iOS and all device models.
// * iOS only terminates apps for this reason when app transition from the `background`
//   to `suspended` states.  iOS main apps can delay being suspended by creating a
//   "background task", but this only defers the issue briefly as there are strict
//   limits on the duration of "background tasks".
// * `0xdead10cc` terminations don't occur in the simulator and won't occur on devices
//   if the debugger is attached.
// * These `0xdead10cc` terminations usually don't yield crash logs on the device, but
//   always show up in the device console logs.
//
// See:
//
// * https://developer.apple.com/library/content/technotes/tn2408/_index.html
// * References to 0x10deadcc in https://developer.apple.com/library/content/technotes/tn2151/_index.html
//
//
// Solution
//
// The solution is to have SQLCipher encrypt everything _except_ the first 32 bytes of
// the Sqlite file, which corresponds to the first part of the Sqlite header.  This is
// accomplished using the cipher_plaintext_header_size PRAGMA.
//
// The header does not contain any user data.  See:
// https://www.sqlite.org/fileformat.html#the_database_header
//
// However, Sqlite normally uses the first 16 bytes of the Sqlite header to store
// a salt value.  Therefore when using unencrypted headers, it is also necessary
// to explicitly specify a salt value.
//
// It is possible to convert SQLCipher databases with encrypted headers to use
// unencrypted headers.  However, during this conversion, the salt must be extracted
// and preserved by reading the first 16 bytes of the unconverted file.
//
//
// Implementation
//
// To open (a new or existing) YapDatabase using unencrypted headers, you have two
// options:
//
// Option A:
//
// * Use cipherKeyBlock as usual to specify the database password.
// * Use cipherSaltBlock to specify the database salt. It should be kSQLCipherSaltLength long.
// * Use cipherUnencryptedHeaderLength to specify how many bytes to leave unencrypted.
//   This should be kSqliteHeaderLength.
// * Do not use a cipherKeySpecBlock.
//
// Option B:
//
// * Use cipherKeySpecBlock to specify the database key spec. It should be kSQLCipherKeySpecLength long.
// * Use cipherUnencryptedHeaderLength to specify how many bytes to leave unencrypted.
//   This should be kSqliteHeaderLength.
// * The "key spec" includes the key derived from the database password and the salt,
//   so do not use a cipherKeyBlock or cipherSaltBlock.
//
// Option B is more performant than Option A and is therefore recommended.
//
//
// Upgrading legacy databases to use unencrypted headers:
//
// * Call the convertDatabaseIfNecessary method of this class _before_
//   trying to open any YapDatabase that may need to be converted.
// * This method will have no effect if the YapDatabase has already been converted.
// * This method should always be pretty fast, and should be safe to
//   call from within [UIApplicationDelegate application: didFinishLaunchingWithOptions:].
// * If convertDatabaseIfNecessary converts the database, it will use its
//   saltBlock and keySpecBlock parameters to inform you of the salt
//   and keyspec for this database.  These values will be needed when
//   opening the database, so they should presumably stored in the
//   keychain (like the database password).
//
//
// Creating new databases with unencrypted headers:
//
// * Randomly generate a database password and salt, presumably using SecRandomCopyBytes().
// * Derive a keyspec using databaseKeySpecForPassword:.
// * You probably should store these values in the keychain.
//
//
// Note and Disclaimer
//
// There is no authoritative documentation from Apple about iOS' usage of the Sqlite
// file header to make an exception for suspended apps with a file lock on database
// files in the shared container.  Our usage of the first 32 bytes as being sufficient
// is only empirical.
@interface YapDatabaseCryptoUtils : NSObject

- (instancetype)init NS_UNAVAILABLE;

// Returns YES IFF the database appears to have encrypted headers.
+ (BOOL)doesDatabaseNeedToBeConverted:(NSString *)databaseFilePath;

// * Call the convertDatabaseIfNecessary method of this class _before_
//   trying to open any YapDatabase that may need to be converted.
// * This method will have no effect if the YapDatabase has already been converted.
// * This method should always be pretty fast, and should be safe to
//   call from within [UIApplicationDelegate application: didFinishLaunchingWithOptions:].
// * If convertDatabaseIfNecessary converts the database, it will use its
//   saltBlock and keySpecBlock parameters to inform you of the salt
//   and keyspec for this database.  These values will be needed when
//   opening the database, so they should presumably stored in the
//   keychain (like the database password).
+ (nullable NSError *)convertDatabaseIfNecessary:(NSString *)databaseFilePath
                                databasePassword:(NSData *)databasePassword
                                       saltBlock:(YapDatabaseSaltBlock)saltBlock
                                    keySpecBlock:(YapDatabaseKeySpecBlock)keySpecBlock;

// This method can be used to derive a SQLCipher "key spec" from a
// database password and salt.  Key spec derivation is somewhat costly.
// The key spec is needed every time the database file is opened
// (including every time YapDatabse makes a new database connection),
// So it benefits performance to pass a pre-derived key spec to
// YapDatabase.
+ (nullable NSData *)databaseKeySpecForPassword:(NSData *)passwordData saltData:(NSData *)saltData;

#pragma mark - Utils

+ (NSString *)hexadecimalStringForData:(NSData *)data;

@end

#endif

NS_ASSUME_NONNULL_END
