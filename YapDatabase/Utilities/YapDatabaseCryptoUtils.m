//
//  Copyright (c) 2018 Open Whisper Systems. All rights reserved.
//

#import "YapDatabaseCryptoUtils.h"
#import "YapDatabase.h"
#import "YapDatabaseLogging.h"
#import "sqlite3.h"
#import <CommonCrypto/CommonCrypto.h>

NS_ASSUME_NONNULL_BEGIN

#ifdef SQLITE_HAS_CODEC

#if DEBUG
static const int ydbLogLevel = YDB_LOG_LEVEL_INFO;
#else
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif
#pragma unused(ydbLogLevel)

#ifdef DEBUG

#define USE_ASSERTS

#define YAP_CONVERT_TO_STRING(X) #X
#define YAP_CONVERT_EXPR_TO_STRING(X) YAP_CONVERT_TO_STRING(X)

// YapAssert() and YapFail() should be used in Obj-C methods.
// YapCAssert() and YapCFail() should be used in free functions.

#define YapAssert(X)                                                                                                   \
if (!(X)) {                                                                                                        \
YDBLogError(@"%s Assertion failed: %s", __PRETTY_FUNCTION__, YAP_CONVERT_EXPR_TO_STRING(X));                        \
[DDLog flushLog];                                                                                              \
NSAssert(0, @"Assertion failed: %s", YAP_CONVERT_EXPR_TO_STRING(X));                                               \
}

#define YapCAssert(X)                                                                                                  \
if (!(X)) {                                                                                                        \
YDBLogError(@"%s Assertion failed: %s", __PRETTY_FUNCTION__, YAP_CONVERT_EXPR_TO_STRING(X));                        \
[DDLog flushLog];                                                                                              \
NSCAssert(0, @"Assertion failed: %s", YAP_CONVERT_EXPR_TO_STRING(X));                                              \
}

#define YapFail(message, ...)                                                                                          \
{                                                                                                                  \
NSString *formattedMessage = [NSString stringWithFormat:message, ##__VA_ARGS__];                               \
YDBLogError(@"%s %@", __PRETTY_FUNCTION__, formattedMessage);                                                   \
[DDLog flushLog];                                                                                              \
NSAssert(0, formattedMessage);                                                                                 \
}

#define YapCFail(message, ...)                                                                                         \
{                                                                                                                  \
NSString *formattedMessage = [NSString stringWithFormat:message, ##__VA_ARGS__];                               \
YDBLogError(@"%s %@", __PRETTY_FUNCTION__, formattedMessage);                                                   \
[DDLog flushLog];                                                                                              \
NSCAssert(0, formattedMessage);                                                                                \
}

#define YapFailNoFormat(message)                                                                                       \
{                                                                                                                  \
YDBLogError(@"%s %@", __PRETTY_FUNCTION__, message);                                                            \
[DDLog flushLog];                                                                                              \
NSAssert(0, message);                                                                                          \
}

#define YapCFailNoFormat(message)                                                                                      \
{                                                                                                                  \
YDBLogError(@"%s %@", __PRETTY_FUNCTION__, message);                                                            \
[DDLog flushLog];                                                                                              \
NSCAssert(0, message);                                                                                         \
}

#else

#define YapAssert(X)
#define YapCAssert(X)
#define YapFail(message, ...)
#define YapCFail(message, ...)
#define YapFailNoFormat(X)
#define YapCFailNoFormat(X)

#endif

#define YapRaiseException(name, formatString, ...) \
{ \
    [DDLog flushLog]; \
    [NSException raise:name format:formatString, ##__VA_ARGS__]; \
}

const NSUInteger kSqliteHeaderLength = 32;
const NSUInteger kSQLCipherSaltLength = 16;
const NSUInteger kSQLCipherDerivedKeyLength = 32;
const NSUInteger kSQLCipherKeySpecLength = 48;

NSString *const YapDatabaseErrorDomain = @"YapDatabaseErrorDomain";

NSError *YDBErrorWithDescription(NSString *description)
{
    return [NSError errorWithDomain:YapDatabaseErrorDomain
                               code:0
                           userInfo:@{ NSLocalizedDescriptionKey: description }];
}

#pragma mark -

@implementation YapDatabaseCryptoUtils

+ (NSData *)readFirstNBytesOfDatabaseFile:(NSString *)filePath byteCount:(NSUInteger)byteCount
{
    YapAssert(filePath.length > 0);

    @autoreleasepool {
        NSError *error;
        // Use memory-mapped NSData to avoid reading the entire file into memory.
        //
        // We use NSDataReadingMappedAlways instead of NSDataReadingMappedIfSafe because
        // we know the database will always exist for the duration of this instance of NSData.
        NSData *_Nullable data = [NSData dataWithContentsOfURL:[NSURL fileURLWithPath:filePath]
                                                       options:NSDataReadingMappedAlways
                                                         error:&error];
        if (!data || error) {
            YDBLogError(@"%@ Couldn't read database file header.", self.logTag);
            YapRaiseException(@"Couldn't read database file header", @"");
        }
        // Pull this constant out so that we can use it in our YapDatabase fork.
        NSData *_Nullable headerData = [data subdataWithRange:NSMakeRange(0, byteCount)];
        if (!headerData || headerData.length != byteCount) {
            YapRaiseException(@"Database file header has unexpected length", @"Database file header has unexpected length: %zd", headerData.length);
        }
        return [headerData copy];
    }
}

+ (BOOL)doesDatabaseNeedToBeConverted:(NSString *)databaseFilePath
{
    YapAssert(databaseFilePath.length > 0);

    if (![[NSFileManager defaultManager] fileExistsAtPath:databaseFilePath]) {
        YDBLogVerbose(@"%@ database file not found.", self.logTag);
        return nil;
    }

    NSData *headerData = [self readFirstNBytesOfDatabaseFile:databaseFilePath byteCount:kSqliteHeaderLength];
    YapAssert(headerData);

    NSString *kUnencryptedHeader = @"SQLite format 3\0";
    NSData *unencryptedHeaderData = [kUnencryptedHeader dataUsingEncoding:NSUTF8StringEncoding];
    BOOL isUnencrypted = [unencryptedHeaderData
        isEqualToData:[headerData subdataWithRange:NSMakeRange(0, unencryptedHeaderData.length)]];
    if (isUnencrypted) {
        YDBLogVerbose(@"%@ doesDatabaseNeedToBeConverted; legacy database header already decrypted.", self.logTag);
        return NO;
    }

    return YES;
}

+ (nullable NSError *)convertDatabaseIfNecessary:(NSString *)databaseFilePath
                                databasePassword:(NSData *)databasePassword
                                       saltBlock:(YapDatabaseSaltBlock)saltBlock
                                    keySpecBlock:(YapDatabaseKeySpecBlock)keySpecBlock
{
    if (![self doesDatabaseNeedToBeConverted:databaseFilePath]) {
        return nil;
    }

    return [self convertDatabase:databaseFilePath
                databasePassword:databasePassword
                       saltBlock:saltBlock
                    keySpecBlock:keySpecBlock];
}

+ (nullable NSError *)convertDatabase:(NSString *)databaseFilePath
                     databasePassword:(NSData *)databasePassword
                            saltBlock:(YapDatabaseSaltBlock)saltBlock
                         keySpecBlock:(YapDatabaseKeySpecBlock)keySpecBlock
{
    YapAssert(databaseFilePath.length > 0);
    YapAssert(databasePassword.length > 0);
    YapAssert(saltBlock);
    YapAssert(keySpecBlock);

    NSData *saltData;
    {
        NSData *headerData = [self readFirstNBytesOfDatabaseFile:databaseFilePath byteCount:kSqliteHeaderLength];
        YapAssert(headerData);

        YapAssert(headerData.length >= kSQLCipherSaltLength);
        saltData = [headerData subdataWithRange:NSMakeRange(0, kSQLCipherSaltLength)];

        // Make sure we successfully persist the salt (persumably in the keychain) before
        // proceeding with the database conversion or we could leave the app in an
        // unrecoverable state.
        saltBlock(saltData);
    }

    {
        NSData *_Nullable keySpecData = [self databaseKeySpecForPassword:databasePassword saltData:saltData];
        if (!keySpecData || keySpecData.length != kSQLCipherKeySpecLength) {
            YDBLogError(@"Error deriving key spec");
            return YDBErrorWithDescription(@"Invalid key spec");
        }

        YapAssert(keySpecData.length == kSQLCipherKeySpecLength);

        // Make sure we successfully persist the key spec (persumably in the keychain) before
        // proceeding with the database conversion or we could leave the app in an
        // unrecoverable state.
        keySpecBlock(keySpecData);
    }

    // -----------------------------------------------------------
    //
    // This block was derived from [Yapdatabase openDatabase].
    sqlite3 *db;
    int status;
    {
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE;
        status = sqlite3_open_v2([databaseFilePath UTF8String], &db, flags, NULL);
        if (status != SQLITE_OK) {
            // There are a few reasons why the database might not open.
            // One possibility is if the database file has become corrupt.

            // Sometimes the open function returns a db to allow us to query it for the error message.
            // The openConfigCreate block will close it for us.
            if (db) {
                YDBLogError(@"Error opening database: %d %s", status, sqlite3_errmsg(db));
            } else {
                YDBLogError(@"Error opening database: %d", status);
            }

            return YDBErrorWithDescription(@"Failed to open database");
        }
    }

    // -----------------------------------------------------------
    //
    // This block was derived from [Yapdatabase configureEncryptionForDatabase].
    {
        NSData *keyData = databasePassword;

        status = sqlite3_key(db, [keyData bytes], (int)[keyData length]);
        if (status != SQLITE_OK) {
            YDBLogError(@"Error setting SQLCipher key: %d %s", status, sqlite3_errmsg(db));
            return YDBErrorWithDescription(@"Failed to set SQLCipher key");
        }
    }

    // -----------------------------------------------------------
    //
    // This block was derived from [Yapdatabase configureDatabase].
    {
        NSError *_Nullable error = [self executeSql:@"PRAGMA journal_mode = WAL;"
                                                 db:db
                                              label:@"PRAGMA journal_mode = WAL"];
        if (error) {
            return error;
        }

        // Set synchronous to normal for THIS sqlite instance.
        //
        // This does NOT affect normal connections.
        // That is, this does NOT affect YapDatabaseConnection instances.
        // The sqlite connections of normal YapDatabaseConnection instances will follow the set pragmaSynchronous value.
        //
        // The reason we hardcode normal for this sqlite instance is because
        // it's only used to write the initial snapshot value.
        // And this doesn't need to be durable, as it is initialized to zero everytime.
        //
        // (This sqlite db is also used to perform checkpoints.
        //  But a normal value won't affect these operations,
        //  as they will perform sync operations whether the connection is normal or full.)
        
        error = [self executeSql:@"PRAGMA synchronous = NORMAL;"
                              db:db
                           label:@"PRAGMA synchronous = NORMAL"];
        // Any error isn't critical, so we can continue.

        // Set journal_size_imit.
        //
        // We only need to do set this pragma for THIS connection,
        // because it is the only connection that performs checkpoints.

        NSInteger defaultPragmaJournalSizeLimit = 0;
        NSString *pragma_journal_size_limit =
            [NSString stringWithFormat:@"PRAGMA journal_size_limit = %ld;", (long)defaultPragmaJournalSizeLimit];
        error = [self executeSql:pragma_journal_size_limit
                                                 db:db
                                              label:@"PRAGMA journal_size_limit"];
        // Any error isn't critical, so we can continue.

        // Disable autocheckpointing.
        //
        // YapDatabase has its own optimized checkpointing algorithm built-in.
        // It knYap the state of every active connection for the database,
        // so it can invoke the checkpoint methods at the precise time in which a checkpoint can be most effective.
        sqlite3_wal_autocheckpoint(db, 0);

        // END DB setup copied from YapDatabase
        // BEGIN SQLCipher migration
    }

#ifdef DEBUG
    // We can obtain the database salt in two ways: by reading the first 16 bytes of the encrypted
    // header OR by using "PRAGMA cipher_salt".  In DEBUG builds, we verify that these two values
    // match.
    {
        NSString *_Nullable saltString =
            [self executeSingleStringQuery:@"PRAGMA cipher_salt;" db:db label:@"extracting database salt"];

        YapAssert([[self hexadecimalStringForData:saltData] isEqualToString:saltString]);
    }
#endif

    // -----------------------------------------------------------
    //
    // SQLCipher migration
    {
        NSString *setPlainTextHeaderPragma =
        [NSString stringWithFormat:@"PRAGMA cipher_plaintext_header_size = %zd;", kSqliteHeaderLength];
        NSError *_Nullable error = [self executeSql:setPlainTextHeaderPragma
                                                 db:db
                                              label:setPlainTextHeaderPragma];
        if (error) {
            return error;
        }

        // Modify the first page, so that SQLCipher will overwrite, respecting our new cipher_plaintext_header_size
        NSString *tableName = [NSString stringWithFormat:@"signal-migration-%@", [NSUUID new].UUIDString];
        NSString *modificationSQL =
        [NSString stringWithFormat:@"CREATE TABLE \"%@\"(a integer); INSERT INTO \"%@\"(a) VALUES (1);",
         tableName,
         tableName];
        error = [self executeSql:modificationSQL
                              db:db
                           label:modificationSQL];
        if (error) {
            return error;
        }

        // Force a checkpoint so that the plaintext is written to the actual DB file, not just living in the WAL.
        int log, ckpt;
        status = sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_FULL, &log, &ckpt);
        if (status != SQLITE_OK) {
            YDBLogError(@"%@ Error forcing checkpoint. status: %d, log: %d, ckpt: %d, error: %s", self.logTag, status, log, ckpt, sqlite3_errmsg(db));
            return YDBErrorWithDescription(@"Error forcing checkpoint.");
        }

        sqlite3_close(db);
    }

    return nil;
}

+ (nullable NSError *)executeSql:(NSString *)sql
                              db:(sqlite3 *)db
                           label:(NSString *)label
{
    YapAssert(db);
    YapAssert(sql.length > 0);
    
    int status = sqlite3_exec(db, [sql UTF8String], NULL, NULL, NULL);
    if (status != SQLITE_OK) {
        YDBLogError(@"Error %@: status: %d, error: %s",
                   label,
                   status,
                   sqlite3_errmsg(db));
        return YDBErrorWithDescription([NSString stringWithFormat:@"Failed to set %@", label]);
    }
    return nil;
}

+ (nullable NSString *)executeSingleStringQuery:(NSString *)sql db:(sqlite3 *)db label:(NSString *)label
{
    sqlite3_stmt *statement;

    int status = sqlite3_prepare_v2(db, sql.UTF8String, -1, &statement, NULL);
    if (status != SQLITE_OK) {
        YDBLogError(@"%@ Error %@: %d, error: %s", self.logTag, label, status, sqlite3_errmsg(db));
        return nil;
    }

    status = sqlite3_step(statement);
    if (status != SQLITE_ROW) {
        YDBLogError(@"%@ Missing %@: %d, error: %s", self.logTag, label, status, sqlite3_errmsg(db));
        return nil;
    }

    const unsigned char *valueBytes = sqlite3_column_text(statement, 0);
    int valueLength = sqlite3_column_bytes(statement, 0);
    YapAssert(valueLength == kSqliteHeaderLength);
    YapAssert(valueBytes != NULL);

    NSString *result =
        [[NSString alloc] initWithBytes:valueBytes length:(NSUInteger)valueLength encoding:NSUTF8StringEncoding];

    sqlite3_finalize(statement);
    statement = NULL;

    return result;
}

+ (nullable NSData *)deriveDatabaseKeyForPassword:(NSData *)passwordData saltData:(NSData *)saltData
{
    YapAssert(passwordData.length > 0);
    YapAssert(saltData.length == kSQLCipherSaltLength);

    unsigned char *derivedKeyBytes = malloc((size_t)kSQLCipherDerivedKeyLength);
    YapAssert(derivedKeyBytes);
    // See: PBKDF2_ITER.
    const unsigned int workfactor = 64000;

    int result = CCKeyDerivationPBKDF(kCCPBKDF2,
        passwordData.bytes,
        (size_t)passwordData.length,
        saltData.bytes,
        (size_t)saltData.length,
        kCCPRFHmacAlgSHA1,
        workfactor,
        derivedKeyBytes,
        kSQLCipherDerivedKeyLength);
    if (result != kCCSuccess) {
        YDBLogError(@"Error deriving key: %d", result);
        return nil;
    }

    NSData *_Nullable derivedKeyData = [NSData dataWithBytes:derivedKeyBytes length:kSQLCipherDerivedKeyLength];
    if (!derivedKeyData || derivedKeyData.length != kSQLCipherDerivedKeyLength) {
        YDBLogError(@"Invalid derived key: %d", result);
        return nil;
    }

    return derivedKeyData;
}

+ (nullable NSData *)databaseKeySpecForPassword:(NSData *)passwordData saltData:(NSData *)saltData
{
    YapAssert(passwordData.length > 0);
    YapAssert(saltData.length == kSQLCipherSaltLength);

    NSData *_Nullable derivedKeyData = [self deriveDatabaseKeyForPassword:passwordData saltData:saltData];
    if (!derivedKeyData || derivedKeyData.length != kSQLCipherDerivedKeyLength) {
        YDBLogError(@"Error deriving key");
        return nil;
    }
    NSMutableData *keySpecData = [NSMutableData new];
    [keySpecData appendData:derivedKeyData];
    [keySpecData appendData:saltData];

    YapAssert(keySpecData.length == kSQLCipherKeySpecLength);

    return keySpecData;
}

#pragma mark - Utils

+ (NSString *)hexadecimalStringForData:(NSData *)data {
    /* Returns hexadecimal string of NSData. Empty string if data is empty. */
    const unsigned char *dataBuffer = (const unsigned char *)[data bytes];
    if (!dataBuffer) {
        return @"";
    }
    
    NSUInteger dataLength = [data length];
    NSMutableString *hexString = [NSMutableString stringWithCapacity:(dataLength * 2)];
    
    for (NSUInteger i = 0; i < dataLength; ++i) {
        [hexString appendFormat:@"%02x", dataBuffer[i]];
    }
    return [hexString copy];
}

#pragma mark - Logging

+ (NSString *)logTag
{
    return [NSString stringWithFormat:@"[%@]", self.class];
}

- (NSString *)logTag
{
    return self.class.logTag;
}

@end

#endif

NS_ASSUME_NONNULL_END
