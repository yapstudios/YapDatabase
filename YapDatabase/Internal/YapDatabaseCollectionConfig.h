/**
 * YapDatabase — a collection/key/value store and so much more
 *
 * GitHub Project : https://github.com/yapstudios/YapDatabase
 * Documentation  : https://github.com/yapstudios/YapDatabase/wiki
 * API Reference  : https://yapstudios.github.io/YapDatabase/
**/

#import "YapDatabaseTypes.h"

NS_ASSUME_NONNULL_BEGIN

@interface YapDatabaseCollectionConfig : NSObject

- (instancetype)initWithObjectSerializer:(YapDatabaseSerializer)objectSerializer
                      metadataSerializer:(YapDatabaseSerializer)metadataSerializer
                      objectPreSanitizer:(YapDatabasePreSanitizer)objectPreSanitizer
                    metadataPreSanitizer:(YapDatabasePreSanitizer)metadataPreSanitizer
                     objectPostSanitizer:(YapDatabasePostSanitizer)objectPostSanitizer
                   metadataPostSanitizer:(YapDatabasePostSanitizer)metadataPostSanitizer
                            objectPolicy:(YapDatabasePolicy)objectPolicy
                          metadataPolicy:(YapDatabasePolicy)metadataPolicy;

@property (nonatomic, strong, readonly) YapDatabaseSerializer objectSerializer;
@property (nonatomic, strong, readonly) YapDatabaseSerializer metadataSerializer;

@property (nonatomic, strong, readonly) YapDatabasePreSanitizer objectPreSanitizer;
@property (nonatomic, strong, readonly) YapDatabasePreSanitizer metadataPreSanitizer;

@property (nonatomic, strong, readonly) YapDatabasePostSanitizer objectPostSanitizer;
@property (nonatomic, strong, readonly) YapDatabasePostSanitizer metadataPostSanitizer;

@property (nonatomic, assign, readonly) YapDatabasePolicy objectPolicy;
@property (nonatomic, assign, readonly) YapDatabasePolicy metadataPolicy;

@end

NS_ASSUME_NONNULL_END
