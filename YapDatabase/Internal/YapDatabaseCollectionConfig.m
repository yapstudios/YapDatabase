/**
 * YapDatabase — a collection/key/value store and so much more
 *
 * GitHub Project : https://github.com/yapstudios/YapDatabase
 * Documentation  : https://github.com/yapstudios/YapDatabase/wiki
 * API Reference  : https://yapstudios.github.io/YapDatabase/
**/

#import "YapDatabaseCollectionConfig.h"

@implementation YapDatabaseCollectionConfig

@synthesize objectSerializer = _objectSerializer;
@synthesize metadataSerializer = _metadataSerializer;

@synthesize objectPreSanitizer = _objectPreSanitizer;
@synthesize metadataPreSanitizer = _metadataPreSanitizer;

@synthesize objectPostSanitizer = _objectPostSanitizer;
@synthesize metadataPostSanitizer = _metadataPostSanitizer;

@synthesize objectPolicy = _objectPolicy;
@synthesize metadataPolicy = _metadataPolicy;

- (instancetype)initWithObjectSerializer:(YapDatabaseSerializer)objectSerializer
                      metadataSerializer:(YapDatabaseSerializer)metadataSerializer
                      objectPreSanitizer:(YapDatabasePreSanitizer)objectPreSanitizer
                    metadataPreSanitizer:(YapDatabasePreSanitizer)metadataPreSanitizer
                     objectPostSanitizer:(YapDatabasePostSanitizer)objectPostSanitizer
                   metadataPostSanitizer:(YapDatabasePostSanitizer)metadataPostSanitizer
                            objectPolicy:(YapDatabasePolicy)objectPolicy
                          metadataPolicy:(YapDatabasePolicy)metadataPolicy
{
	if ((self = [super init]))
	{
		_objectSerializer = objectSerializer;
		_metadataSerializer = metadataSerializer;
		
		_objectPreSanitizer = objectPreSanitizer;
		_metadataPreSanitizer = metadataPreSanitizer;
		
		_objectPostSanitizer = objectPostSanitizer;
		_metadataPostSanitizer = metadataPostSanitizer;
		
		_objectPolicy = objectPolicy;
		_metadataPolicy = metadataPolicy;
	}
	return self;
}

@end
