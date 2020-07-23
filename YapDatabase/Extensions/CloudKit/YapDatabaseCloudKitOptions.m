#import "YapDatabaseCloudKitOptions.h"

@implementation YapDatabaseCloudKitOptions

@synthesize allowedCollections = allowedCollections;
@synthesize maxChangesPerChangeRequest = maxChangesPerChangeRequest;

- (id)copyWithZone:(NSZone *)zone
{
	YapDatabaseCloudKitOptions *copy = [[[self class] alloc] init]; // [self class] required to support subclassing
	copy->allowedCollections = allowedCollections;
	copy->maxChangesPerChangeRequest = maxChangesPerChangeRequest;
	
	return copy;
}

@end
