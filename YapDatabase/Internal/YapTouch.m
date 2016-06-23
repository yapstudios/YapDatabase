#import "YapTouch.h"


@implementation YapTouch

static YapTouch *singleton;

+ (instancetype)touch
{
	static dispatch_once_t onceToken;
	dispatch_once(&onceToken, ^{
		singleton = [[YapTouch alloc] init];
	});
	
	return singleton;
}

- (instancetype)init
{
	NSAssert(singleton == nil, @"Must use singleton via [YapTouch touch]");
	
	self = [super init];
	return self;
}

@end
