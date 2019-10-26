/**
 * YapDatabase — a collection/key/value store and so much more
 *
 * GitHub Project : https://github.com/yapstudios/YapDatabase
 * Documentation  : https://github.com/yapstudios/YapDatabase/wiki
 * API Reference  : https://yapstudios.github.io/YapDatabase/
**/

#import "YDBLogMessage.h"

@implementation YDBLogMessage

@synthesize message = _message;
@synthesize level = _level;
@synthesize flag = _flag;
@synthesize file = _file;
@synthesize function = _function;
@synthesize line = _line;

@dynamic fileName;

- (instancetype)initWithMessage:(NSString *)message
                          level:(YDBLogLevel)level
                           flag:(YDBLogFlag)flag
                           file:(NSString *)file
                       function:(NSString *)function
                           line:(NSUInteger)line
{
	if ((self = [super init]))
	{
		_message  = [message copy];
		_level    = level;
		_flag     = flag;
		_file     = file;     // Not copying here since parameter supplied via __FILE__
		_function = function; // Not copying here since parameter supplied via __FUNCTION__
		_line     = line;
	}
	return self;
}

- (NSString *)fileName
{
	NSString *fileName = [_file lastPathComponent];
	
	NSUInteger dotLocation = [fileName rangeOfString:@"." options:NSBackwardsSearch].location;
	if (dotLocation != NSNotFound) {
		 fileName = [fileName substringToIndex:dotLocation];
	}
	
	return fileName;
}

@end
