/**
 * YapDatabase — a collection/key/value store and so much more
 *
 * GitHub Project : https://github.com/yapstudios/YapDatabase
 * Documentation  : https://github.com/yapstudios/YapDatabase/wiki
 * API Reference  : https://yapstudios.github.io/YapDatabase/
**/

#import "YDBLogMessage.h"

#import <pthread.h>

@implementation YDBLogMessage

@synthesize message = _message;
@synthesize level = _level;
@synthesize flag = _flag;
@synthesize file = _file;
@synthesize fileName = _fileName;
@synthesize function = _function;
@synthesize line = _line;
@synthesize timestamp = _timestamp;
@synthesize threadID = _threadID;
@synthesize threadName = _threadName;
@synthesize queueLabel = _queueLabel;

- (instancetype)initWithMessage:(NSString *)message
                          level:(YDBLogLevel)level
                           flag:(YDBLogFlag)flag
                           file:(NSString *)file
                       function:(NSString *)function
                           line:(NSUInteger)line
{
	if ((self = [super init]))
	{
		_message      = [message copy];
		_level        = level;
		_flag         = flag;

		_file     = file;     // Not copying here since parameter supplied via __FILE__
		_function = function; // Not copying here since parameter supplied via __FUNCTION__

		_line         = line;
		_timestamp    = [NSDate new];

		__uint64_t tid;
		if (pthread_threadid_np(NULL, &tid) == 0) {
			 _threadID = [[NSString alloc] initWithFormat:@"%llu", tid];
		} else {
			 _threadID = @"missing threadId";
		}
		_threadName   = [[NSThread currentThread] name];

		// Get the file name without extension
		_fileName = [_file lastPathComponent];
		NSUInteger dotLocation = [_fileName rangeOfString:@"." options:NSBackwardsSearch].location;
		if (dotLocation != NSNotFound)
		{
			 _fileName = [_fileName substringToIndex:dotLocation];
		}

		// Try to get the current queue's label
		_queueLabel = [[NSString alloc] initWithFormat:@"%s", dispatch_queue_get_label(DISPATCH_CURRENT_QUEUE_LABEL)];
	}
	return self;
}

@end
