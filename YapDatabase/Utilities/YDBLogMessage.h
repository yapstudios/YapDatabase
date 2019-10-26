/**
 * YapDatabase — a collection/key/value store and so much more
 *
 * GitHub Project : https://github.com/yapstudios/YapDatabase
 * Documentation  : https://github.com/yapstudios/YapDatabase/wiki
 * API Reference  : https://yapstudios.github.io/YapDatabase/
**/

#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/**
 * Log flags are a bitmask, which are biwise-OR'd with the log level
 * to determine if the log message should be emitted.
 */
typedef NS_OPTIONS(NSUInteger, YDBLogFlag){
	/**
	 *  Bitmask: 0...00001
	 */
	YDBLogFlagError   = (1 << 0),
	
	/**
	 *  Bitmask: 0...00010
	 */
	YDBLogFlagWarning = (1 << 1),
    
	/**
	 *  Bitmask: 0...00100
	 */
	YDBLogFlagInfo    = (1 << 2),
    
	/**
	 *  Bitmask: 0...01000
	 */
	YDBLogFlagVerbose = (1 << 3),
	
	/**
	 *  Bitmask: 0...10000
	 */
	YDBLogFlagTrace   = (1 << 4)
};

/**
 *  Log levels are used to filter out logs. Used together with flags.
 */
typedef NS_ENUM(NSUInteger, YDBLogLevel){
	/**
	 *  No logs
	*/
	YDBLogLevelOff       = 0,
	
	/**
	 *  Error logs only
	 */
	YDBLogLevelError     = (YDBLogFlagError),
	
	/**
	 *  Error and warning logs
	 */
	YDBLogLevelWarning   = (YDBLogLevelError   | YDBLogFlagWarning),
	
	/**
	 *  Error, warning and info logs
	 */
	YDBLogLevelInfo      = (YDBLogLevelWarning | YDBLogFlagInfo),
	
	/**
	 *  Error, warning, info, and verbose logs
	 */
	YDBLogLevelVerbose   = (YDBLogLevelInfo    | YDBLogFlagVerbose),
	
	/**
	 *  All logs (1...11111)
	 */
	YDBLogLevelAll       = NSUIntegerMax
};


/**
 * Ecapsulates detailed information about an emitted log message.
 */
@interface YDBLogMessage : NSObject

/**
 * Standard init method
 */
- (instancetype)initWithMessage:(NSString *)message
                          level:(YDBLogLevel)level
                           flag:(YDBLogFlag)flag
                           file:(NSString *)file
                       function:(NSString *)function
                           line:(NSUInteger)line;

/**
 * The log message. (e.g. "sqlite failed to do X because Y")
 */
@property (readonly, nonatomic) NSString *message;

/**
 * The configured `ydbLogLevel` of the file from which the log was emitted.
 */
@property (readonly, nonatomic) YDBLogLevel level;

/**
 * Tells you which flag triggered the log.
 * For example, `if flag == YDBLogFlagError`, then this is an error log message, emitted via YDBLogError()
 */
@property (readonly, nonatomic) YDBLogFlag flag;

/**
 * The full filePath (e.g. /Users/alice/code/myproject/YapDatabase/Extensions/YapFooBarTransaction.m)
 * This comes from `__FILE__`
 */
@property (readonly, nonatomic) NSString *file;

/**
 * The lastPathComponent of the filePath, with the fileExtension removed. (e.g. YapFooBarTransaction)
 */
@property (readonly, nonatomic) NSString *fileName;

/**
 * The name of function that triggered the log message.
 * This comes from __PRETTY_FUNCTION__
 */
@property (readonly, nonatomic) NSString *function;

/**
 * The line number within the file. (i.e. location of emitted log message)
 */
@property (readonly, nonatomic) NSUInteger line;

@end

NS_ASSUME_NONNULL_END
