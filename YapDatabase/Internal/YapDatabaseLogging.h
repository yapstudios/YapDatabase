#import <Foundation/Foundation.h>

#import "YapDatabase.h"
#import "YDBLogMessage.h"

/**
 * Logging plays a very important role in open-source libraries.
 *
 * Good documentation and comments decrease the learning time required to use a library.
 * But proper logging takes this futher by:
 * - Providing a way to trace the execution of the library
 * - Allowing developers to quickly identify subsets of the code that need analysis
 * - Making it easier for developers to find potential bugs, either in their code or the library
 * - Drawing attention to potential mis-configurations or mis-uses of the API
 *
 * Ultimately logging is an interactive extension to comments.
 */

@interface YapDatabase ()

+ (void)log:(YDBLogLevel)level
       flag:(YDBLogFlag)flag
       file:(const char *)file
   function:(const char *)function
       line:(NSUInteger)line
     format:(NSString *)format, ... NS_FORMAT_FUNCTION(6,7);

@end

#define YDB_LOG_MACRO(lvl, flg, frmt, ...)   \
        [YapDatabase log : lvl                     \
                    flag : flg                     \
                    file : __FILE__                \
                function : __PRETTY_FUNCTION__     \
                    line : __LINE__                \
                  format : (frmt), ## __VA_ARGS__]

#define YDB_LOG_MAYBE(lvl, flg, frmt, ...)                       \
        do { if(lvl & flg) YDB_LOG_MACRO(lvl, flg, frmt, ##__VA_ARGS__); } while(0)

#define YDBLogError(frmt, ...)   YDB_LOG_MAYBE(ydbLogLevel, YDBLogFlagError,   frmt, ##__VA_ARGS__)
#define YDBLogWarn(frmt, ...)    YDB_LOG_MAYBE(ydbLogLevel, YDBLogFlagWarning, frmt, ##__VA_ARGS__)
#define YDBLogInfo(frmt, ...)    YDB_LOG_MAYBE(ydbLogLevel, YDBLogFlagInfo,    frmt, ##__VA_ARGS__)
#define YDBLogVerbose(frmt, ...) YDB_LOG_MAYBE(ydbLogLevel, YDBLogFlagVerbose, frmt, ##__VA_ARGS__)
#define YDBLogAutoTrace()        YDB_LOG_MAYBE(ydbLogLevel, YDBLogFlagTrace,   @"")
