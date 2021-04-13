#import <Cocoa/Cocoa.h>


@interface AppDelegate : NSObject <NSApplicationDelegate>

@property (nonatomic, assign) IBOutlet NSWindow *window;

@property (nonatomic, strong, readwrite) IBOutlet NSButton *databaseBenchmarksButton;
@property (nonatomic, strong, readwrite) IBOutlet NSButton *cacheBenchmarksButton;

- (IBAction)runDatabaseBenchmarks:(id)sender;
- (IBAction)runCacheBenchmarks:(id)sender;

@end
