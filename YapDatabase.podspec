Pod::Spec.new do |s|
  s.name         = "YapDatabase"
  s.version      = "2.6.5"
  s.summary      = "A key/value store built atop sqlite for iOS & Mac."
  s.homepage     = "https://github.com/yapstudios/YapDatabase"
  s.license      = 'MIT'
  s.ios.deployment_target = '6.0'
  s.osx.deployment_target = '10.8'
  s.author       = { "Robbie Hanson" => "robbiehanson@deusty.com" }
  s.source       = { :git => "https://github.com/yapstudios/YapDatabase.git", :tag => s.version.to_s }
  s.module_map = "module.modulemap"

  s.default_subspec = 'standard'

  # use a builtin version of sqlite3
  s.subspec 'standard' do |ss|
    ss.library = 'sqlite3'
    ss.dependency 'CocoaLumberjack', '~> 2'
    ss.source_files = 'YapDatabase/**/*.{h,m,mm}'
    ss.private_header_files = 'YapDatabase/**/Internal/*.h'
    ss.xcconfig = { 'OTHER_LDFLAGS' => '-weak_library /usr/lib/libc++.dylib' }
    ss.requires_arc = true
  end

  # use SQLCipher and enable -DSQLITE_HAS_CODEC flag
  s.subspec 'SQLCipher' do |ss|
    ss.dependency 'SQLCipher/fts'
    ss.dependency 'CocoaLumberjack', '~> 2'
    ss.source_files = 'YapDatabase/**/*.{h,m,mm}'
    ss.private_header_files = 'YapDatabase/**/Internal/*.h'
    ss.xcconfig = { 'OTHER_LDFLAGS' => '-weak_library /usr/lib/libc++.dylib', 'OTHER_CFLAGS' => '$(inherited) -DSQLITE_HAS_CODEC' }
    ss.requires_arc = true
  end

end
