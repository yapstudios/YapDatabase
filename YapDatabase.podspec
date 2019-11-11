Pod::Spec.new do |s|
	s.name         = "YapDatabase"
	s.version      = "4.0"
	s.summary      = "A collection/key/value store built atop sqlite for iOS & Mac."
	s.homepage     = "https://github.com/yapstudios/YapDatabase"
	s.license      = 'MIT'

	s.author = {
		"Robbie Hanson" => "robbiehanson@deusty.com"
	}
	s.source = {
		:git => "https://github.com/yapstudios/YapDatabase.git",
		:tag => s.version.to_s
	}

	s.osx.deployment_target = '10.12'
	s.ios.deployment_target = '10.0'
	s.tvos.deployment_target = '10.0'
	s.watchos.deployment_target = '3.0'

	s.swift_version = '5.0'

	s.libraries = 'c++'

	# https://github.com/CocoaPods/CocoaPods/issues/9292
#	s.exclude_files = 'Docs/**/*.html'
#	s.resources = '!Docs/**/*'
#	s.resources = !'Docs/**/*'
#	s.resources = '!(Docs/**/*)'
#	s.resources = '[!D]*'
#	s.resources = '[!D]*/**/*'

	s.default_subspecs = 'Standard'

	# There are 2 primary flavors you can choose from:
	#
	# - "Standard" uses the builtin version of sqlite3
	# - "SQLCipher" uses a version of sqlite3 compiled with SQLCipher included
	#
	# If you want to encrypt your database, you should choose "SQLCipher".
	# For more information about setting up encryption, see the wiki article:
	# https://github.com/yapstudios/YapDatabase/wiki/Encryption
	#
	# Additionaly, you can choose between:
	#
	# - Objective-C with Swift extensions (the default)
	# - Objective-C only
	# 
	# Examples:
	# If you wanted to import ALL of YapDatabase:
	#
	# pod 'YapDatabase'                <- Uses Standard, including Swift extensions
	# pod 'YapDatabase/Standard-ObjC'  <- All of YapDatabase, excluding Swift extensions
	# pod 'YapDatbaase/SQLCipher'      <- All of YapDatabase, with SQLCipher & Swift extensions
	# pod 'YapDatbaase/SQLCipher-ObjC' <- All of YapDatabase, with SQLCipher, excluding Swift extensions
	#
	# In addition to this, you can optionally import ONLY the 'Core' of YapDatabase,
	# and then pick-and-choose which individual extensions you want.
	# This is helpful if you only use a few extensions,
	# of if you're concerned about app-size or additional imports.
	#
	# To do so, you just need to be more explicit in your Podfile:
	#
	# // pod 'YapDatabase/Standard'                     <- This would import EVERYTHING
	# pod 'YapDatabase/Standard/Core'                   <- Only the YapDatabase core
	# pod 'YapDatabase/Standard/Extensions/AutoView'    <- Just AutoView   (+dependencies= Core, View)
	# pod 'YapDatabase/Standard/Extensions/RTreeIndex'  <- Just RTreeIndex (+dependencies= Core)
	#
	# This works exactly the same way if you're using the SQLCipher flavor:
	# 
	# // pod 'YapDatabase/SQLCipher'                    <- This would import EVERYTHING
	# pod 'YapDatabase/SQLCipher/Core'                  <- Only the YapDatabase core
	# pod 'YapDatabase/SQLCipher/Extensions/AutoView'   <- Just AutoView   (+dependencies= Core, View)
	# pod 'YapDatabase/SQLCipher/Extensions/RTreeIndex' <- Just RTreeIndex (+dependencies= Core)
	#
	# Enjoy, and remember to check the wiki for more information / documentation:
	# https://github.com/yapstudios/YapDatabase/wiki

	
	s.subspec 'Standard-ObjC' do |ss|
		
		ss.subspec 'Core' do |ssc|
			ssc.xcconfig = { 'OTHER_CFLAGS' => '$(inherited) -DYAP_STANDARD_SQLITE' }
			ssc.library = 'sqlite3'
			ssc.source_files = 'YapDatabase/*.{h,m,mm,c}', 'YapDatabase/{Internal,Utilities}/*.{h,m,mm,c}', 'YapDatabase/Extensions/Protocol/**/*.{h,m,mm,c}'
			ssc.private_header_files = 'YapDatabase/Internal/*.h'

		end

		ss.subspec 'Extensions' do |sse|
			sse.dependency 'YapDatabase/Standard-ObjC/Core'
      
			sse.subspec 'View' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/View/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/View/Internal/*.h'
			end
      
			sse.subspec 'AutoView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/AutoView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/AutoView/Internal/*.h'
			end

			sse.subspec 'ManualView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/ManualView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/ManualView/Internal/*.h'
			end

			sse.subspec 'SecondaryIndex' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/SecondaryIndex/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/SecondaryIndex/Internal/*.h'
			end

			sse.subspec 'CrossProcessNotification' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/CrossProcessNotification/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/CrossProcessNotification/Internal/*.h'
			end

			sse.subspec 'Relationships' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/Relationships/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/Relationships/Internal/*.h'
			end

			sse.subspec 'FullTextSearch' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/FullTextSearch/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/FullTextSearch/Internal/*.h'
			end

			sse.subspec 'Hooks' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/Hooks/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/Hooks/Internal/*.h'
			end
      
			sse.subspec 'FilteredView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/FilteredView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/FilteredView/Internal/*.h'
			end
      
			sse.subspec 'SearchResultsView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/AutoView'
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/FullTextSearch'
				ssee.source_files = 'YapDatabase/Extensions/SearchResultsView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/SearchResultsView/Internal/*.h'
			end

			sse.subspec 'CloudKit' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/CloudKit/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/CloudKit/Internal/*.h'
			end

			sse.subspec 'RTreeIndex' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/RTreeIndex/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/RTreeIndex/Internal/*.h'
			end

			sse.subspec 'ActionManager' do |ssee|
				ssee.osx.framework   = 'SystemConfiguration'
				ssee.ios.framework   = 'SystemConfiguration'
				ssee.tvos.framework  = 'SystemConfiguration'
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/AutoView'
				ssee.source_files = 'YapDatabase/Extensions/ActionManager/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/ActionManager/Internal/*.h'
			end

			sse.subspec 'CloudCore' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/CloudCore/**/*.{h,m,mm,c}'
			end

		end # Extensions

	end #Standard

	####################################################################################################

	s.subspec 'Standard' do |ss|

		ss.subspec 'Core' do |ssc|
			ssc.dependency 'YapDatabase/Standard-ObjC/Core'
			ssc.source_files = 'YapDatabase/Swift/*.swift'
		end

		ss.subspec 'Extensions' do |sse|
			sse.dependency 'YapDatabase/Standard/Core'
      
			sse.subspec 'View' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/View/Swift/*.swift'
			end
      
			sse.subspec 'AutoView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/AutoView'
				ssee.dependency 'YapDatabase/Standard/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/AutoView/Swift/*.swift'
			end

			sse.subspec 'ManualView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/ManualView'
				ssee.dependency 'YapDatabase/Standard/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/ManualView/Swift/*.swift'
			end

			sse.subspec 'SecondaryIndex' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/SecondaryIndex'
				ssee.source_files = 'YapDatabase/Extensions/SecondaryIndex/Swift/*.swift'
			end

			sse.subspec 'CrossProcessNotification' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/CrossProcessNotification'
				ssee.source_files = 'YapDatabase/Extensions/CrossProcessNotification/Swift/*.swift'
			end

			sse.subspec 'Relationships' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/Relationships'
				ssee.source_files = 'YapDatabase/Extensions/Relationships/Swift/*.swift'
			end

			sse.subspec 'FullTextSearch' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/FullTextSearch'
				ssee.source_files = 'YapDatabase/Extensions/FullTextSearch/Swift/*.swift'
			end

			sse.subspec 'Hooks' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/Hooks'
				ssee.source_files = 'YapDatabase/Extensions/Hooks/Swift/*.swift'
			end
      
			sse.subspec 'FilteredView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/FilteredView'
				ssee.dependency 'YapDatabase/Standard/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/FilteredView/Swift/*.swift'
			end
      
			sse.subspec 'SearchResultsView' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/SearchResultsView'
				ssee.dependency 'YapDatabase/Standard/Extensions/AutoView'
				ssee.dependency 'YapDatabase/Standard/Extensions/FullTextSearch'
				ssee.source_files = 'YapDatabase/Extensions/SearchResultsView/Swift/*.swift'
			end

			sse.subspec 'CloudKit' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/CloudKit'
				ssee.source_files = 'YapDatabase/Extensions/CloudKit/Swift/*.swift'
			end

			sse.subspec 'RTreeIndex' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/RTreeIndex'
				ssee.source_files = 'YapDatabase/Extensions/RTreeIndex/Swift/*.swift'
			end

			sse.subspec 'ActionManager' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/ActionManager'
				ssee.dependency 'YapDatabase/Standard/Extensions/AutoView'
				ssee.source_files = 'YapDatabase/Extensions/ActionManager/Swift/*.swift'
				ssee.osx.framework   = 'SystemConfiguration'
				ssee.ios.framework   = 'SystemConfiguration'
				ssee.tvos.framework  = 'SystemConfiguration'
			end

			sse.subspec 'CloudCore' do |ssee|
				ssee.dependency 'YapDatabase/Standard-ObjC/Extensions/CloudCore'
				ssee.source_files = 'YapDatabase/Extensions/CloudCore/Swift/*.swift'
			end

		end # Extensions

	end # Standard

	####################################################################################################
	
	# The ModuleMap option is a workaround for Issue #479:
	# https://github.com/yapstudios/YapDatabase/pull/479
	# 
	# You may need to use this if:
	# - You're using Swift, AND
	# - You also need to subclass YapDatabaseCloudCore (need access to private header files)
	#
	s.subspec 'Standard+ModuleMap' do |ss|

		ss.osx.module_map = 'Framework/Mac/module.modulemap'
		ss.ios.module_map = 'Framework/iOS/module.modulemap'
		ss.tvos.module_map = 'Framework/tvOS/module.modulemap'
		ss.watchos.module_map = 'Framework/watchOS/module.modulemap'

		ss.xcconfig = { 'OTHER_CFLAGS' => '$(inherited) -DYAP_STANDARD_SQLITE' }
		ss.library = 'sqlite3'
		ss.source_files = 'YapDatabase/*.{h,m,mm,c}', 'YapDatabase/{Internal,Utilities}/*.{h,m,mm,c}', 'YapDatabase/Extensions/**/*.{h,m,mm,c}'
		ss.private_header_files = 'YapDatabase/Internal/*.h', 'YapDatabase/Extensions/**/Internal/*.h'

		ss.osx.framework   = 'SystemConfiguration'
		ss.ios.framework   = 'SystemConfiguration'
		ss.tvos.framework  = 'SystemConfiguration'

	end #Standard+ModuleMap

	####################################################################################################

	# use SQLCipher and enable -DSQLITE_HAS_CODEC flag
	s.subspec 'SQLCipher-ObjC' do |ss|

		ss.subspec 'Core' do |ssc|
			ssc.xcconfig = { 'OTHER_CFLAGS' => '$(inherited) -DSQLITE_HAS_CODEC' }
			ssc.dependency 'SQLCipher', '>= 3.4.0'
			ssc.source_files = 'YapDatabase/*.{h,m,mm,c}', 'YapDatabase/{Internal,Utilities}/*.{h,m,mm,c}', 'YapDatabase/Extensions/Protocol/**/*.{h,m,mm,c}'
			ssc.private_header_files = 'YapDatabase/Internal/*.h'

		end

		ss.subspec 'Extensions' do |sse|
			sse.dependency 'YapDatabase/SQLCipher-ObjC/Core'
      
			sse.subspec 'View' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/View/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/View/Internal/*.h'
			end

			sse.subspec 'AutoView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/AutoView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/AutoView/Internal/*.h'
			end
      
			sse.subspec 'ManualView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/ManualView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/ManualView/Internal/*.h'
			end

			sse.subspec 'SecondaryIndex' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/SecondaryIndex/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/SecondaryIndex/Internal/*.h'
			end

			sse.subspec 'CrossProcessNotification' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/CrossProcessNotification/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/CrossProcessNotification/Internal/*.h'
			end

			sse.subspec 'Relationships' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/Relationships/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/Relationships/Internal/*.h'
			end

			sse.subspec 'FullTextSearch' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/FullTextSearch/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/FullTextSearch/Internal/*.h'
			end

			sse.subspec 'Hooks' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/Hooks/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/Hooks/Internal/*.h'
			end

			sse.subspec 'FilteredView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/FilteredView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/FilteredView/Internal/*.h'
			end
      
			sse.subspec 'SearchResultsView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/AutoView'
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/FullTextSearch'
				ssee.source_files = 'YapDatabase/Extensions/SearchResultsView/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/SearchResultsView/Internal/*.h'
			end

			sse.subspec 'CloudKit' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/CloudKit/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/CloudKit/Internal/*.h'
			end

			sse.subspec 'RTreeIndex' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/RTreeIndex/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/RTreeIndex/Internal/*.h'
			end

			sse.subspec 'ActionManager' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/AutoView'
				ssee.source_files = 'YapDatabase/Extensions/ActionManager/**/*.{h,m,mm,c}'
				ssee.private_header_files = 'YapDatabase/Extensions/ActionManager/Internal/*.h'
				ssee.osx.framework   = 'SystemConfiguration'
				ssee.ios.framework   = 'SystemConfiguration'
				ssee.tvos.framework  = 'SystemConfiguration'
			end

			sse.subspec 'CloudCore' do |ssee|
				ssee.source_files = 'YapDatabase/Extensions/CloudCore/**/*.{h,m,mm,c}'
			end

		end # Extensions

	end # SQLCipher

	####################################################################################################

	s.subspec 'SQLCipher' do |ss|

		ss.subspec 'Core' do |ssc|
			ssc.dependency 'YapDatabase/SQLCipher-ObjC/Core'
			ssc.source_files = 'YapDatabase/Swift/*.swift', 'YapDatabase/Extensions/Protocol/Swift/*.swift'
		end

		ss.subspec 'Extensions' do |sse|
			sse.dependency 'YapDatabase/SQLCipher/Core'
      
			sse.subspec 'View' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/View/Swift/*.swift'
			end
      
			sse.subspec 'AutoView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/AutoView'
				ssee.dependency 'YapDatabase/SQLCipher/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/AutoView/Swift/*.swift'
			end

			sse.subspec 'ManualView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/ManualView'
				ssee.dependency 'YapDatabase/SQLCipher/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/ManualView/Swift/*.swift'
			end

			sse.subspec 'SecondaryIndex' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/SecondaryIndex'
				ssee.source_files = 'YapDatabase/Extensions/SecondaryIndex/Swift/*.swift'
			end

			sse.subspec 'CrossProcessNotification' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/CrossProcessNotification'
				ssee.source_files = 'YapDatabase/Extensions/CrossProcessNotification/Swift/*.swift'
			end

			sse.subspec 'Relationships' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/Relationships'
				ssee.source_files = 'YapDatabase/Extensions/Relationships/Swift/*.swift'
			end

			sse.subspec 'FullTextSearch' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/FullTextSearch'
				ssee.source_files = 'YapDatabase/Extensions/FullTextSearch/Swift/*.swift'
			end

			sse.subspec 'Hooks' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/Hooks'
				ssee.source_files = 'YapDatabase/Extensions/Hooks/Swift/*.swift'
			end
      
			sse.subspec 'FilteredView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/FilteredView'
				ssee.dependency 'YapDatabase/SQLCipher/Extensions/View'
				ssee.source_files = 'YapDatabase/Extensions/FilteredView/Swift/*.swift'
			end
      
			sse.subspec 'SearchResultsView' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/SearchResultsView'
				ssee.dependency 'YapDatabase/SQLCipher/Extensions/AutoView'
				ssee.dependency 'YapDatabase/SQLCipher/Extensions/FullTextSearch'
				ssee.source_files = 'YapDatabase/Extensions/SearchResultsView/Swift/*.swift'
			end

			sse.subspec 'CloudKit' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/CloudKit'
				ssee.source_files = 'YapDatabase/Extensions/CloudKit/Swift/*.swift'
			end

			sse.subspec 'RTreeIndex' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/RTreeIndex'
				ssee.source_files = 'YapDatabase/Extensions/RTreeIndex/Swift/*.swift'
			end

			sse.subspec 'ActionManager' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/ActionManager'
				ssee.dependency 'YapDatabase/SQLCipher/Extensions/AutoView'
				ssee.source_files = 'YapDatabase/Extensions/ActionManager/Swift/*.swift'
				ssee.osx.framework   = 'SystemConfiguration'
				ssee.ios.framework   = 'SystemConfiguration'
				ssee.tvos.framework  = 'SystemConfiguration'
			end

			sse.subspec 'CloudCore' do |ssee|
				ssee.dependency 'YapDatabase/SQLCipher-ObjC/Extensions/CloudCore'
				ssee.source_files = 'YapDatabase/Extensions/CloudCore/Swift/*.swift'
			end

		end # Extensions
	end

	####################################################################################################

	# The ModuleMap option is a workaround for Issue #479:
	# https://github.com/yapstudios/YapDatabase/pull/479
	# 
	# You may need to use this if:
	# - You're using Swift, AND
	# - You also need to subclass YapDatabaseCloudCore (need access to private header files)
	#
	s.subspec 'SQLCipher+ModuleMap' do |ss|

		ss.osx.module_map = 'Framework/Mac/module.modulemap'
		ss.ios.module_map = 'Framework/iOS/module.modulemap'
		ss.tvos.module_map = 'Framework/tvOS/module.modulemap'
		ss.watchos.module_map = 'Framework/watchOS/module.modulemap'

		ss.xcconfig = { 'OTHER_CFLAGS' => '$(inherited) -DSQLITE_HAS_CODEC' }
		ss.dependency 'SQLCipher', '>= 3.4.0'
		ss.source_files = 'YapDatabase/*.{h,m,mm,c}', 'YapDatabase/{Internal,Utilities}/*.{h,m,mm,c}', 'YapDatabase/Extensions/**/*.{h,m,mm,c}'
		ss.private_header_files = 'YapDatabase/Internal/*.h', 'YapDatabase/Extensions/**/Internal/*.h'

		ss.osx.framework   = 'SystemConfiguration'
		ss.ios.framework   = 'SystemConfiguration'
		ss.tvos.framework  = 'SystemConfiguration'

	end #SQLCipher+ModuleMap

end
