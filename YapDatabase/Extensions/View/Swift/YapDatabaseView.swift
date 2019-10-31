/// Add Swift extensions here

extension YapDatabaseViewConnection {
	
	public func getChanges(forNotifications notifications: [Notification], withMappings mappings: YapDatabaseViewMappings) -> (sectionChanges: [YapDatabaseViewSectionChange], rowChanges: [YapDatabaseViewRowChange]) {
		
		var sectionChanges = NSArray()
		var rowChanges = NSArray()
		
		self.__getSectionChanges(&sectionChanges, rowChanges: &rowChanges, for: notifications, with: mappings)
		
		return (sectionChanges as! [YapDatabaseViewSectionChange], rowChanges as! [YapDatabaseViewRowChange])
	}
}

extension YapDatabaseViewTransaction {
	
	public func getCollectionKey(atIndexPath indexPath: IndexPath, withMappings mappings: YapDatabaseViewMappings) -> (String, String)? {
		
		var key: NSString? = nil
		var collection: NSString? = nil
		
		self.__getKey(&key, collection: &collection, at: indexPath, with: mappings)
		
		if let collection = collection as String?,
		   let key = key as String? {
			
			return (collection, key)
		}
		else {
			
			return nil
		}
	}
	
	public func getCollectionKey(forRow row: Int, section: Int, withMappings mappings: YapDatabaseViewMappings) -> (String, String)? {
		
		var key: NSString? = nil
		var collection: NSString? = nil
		
		self.__getKey(&key, collection: &collection, forRow: UInt(row), inSection: UInt(section), with: mappings)
		
		if let collection = collection as String?,
		   let key = key as String? {
			
			return (collection, key)
		}
		else {
			
			return nil
		}
	}
}
