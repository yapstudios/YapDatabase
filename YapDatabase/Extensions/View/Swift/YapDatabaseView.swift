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
}
