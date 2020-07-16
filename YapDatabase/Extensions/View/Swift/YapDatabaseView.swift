import Foundation

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
	
	public func getCollectionKey(atIndex index: Int, inGroup group: String) -> (String, String)? {
		
		var key: NSString? = nil
		var collection: NSString? = nil
		
		self.__getKey(&key, collection: &collection, at: UInt(index), inGroup: group)
		
		if let collection = collection as String?,
		   let key = key as String? {
			
			return (collection, key)
		}
		else {
			
			return nil
		}
	}
	
	public func getGroupIndex(forKey key: String, inCollection collection: String?) -> (String, Int)? {
		
		var group: NSString? = nil
		var index: UInt = 0
		
		self.__getGroup(&group, index: &index, forKey: key, inCollection: collection)
		
		if let group = group as String? {
			return (group, Int(index))
		} else {
			return nil
		}
	}
	
	// MARK: Iteration
	
	public func iterateKeys(inGroup group: String, using block: (String, String, Int, inout Bool) -> Void) {
	
		self.iterateKeys(inGroup: group, reversed: false, using: block)
	}
	
	public func iterateKeys(inGroup group: String, reversed: Bool, using block: (String, String, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateKeys(inGroup: group, with: options, using: enumBlock)
	}
	
	public func iterateKeys(inGroup group: String, reversed: Bool, range: NSRange, using block: (String, String, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateKeys(inGroup: group, with: options, range: range, using: enumBlock)
	}
	
	public func iterateKeysAndMetadata(inGroup group: String, using block: (String, String, Any?, Int, inout Bool) -> Void) {
		
		self.iterateKeysAndMetadata(inGroup: group, reversed: false, using: block)
	}
	
	public func iterateKeysAndMetadata(inGroup group: String, reversed: Bool, using block: (String, String, Any?, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, metadata: Any?, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, metadata, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateKeysAndMetadata(inGroup: group, with: options, using: enumBlock)
	}
	
	public func iterateKeysAndMetadata(inGroup group: String, reversed: Bool, range: NSRange, using block: (String, String, Any?, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, metadata: Any?, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, metadata, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateKeysAndMetadata(inGroup: group, with: options, range: range, using: enumBlock)
	}
	
	public func iterateKeysAndObjects(inGroup group: String, using block: (String, String, Any, Int, inout Bool) -> Void) {
		
		self.iterateKeysAndObjects(inGroup: group, reversed: false, using: block)
	}
	
	public func iterateKeysAndObjects(inGroup group: String, reversed: Bool, using block: (String, String, Any, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, object: Any, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateKeysAndObjects(inGroup: group, with: options, using: enumBlock)
	}
	
	public func iterateKeysAndObjects(inGroup group: String, reversed: Bool, range: NSRange, using block: (String, String, Any?, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, object: Any, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateKeysAndObjects(inGroup: group, with: options, range: range, using: enumBlock)
	}
	
	public func iterateRows(inGroup group: String, using block: (String, String, Any, Any?, Int, inout Bool) -> Void) {
		
		self.iterateRows(inGroup: group, reversed: false, using: block)
	}
	
	public func iterateRows(inGroup group: String, reversed: Bool, using block: (String, String, Any, Any?, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, object: Any, metadata: Any?, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, metadata, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateRows(inGroup: group, with: options, using: enumBlock)
	}
	
	public func iterateRows(inGroup group: String, reversed: Bool, range: NSRange, using block: (String, String, Any, Any?, Int, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, key: String, object: Any, metadata: Any?, index: UInt, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, metadata, Int(index), &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		let options: NSEnumerationOptions = reversed ? .reverse : .init()
		self.__enumerateRows(inGroup: group, with: options, range: range, using: enumBlock)
	}
	
	// MARK: Mappings
	
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
