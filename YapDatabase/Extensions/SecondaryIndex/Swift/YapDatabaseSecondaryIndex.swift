import Foundation

/// Add Swift extensions here

extension YapDatabaseSecondaryIndexTransaction {
	
	/// Iterates matches from the secondary index using the given query.
	///
	/// The query that you input is an SQL style query (appropriate for SQLite semantics),
	/// excluding the "SELECT ... FROM 'tableName'" component.
	///
	/// For example:
	///
	/// let query = YapDatabaseQuery(string: "WHERE age >= 62")
	/// let idxTransaction = transaction.ext("idx") as! YapDatabaseSecondaryIndexTransaction
	/// idxTransaction.iterateKeys(matching: query) {(collection, key, stop) in
	///   // ...
	/// }
	///
	/// You can also pass parameters to the query using the standard SQLite placeholder:
	///
	/// let query = YapDatabaseQuery(string: "WHERE age >= ? AND state == ?", parameters: [age, state])
	/// let idxTransaction = transaction.ext("idx") as! YapDatabaseSecondaryIndexTransaction
	/// idxTransaction.iterateKeys(matching: query) {(collection, key, stop) in
	///   // ...
	/// }
	///
	/// For more information, and more examples, please see YapDatabaseQuery.
	///
	/// - Returns: false if there was a problem with the given query. true otherwise.
	///
	public func iterateKeys(matching query: YapDatabaseQuery, using block: (String, String, inout Bool) -> Void) -> Bool {
		
		let enumBlock = {(collection: String, key: String, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		return self.__enumerateKeys(matching: query, using: enumBlock)
	}
	
	public func iterateKeysAndMetadata(matching query: YapDatabaseQuery, using block: (String, String, Any?, inout Bool) -> Void) -> Bool {
		
		let enumBlock = {(collection: String, key: String, metadata: Any?, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, metadata, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		return self.__enumerateKeysAndMetadata(matching: query, using: enumBlock)
	}
	
	public func iterateKeysAndObjects(matching query: YapDatabaseQuery, using block: (String, String, Any?, inout Bool) -> Void) -> Bool {
		
		let enumBlock = {(collection: String, key: String, object: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		return self.__enumerateKeysAndObjects(matching: query, using: enumBlock)
	}
	
	public func iterateRows(matching query: YapDatabaseQuery, using block: (String, String, Any, Any?, inout Bool) -> Void) -> Bool {
		
		let enumBlock = {(collection: String, key: String, object: Any, metadata: Any?, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, metadata, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		return self.__enumerateRows(matching: query, using: enumBlock)
	}
	
	public func iterateIndexedValues(inColumn column: String, matching query: YapDatabaseQuery, using block: (Any, inout Bool) -> Void) -> Bool {
		
		let enumBlock = {(value: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(value, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		return self.__enumerateIndexedValues(inColumn: column, matching: query, using: enumBlock)
	}
	
	public func numberOfRows(matching query: YapDatabaseQuery) -> UInt? {
		
		var count: UInt = 0
		if self.__getNumberOfRows(&count, matching: query) {
			return count
		} else {
			return nil
		}
	}
}
