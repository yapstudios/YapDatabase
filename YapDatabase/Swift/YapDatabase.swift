import Foundation

extension YapDatabase {
	
	public class func codableSerializer<T>(_ type: T.Type) -> (String, String, Any) -> Data where T: Codable {
		
		let serializer = {(collection: String, key: String, object: Any) -> Data in
			
			if let object = object as? T {
			
				let encoder = PropertyListEncoder()
				do {
					return try encoder.encode(object)
				} catch {
					return Data()
				}
				
			} else {
				return Data()
			}
		}
		
		return serializer
	}
	
	public class func codableDeserializer<T>(_ type: T.Type) -> (String, String, Data) -> T? where T: Codable {
		
		let deserializer = {(collection: String, key: String, data: Data) -> T? in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(T.self, from: data)
			} catch {
				return nil
			}
		}
		
		return deserializer
	}
	
	public func registerCodableSerialization<T>(_ type: T.Type, forCollection collection: String?) where T: Codable {
		
		let serializer = YapDatabase.codableSerializer(type)
		let deserializer = YapDatabase.codableDeserializer(type)
		
		self.registerSerializer(serializer, forCollection: collection)
		self.registerDeserializer(deserializer, forCollection: collection)
	}
}

extension YapDatabaseReadTransaction {
	
	public func row<O, M>(forKey key: String, inCollection collection: String?) -> (object: O, metadata: M?)? {
		
		var object: AnyObject? = nil
		var metadata: AnyObject? = nil
		let _ = self.__getObject( &object,
		                metadata: &metadata,
		                  forKey: key,
		            inCollection: collection)

		if let object = object as? O {
			return (object, metadata as? M)
		} else {
			return nil
		}
	}
	
	public func iterateCollections(_ block: (String, inout Bool) -> Void) {
		
		let enumBlock = {(collection: String, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateCollections(enumBlock)
	}
	
	public func iterateKeys(inCollection collection: String?, using block: (String, inout Bool) -> Void) {
		
		let enumBlock = {(key: String, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(key, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeys(inCollection: collection, using: enumBlock)
	}
	
	public func iterateKeysAndObjects<T>(inCollection collection: String?, using block: (String, T, inout Bool) -> Void) {
		
		self.iterateKeysAndObjects(inCollection: collection, using: block, filter: nil)
	}
	
	public func iterateKeysAndObjects<T>(inCollection collection: String?, using block: (String, T, inout Bool) -> Void, filter: ((String) -> Bool)?) {
		
		let enumBlock = {(key: String, object: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			if let object = object as? T {
				
				var innerStop = false
				block(key, object, &innerStop)
				
				if innerStop {
					outerStop.pointee = true
				}
			}
		}
		
		self.__enumerateKeysAndObjects(inCollection: collection, using: enumBlock, withFilter: filter)
	}
	
	public func iterateKeysAndObjectsInAllCollections(_ block: (String, String, Any, inout Bool) -> Void) {
		
		self.iterateKeysAndObjectsInAllCollections(block, filter: nil)
	}
	
	public func iterateKeysAndObjectsInAllCollections(_ block: (String, String, Any, inout Bool) -> Void, filter: ((String, String) -> Bool)?) {
		
		let enumBlock = {(collection: String, key: String, object: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeysAndObjectsInAllCollections(enumBlock, withFilter: filter)
	}
	
	public func iterateKeysAndMetadata<T>(inCollection collection: String?, using block: (String, T?, inout Bool) -> Void) {
		
		self.iterateKeysAndMetadata(inCollection: collection, using: block, filter: nil)
	}
	
	public func iterateKeysAndMetadata<T>(inCollection collection: String?, using block: (String, T?, inout Bool) -> Void, filter: ((String) -> Bool)?) {
		
		let enumBlock = {(key: String, metadata: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(key, metadata as? T, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeysAndMetadata(inCollection: collection, using: enumBlock, withFilter: filter)
	}
	
	public func iterateKeysAndMetadataInAllCollections(_ block: (String, String, Any?, inout Bool) -> Void) {
		
		self.iterateKeysAndMetadataInAllCollections(block, filter: nil)
	}
	
	public func iterateKeysAndMetadataInAllCollections(_ block: (String, String, Any?, inout Bool) -> Void, filter: ((String, String) -> Bool)?) {
		
		let enumBlock = {(collection: String, key: String, metadata: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, metadata, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeysAndMetadataInAllCollections(enumBlock, withFilter: filter)
	}
	
	public func iterateRows<O, M>(inCollection collection: String?, using block: (String, O, M?, inout Bool) -> Void) {
		
		self.iterateRows(inCollection: collection, using: block, filter: nil)
	}
	
	public func iterateRows<O, M>(inCollection collection: String?, using block: (String, O, M?, inout Bool) -> Void, filter: ((String) -> Bool)?) {
		
		let enumBlock = {(key: String, object: Any, metadata: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			if let object = object as? O {
				
				var innerStop = false
				block(key, object, metadata as? M, &innerStop)
				
				if innerStop {
					outerStop.pointee = true
				}
			}
		}
		
		self.__enumerateRows(inCollection: collection, using: enumBlock, withFilter: filter)
	}
	
	public func iterateRowsInAllCollections(_ block: (String, String, Any, Any?, inout Bool) -> Void) {
		
		self.iterateRowsInAllCollections(block, filter: nil)
	}
	
	public func iterateRowsInAllCollections(_ block: (String, String, Any, Any?, inout Bool) -> Void, filter: ((String, String) -> Bool)?) {
		
		let enumBlock = {(collection: String, key: String, object: Any, metadata: Any?, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(collection, key, object, metadata, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateRowsInAllCollections(enumBlock, withFilter: filter)
	}
}
