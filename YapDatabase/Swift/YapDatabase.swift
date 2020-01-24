import Foundation

extension YapDatabase {
	
	/// Creates and returns a `YapDatabaseSerializer` that works with Codable types,
	/// and can be registered with the database.
	/// 
	/// Example:
	/// ```
	/// let serializer = YapDatabase.codableSerializer(MyCodableClass.self)
	/// ```
	/// 
	/// However, it's more common to use `YapDatabase.registerCodableSerialization()`,
	/// which automatically creates  serializer/deserializer pair, and registers them both:
	/// ```
	/// yapdb.registerCodableSerialization(MyCodableClass.self, forCollection: "foo")
	/// ```
	/// 
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
	
	/// Creates and returns a `YapDatabaseDeserializer` that works with Codable types,
	/// and can be registered with the database.
	/// 
	/// Example:
	/// ```
	/// let serializer = YapDatabase.codableSerializer(MyCodableClass.self)
	/// ```
	/// 
	/// However, it's more common to use `YapDatabase.registerCodableSerialization()`,
	/// which automatically creates  serializer/deserializer pair, and registers them both:
	/// ```
	/// yapdb.registerCodableSerialization(MyCodableClass.self, forCollection: "foo")
	/// ```
	/// 
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
	
	/// Registers a serializer & deserializer pair designed for the given Codable type.
	/// 
	/// Do this for each {CodableType, collection} tuple you intend to use:
	/// ```
	/// yapdb.registerCodableSerialization(MyCodableClass.self, forCollection: "foo")
	/// ```
	///
	public func registerCodableSerialization<T>(_ type: T.Type, forCollection collection: String?) where T: Codable {
		
		let serializer = YapDatabase.codableSerializer(type)
		let deserializer = YapDatabase.codableDeserializer(type)
		
		self.registerSerializer(serializer, forCollection: collection)
		self.registerDeserializer(deserializer, forCollection: collection)
	}

    /// Registers a serializer & deserializer pair designed for the given Codable type.
    ///
    /// Do this for each {CodableType, collection} tuple you intend to use:
    /// ```
    /// yapdb.registerCodableSerialization(MyCodableClass.self, metadata: MyCodableMetadataClass.self, forCollection: "foo")
    /// ```
    ///
    public func registerCodableSerialization<O, M>(_ objectType: O.Type, metadata metadataType: M.Type, forCollection collection: String?) where O: Codable, M: Codable {

        self.registerCodableSerialization(objectType, forCollection: collection)

        let metadataSerializer = YapDatabase.codableSerializer(metadataType)
        let metadataDeserializer = YapDatabase.codableDeserializer(metadataType)

        self.registerMetadataSerializer(metadataSerializer, forCollection: collection)
        self.registerMetadataDeserializer(metadataDeserializer, forCollection: collection)
    }
}

extension YapDatabaseReadTransaction {
	
	/// Returns the {object, metadata} tuple for the given row.
	/// If you need both, this is faster than fetching them separately.
	/// 
	/// ```
	/// // If you only need one or the other:
	/// let obj = transaction.object(forKey: "foo", inCollection: "bar")
	/// let metadata = transaction.metadata(forKey: "foo", inCollection: "bar")
	///
	/// // If you need both
	/// let {obj, metadata} = transaction.row(forKey: "foo", inCollection: "bar")
	/// ```
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
	
	/// Iterates over all the collections in the database.
	/// 
	/// That is, every collection for which there are actaully rows stored in the database with that collection.
	/// 
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
	
	/// Iterates over every key in the given collection.
	///
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
	
	/// Iterates over every {key, object} tuple within the given collection.
	/// 
	/// The iterator uses generics, so you can strongly type your objects:
	/// ```
	/// transaction.iterateKeysAndObjects(inCollection: "lists") { (key, list: List, stop) in
	///                        // Add type to your parameter, like this: ^^^^^^^^^^     
	/// }
	/// ```
	///
	public func iterateKeysAndObjects<T>(inCollection collection: String?, using block: (String, T, inout Bool) -> Void) {
		
		self.iterateKeysAndObjects(inCollection: collection, using: block, filter: nil)
	}
	
	/// Iterates over every {key, object} tuple within the given collection.
	/// 
	/// An optional filter block allows you to skip objects you don't need.
	/// There's a performance benefit to this, as it means the database doesn't have to deserialize the the object.
	/// (i.e. can skip performing conversion: data=> object)
	/// 
	/// The iterator uses generics, so you can strongly type your objects:
	/// ```
	/// transaction.iterateKeysAndObjects(inCollection: "lists") { (key, list: List, stop) in
	///                        // Add type to your parameter, like this: ^^^^^^^^^^     
	/// }
	/// ```
	///
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
	
	/// Iterates over every {collection, key, object} in the database.
	/// 
	public func iterateKeysAndObjectsInAllCollections(_ block: (String, String, Any, inout Bool) -> Void) {
		
		self.iterateKeysAndObjectsInAllCollections(block, filter: nil)
	}
	
	/// Iterates over every {collection, key, object} in the database.
	///
	/// An optional filter block allows you to skip objects you don't need.
	/// There's a performance benefit to this, as it means the database doesn't have to deserialize the the object.
	/// (i.e. can skip performing conversion: data=> object)
	///
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
	
	/// Iterates over every {key, metadata} in the given collection.
	/// 
	/// The iterator uses generics, so you can strongly type your objects:
	/// ```
	/// transaction.iterateKeysAndMetadata(inCollection: "cache") { (key, ts: Date, stop) in
	///                        // Add type to your parameter, like this: ^^^^^^^^^^     
	/// }
	/// ```
	/// 
	public func iterateKeysAndMetadata<T>(inCollection collection: String?, using block: (String, T?, inout Bool) -> Void) {
		
		self.iterateKeysAndMetadata(inCollection: collection, using: block, filter: nil)
	}
	
	/// Iterates over every {key, metadata} in the given collection.
	/// 
	/// An optional filter block allows you to skip objects you don't need.
	/// There's a performance benefit to this, as it means the database doesn't have to deserialize the the object.
	/// (i.e. can skip performing conversion: data=> object)
	/// 
	/// The iterator uses generics, so you can strongly type your objects:
	/// ```
	/// transaction.iterateKeysAndMetadata(inCollection: "cache") { (key, ts: Date, stop) in
	///                        // Add type to your parameter, like this: ^^^^^^^^^^     
	/// }
	/// ```
	///
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
	
	/// Iterates over every {collection, key, metadata} in the database.
	/// 
	public func iterateKeysAndMetadataInAllCollections(_ block: (String, String, Any?, inout Bool) -> Void) {
		
		self.iterateKeysAndMetadataInAllCollections(block, filter: nil)
	}
	
	/// Iterates over every {collection, key, metadata} in the database.
	/// 
	/// An optional filter block allows you to skip objects you don't need.
	/// There's a performance benefit to this, as it means the database doesn't have to deserialize the the object.
	/// (i.e. can skip performing conversion: data=> object)
	/// 
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
	
	/// Iterates over every {key, object, metadata} in the given collection.
	/// 
	/// The iterator uses generics, so you can strongly type your objects:
	/// ```
	/// transaction.iterateRows(inCollection: "foo") { (key, obj: ObjType, metadata: MetaType, stop) in
	///            // Add type to your parameter, like this: ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^     
	/// }
	/// ```
	/// 
	public func iterateRows<O, M>(inCollection collection: String?, using block: (String, O, M?, inout Bool) -> Void) {
		
		self.iterateRows(inCollection: collection, using: block, filter: nil)
	}
	
	/// Iterates over every {key, object, metadata} in the given collection.
	/// 
	/// An optional filter block allows you to skip rows you don't need.
	/// There's a performance benefit to this, as it means the database doesn't have to deserialize the the object.
	/// (i.e. can skip performing conversion: data=> object)
	///  
	/// The iterator uses generics, so you can strongly type your objects:
	/// ```
	/// transaction.iterateRows(inCollection: "foo") { (key, obj: ObjType, metadata: MetaType, stop) in
	///            // Add type to your parameter, like this: ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^     
	/// }
	/// ```
	///
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
	
	/// Iterates over every {collection, key, object, metadata} in the database.
	///
	public func iterateRowsInAllCollections(_ block: (String, String, Any, Any?, inout Bool) -> Void) {
		
		self.iterateRowsInAllCollections(block, filter: nil)
	}
	
	/// Iterates over every {collection, key, object, metadata} in the database.
	///
	/// An optional filter block allows you to skip rows you don't need.
	/// There's a performance benefit to this, as it means the database doesn't have to deserialize the the object.
	/// (i.e. can skip performing conversion: data=> object)
	///
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
