import Foundation

extension YapDatabaseReadTransaction {
	
	public func object<T>(key: String, collection: String?) -> T? where T: NSCoding {
		
		return self.__object(forKey: key, inCollection: collection) as? T
	}
	
	public func object<T>(key: String, collection: String?) -> T? where T: Codable {
		
		let deserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(T.self, from: data)
			} catch {
				return nil
			}
		}
		
		return self.__object(forKey: key, inCollection: collection, withDeserializer: deserializer) as? T
	}
	
	public func metadata<T>(key: String, collection: String?) -> T? where T: NSCoding {
		
		return self.__metadata(forKey: key, inCollection: collection) as? T
	}
	
	public func metadata<T>(key: String, collection: String?) -> T? where T: Codable {
	
		let deserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(T.self, from: data)
			} catch {
				return nil
			}
		}
		
		return self.__metadata(forKey: key, inCollection: collection, withDeserializer: deserializer) as? T
	}
	
	public func row<O, M>(key: String, collection: String?) -> (object: O, metadata: M?)? where O: NSCoding, M: NSCoding {
		
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
	
	public func row<O, M>(key: String, collection: String?) -> (object: O, metadata: M?)? where O: Codable, M: Codable {
		
		let objectDeserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(O.self, from: data)
			} catch {
				return nil
			}
		}
		let metadataDeserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(M.self, from: data)
			} catch {
				return nil
			}
		}
		
		var object: AnyObject? = nil
		var metadata: AnyObject? = nil
		let _ = self.__getObject( &object,
		                metadata: &metadata,
		                  forKey: key,
		            inCollection: collection,
		  withObjectDeserializer: objectDeserializer,
		    metadataDeserializer: metadataDeserializer)

		if let object = object {
			return (object as! O, metadata as! M?)
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
	
	public func iterateKeys(collection: String?, using block: (String, inout Bool) -> Void) {
		
		let enumBlock = {(key: String, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(key, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeys(inCollection: collection, using: enumBlock)
	}
	
	public func iterateKeysAndObjects<T>(collection: String?, using block: (String, T, inout Bool) -> Void) where T: NSCoding {
		
		let enumBlock = {(key: String, object: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			if let object = object as? T {
				
				var innerStop = false
				block(key, object, &innerStop)
				
				if innerStop {
					outerStop.pointee = true
				}
			}
		}
		
		self.__enumerateKeysAndObjects(inCollection: collection,
		                                      using: enumBlock)
	}
	
	public func iterateKeysAndObjects<T>(collection: String?, using block: (String, T, inout Bool) -> Void) where T: Codable {
		
		let deserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(T.self, from: data)
			} catch {
				return nil
			}
		}
		let enumBlock = {(key: String, object: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
	
			var innerStop = false
			block(key, object as! T, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeysAndObjects(inCollection: collection,
		                                      using: enumBlock,
		                                 withFilter: nil,
		                               deserializer: deserializer)
	}
	
	public func iterateKeysAndMetadata<T>(collection: String?, using block: (String, T?, inout Bool) -> Void) where T: NSCoding {
		
		let enumBlock = {(key: String, metadata: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			var innerStop = false
			block(key, metadata as? T, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeysAndMetadata(inCollection: collection,
		                                       using: enumBlock)
	}
	
	public func iterateKeysAndMetadata<T>(collection: String?, using block: (String, T?, inout Bool) -> Void) where T: Codable {
		
		let deserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(T.self, from: data)
			} catch {
				return nil
			}
		}
		let enumBlock = {(key: String, metadata: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
		
			var innerStop = false
			block(key, metadata as? T, &innerStop)
			
			if innerStop {
				outerStop.pointee = true
			}
		}
		
		self.__enumerateKeysAndMetadata(inCollection: collection,
		                                       using: enumBlock,
		                                  withFilter: nil,
		                                deserializer: deserializer)
	}
	
	public func iterateRows<O, M>(collection: String?, using block: (String, O, M?, inout Bool) -> Void) where O: NSCoding, M: NSCoding {
		
		let enumBlock = {(key: String, object: Any, metadata: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
			
			if let object = object as? O {
				
				var innerStop = false
				block(key, object, metadata as? M, &innerStop)
				
				if innerStop {
					outerStop.pointee = true
				}
			}
		}
		
		self.__enumerateRows(inCollection: collection,
		                            using: enumBlock)
	}
	
	public func iterateRows<O, M>(collection: String?, using block: (String, O, M?, inout Bool) -> Void) where O: Codable, M: Codable {
		
		let objectDeserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(O.self, from: data)
			} catch {
				return nil
			}
		}
		let metadataDeserializer: YapDatabaseDeserializer = {(_, _, data) in
			
			let decoder = PropertyListDecoder()
			do {
				return try decoder.decode(M.self, from: data)
			} catch {
				return nil
			}
		}
		
		let enumBlock = {(key: String, object: Any, metadata: Any, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in
		
			if let object = object as? O {
				
				var innerStop = false
				block(key, object, metadata as? M, &innerStop)
				
				if innerStop {
					outerStop.pointee = true
				}
			}
		}
		
		self.__enumerateRows(inCollection: collection,
		                            using: enumBlock,
		                       withFilter: nil,
		               objectDeserializer: objectDeserializer,
		             metadataDeserializer: metadataDeserializer)
	}
}

extension YapDatabaseReadWriteTransaction {
	
	public func setObject<T>(_ object: T, key: String, collection: String?) where T: NSCoding {

		self.__setObject(object, forKey: key, inCollection: collection)
	}
	
	public func setObject<T>(_ object: T, key: String, collection: String?) where T: Codable {
		
		let encoder = PropertyListEncoder()
		do {
			let data = try encoder.encode(object)
			
			self.setObject(object, forKey: key,
			                   inCollection: collection,
			                   withMetadata: nil,
			               serializedObject: data,
			             serializedMetadata: nil)
			
		} catch {}
	}
	
	public func setObject<O, M>(_ object: O, key: String, collection: String?, metadata: M?) where O: Codable, M: Codable {
		
		let encoder = PropertyListEncoder()
		do {
			let oData = try encoder.encode(object)
			var mData: Data? = nil
			if let metadata = metadata {
				mData = try encoder.encode(metadata)
			}
			
			self.setObject(object, forKey: key,
			                 inCollection: collection,
			                 withMetadata: metadata,
			             serializedObject: oData,
			           serializedMetadata: mData)
			
		} catch {}
	}
	
	public func replaceObject<T>(_ object: T, key: String, collection: String?) where T: NSCoding {
		
		self.__replace(object, forKey: key, inCollection: collection)
	}
	
	public func replaceObject<T>(_ object: T, key: String, collection: String?) where T: Codable {
		
		let encoder = PropertyListEncoder()
		do {
			let data = try encoder.encode(object)
			
			self.replace(object, forKey: key, inCollection: collection, withSerializedObject: data)
			
		} catch {}
	}
	
	public func replaceMetadata<T>(_ metadata: T, key: String, collection: String?) where T: NSCoding {
		
		self.__replaceMetadata(metadata, forKey: key, inCollection: collection)
	}
	
	public func replaceMetadata<T>(_ metadata: T, key: String, collection: String?) where T: Codable {
		
		let encoder = PropertyListEncoder()
		do {
			let data = try encoder.encode(metadata)
			
			self.replaceMetadata(metadata, forKey: key, inCollection: collection, withSerializedMetadata: data)
			
		} catch {}
	}
}
