import Foundation


/// All `List` objects get stored in the database using this collection.
///
let kCollection_List = "List"


class List: NSCopying, Codable {

	enum CodingKeys: String, CodingKey {
		case uuid = "uuid"
		case title = "title"
	}
	
	let uuid: String
	var title: String
	
	init(uuid: String,
	     title: String)
	{
		self.uuid = uuid
		self.title = title
	}
	
	convenience init(title: String) {
		let _uuid = UUID().uuidString
		self.init(uuid: _uuid, title: title)
	}
	
	convenience init(copy source: List, uuid: String) {
		
		self.init(uuid: uuid, title: source.title)
	}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MARK: NSCopying
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 
	func copy(with zone: NSZone? = nil) -> Any {

		let copy = List(uuid  : uuid,
		                title : title)
		return copy
	}
}

