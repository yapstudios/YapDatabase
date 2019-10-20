import Foundation

/// All `Task` objects get stored in the database using this collection.
///
let kCollection_Task = "Task"


@objc enum TaskPriority: Int, Codable {
	case low    = 0
	case normal = 1
	case high   = 2
}

class Task: Codable {

	enum CodingKeys: String, CodingKey {
		case uuid = "uuid"
		case listID = "listID"
		case title = "title"
		case priority = "priority"
		case completed = "completed"
		case creationDate = "creationDate"
		case localLastModified = "localLastModified"
		case cloudLastModified = "cloudLastModified"
	}
	
	let uuid: String
	var listID: String
	var title: String
	var priority: TaskPriority
	var completed: Bool
	var creationDate: Date
	var localLastModified: Date
	var cloudLastModified: Date?

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MARK: Init
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	required init() {
		fatalError("init() not supported. Use init(listID:title:)")
	}
	
	init(uuid: String,
	     listID: String,
	     title: String,
	     priority: TaskPriority,
		  completed: Bool,
	     creationDate: Date,
	     localLastModified: Date,
	     cloudLastModified: Date?)
	{
		self.uuid = uuid;
		self.listID = listID
		self.title = title
		self.priority = priority
		self.completed = completed
		self.creationDate = creationDate
		self.localLastModified = localLastModified
		self.cloudLastModified = cloudLastModified
	}
	
	init(listID: String, title: String) {
		self.uuid = UUID().uuidString
		self.listID = listID
		self.title = title
		self.priority = .normal
		self.completed = false
		
		let now = Date()
		self.creationDate = now
		self.localLastModified = now
	}
	
	init(copy source: Task, uuid: String) {
		
		self.uuid              = uuid
		self.listID            = source.listID
		self.title             = source.title
		self.priority          = source.priority
		self.completed         = source.completed
		self.creationDate      = source.creationDate
		self.localLastModified = source.localLastModified
		self.cloudLastModified = source.cloudLastModified
	}
	
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MARK: Convenience Functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	func lastModified()-> Date {
		
		if let cloudLastModified = cloudLastModified {
			
			if cloudLastModified.compare(localLastModified) == .orderedAscending {
				return localLastModified
			}
			
			return cloudLastModified
		}
		
		return localLastModified
	}
}
