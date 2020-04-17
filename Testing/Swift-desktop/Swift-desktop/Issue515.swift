import Foundation
import YapDatabase

struct Issue515: Codable, YapDatabaseRelationshipNode {
	
	let foobar: Int
	
	func yapDatabaseRelationshipEdges() -> [YapDatabaseRelationshipEdge]? {
		
		print("swift called")
		return nil
	}
}
