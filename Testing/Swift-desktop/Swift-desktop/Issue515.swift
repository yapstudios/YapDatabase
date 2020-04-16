import Foundation
import YapDatabase

struct Issue515: YapDatabaseRelationshipNode {
	
	let foobar: Int
	
	func yapDatabaseRelationshipEdges() -> [YapDatabaseRelationshipEdge]? {
		return nil
	}
}
