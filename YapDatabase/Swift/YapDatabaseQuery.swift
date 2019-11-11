import Foundation

extension YapDatabaseQuery {
	
	convenience init(string queryString: String) {
		
		self.init(string: queryString, parameters: [])
	}
}
