import Foundation

extension YapDatabaseQuery {
	
	convenience init(string queryString: String) {
		
		self.init(string: queryString, parameters: [])
	}
	
	convenience init(aggregateFunction: String, string queryString: String) {
		
		self.init(aggregateFunction: aggregateFunction, string: queryString, parameters: [])
	}

}
