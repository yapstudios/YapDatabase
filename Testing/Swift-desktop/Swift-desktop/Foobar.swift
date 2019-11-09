import Foundation

struct Foobar: Codable {
	
	enum CodingKeys: String, CodingKey {
		case version = "version"
		case name = "name"
		case age = "age"
	}
	
	private let version: Int = 1
	
	public var name: String
	public var age: UInt
	
	init(name: String, age: UInt) {
		self.name = name
		self.age = age
	}
	
	init(from decoder: Decoder) throws {
		
		let values = try decoder.container(keyedBy: CodingKeys.self)
		let version = try values.decodeIfPresent(Int.self, forKey: CodingKeys.version) ?? 0
		
		name = try values.decode(String.self, forKey: CodingKeys.name)
		
		// We added the 'age' property in version 1
		if version == 0 {
			age = 18 // use default age
		} else {
			age = try values.decode(UInt.self, forKey: CodingKeys.age)
		}
	}
}
