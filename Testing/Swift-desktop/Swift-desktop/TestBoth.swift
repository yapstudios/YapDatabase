//
//  TestBoth.swift
//  WhatsZapp
//
//  Created by Robbie Hanson on 10/21/19.
//  Copyright © 2019 4th-A Technologies. All rights reserved.
//

import Foundation

class TestBoth: NSCopying, NSCoding, Codable {
	
	enum CodingKeys: String, CodingKey {
		case uuid = "uuid"
	}
	
	let uuid: String
	
	init() {
		uuid = UUID().uuidString
	}
	
	init(uuid: String) {
		self.uuid = uuid
	}
	
	required init?(coder: NSCoder) {
		self.uuid = coder.decodeObject(forKey: "uuid") as! String
	}
	
	func encode(with coder: NSCoder) {
	
		coder.encode(uuid, forKey: "uuid")
	}
	
	func copy(with zone: NSZone? = nil) -> Any {

		let copy = TestCoding(uuid: self.uuid)
		return copy
	}
}
