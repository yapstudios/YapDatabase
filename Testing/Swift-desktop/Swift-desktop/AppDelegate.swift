import Cocoa
import YapDatabase


@NSApplicationMain
class AppDelegate: NSObject, NSApplicationDelegate {

	func applicationDidFinishLaunching(_ aNotification: Notification) {
	//	testDatabase()
	//	testUpgrade()
		testIssue515()
	}
	
	private func testDatabase() {
		
		let baseDirs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
		let baseDir = baseDirs[0]

		let databaseURL = baseDir.appendingPathComponent("database.sqlite")

		let database = YapDatabase(url: databaseURL)
		database?.registerCodableSerialization(List.self, forCollection: kCollection_List)

		let databaseConnection = database?.newConnection()
		let uuid = "fobar"

		databaseConnection?.asyncReadWrite({ (transaction) in

			let list = List(uuid: uuid, title: "Groceries")
			transaction.setObject(list, forKey: list.uuid, inCollection: kCollection_List)
		})

		databaseConnection?.asyncRead({ (transaction) in

			if let list: List = transaction.object(forKey: uuid, inCollection: kCollection_List) as? List {
				print("Read list: \(list.title)")
			} else {
				print("wtf")
			}

			transaction.iterateCollections { (collection, stop) in

				print("row: collection: \(collection)")
			}

			transaction.iterateKeys(inCollection: kCollection_List) { (key, stop) in

				print("row: key: \(key)")
			}

			transaction.iterateKeysAndObjects(inCollection: kCollection_List) { (key, list: List, stop) in

				print("Iterate list: \(list.title)")
			}
		})
	}
	
	private func testUpgrade() {
		
		let baseDirs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
		let baseDir = baseDirs[0]

		let databaseURL = baseDir.appendingPathComponent("database.sqlite")

		let database = YapDatabase(url: databaseURL)
		database?.registerCodableSerialization(Foobar.self, forCollection: "upgrade")

		let databaseConnection = database?.newConnection()
		
//		databaseConnection?.asyncReadWrite { (transaction) in
//
//			let foobar = Foobar(name: "Fancy Pants")
//			transaction.setObject(foobar, forKey: "1", inCollection: "upgrade")
//		}
		
		databaseConnection?.asyncRead {(transaction) in
			
			if let foobar = transaction.object(forKey: "1", inCollection: "upgrade") as? Foobar {
				print("read foobar: name(\(foobar.name)) age(\(foobar.age))")
			}
			else {
				print("no foobar for you")
			}
		}
	}
	
	private func testIssue515() {
		
		let baseDirs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
		let baseDir = baseDirs[0]

		let databaseURL = baseDir.appendingPathComponent("database.sqlite")
		
		let database = YapDatabase(url: databaseURL)
		
		let collection = "issue515"
		database?.registerCodableSerialization(Issue515.self, forCollection: collection)
		
		let ext = YapDatabaseRelationship()
		database?.register(ext, withName: "relationships")
		
		let databaseConnection = database?.newConnection()
		
		databaseConnection?.asyncReadWrite {(transaction) in
			
			let test = Issue515(foobar: 42)
			
			transaction.setObject(test, forKey: "key", inCollection: collection)
		}
	}
}
