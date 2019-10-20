import Cocoa
import YapDatabase


@NSApplicationMain
class AppDelegate: NSObject, NSApplicationDelegate {

	func applicationDidFinishLaunching(_ aNotification: Notification) {
		testDatabase()
	}
	
	private func testDatabase() {
		
		let baseDir =
			NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first ??
			NSTemporaryDirectory()
		
		let databasePath = baseDir.appending("database.sqlite")
		
		let database = YapDatabase(path: databasePath)
		let databaseConnection = database?.newConnection()
		
		let uuid = "fobar"
		
		databaseConnection?.asyncReadWrite({ (transaction) in
			
			let list = List(uuid: uuid, title: "Groceries")
			
			transaction.setObject(list, key: list.uuid, collection: kCollection_List)
		})
		
		databaseConnection?.asyncRead({ (transaction) in
			
			if let list: List = transaction.object(key: uuid, collection: kCollection_List) {
				print("Read list: \(list.title)")
			} else {
				print("wtf")
			}
			
			transaction.iterateCollections { (collection, stop) in
				
				print("row: collection: \(collection)")
			}
			
			transaction.iterateKeys(collection: kCollection_List) { (key, stop) in
				
				print("row: key: \(key)")
			}
			
			transaction.iterateKeysAndObjects(collection: kCollection_List) { (key, list: List, stop) in
		
				print("Iterate list: \(list.title)")
			}
		})
		
	}
}
