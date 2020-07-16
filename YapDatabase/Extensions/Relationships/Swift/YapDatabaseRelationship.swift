import Foundation

/// Add Swift extensions here

extension YapDatabaseRelationshipTransaction {
	
	@objc func swiftBridge_yapDatabaseRelationshipEdges(_ obj: AnyObject) -> [YapDatabaseRelationshipEdge]? {
		
		var edges: [YapDatabaseRelationshipEdge]? = nil
		if let node = obj as? YapDatabaseRelationshipNode {
			edges = node.yapDatabaseRelationshipEdges()
		}
		
		return edges
	}
}

/// The objective-c protocol is automatically imported as a class-only protocol.
/// By redefining the protocol in swift, we allow both classes & structs.
///
public protocol YapDatabaseRelationshipNode {
	
	/**
	 * Implement this method in order to return the edges that start from this node.
	 * Note that although edges are directional, the associated rules are bidirectional.
	 *
	 * In terms of edge direction, this object is the "source" of the edge.
	 * And the object at the other end of the edge is called the "destination".
	 *
	 * Every edge also has a name (which can be any string you specify), and a bidirectional rule.
	 * For example, you could specify either of the following:
	 * - delete the destination if I am deleted
	 * - delete me if the destination is deleted
	 *
	 * In fact, you could specify both of those rules simultaneously for a single edge.
	 * And there are similar rules if your graph is one-to-many for this node.
	 *
	 * Thus it is unnecessary to duplicate the edge on the destination node.
	 * So you can pick which node you'd like to create the edge(s) from.
	 * Either side is fine, just pick whichever is easier, or whichever makes more sense for your data model.
	 *
	 * YapDatabaseRelationship supports one-to-one, one-to-many, and even many-to-many relationships.
	 *
	 * @see YapDatabaseRelationshipEdge
	 */
	func yapDatabaseRelationshipEdges() -> [YapDatabaseRelationshipEdge]?
	
	/**
	 * OPTIONAL
	 *
	 * If an edge is deleted due to one of two associated nodes being deleted,
	 * and the edge has a notify rule associated with it (YDB_NotifyIfSourceDeleted or YDB_NotifyIfDestinationDeleted),
	 * then this method may be invoked on the remaining node.
	 *
	 * For example, if YDB_NotifyIfDestinationDeleted is specified, and the destination node is deleted,
	 * and the source node implements this method, then this method will be invoked on the remaining source node.
	 *
	 * This method is designed to support "weak" references.
	 * For example:
	 *
	 *   A source node might contain a property named "cachedServerResponse", which points to a cached response object
	 *   that's stored in the database. However, this cached object may be deleted at any time for various reasones
	 *   (e.g. becomes stale, access token expiration, user logout). The desire is for the
	 *   sourceNode.cachedServerResponse property to be automatically set to nil if/when the "cachedServerResponse"
	 *   object is deleted from the database. This method helps automate that.
	 *
	 *   Simply create a relationship between the source node and the "cachedServerResponse" object, and set the
	 *   YDB_NotifyIfDestinationDeleted flag on the edge. Then, when the "cachedServerResponse" object is deleted,
	 *   this method is automatically invoked on the source node. At that point, the source node simply sets its
	 *   "cachedServerResponse" property to nil, and return self.
	 *
	 * If you return an object, that object automatically replaces the previous object in the database.
	 * Specifically, the code invokes 'replaceObject:forKey:inCollection:'.
	 * I.E. the object is replaced, but any existing metadata remains as is.
	 *
	 * If you return nil, then nothing happens.
	*/
	func yapDatabaseRelationshipEdgeDeleted(_ edge: YapDatabaseRelationshipEdge, with reason: YDB_NotifyReason) -> Any?
}

public extension YapDatabaseRelationshipNode {
	
	/// Default implementation (does nothing)
	/// 
	func yapDatabaseRelationshipEdgeDeleted(_ edge: YapDatabaseRelationshipEdge, with reason: YDB_NotifyReason) -> Any? {
		
		return nil
	}
}
