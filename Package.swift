// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
  name: "YapDatabase",
  platforms: [
    .iOS(.v8),
    .macOS(.v10_10),
    .tvOS(.v9),
    .watchOS(.v2)
  ],
  products: [
    .library(name: "YapDatabase", targets: ["YapDatabase"]),
    .library(name: "YapDatabaseSwift", targets: ["YapDatabaseSwift"])
  ],
  dependencies: [
    .package(url: "git@github.com:CocoaLumberjack/CocoaLumberjack", .exact("3.6.0"))
  ],
  targets: [
    .target(
      name: "YapDatabase",
      dependencies: ["CocoaLumberjack"],
      path: "YapDatabase",
      exclude: [
        "Swift",
        "Extensions/ActionManager/Swift",
        "Extensions/AutoView/Swift",
        "Extensions/CloudCore/Swift",
        "Extensions/CloudKit/Swift",
        "Extensions/ConnectionPool/Swift",
        "Extensions/ConnectionProxy/Swift",
        "Extensions/CrossProcessNotification/Swift",
        "Extensions/FilteredView/Swift",
        "Extensions/FullTextSearch/Swift",
        "Extensions/Hooks/Swift",
        "Extensions/ManualView/Swift",
        "Extensions/Protocol/Swift",
        "Extensions/Relationships/Swift",
        "Extensions/RTreeIndex/Swift",
        "Extensions/SearchResultsView/Swift",
        "Extensions/SecondaryIndex/Swift",
        "Extensions/View/Swift"
      ],
      sources: ["Extensions", "Internal", "Utilities"],
      cxxSettings: [
        .headerSearchPath("./"),
        .headerSearchPath("./Extensions/**"),
        .headerSearchPath("./Internal/**"),
        .headerSearchPath("./Utilities/**")
      ]
    ),
    .target(
      name: "YapDatabaseSwift",
      dependencies: ["YapDatabase"],
      path: "YapDatabase",
      sources: [
        "Swift",
        "Extensions/ActionManager/Swift",
        "Extensions/AutoView/Swift",
        "Extensions/CloudCore/Swift",
        "Extensions/CloudKit/Swift",
        "Extensions/ConnectionPool/Swift",
        "Extensions/ConnectionProxy/Swift",
        "Extensions/CrossProcessNotification/Swift",
        "Extensions/FilteredView/Swift",
        "Extensions/FullTextSearch/Swift",
        "Extensions/Hooks/Swift",
        "Extensions/ManualView/Swift",
        "Extensions/Protocol/Swift",
        "Extensions/Relationships/Swift",
        "Extensions/RTreeIndex/Swift",
        "Extensions/SearchResultsView/Swift",
        "Extensions/SecondaryIndex/Swift",
        "Extensions/View/Swift"
      ]
    )
  ],
  cLanguageStandard: .gnu99,
  cxxLanguageStandard: .gnucxx11
)
