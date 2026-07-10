// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "DataStoreKit",
    platforms: [
        .iOS(.v18),
        .macOS(.v15),
        .tvOS(.v18),
        .visionOS(.v2),
        .watchOS(.v11)
    ],
    products: [
        .library(name: "DataStoreKit", targets: ["DataStoreKit"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-collections.git", "1.2.0"..<"1.3.0"),
        .package(url: "https://github.com/apple/swift-log.git", "1.6.4"..<"1.8.0"),
        .package(url: "https://github.com/swiftlang/swift-docc-plugin", "1.1.0"..<"1.2.0")
    ],
    targets: [
        .target(
            name: "DataStoreKit",
            dependencies: [
                "_DatabaseStore",
                "DataStoreCore",
                "DataStoreRuntime",
                "DataStoreSupport",
                "SQLiteHandle",
                "SQLiteStatement",
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreKit",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "_DatabaseStore",
            dependencies: [
                "DataStoreCore",
                "DataStoreRuntime",
                "DataStoreSupport",
                "SQLiteHandle",
                "SQLiteStatement",
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DatabaseStore",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "DataStoreRuntime",
            dependencies: [
                "DataStoreCore",
                "DataStoreSupport",
                "SQLiteHandle",
                "SQLiteStatement",
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreRuntime",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "DataStoreCore",
            dependencies: [
                "DataStoreSupport",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreCore",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "DataStoreSupport",
            dependencies: [
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreSupport",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "SQLiteHandle",
            dependencies: [
                "DataStoreCore",
                "DataStoreSupport",
                "SQLiteStatement",
                "SQLSupport",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/SQLiteHandle",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "SQLiteStatement",
            dependencies: [
                "DataStoreSupport",
                "SQLSupport",
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "BitCollections", package: "swift-collections"),
                .product(name: "DequeModule", package: "swift-collections"),
                .product(name: "HashTreeCollections", package: "swift-collections"),
                .product(name: "HeapModule", package: "swift-collections"),
                .product(name: "OrderedCollections", package: "swift-collections"),
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/SQLiteStatement",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "SQLSupport",
            dependencies: [
                "DataStoreSupport",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/SQLSupport",
            swiftSettings: [.unsafeFlags(["-package-name", "DataStoreKit"])]
        ),
        .target(
            name: "TestSupport",
            dependencies: ["DataStoreKit"],
            path: "Tests/TestSupport"
        ),
        .testTarget(
            name: "DataStoreKitTests",
            dependencies: ["DataStoreKit", "TestSupport"],
            path: "Tests/DataStoreKitTests"
        ),
        .testTarget(
            name: "SwiftDataTests",
            dependencies: ["DataStoreKit", "TestSupport"],
            path: "Tests/SwiftDataTests"
        )
    ]
)
