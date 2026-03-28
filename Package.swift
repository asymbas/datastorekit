// swift-tools-version: 6.0

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
            swiftSettings: swiftSettings
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
            swiftSettings: swiftSettings
        ),
        .target(
            name: "DataStoreRuntime",
            dependencies: [
                "DataStoreCore",
                "DataStoreSQL",
                "DataStoreSupport",
                "SQLiteHandle",
                "SQLiteStatement",
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreRuntime",
            swiftSettings: swiftSettings
        ),
        .target(
            name: "DataStoreCore",
            dependencies: [
                "DataStoreSupport",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreCore",
            swiftSettings: swiftSettings
        ),
        .target(
            name: "DataStoreSQL",
            dependencies: [
                "DataStoreCore",
                "DataStoreSupport",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreSQL",
            swiftSettings: swiftSettings
        ),
        .target(
            name: "DataStoreSupport",
            dependencies: [
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/DataStoreSupport",
            swiftSettings: swiftSettings
        ),
        .target(
            name: "SQLiteHandle",
            dependencies: [
                "DataStoreCore",
                "DataStoreSQL",
                "DataStoreSupport",
                "SQLiteStatement",
                "SQLSupport",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/SQLiteHandle",
            swiftSettings: swiftSettings
        ),
        .target(
            name: "SQLiteStatement",
            dependencies: [
                "DataStoreSQL",
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
            swiftSettings: swiftSettings
        ),
        .target(
            name: "SQLSupport",
            dependencies: [
                "DataStoreSupport",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/SQLSupport",
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "DataStoreKitTests",
            dependencies: ["DataStoreKit"],
            path: "Tests/DataStoreKitTests"
        )
    ]
)

#if compiler(>=6.2)
let swiftSettings: [SwiftSetting] = [.unsafeFlags(["-package-name", "DataStoreKit"])]
#else
let swiftSettings: [SwiftSetting] = []
#endif
