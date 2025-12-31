// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "DataStoreKit",
    platforms: [.iOS(.v18), .macOS(.v15), .tvOS(.v18), .watchOS(.v11), .visionOS(.v2)],
    products: [
        .library(
            name: "DataStoreKit",
            targets: ["DataStoreKit"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-async-algorithms", .upToNextMajor(from: "1.0.0")),
        .package(url: "https://github.com/apple/swift-collections.git", "1.2.0"..<"1.3.0"),
        .package(url: "https://github.com/apple/swift-log.git", "1.0.0"..<"2.0.0")
    ],
    targets: [
        .target(
            name: "DataStoreKit",
            dependencies: [
                .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "Logging", package: "swift-log")
            ],
        ),
    ]
)
