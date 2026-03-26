//
//  DataStoreOptions.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public struct DataStoreOptions: OptionSet, Sendable {
    /// Inherited from `RawRepresentable.rawValue`.
    nonisolated public let rawValue: Int
    
    /// Inherited from `OptionSet.init(rawValue:)`.
    nonisolated public init(rawValue: RawValue) {
        precondition(
            ~Self.allowedMask & rawValue == 0,
             "Invalid DataStoreOptions: \(rawValue)"
        )
        self.init(uncheckedRawValue: rawValue)
    }
}

extension DataStoreOptions {
    nonisolated private static var allowedMask: Int { (1 << 22) - 1 } // bits 0...21
    
    nonisolated private init(uncheckedRawValue: Int) {
        self.rawValue = uncheckedRawValue
    }
}

public extension DataStoreOptions {
    /// Used for testing, experimenting, or observing unknown and undocumented behaviors.
    nonisolated static var _internal: Self {
        .init(uncheckedRawValue: 1 << 0)
    }
    /// Deletes the database file on launch before `DataStore` is instantiated.
    nonisolated static var eraseDatabaseOnSetup: Self {
        .init(uncheckedRawValue: 1 << 1)
    }
    /// Sets up the database in a temporary directory.
    nonisolated static var temporary: Self {
        .init(uncheckedRawValue: 1 << 2)
    }
    /// Logs additional information.
    nonisolated static var useDetailedLogging: Self {
        .init(uncheckedRawValue: 1 << 3)
    }
    /// Increases the logging frequency.
    nonisolated static var useVerboseLogging: Self {
        .init(uncheckedRawValue: 1 << 4)
    }
    /// Changes the log level of save requests from `info` to `notice`.
    nonisolated static var logSaveRequests: Self {
        .init(uncheckedRawValue: 1 << 5)
    }
    /// Logs the translated `FetchDescriptor` used to fetch from the data store.
    nonisolated static var logTranslatedFetchDescriptors: Self {
        .init(uncheckedRawValue: 1 << 6)
    }
    /// Create snapshots from a fetch request synchronously.
    nonisolated static var synchronouslyCreateSnapshots: Self {
        .init(uncheckedRawValue: 1 << 7)
    }
    /// Only allow the `ModelManager` to maintain references to the snapshot caches.
    nonisolated static var centralizedSnapshotCaching: Self {
        .init(uncheckedRawValue: 1 << 8)
    }
    /// Snapshots will no longer be cached into `ModelManager` and/or any `SnapshotRegistry`.
    nonisolated static var disableSnapshotCaching: Self {
        .init(uncheckedRawValue: 1 << 9)
    }
    /// Previously cached snapshots will not be reused as related models and prefetched relationships.
    nonisolated static var disableImplicitPrefetchingUsingCaches: Self {
        .init(uncheckedRawValue: 1 << 10)
    }
    /// Do not include related backing data from being requested in the predicate.
    nonisolated static var disableImplicitPrefetchingFromPredicate: Self {
        .init(uncheckedRawValue: 1 << 11)
    }
    /// Always fetch foreign keys from the data store when acquiring references for to-many relationships.
    nonisolated static var disableReferenceCaching: Self {
        .init(uncheckedRawValue: 1 << 12)
    }
    /// Prevents hashing the fetch request for storing and reusing its fetch result.
    nonisolated static var disablePredicateCaching: Self {
        .init(uncheckedRawValue: 1 << 13)
    }
    /// Restricts key paths to only use from the schema.
    nonisolated static var disableKeyPathVariants: Self {
        .init(uncheckedRawValue: 1 << 14)
    }
    /// Overwrite the encoded SwiftData `Schema` data.
    nonisolated static var forceSchemaOverwrite: Self {
        .init(uncheckedRawValue: 1 << 15)
    }
    /// Prevents any schema migrations checking.
    nonisolated static var disableSchemaMigrations: Self {
        .init(uncheckedRawValue: 1 << 16)
    }
    /// Prevents automatic execution of any lightweight schema migrations.
    nonisolated static var disableLightweightSchemaMigrations: Self {
        .init(uncheckedRawValue: 1 << 17)
    }
    /// Skips recording and managing transactions in persistent history tracking.
    nonisolated static var disablePersistentHistoryTracking: Self {
        .init(uncheckedRawValue: 1 << 18)
    }
    /// Maintain inline transaction history and do not copy it externally.
    nonisolated static var neverArchiveTransactionHistory: Self {
        .init(uncheckedRawValue: 1 << 19)
    }
    /// Checks if `PRAGMA foreign_keys` is enabled and runs `PRAGMA foreign_key_check` for each table on setup.
    nonisolated static var verifyForeignKeysOnSetup: Self {
        .init(uncheckedRawValue: 1 << 20)
    }
    /// Prevents throwing any errors when the configuration is being validated.
    nonisolated static var ignoreStoreValidationErrors: Self {
        .init(uncheckedRawValue: 1 << 21)
    }
}
