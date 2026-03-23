//
//  CloudKitDatabase.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreRuntime

#if canImport(CloudKit)

import CloudKit

extension DatabaseConfiguration {
    public struct CloudKitDatabase: DataStoreSynchronizerConfiguration {
        public typealias Synchronizer = Replicator
        public typealias Store = DatabaseStore
        nonisolated public let id: String = "cloudkit"
        nonisolated internal let containerIdentifier: String?
        nonisolated public let remoteAuthor: String
        nonisolated internal let zoneName: String
        nonisolated internal let databaseScope: CKDatabase.Scope
        nonisolated internal let delegate: any CloudKitDatabase.Replicator.Delegate
        
        nonisolated public init(
            containerIdentifier: String?,
            remoteAuthor: String = "CloudKit",
            zoneName: String? = nil,
            databaseScope: CKDatabase.Scope = .private,
            delegate: (any Replicator.Delegate)? = nil
        ) {
            self.containerIdentifier = containerIdentifier
            self.remoteAuthor = remoteAuthor
            self.zoneName = zoneName
            ?? "\((Bundle.main.bundleIdentifier ?? "Application")).DataStoreKit"
            self.databaseScope = databaseScope
            self.delegate = delegate ?? Replicator.DefaultSyncDelegate(
                excludedEntities: [
                    StateTable.tableName,
                    RecordMetadataTable.tableName,
                    HistoryTable.tableName,
                    ArchiveTable.tableName,
                    InternalTable.tableName
                ]
            )
        }
        
        nonisolated public static func `private`(_ privateDatabaseName: String) -> Self {
            self.init(
                containerIdentifier: nil,
                remoteAuthor: "CloudKit",
                zoneName: privateDatabaseName,
                databaseScope: .private,
                delegate: nil
            )
        }
        
        public func makeSynchronizer(store: Store) -> any DataStoreSynchronizer {
            Synchronizer(store: store, configuration: self)
        }
    }
}

#endif
