//
//  CloudKitDatabase.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreRuntime
public import DataStoreCore

#if canImport(CloudKit)

internal import CloudKit

extension DatabaseConfiguration {
    nonisolated public struct CloudKitDatabase: DataStoreSynchronizerConfiguration {
        public typealias Synchronizer = Replicator
        public typealias Store = DatabaseStore
        internal let containerIdentifier: String?
        internal let zoneName: String
        internal let databaseScope: CKDatabase.Scope
        internal let delegate: any CloudKitDatabase.Replicator.Delegate
        public let id: String = "cloudkit"
        public let remoteAuthor: String
        
        internal init(
            containerIdentifier: String?,
            remoteAuthor: String = "CloudKit",
            zoneName: String? = nil,
            databaseScope: CKDatabase.Scope = .private,
            delegate: (any Replicator.Delegate)? = nil
        ) {
            self.containerIdentifier = containerIdentifier
            self.remoteAuthor = remoteAuthor
            self.zoneName = zoneName ?? "DataStoreKit"
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
        
        public static func `private`(_ privateDatabaseName: String) -> Self {
            self.init(
                containerIdentifier: privateDatabaseName,
                remoteAuthor: "CloudKit",
                zoneName: "DataStoreKit",
                databaseScope: .private,
                delegate: nil
            )
        }
        
        public func makeSynchronizer(store: Store) -> any DataStoreSynchronizer<Store> {
            Synchronizer(store: store, configuration: self)
        }
    }
}

#endif
