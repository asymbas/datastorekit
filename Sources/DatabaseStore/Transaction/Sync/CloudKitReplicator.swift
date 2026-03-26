//
//  CloudKitReplicator.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreRuntime
import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SQLiteHandle
import SQLSupport
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

#if canImport(CloudKit)
import CloudKit
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.cloudkit")

#if canImport(CloudKit)

/*
 Prerequisites for CKSyncEngine:
 - Add CloudKit capability.
 - Add remote notification capability.
 - Default container identifier resolves to: `iCloud.com.asymbas.Editor`
 */

extension DatabaseConfiguration.CloudKitDatabase {
    public final class Replicator: DataStoreSynchronizer {
        public typealias Store = DatabaseStore
        public typealias SyncConfiguration = Store.Configuration.CloudKitDatabase
        private var syncEngine: CKSyncEngine?
        nonisolated internal unowned let store: Store
        nonisolated internal let configuration: SyncConfiguration
        nonisolated internal let container: CKContainer
        nonisolated internal let database: CKDatabase
        nonisolated internal let zoneID: CKRecordZone.ID
        internal var identifiers: [RecordIdentifier: RecordMetadata] = [:]
        internal var enqueuedChangesByRecordID: [CKRecord.ID: EnqueuedRecord] = [:]
        internal var didPrepare: Bool = false
        internal var isHandlingAccountChange: Bool = false
        nonisolated public let id: String
        internal var stagingHandle: DatabaseConnection<Store>?
        
        nonisolated public var remoteAuthor: String {
            configuration.remoteAuthor
        }
        
        nonisolated internal init(store: Store, configuration: SyncConfiguration) {
            let container: CKContainer = {
                if let identifier = configuration.containerIdentifier {
                    return .init(identifier: identifier)
                }
                return .default()
            }()
            let database: CKDatabase
            switch configuration.databaseScope {
            case .private: database = container.privateCloudDatabase
            case .public: database = container.publicCloudDatabase
            case .shared: database = container.sharedCloudDatabase
            @unknown default:
                fatalError("Unsupported CKDatabase.Scope: \(configuration.databaseScope)")
            }
            self.id = configuration.id
            self.store = store
            self.configuration = configuration
            self.container = container
            self.database = database
            self.zoneID = .init(zoneName: configuration.zoneName, ownerName: CKCurrentUserDefaultName)
            logger.debug(
                "Initialized CloudKit replicator.",
                metadata: [
                    "id": "\(configuration.id)",
                    "store_identifier": "\(store.identifier)",
                    "container_identifier": "\(configuration.containerIdentifier ?? "<default>")",
                    "database_scope": "\(configuration.databaseScope)",
                    "zone_name": "\(configuration.zoneName)"
                ]
            )
        }
        
        internal struct State: Sendable {
            nonisolated internal var lastEnqueuedHistoryPrimaryKey: Int64
            nonisolated internal var stateSerialization: CKSyncEngine.State.Serialization?
            nonisolated internal var didBootstrapZone: Bool
            nonisolated internal var lastErrorCode: String?
        }
        
        internal struct RecordMetadata {
            nonisolated internal let recordType: String
            nonisolated internal let recordName: String
            nonisolated internal let identifier: RecordIdentifier
            nonisolated internal let targetPrimaryKey: String?
            
            nonisolated internal var entityName: String { identifier.tableName }
            nonisolated internal var primaryKey: String { identifier.primaryKey.description }
        }
        
        internal struct OrderedInsertPlan {
            nonisolated internal let insertLayers: [[Store.Snapshot]]
            nonisolated internal let updateSnapshots: [Store.Snapshot]
        }
        
        internal struct EnqueuedRecord {
            nonisolated internal let operation: DataStoreOperation
            nonisolated internal let identifier: RecordIdentifier
            nonisolated internal let changedPropertyNames: Set<String>?
            nonisolated internal let isReferenceRecord: Bool
            nonisolated internal var entityName: String { identifier.tableName }
            nonisolated internal var primaryKey: String { identifier.primaryKey.description }
        }
        
        internal struct PendingChange: Sendable {
            nonisolated internal let identifier: RecordIdentifier
            nonisolated internal var operation: DataStoreOperation
            nonisolated internal var historyPrimaryKey: Int64
            nonisolated internal var transactionIdentifier: Int64
            nonisolated internal var changedPropertyNames: Set<String>?
            nonisolated internal var author: String?
            nonisolated internal var entityName: String { identifier.tableName }
            nonisolated internal var primaryKey: String { identifier.primaryKey.description }
        }
        
        internal struct IntermediaryRecordDescriptor {
            nonisolated internal let ownerEntityName: String
            nonisolated internal let property: PropertyMetadata
            nonisolated internal let destinationEntityName: String
            nonisolated internal let sourceFieldName: String
            nonisolated internal let destinationFieldName: String
        }
        
        
        internal enum ProjectedRecordOwnership {
            case root(entityName: String, primaryKey: String)
            case reference(
                entityName: String,
                primaryKey: String,
                intermediaryTableName: String,
                targetPrimaryKey: String
            )
        }
        
        internal enum Error: Swift.Error, Sendable {
            case noSyncEngine
            case unsupportedDatabaseScope(CKDatabase.Scope)
        }
        
        public func prepare() async throws {
            logger.trace("Preparing CloudKit replicator.", metadata: [
                "did_prepare": "\(didPrepare)",
                "sync_engine_exists": "\(syncEngine != nil)"
            ])
            if didPrepare {
                logger.trace("Skipped preparation because the replicator is already prepared.")
                return
            }
            logger.trace("Validating CloudKit configuration.", metadata: [
                "database_scope": "\(configuration.databaseScope.rawValue)"
            ])
            guard configuration.databaseScope == .private else {
                logger.trace("Rejected unsupported CloudKit database scope.", metadata: [
                    "database_scope": "\(configuration.databaseScope.rawValue)"
                ])
                throw Self.Error.unsupportedDatabaseScope(configuration.databaseScope)
            }
            logger.trace("Validated CloudKit configuration.")
            let accountStatus = try await container.accountStatus()
            logger.trace("Loaded CloudKit account status.", metadata: [
                "account_status": "\(String(describing: accountStatus))"
            ])
            try createCloudKitTables()
            try initializeSyncEngineIfNeeded()
            let state = try loadState()
            logger.trace("Loaded sync state during preparation.", metadata: [
                "last_enqueued_history_primary_key": "\(state.lastEnqueuedHistoryPrimaryKey)",
                "did_bootstrap_zone": "\(state.didBootstrapZone)",
                "has_state_serialization": "\(state.stateSerialization != nil)",
                "last_error_code": "\(String(describing: state.lastErrorCode))"
            ])
            if state.didBootstrapZone == false {
                logger.trace("Bootstrapping CloudKit zone.", metadata: [
                    "zone_name": "\(zoneID.zoneName)",
                    "owner_name": "\(zoneID.ownerName)"
                ])
                syncEngine?.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
                try saveState(clearErrorCode: true, didBootstrapZone: true)
                logger.trace("Saved bootstrap state.")
            }
            self.didPrepare = true
            logger.trace("Completed preparation.", metadata: ["did_prepare": "\(didPrepare)"])
        }
        
        public func sync() async throws {
            logger.trace("Starting CloudKit sync.")
            try enqueuePendingChanges()
            guard let syncEngine = self.syncEngine else {
                throw Self.Error.noSyncEngine
            }
            try await syncEngine.sendChanges(.init())
            try await syncEngine.fetchChanges(.init())
            logger.trace("Completed CloudKit sync.")
        }
        
        public func fetchChanges() async throws {
            logger.trace("Fetching CloudKit changes.")
            try await syncEngine?.fetchChanges(.init())
            logger.trace("Completed fetching CloudKit changes.")
        }
        
        public func sendChanges() async throws {
            logger.trace("Sending CloudKit changes.")
            guard let syncEngine else {
                throw Self.Error.noSyncEngine
            }
            logPendingCloudKitCounts(syncEngine)
            try await syncEngine.sendChanges(.init())
            logger.trace("Completed sending CloudKit changes.")
        }
        
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal typealias StateTable = DatabaseConfiguration.CloudKitDatabase.StateTable
    internal typealias RecordMetadataTable = DatabaseConfiguration.CloudKitDatabase.RecordMetadataTable
    
    private func logPendingCloudKitCounts(_ syncEngine: CKSyncEngine) {
        let pending = syncEngine.state.pendingRecordZoneChanges
        let saveCount = pending.reduce(into: 0) { count, change in
            if case .saveRecord = change { count += 1 }
        }
        let deleteCount = pending.reduce(into: 0) { count, change in
            if case .deleteRecord = change { count += 1 }
        }
        logger.notice("CloudKit pending change summary.", metadata: [
            "pending_total": "\(pending.count)",
            "pending_save_count": "\(saveCount)",
            "pending_delete_count": "\(deleteCount)"
        ])
    }
    
    public func eraseCloudKitData(recreateEmptyZone: Bool = false) async throws {
        logger.notice("Erasing CloudKit data.", metadata: [
            "zone_name": "\(zoneID.zoneName)",
            "database_scope": "\(configuration.databaseScope)",
            "recreate_empty_zone": "\(recreateEmptyZone)"
        ])
        guard configuration.databaseScope == .private else {
            throw Self.Error.unsupportedDatabaseScope(configuration.databaseScope)
        }
        try createCloudKitTables()
        enqueuedChangesByRecordID.removeAll()
        self.syncEngine = nil
        self.didPrepare = false
        self.isHandlingAccountChange = false
        do {
            _ = try await database.modifyRecordZones(saving: [], deleting: [zoneID])
            logger.notice("Deleted CloudKit zone: \(zoneID.zoneName)")
        } catch let error as CKError where error.code == .zoneNotFound {
            logger.trace("CloudKit zone was already absent during erase: \(zoneID.zoneName)")
        }
        try resetLocalState(deleteRecordMetadata: true)
        if recreateEmptyZone {
            _ = try await database.modifyRecordZones(saving: [CKRecordZone(zoneID: zoneID)], deleting: [])
            logger.notice("Recreated empty CloudKit zone: \(zoneID.zoneName)")
        }
        logger.notice("Completed CloudKit data erase: \(zoneID.zoneName)")
    }
    
    internal func initializeSyncEngineIfNeeded() throws {
        logger.trace("Initializing sync engine if needed.", metadata: ["sync_engine": "\(syncEngine != nil)"])
        guard syncEngine == nil else { return }
        let state = try loadState()
        var configuration = CKSyncEngine.Configuration(
            database: database,
            stateSerialization: state.stateSerialization,
            delegate: self
        )
        configuration.automaticallySync = true
        self.syncEngine = CKSyncEngine(configuration)
        logger.trace("Created sync engine.", metadata: ["automatically_sync": "\(configuration.automaticallySync)"])
    }
    
    private func createCloudKitTables() throws {
        let connection = try store.queue.connection(.writer)
        for sql in store.createCloudKitTablesSQL {
            try connection.execute(sql)
            logger.trace("Created CloudKit table.", metadata: ["sql": "\(sql)"])
        }
        try ensureRequiredColumnsExist(
            table: StateTable.tableName,
            columns: StateTable.requiredColumns,
            connection: connection
        )
        try ensureRequiredColumnsExist(
            table: RecordMetadataTable.tableName,
            columns: RecordMetadataTable.requiredColumns,
            connection: connection
        )
        try ensureStateRowExists(connection: connection)
    }
    
    private func ensureRequiredColumnsExist(
        table: String,
        columns: [(String, String)],
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        logger.trace("Ensuring required table columns exist.", metadata: [
            "table_name": "\(table)",
            "requested_columns": "\(columns.map(\.0))"
        ])
        let sql = "PRAGMA table_info(\(table))"
        let existingColumns = Set(try connection.query(sql).compactMap { $0["name"] as? String })
        logger.trace("Loaded existing table columns.", metadata: [
            "table_name": "\(table)",
            "existing_columns": "\(Array(existingColumns).sorted())"
        ])
        for (column, definition) in columns where existingColumns.contains(column) == false {
            logger.trace("Adding missing table column.", metadata: [
                "table_name": "\(table)",
                "column_name": "\(column)",
                "column_definition": "\(definition)"
            ])
            try connection.execute("ALTER TABLE \(table) ADD COLUMN \(column) \(definition)")
        }
        logger.trace("Completed table column validation.", metadata: ["table_name": "\(table)"])
    }
    
    private func ensureStateRowExists(connection: borrowing DatabaseConnection<Store>) throws {
        let rowExists = (try connection.query(
            """
            SELECT 1
            FROM \(StateTable.tableName)
            WHERE \(StateTable.storeIdentifier.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier]
        ).first) != nil
        if rowExists { return }
        try connection.execute.insert(into: StateTable.tableName, values: [
            StateTable.storeIdentifier.rawValue: store.identifier,
            StateTable.didBootstrapZone.rawValue: 0,
            StateTable.lastSyncAtMicroseconds.rawValue: Int64(0),
            StateTable.lastErrorCode.rawValue: NSNull(),
            StateTable.lastEnqueuedHistoryPrimaryKey.rawValue: NSNull(),
            StateTable.stateSerialization.rawValue: NSNull()
        ])
        logger.trace("Inserted initial local sync state row.")
    }
    
    private func loadState() throws -> State {
        let connection = try store.queue.connection(.reader)
        let row = try connection.query(
            """
            SELECT
                \(StateTable.lastEnqueuedHistoryPrimaryKey.rawValue) AS history_pk,
                \(StateTable.stateSerialization.rawValue) AS state_serialization,
                \(StateTable.didBootstrapZone.rawValue) AS did_bootstrap_zone,
                \(StateTable.lastErrorCode.rawValue) AS last_error_code
            FROM \(StateTable.tableName)
            WHERE \(StateTable.storeIdentifier.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier]
        ).first
        let stateSerialization: CKSyncEngine.State.Serialization? = {
            guard let data = row?["state_serialization"] as? Data else {
                return nil
            }
            return try? JSONDecoder().decode(CKSyncEngine.State.Serialization.self, from: data)
        }()
        let state = State(
            lastEnqueuedHistoryPrimaryKey: row?["history_pk"] as? Int64 ?? 0,
            stateSerialization: stateSerialization,
            didBootstrapZone: (row?["did_bootstrap_zone"] as? Int64 ?? 0) != 0,
            lastErrorCode: row?["last_error_code"] as? String
        )
        logger.trace("Loaded local sync state.", metadata: [
            "last_enqueued_history_primary_key": "\(state.lastEnqueuedHistoryPrimaryKey)",
            "did_bootstrap_zone": "\(state.didBootstrapZone)",
            "has_state_serialization": "\(state.stateSerialization != nil)",
            "last_error_code": "\(String(describing: state.lastErrorCode))"
        ])
        return state
    }
    
    internal func saveState(
        lastEnqueuedHistoryPrimaryKey: Int64? = nil,
        stateSerialization: CKSyncEngine.State.Serialization? = nil,
        errorCode: String? = nil,
        clearErrorCode: Bool = false,
        didBootstrapZone: Bool? = nil
    ) throws {
        let connection = try store.queue.connection(.writer)
        let current = try loadState()
        let resolvedStateSerialization = stateSerialization ?? current.stateSerialization
        let encodedStateSerialization: any Sendable = {
            guard let resolvedStateSerialization,
                  let data = try? JSONEncoder().encode(resolvedStateSerialization) else {
                return NSNull()
            }
            return data
        }()
        let resolvedErrorCode: any Sendable = {
            if clearErrorCode { return NSNull() }
            return errorCode ?? current.lastErrorCode ?? NSNull()
        }()
        let now = Int64(Date().timeIntervalSince1970 * 1_000_000)
        try PreparedStatement(
            sql: """
            UPDATE \(StateTable.tableName)
            SET
                \(StateTable.lastEnqueuedHistoryPrimaryKey.rawValue) = ?,
                \(StateTable.stateSerialization.rawValue) = ?,
                \(StateTable.didBootstrapZone.rawValue) = ?,
                \(StateTable.lastSyncAtMicroseconds.rawValue) = ?,
                \(StateTable.lastErrorCode.rawValue) = ?
            WHERE \(StateTable.storeIdentifier.rawValue) = ?
            """,
            bindings: [
                lastEnqueuedHistoryPrimaryKey ?? current.lastEnqueuedHistoryPrimaryKey,
                encodedStateSerialization,
                (didBootstrapZone ?? current.didBootstrapZone) ? 1 : 0,
                now,
                resolvedErrorCode,
                store.identifier
            ],
            handle: connection.handle
        ).run()
        logger.trace("Saved local sync state.", metadata: [
            "resolved_last_enqueued_history_primary_key": "\(lastEnqueuedHistoryPrimaryKey ?? current.lastEnqueuedHistoryPrimaryKey)",
            "has_encoded_state_serialization": "\(resolvedStateSerialization != nil)",
            "resolved_did_bootstrap_zone": "\(didBootstrapZone ?? current.didBootstrapZone)",
            "last_sync_at_microseconds": "\(now)"
        ])
    }
    
    private func resetLocalState(deleteRecordMetadata: Bool = true) throws {
        logger.trace("Resetting local sync state.", metadata: ["delete_record_metadata": "\(deleteRecordMetadata)"])
        let connection = try store.queue.connection(.writer)
        try PreparedStatement(
            sql: """
            UPDATE \(StateTable.tableName)
            SET
                \(StateTable.didBootstrapZone.rawValue) = 0,
                \(StateTable.lastSyncAtMicroseconds.rawValue) = 0,
                \(StateTable.lastErrorCode.rawValue) = NULL,
                \(StateTable.lastEnqueuedHistoryPrimaryKey.rawValue) = 0,
                \(StateTable.stateSerialization.rawValue) = NULL
            WHERE \(StateTable.storeIdentifier.rawValue) = ?
            """,
            bindings: [store.identifier],
            handle: connection.handle
        ).run()
        logger.trace("Reset local sync state row.")
        if deleteRecordMetadata {
            try PreparedStatement(
                sql: """
                DELETE FROM \(RecordMetadataTable.tableName)
                WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
                """,
                bindings: [store.identifier],
                handle: connection.handle
            ).run()
            logger.trace("Deleted record metadata rows for the store.")
        }
        logger.trace("Completed local sync state reset.")
    }
}

// MARK: Outgoing to CloudKit

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    private func enqueuePendingChanges() throws {
        try initializeSyncEngineIfNeeded()
        let state = try loadState()
        let rows = try fetchHistory(
            afterHistoryPrimaryKey: state.lastEnqueuedHistoryPrimaryKey,
            excludingAuthor: configuration.remoteAuthor
        )
        let changes = try coalescePendingChanges(rows)
        var lastEnqueuedHistoryPrimaryKey = state.lastEnqueuedHistoryPrimaryKey
        for change in changes {
            let pendingRecordZoneChanges = try makePendingRecordZoneChanges(for: change)
            if pendingRecordZoneChanges.isEmpty == false {
                syncEngine?.state.add(pendingRecordZoneChanges: pendingRecordZoneChanges)
            }
            lastEnqueuedHistoryPrimaryKey = change.historyPrimaryKey
            try saveState(
                lastEnqueuedHistoryPrimaryKey: lastEnqueuedHistoryPrimaryKey,
                clearErrorCode: true
            )
        }
    }
    
    internal func makePendingRecordZoneChanges(for change: PendingChange)
    throws -> [CKSyncEngine.PendingRecordZoneChange] {
        let existingMetadata = try loadOwnedRecordMetadata(for: change.identifier)
        switch change.operation {
        case .insert, .update:
            guard let currentSnapshot = try snapshot(for: change.identifier) else {
                let deleteChange = PendingChange(
                    identifier: change.identifier,
                    operation: .delete,
                    historyPrimaryKey: change.historyPrimaryKey,
                    transactionIdentifier: change.transactionIdentifier,
                    changedPropertyNames: change.changedPropertyNames,
                    author: change.author
                )
                return try makePendingRecordZoneChanges(for: deleteChange)
            }
            let projected = try projectedRecords(for: currentSnapshot)
            let projectedIDs = Set(projected.map(\.recordID))
            var pending = [CKSyncEngine.PendingRecordZoneChange]()
            for record in projected {
                enqueuedChangesByRecordID[record.recordID] = .init(
                    operation: .insert,
                    identifier: change.identifier,
                    changedPropertyNames: change.changedPropertyNames,
                    isReferenceRecord: record.recordType != makeRecordType(change.entityName)
                )
                pending.append(.saveRecord(record.recordID))
            }
            for metadata in existingMetadata {
                let recordID = makeRecordID(recordName: metadata.recordName)
                guard projectedIDs.contains(recordID) == false else {
                    continue
                }
                enqueuedChangesByRecordID[recordID] = .init(
                    operation: .delete,
                    identifier: metadata.identifier,
                    changedPropertyNames: change.changedPropertyNames,
                    isReferenceRecord: metadata.recordType != makeRecordType(metadata.entityName)
                )
                pending.append(.deleteRecord(recordID))
            }
            return pending
        case .delete:
            guard !existingMetadata.isEmpty else {
                return []
            }
            var pending = existingMetadata.map { metadata in
                let recordID = makeRecordID(recordName: metadata.recordName)
                enqueuedChangesByRecordID[recordID] = .init(
                    operation: .delete,
                    identifier: metadata.identifier,
                    changedPropertyNames: change.changedPropertyNames,
                    isReferenceRecord: metadata.recordType != makeRecordType(metadata.entityName)
                )
                return CKSyncEngine.PendingRecordZoneChange.deleteRecord(recordID)
            }
            let relatedMetadata = try loadRelatedRecordMetadata(targetPrimaryKey: change.primaryKey)
            for metadata in relatedMetadata {
                let recordID = makeRecordID(recordName: metadata.recordName)
                enqueuedChangesByRecordID[recordID] = .init(
                    operation: .delete,
                    identifier: metadata.identifier,
                    changedPropertyNames: nil,
                    isReferenceRecord: true
                )
                pending.append(.deleteRecord(recordID))
            }
            return pending
     
        }
    }
    
    private func fetchHistory(
        afterHistoryPrimaryKey historyPrimaryKey: Int64,
        excludingAuthor author: String
    ) throws -> [HistoryTable.Row] {
        logger.trace("Fetching history rows.", metadata: [
            "history_primary_key": "\(historyPrimaryKey)",
            "excluding_author": "\(author)"
        ])
        let connection = try store.queue.connection(.reader)
        var rows = try queryHistoryRows(
            databaseName: "main",
            afterHistoryPrimaryKey: historyPrimaryKey,
            excludingAuthor: author,
            connection: connection
        )
        logger.trace("Loaded history rows from the main database.", metadata: [
            "row_count": "\(rows.count)"
        ])
        if let mainURL = try connection.mainDatabaseURL() {
            let archiveURLs = archiveDatabaseURLs(mainURL: mainURL)
            logger.trace("Loaded history archive locations.", metadata: [
                "main_database_url": "\(mainURL.path)",
                "archive_count": "\(archiveURLs.count)"
            ])
            for archiveURL in archiveURLs {
                let databaseName = "archive_\(archiveURL.deletingPathExtension().lastPathComponent)"
                do {
                    logger.trace("Attaching history archive database.", metadata: [
                        "database_name": "\(databaseName)",
                        "archive_path": "\(archiveURL.path)"
                    ])
                    try connection.attachDatabase(at: archiveURL, as: databaseName)
                    defer {
                        logger.trace("Detaching history archive database.", metadata: [
                            "database_name": "\(databaseName)"
                        ])
                        try? connection.detachDatabaseIfAttached(named: databaseName)
                    }
                    let archiveRows = try queryHistoryRows(
                        databaseName: databaseName,
                        afterHistoryPrimaryKey: historyPrimaryKey,
                        excludingAuthor: author,
                        connection: connection
                    )
                    logger.trace("Loaded history rows from archive database.", metadata: [
                        "database_name": "\(databaseName)",
                        "row_count": "\(archiveRows.count)"
                    ])
                    rows += archiveRows
                } catch {
                    logger.trace("Failed to read history archive database.", metadata: [
                        "database_name": "\(databaseName)",
                        "error": "\(error)"
                    ])
                    try? connection.detachDatabaseIfAttached(named: databaseName)
                }
            }
        } else {
            logger.trace("Skipped archive lookup because the main database URL was unavailable.")
        }
        rows.sort { $0.pk < $1.pk }
        logger.trace("Completed fetching history rows.", metadata: ["row_count": "\(rows.count)"])
        return rows
    }
    
    private func queryHistoryRows(
        databaseName: String,
        afterHistoryPrimaryKey historyPrimaryKey: Int64,
        excludingAuthor author: String,
        connection: borrowing DatabaseConnection<Store>
    ) throws -> [HistoryTable.Row] {
        try connection.query(
            """
            SELECT
                \(HistoryTable.pk.rawValue) AS history_pk,
                \(HistoryTable.event.rawValue) AS change_type,
                \(HistoryTable.timestamp.rawValue) AS transaction_identifier,
                \(HistoryTable.entityName.rawValue) AS entity_name,
                \(HistoryTable.entityPrimaryKey.rawValue) AS entity_pk,
                \(HistoryTable.author.rawValue) AS author,
                \(HistoryTable.context.rawValue) AS context
            FROM \(databaseName).\(HistoryTable.tableName)
            WHERE \(HistoryTable.storeIdentifier.rawValue) = ?
            AND \(HistoryTable.pk.rawValue) > ?
            AND (
                \(HistoryTable.author.rawValue) IS NULL
                OR \(HistoryTable.author.rawValue) != ?
            )
            ORDER BY \(HistoryTable.pk.rawValue) ASC
            """,
            bindings: [store.identifier, historyPrimaryKey, author]
        ).compactMap { row in
            guard let historyPrimaryKey = row["history_pk"] as? Int64,
                  let changeType = row["change_type"] as? String,
                  let transactionIdentifier = row["transaction_identifier"] as? Int64,
                  let entityName = row["entity_name"] as? String,
                  let entityPrimaryKey = row["entity_pk"] as? String else {
                return nil
            }
            return .init(
                pk: historyPrimaryKey,
                changeType: .init(rawValue: changeType)!,
                transactionIdentifier: transactionIdentifier,
                entityName: entityName,
                entityPrimaryKey: entityPrimaryKey,
                author: row["author"] as? String,
                context: row["context"] as? String
            )
        }
    }
    
    private func archiveDatabaseURLs(mainURL: URL) -> [URL] {
        let directoryURL = HistoryTable.archiveDirectoryURL(mainURL: mainURL)
        logger.trace("Scanning history archive directory.", metadata: ["directory": "\(directoryURL.path)"])
        guard let urls = try? FileManager.default.contentsOfDirectory(
            at: directoryURL,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            logger.trace("Skipped archive scan because the directory was unavailable or unreadable.", metadata: [
                "directory": "\(directoryURL.path)"
            ])
            return []
        }
        let archiveURLs = urls
            .filter { $0.pathExtension == "archive" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
        logger.trace("Loaded history archive files.", metadata: [
            "archive_count": "\(archiveURLs.count)",
            "files": "\(archiveURLs.map(\.lastPathComponent))"
        ])
        return archiveURLs
    }
    
    private func coalescePendingChanges(_ rows: [HistoryTable.Row]) throws -> [PendingChange] {
        logger.trace("Coalescing history rows into pending changes.", metadata: ["row_count": "\(rows.count)"])
        var changes = [RecordIdentifier: PendingChange]()
        changes.reserveCapacity(rows.count)
        for row in rows {
            logger.trace("Processing history row for pending change coalescing.", metadata: [
                "entity_name": "\(row.entityName)",
                "entity_primary_key": "\(row.entityPrimaryKey)",
                "history_primary_key": "\(row.pk)",
                "change_type": "\(row.changeType)",
                "transaction_identifier": "\(row.transactionIdentifier)",
                "author": "\(row.author, default: "nil")",
                "context": "\(row.context, default: "nil")"
            ])
            guard configuration.delegate.shouldSyncEntity(row.entityName) else {
                logger.trace("Skipped entity because syncing is disabled by the delegate: \(row.entityName)")
                continue
            }
            let identifier = RecordIdentifier(
                for: store.identifier,
                tableName: row.entityName,
                primaryKey: row.entityPrimaryKey
            )
            switch row.changeType {
            case .delete:
                changes[identifier] = .init(
                    identifier: identifier,
                    operation: .delete,
                    historyPrimaryKey: row.pk,
                    transactionIdentifier: row.transactionIdentifier,
                    changedPropertyNames: nil,
                    author: row.author
                )
                logger.trace("Recorded delete change.", metadata: [
                    "entity_name": "\(identifier.tableName)",
                    "primary_key": "\(identifier.primaryKey)"
                ])
            case .insert:
                changes[identifier] = .init(
                    identifier: identifier,
                    operation: .insert,
                    historyPrimaryKey: row.pk,
                    transactionIdentifier: row.transactionIdentifier,
                    changedPropertyNames: nil,
                    author: row.author
                )
                logger.trace("Recorded insert as upsert change.", metadata: [
                    "entity_name": "\(identifier.tableName)",
                    "primary_key": "\(identifier.primaryKey)"
                ])
            case .update:
                var existing = changes[identifier] ?? .init(
                    identifier: identifier,
                    operation: .update,
                    historyPrimaryKey: row.pk,
                    transactionIdentifier: row.transactionIdentifier,
                    changedPropertyNames: [],
                    author: row.author
                )
                existing.operation = .update
                existing.historyPrimaryKey = row.pk
                existing.transactionIdentifier = row.transactionIdentifier
                existing.author = row.author
                if existing.changedPropertyNames != nil {
                    existing.changedPropertyNames?.formUnion(HistoryTable.changedPropertyNames(row.context))
                }
                changes[identifier] = existing
                logger.trace("Merged update change.", metadata: [
                    "entity_name": "\(identifier.tableName)",
                    "primary_key": "\(identifier.primaryKey)",
                    "changed_property_names": "\(existing.changedPropertyNames, default: "nil")"
                ])
            }
        }
        let result = changes.values.sorted { $0.historyPrimaryKey < $1.historyPrimaryKey }
        logger.trace("Completed coalescing pending changes.", metadata: ["result_count": "\(result.count)"])
        return result
    }
}

// MARK: Incoming from CloudKit

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func applyRemoteChanges(
        changed: [CKDatabase.RecordZoneChange.Modification],
        deleted: [CKDatabase.RecordZoneChange.Deletion]
    ) throws {
        guard changed.isEmpty == false || deleted.isEmpty == false else {
            return
        }
        var allRemappedIdentifiers = [PersistentIdentifier: PersistentIdentifier]()
        var changedRecordsToPersist = [CKRecord]()
        var deletedRecordIDsToCleanup = [CKRecord.ID]()
        var operations = [DataStoreOperation: [Store.Snapshot]]()
        var recordNameToPrimaryKey = [String: String]()
        for event in changed {
            let record = event.record
            if let primaryKey = record[pk] as? String {
                recordNameToPrimaryKey[record.recordID.recordName] = primaryKey
            }
        }
        for event in changed.lazy {
            if let root = try resolveRootRecordOwnership(for: event.record) {
                guard configuration.delegate.shouldSyncEntity(root.tableName) else {
                    continue
                }
                let existingSnapshot = try snapshot(for: root)
                let incomingSnapshot = try Store.Snapshot(
                    existingSnapshot,
                    record: event.record,
                    store: store
                ) { recordName, destinationEntityName in
                    if let primaryKey = recordNameToPrimaryKey[recordName] {
                        return primaryKey
                    }
                    return try self.loadRecordMetadata(recordName: recordName)?.primaryKey
                }
                operations[existingSnapshot == nil ? .insert : .update, default: []].append(incomingSnapshot)
                let _ = incomingSnapshot.persistentIdentifier
                changedRecordsToPersist.append(event.record)
                logger.debug("Changed event for root record: \(event.record.recordType)")
            } else {
                if let descriptor = intermediaryRecordDescriptor(for: event.record.recordType) {
                    let sourceFieldName = descriptor.sourceFieldName
                    let destinationFieldName = descriptor.destinationFieldName
                    if let sourceRecordName = event.record[sourceFieldName] as? String,
                       let destinationRecordName = event.record[destinationFieldName] as? String {
                        let sourcePrimaryKey = recordNameToPrimaryKey[sourceRecordName]
                        ?? (try? loadRecordMetadata(recordName: sourceRecordName)?.primaryKey)
                        let destinationPrimaryKey = recordNameToPrimaryKey[destinationRecordName]
                        ?? (try? loadRecordMetadata(recordName: destinationRecordName)?.primaryKey)
                        if let sourcePrimaryKey, let destinationPrimaryKey {
                            let connection = try store.queue.connection(.writer)
                            try upsertRecordMetadata(
                                recordType: event.record.recordType,
                                recordName: event.record.recordID.recordName,
                                entityName: descriptor.ownerEntityName,
                                primaryKey: sourcePrimaryKey,
                                targetPrimaryKey: destinationPrimaryKey,
                                systemFields: event.record.systemFieldsData(),
                                connection: connection
                            )
                            logger.debug("Persisted intermediary record metadata: \(event.record.recordType)")
                        } else {
                            logger.debug("Deferred intermediary record — dependencies not yet available: \(event.record.recordType)")
                        }
                    }
                }
            }
        }
        for event in deleted.lazy {
            let recordID = event.recordID
            if let owner = try resolveRootRecordOwnership(for: recordID),
               let existingSnapshot = try snapshot(for: owner) {
                operations[.delete, default: []].append(existingSnapshot)
            }
            deletedRecordIDsToCleanup.append(recordID)
        }
        let orderedInsertPlan = orderedInsertPlan(
            inserted: operations[.insert] ?? [],
            updated: operations[.update] ?? []
        )
        if !orderedInsertPlan.insertLayers.isEmpty {
            for layer in orderedInsertPlan.insertLayers {
                let result = try saveRemoteChanges(inserted: layer, updated: [], deleted: [])
                allRemappedIdentifiers.merge(result.remappedIdentifiers) { _, new in new }
            }
        }
        if !orderedInsertPlan.updateSnapshots.isEmpty {
            let result = try saveRemoteChanges(inserted: [], updated: orderedInsertPlan.updateSnapshots, deleted: [])
            allRemappedIdentifiers.merge(result.remappedIdentifiers) { _, new in new }
        }
        if let deleted = operations[.delete] {
            let result = try saveRemoteChanges(inserted: [], updated: [], deleted: deleted)
            allRemappedIdentifiers.merge(result.remappedIdentifiers) { _, new in new }
        }
        for record in changedRecordsToPersist {
            try persistSavedRecordMetadata(record)
        }
        for recordID in deletedRecordIDsToCleanup {
            try removeAppliedDeletedRecordMetadata(recordID)
        }
    }
    
    @discardableResult internal func saveRemoteChanges(
        inserted: [Store.Snapshot],
        updated: [Store.Snapshot],
        deleted: [Store.Snapshot]
    ) throws -> DatabaseSaveChangesResult<Store.Snapshot> {
        guard !inserted.isEmpty || !updated.isEmpty || !deleted.isEmpty else {
            return .init(for: store.identifier, remappedIdentifiers: [:], snapshotsToReregister: [:])
        }
        let request = DatabaseSaveChangesRequest(
            editingState: DatabaseEditingState(author: remoteAuthor),
            inserted: inserted,
            updated: updated,
            deleted: deleted
        )
        let result: DatabaseSaveChangesResult = try store.save(request)
        return result
    }
    
    internal func orderedInsertPlan(inserted: [Store.Snapshot], updated: [Store.Snapshot]) -> OrderedInsertPlan {
        var insertLayers = [[Store.Snapshot]]()
        insertLayers.reserveCapacity(inserted.count)
        let insertSnapshots = Dictionary(inserted.map { ($0.persistentIdentifier, $0) }, uniquingKeysWith: { $1 })
        var updateSnapshots = Dictionary(updated.map { ($0.persistentIdentifier, $0) }, uniquingKeysWith: { $1 })
        var remainingInsertSnapshots = insertSnapshots
        updateSnapshots.reserveCapacity(inserted.count + updated.count)
        while !remainingInsertSnapshots.isEmpty {
            var layer = [Store.Snapshot]()
            layer.reserveCapacity(remainingInsertSnapshots.count)
            for snapshot in Array(remainingInsertSnapshots.values).lazy {
                var insert = snapshot
                var hasRequiredDependency = false
                var requiresFollowUpUpdate = false
                for property in insert.properties {
                    guard let relationship = property.metadata as? Schema.Relationship else {
                        continue
                    }
                    if relationship.isToOneRelationship {
                        guard let relatedIdentifier = snapshot.values[property.index] as? PersistentIdentifier else {
                            continue
                        }
                        if remainingInsertSnapshots[relatedIdentifier] != nil {
                            if property.isOptional {
                                insert.values[property.index] = SQLNull()
                                requiresFollowUpUpdate = true
                                continue
                            }
                            hasRequiredDependency = true
                            break
                        }
                        if insertSnapshots[relatedIdentifier] != nil {
                            continue
                        }
                        if updateSnapshots[relatedIdentifier] != nil {
                            continue
                        }
                        let identifier = RecordIdentifier(
                            for: store.identifier,
                            tableName: relatedIdentifier.entityName,
                            primaryKey: relatedIdentifier.primaryKey()
                        )
                        if (try? self.snapshot(for: identifier)) != nil {
                            logger.notice("Satisfied dependency from local snapshot: \(property)")
                        }
                        continue
                    }
                    let originalValue = snapshot.values[property.index]
                    if property.isOptional {
                        if originalValue is SQLNull {
                            insert.values[property.index] = SQLNull()
                        } else if let relatedIdentifiers = originalValue as? [PersistentIdentifier] {
                            insert.values[property.index] = [PersistentIdentifier]()
                            if !relatedIdentifiers.isEmpty { requiresFollowUpUpdate = true }
                        } else {
                            insert.values[property.index] = SQLNull()
                        }
                    } else {
                        let relatedIdentifiers = originalValue as? [PersistentIdentifier] ?? []
                        insert.values[property.index] = [PersistentIdentifier]()
                        if !relatedIdentifiers.isEmpty {
                            requiresFollowUpUpdate = true
                        }
                    }
                }
                guard !hasRequiredDependency else {
                    continue
                }
                layer.append(insert)
                if requiresFollowUpUpdate {
                    updateSnapshots[snapshot.persistentIdentifier] = snapshot
                }
            }
            guard !layer.isEmpty else {
                insertLayers.append(Array(remainingInsertSnapshots.values))
                break
            }
            insertLayers.append(layer)
            for snapshot in layer {
                remainingInsertSnapshots[snapshot.persistentIdentifier] = nil
            }
        }
        let insertedIdentifiers = Set(insertLayers.flatMap(\.self).map(\.persistentIdentifier))
        let updateResult = updated + inserted.compactMap { snapshot in
            guard insertedIdentifiers.contains(snapshot.persistentIdentifier),
                  updateSnapshots[snapshot.persistentIdentifier] != nil else {
                return nil
            }
            return snapshot
        }
        return OrderedInsertPlan(insertLayers: insertLayers, updateSnapshots: updateResult)
    }
    
    internal func cloudKitErrorCodeString(_ error: Swift.Error) -> String {
        let string: String
        if let ckError = error as? CKError {
            string = "\(ckError.code.rawValue)"
        } else {
            string = String(describing: error)
        }
        logger.trace("Resolved CloudKit error code string.", metadata: [
            "error": "\(error)",
            "code_string": "\(string)"
        ])
        return string
    }
    
    internal func scheduleInitialUploadIfNeeded() throws {
        logger.trace("Scheduling initial CloudKit upload if needed.")
        try initializeSyncEngineIfNeeded()
        let state = try loadState()
        logger.trace("Loaded bootstrap state before scheduling initial upload.", metadata: [
            "did_bootstrap_zone": "\(state.didBootstrapZone)"
        ])
        guard !state.didBootstrapZone else {
            logger.trace("Skipped initial upload scheduling because the zone is already bootstrapped.")
            return
        }
        syncEngine?.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
        try saveState(clearErrorCode: true, didBootstrapZone: true)
        logger.trace("Completed initial upload scheduling.")
    }
    
    internal func resetForAccountChange() throws {
        logger.trace("Resetting state for CloudKit account change.", metadata: [
            "enqueued_changes_count": "\(enqueuedChangesByRecordID.count)"
        ])
        enqueuedChangesByRecordID.removeAll()
        try resetLocalState(deleteRecordMetadata: true)
        self.syncEngine = nil
        self.didPrepare = false
        logger.trace("Completed CloudKit account reset.")
    }
    
    private func resetForAccountChange(reuploadLocalData: Bool) throws {
        logger.trace("Resetting state for CloudKit account change.", metadata: [
            "reupload_local_data": "\(reuploadLocalData)",
            "enqueued_changes_count": "\(enqueuedChangesByRecordID.count)"
        ])
        enqueuedChangesByRecordID.removeAll()
        try resetLocalState(deleteRecordMetadata: true)
        self.syncEngine = nil
        self.didPrepare = false
        logger.trace("Reset local sync state and cleared the sync engine.")
        try initializeSyncEngineIfNeeded()
        syncEngine?.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
        logger.trace("Re-enqueued CloudKit zone save after account reset.", metadata: [
            "zone_name": "\(zoneID.zoneName)"
        ])
        if reuploadLocalData {
            try saveState(clearErrorCode: true, didBootstrapZone: true)
            logger.trace("Saved bootstrap state after account reset reupload setup.")
        }
        logger.trace("Completed CloudKit account reset.")
    }
}

#endif
