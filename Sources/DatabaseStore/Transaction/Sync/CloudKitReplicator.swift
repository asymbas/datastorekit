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
    public final actor Replicator: DataStoreSynchronizer {
        public typealias Store = DatabaseStore
        public typealias SyncConfiguration = Store.Configuration.CloudKitDatabase
        private var syncEngine: CKSyncEngine?
        nonisolated internal unowned let store: Store
        nonisolated internal let configuration: SyncConfiguration
        nonisolated internal let container: CKContainer
        nonisolated internal let database: CKDatabase
        nonisolated internal let zoneID: CKRecordZone.ID
        internal var identifiers: [PersistentIdentifier: RecordMetadata] = [:]
        internal var enqueuedChangesByRecordID: [CKRecord.ID: PersistentIdentifier] = [:]
        internal var cachedRecordsForBatch: [CKRecord.ID: CKRecord] = [:]
        internal var didPrepare: Bool = false
        internal var isHandlingAccountChange: Bool = false
        nonisolated public let id: String
        internal var stagingHandle: DatabaseConnection<Store>?
        
        nonisolated public var remoteAuthor: String {
            configuration.remoteAuthor
        }
        
        nonisolated public var lastProcessedToken: Store.HistoryType.TokenType? {
            guard let row = try? store.queue.connection(.reader).query(
                """
                SELECT \(DatabaseConfiguration.CloudKitDatabase.StateTable.lastEnqueuedHistoryPrimaryKey.rawValue) AS history_pk
                FROM \(DatabaseConfiguration.CloudKitDatabase.StateTable.tableName)
                WHERE \(DatabaseConfiguration.CloudKitDatabase.StateTable.storeIdentifier.rawValue) = ?
                LIMIT 1
                """,
                bindings: [store.identifier]
            ).first,
                  let watermark = row["history_pk"] as? Int64,
                  watermark > 0 else {
                return nil
            }
            return DatabaseHistoryToken(
                id: Int(watermark),
                tokenValue: [store.identifier: watermark]
            )
        }
        
        internal init(store: Store, configuration: SyncConfiguration) {
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
            logger.debug("Initialized CloudKit replicator.", metadata: [
                "id": "\(configuration.id)",
                "store_identifier": "\(store.identifier)",
                "container_identifier": "\(configuration.containerIdentifier ?? "<default>")",
                "database_scope": "\(configuration.databaseScope)",
                "zone_name": "\(configuration.zoneName)"
            ])
        }
        
        internal struct State: Sendable {
            nonisolated internal var lastEnqueuedHistoryPrimaryKey: Int64
            nonisolated internal var stateSerialization: CKSyncEngine.State.Serialization?
            nonisolated internal var didBootstrapZone: Bool
            nonisolated internal var lastErrorCode: String?
        }
        
        internal struct RecordMetadata: Sendable {
            nonisolated internal let recordType: String
            nonisolated internal let recordName: String
            nonisolated internal var entityName: String
            nonisolated internal var primaryKey: String
            nonisolated internal let targetPrimaryKey: String?
        }
        
        internal struct OrderedInsertPlan {
            nonisolated internal let insertLayers: [[Store.Snapshot]]
            nonisolated internal let updateSnapshots: [Store.Snapshot]
        }
        
        internal struct IntermediaryRecordDescriptor {
            nonisolated internal let ownerEntityName: String
            nonisolated internal let property: PropertyMetadata
            nonisolated internal let destinationEntityName: String
            nonisolated internal let sourceFieldName: String
            nonisolated internal let destinationFieldName: String
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
        
        private func extractChangeIdentifier(_ change: HistoryChange) -> Int64? {
            switch change {
            case .insert(let insert): insert.changeIdentifier as? Int64
            case .update(let update): update.changeIdentifier as? Int64
            case .delete(let delete): delete.changeIdentifier as? Int64
            @unknown default:
                fatalError()
            }
        }
        
        /// Inherited from `DataStoreSynchronizer.sync(transactions:)`.
        /// - Parameter transactions: The transaction delta.
        public func sync(transactions: [Store.HistoryType]) async throws {
            logger.trace("Starting CloudKit sync: \(transactions.count) transactions")
            try initializeSyncEngineIfNeeded()
            for change in coalesceTransactions(transactions) {
                guard let type = Schema.type(for: change.changedPersistentIdentifier.entityName) else {
                    throw SchemaError.entityNotRegistered
                }
                let pendingRecordZoneChanges = try makePendingRecordZoneChanges(for: change, as: type)
                if pendingRecordZoneChanges.isEmpty == false {
                    syncEngine?.state.add(pendingRecordZoneChanges: pendingRecordZoneChanges)
                }
            }
            if let lastChangeIdentifier = transactions
                .flatMap(\.changes)
                .compactMap({ extractChangeIdentifier($0) })
                .max() {
                try saveState(lastEnqueuedHistoryPrimaryKey: lastChangeIdentifier, clearErrorCode: true)
            }
            guard let syncEngine = self.syncEngine else {
                throw Self.Error.noSyncEngine
            }
            try await syncEngine.sendChanges(.init())
            var remainingAttempts = 10
            while syncEngine.state.pendingRecordZoneChanges.isEmpty == false, remainingAttempts > 0 {
                remainingAttempts -= 1
                logger.trace("Pending record zone changes remain after send. Retrying.", metadata: [
                    "pending_count": "\(syncEngine.state.pendingRecordZoneChanges.count)",
                    "remaining_attempts": "\(remainingAttempts)"
                ])
                try await Task.sleep(for: .seconds(1))
                try await syncEngine.sendChanges(.init())
            }
            try await syncEngine.fetchChanges(.init())
            logger.trace("Completed CloudKit sync.")
        }
        
        public func sync() async throws {
            let watermark = self.lastProcessedToken?.watermark(for: store.identifier) ?? 0
            let descriptor = HistoryDescriptor<DatabaseHistoryTransaction>()
            let transactions = try store.fetchHistory(descriptor)
                .filter { transaction in
                    transaction.author != remoteAuthor &&
                    transaction.changes.contains { (extractChangeIdentifier($0) ?? 0) > watermark }
                }
                .sorted { $0.transactionIdentifier < $1.transactionIdentifier }
            try await sync(transactions: transactions)
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
    private func coalesceTransactions(_ transactions: [Store.HistoryType]) -> [HistoryChange] {
        var changes = [PersistentIdentifier: HistoryChange]()
        for transaction in transactions {
            for change in transaction.changes {
                let changedPersistentIdentifier = change.changedPersistentIdentifier
                guard configuration.delegate.shouldSyncEntity(changedPersistentIdentifier.entityName) else {
                    continue
                }
                switch change {
                case .delete(let delete):
                    changes[changedPersistentIdentifier] = .delete(delete)
                case .insert(let insert):
                    changes[changedPersistentIdentifier] = .insert(insert)
                case .update(let update):
                    changes[changedPersistentIdentifier] = changes[changedPersistentIdentifier] ?? .update(update)
                @unknown default:
                    fatalError()
                }
            }
        }
        return Array(changes.values)
    }
    
    /// Prepares the snapshot payload to send out to CloudKit.
    /// - Parameter change: A single event operation.
    internal func makePendingRecordZoneChanges<T: PersistentModel>(for change: HistoryChange, as type: T.Type)
    throws -> [CKSyncEngine.PendingRecordZoneChange] {
        let existingMetadata = try loadOwnedRecordMetadata(for: change.changedPersistentIdentifier)
        switch change {
        case .insert(let insert as DatabaseHistoryInsert<T>):
            guard let currentSnapshot = try snapshot(for: change.changedPersistentIdentifier) else {
                let deleteChange = DatabaseHistoryDelete(
                    as: T.self,
                    transactionIdentifier: insert.transactionIdentifier,
                    changeIdentifier: insert.changeIdentifier,
                    changedPersistentIdentifier: insert.changedPersistentIdentifier,
                    changedPropertyNames: [],
                    preservedValues: nil
                )
                return try makePendingRecordZoneChanges(for: .delete(deleteChange), as: T.self)
            }
            return try enqueueProjectedChanges(
                for: insert.changedPersistentIdentifier,
                snapshot: currentSnapshot,
                existingMetadata: existingMetadata
            )
        case .update(let update as DatabaseHistoryUpdate<T>):
            guard let currentSnapshot = try snapshot(for: change.changedPersistentIdentifier) else {
                let deleteChange = DatabaseHistoryDelete(
                    as: T.self,
                    transactionIdentifier: update.transactionIdentifier,
                    changeIdentifier: update.changeIdentifier,
                    changedPersistentIdentifier: update.changedPersistentIdentifier,
                    changedPropertyNames: [],
                    preservedValues: nil
                )
                return try makePendingRecordZoneChanges(for: .delete(deleteChange), as: T.self)
            }
            return try enqueueProjectedChanges(
                for: update.changedPersistentIdentifier,
                snapshot: currentSnapshot,
                existingMetadata: existingMetadata
            )
        case .delete(let delete as DatabaseHistoryDelete<T>):
            guard !existingMetadata.isEmpty else {
                return []
            }
            var pending = try existingMetadata.map { metadata in
                let recordID = makeRecordID(recordName: metadata.recordName)
                enqueuedChangesByRecordID[recordID] = try .identifier(
                    for: store.identifier,
                    entityName: metadata.entityName,
                    primaryKey: metadata.primaryKey
                )
                return CKSyncEngine.PendingRecordZoneChange.deleteRecord(recordID)
            }
            let relatedMetadata = try loadRelatedRecordMetadata(targetPrimaryKey: delete.changedPersistentIdentifier.primaryKey())
            for metadata in relatedMetadata {
                let recordID = makeRecordID(recordName: metadata.recordName)
                enqueuedChangesByRecordID[recordID] = try .identifier(
                    for: store.identifier,
                    entityName: metadata.entityName,
                    primaryKey: metadata.primaryKey
                )
                pending.append(.deleteRecord(recordID))
            }
            return pending
        @unknown default:
            fatalError()
        }
    }
    
    private func enqueueProjectedChanges(
        for identifier: PersistentIdentifier,
        snapshot: Store.Snapshot,
        existingMetadata: [RecordMetadata]
    ) throws -> [CKSyncEngine.PendingRecordZoneChange] {
        let projected = try projectedRecords(for: snapshot)
        let projectedIDs = Set(projected.map(\.recordID))
        var pending = [CKSyncEngine.PendingRecordZoneChange]()
        for record in projected {
            cachedRecordsForBatch[record.recordID] = record
        }
        for record in projected {
            enqueuedChangesByRecordID[record.recordID] = identifier
            pending.append(.saveRecord(record.recordID))
        }
        for metadata in existingMetadata {
            let recordID = makeRecordID(recordName: metadata.recordName)
            guard projectedIDs.contains(recordID) == false else {
                continue
            }
            enqueuedChangesByRecordID[recordID] = try .identifier(
                for: store.identifier,
                entityName: metadata.entityName,
                primaryKey: metadata.primaryKey
            )
            pending.append(.deleteRecord(recordID))
        }
        return pending
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
        struct JoinTable {
            let joinTable: String
            let sourceColumn: String
            let destinationColumn: String
            let sourcePrimaryKey: String
            let destinationPrimaryKey: String
        }
        var allRemappedIdentifiers = [PersistentIdentifier: PersistentIdentifier]()
        var changedRecordsToPersist = [CKRecord]()
        var deletedRecordIDsToCleanup = [CKRecord.ID]()
        var operations = [DataStoreOperation: [Store.Snapshot]]()
        var recordNameToPrimaryKey = [String: String]()
        var deferredJoinTableInserts = [JoinTable]()
        for event in changed {
            let record = event.record
            if let primaryKey = record[pk] as? String {
                recordNameToPrimaryKey[record.recordID.recordName] = primaryKey
            }
        }
        for event in changed.lazy {
            if let root = try resolveRootRecordOwnership(for: event.record) {
                guard configuration.delegate.shouldSyncEntity(root.entityName) else {
                    continue
                }
                let existingSnapshot = try snapshot(for: .identifier(
                    for: store.identifier,
                    entityName: root.entityName,
                    primaryKey: root.primaryKey
                ))
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
                            let reference = descriptor.property.reference!
                            deferredJoinTableInserts.append(.init(
                                joinTable: reference[0].destinationTable,
                                sourceColumn: reference[0].rhsColumn,
                                destinationColumn: reference[1].lhsColumn,
                                sourcePrimaryKey: sourcePrimaryKey,
                                destinationPrimaryKey: destinationPrimaryKey
                            ))
                            changedRecordsToPersist.append(event.record)
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
               let existingSnapshot = try snapshot(for: .identifier(
                for: store.identifier,
                entityName: owner.entityName,
                primaryKey: owner.primaryKey
               )) {
                operations[.delete, default: []].append(existingSnapshot)
            }
            deletedRecordIDsToCleanup.append(recordID)
        }
        let orderedInsertPlan = orderedInsertPlan(inserted: operations[.insert] ?? [], updated: operations[.update] ?? [])
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
        if !deferredJoinTableInserts.isEmpty {
            let connection = try store.queue.connection(.writer)
            for insert in deferredJoinTableInserts {
                _ = try connection.query(
                    """
                    INSERT OR IGNORE INTO "\(insert.joinTable)" (
                        "\(insert.sourceColumn)",
                        "\(insert.destinationColumn)"
                    ) VALUES (?, ?)
                    """,
                    bindings: [insert.sourcePrimaryKey, insert.destinationPrimaryKey]
                )
            }
            logger.debug("Inserted \(deferredJoinTableInserts.count) deferred join table rows.")
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
                        if (try? self.snapshot(for: relatedIdentifier)) != nil {
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
        return .init(insertLayers: insertLayers, updateSnapshots: updateResult)
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
