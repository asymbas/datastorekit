//
//  CloudKitReplicator.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreSupport
private import Foundation
private import Logging
private import SQLiteHandle
private import SQLSupport
private import Synchronization
internal import DataStoreRuntime
public import DataStoreCore

#if swift(>=6.2)
internal import SwiftData
#else
@preconcurrency internal import SwiftData
#endif

#if canImport(CloudKit)
internal import CloudKit
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.cloudkit")

#if canImport(CloudKit)

/*
 Prerequisites for CKSyncEngine:
 - Add CloudKit capability.
 - Add remote notification capability.
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
        internal var hasSweptPendingResolutions: Bool = false
        internal var hasFailedRemoteApply: Bool = false
        internal var pendingResolvedConflicts: Int = 0
        nonisolated public let id: String
        
        internal init(store: Store, configuration: SyncConfiguration) {
            let container: CKContainer = {
                if let identifier = configuration.containerIdentifier {
                    return .init(identifier: identifier)
                } else {
                    return .default()
                }
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
            logger.trace("Initialized CloudKit replicator.", metadata: [
                "id": "\(configuration.id)",
                "store_identifier": "\(store.identifier)",
                "container_identifier": "\(configuration.containerIdentifier ?? "<default>")",
                "database_scope": "\(configuration.databaseScope)",
                "zone_name": "\(configuration.zoneName)"
            ])
        }
        
        nonisolated internal struct State: Sendable {
            internal var lastEnqueuedHistoryPrimaryKey: Int64
            internal var stateSerialization: CKSyncEngine.State.Serialization?
            internal var didBootstrapZone: Bool
            internal var lastErrorCode: String?
        }
        
        nonisolated internal struct RecordMetadata: Sendable {
            internal let recordType: String
            internal let recordName: String
            internal var entityName: String
            internal var primaryKey: String
            internal let targetPrimaryKey: String?
        }
        
        nonisolated internal struct OrderedInsertPlan {
            internal let insertLayers: [[Store.Snapshot]]
            internal let updateSnapshots: [Store.Snapshot]
            internal let deferredReferences: [String: [UnresolvedToOneReference]]
        }
        
        nonisolated internal struct IntermediaryRecordDescriptor {
            internal let ownerEntityName: String
            internal let property: PropertyMetadata
            internal let destinationEntityName: String
            internal let sourceFieldName: String
            internal let destinationFieldName: String
        }
        
        nonisolated internal struct UnresolvedToOneReference: Sendable {
            internal let entityName: String
            internal let primaryKey: String
            internal let propertyName: String
        }
        
        nonisolated internal enum Error: LocalizedError {
            case noSyncEngine
            case unsupportedDatabaseScope(CKDatabase.Scope)
        }
        
        nonisolated public var remoteAuthor: String {
            configuration.remoteAuthor
        }
        
        nonisolated public var lastProcessedToken: Store.HistoryType.TokenType? {
            guard let row = try? store.queue.connection(.reader).query(
                """
                SELECT \(StateTable.lastEnqueuedHistoryPrimaryKey.rawValue) AS history_pk
                FROM \(StateTable.tableName)
                WHERE \(StateTable.storeIdentifier.rawValue) = ?
                LIMIT 1
                """,
                bindings: [store.identifier]
            ).first,
                  let watermark = row["history_pk"] as? Int64, watermark > 0 else {
                return nil
            }
            return .init(id: Int(watermark), tokenValue: [store.identifier: watermark])
        }
        
        private func extractChangeIdentifier(_ change: HistoryChange) -> Int64? {
            switch change {
            case .insert(let insert): insert.changeIdentifier as? Int64
            case .update(let update): update.changeIdentifier as? Int64
            case .delete(let delete): delete.changeIdentifier as? Int64
            @unknown default: fatalError()
            }
        }
        
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
        
        /// Inherited from `DataStoreSynchronizer.sync(transactions:)`.
        ///
        /// - Parameter transactions: The transaction delta.
        public func sync(transactions: [Store.HistoryType]) async throws {
            try initializeSyncEngineIfNeeded()
            for change in coalesceTransactions(transactions) {
                guard let entity = self.store.schema.entitiesByName[change.changedPersistentIdentifier.entityName],
                      let type = Schema.type(for: entity) else {
                    throw SchemaError.entityNotRegistered
                }
                let pendingRecordZoneChanges = try makePendingRecordZoneChanges(for: change, as: type)
                if !pendingRecordZoneChanges.isEmpty {
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
                logger.trace("Pending record zone changes remain after send. Retrying...", metadata: [
                    "pending_count": "\(syncEngine.state.pendingRecordZoneChanges.count)",
                    "remaining_attempts": "\(remainingAttempts)"
                ])
                try await Task.sleep(for: .seconds(1))
                try await syncEngine.sendChanges(.init())
            }
            try await syncEngine.fetchChanges(.init())
            let unresolvedCount = (try? pendingUnresolvedCount()) ?? 0
            await store.reportPendingUnresolvedCount(for: self.id, count: unresolvedCount)
            logger.trace("Completed CloudKit sync.")
        }
        
        public func sync() async throws {
            let watermark = self.lastProcessedToken?.watermark(for: store.identifier) ?? 0
            let descriptor = HistoryDescriptor<Store.HistoryType>()
            let transactions = try store.fetchHistory(descriptor)
                .filter { transaction in
                    transaction.author != remoteAuthor &&
                    transaction.changes.contains { (extractChangeIdentifier($0) ?? 0) > watermark }
                }
                .sorted {
                    $0.transactionIdentifier < $1.transactionIdentifier
                }
            try await sync(transactions: transactions)
        }
        
        public func fetchChanges() async throws {
            logger.trace("Fetching CloudKit changes...")
            try await syncEngine?.fetchChanges(.init())
            logger.trace("Completed fetching CloudKit changes.")
        }
        
        public func sendChanges() async throws {
            logger.trace("Sending CloudKit changes...")
            guard let syncEngine = self.syncEngine else {
                throw Self.Error.noSyncEngine
            }
            logPendingCloudKitCounts(syncEngine)
            try await syncEngine.sendChanges(.init())
            logger.trace("Completed sending CloudKit changes.")
        }
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal typealias RecordMetadataTable = DatabaseConfiguration.CloudKitDatabase.RecordMetadataTable
    internal typealias StateTable = DatabaseConfiguration.CloudKitDatabase.StateTable
    
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
    
    public func prepare() async throws {
        guard !didPrepare else {
            return
        }
        guard configuration.databaseScope == .private else {
            throw Self.Error.unsupportedDatabaseScope(configuration.databaseScope)
        }
        let accountStatus = try await container.accountStatus()
        try createCloudKitTables()
        try initializeSyncEngineIfNeeded()
        let state = try loadState()
        if state.didBootstrapZone == false {
            syncEngine?.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
            try saveState(clearErrorCode: true, didBootstrapZone: true)
        }
        self.didPrepare = true
        logger.trace("CloudKit replicator is prepared.", metadata: ["account_status": "\(accountStatus)"])
    }
    
    private func initializeSyncEngineIfNeeded() throws {
        guard syncEngine == nil else { return }
        let state = try loadState()
        var configuration = CKSyncEngine.Configuration(
            database: database,
            stateSerialization: state.stateSerialization,
            delegate: self
        )
        configuration.automaticallySync = true // false
        self.syncEngine = CKSyncEngine(configuration)
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
        connection: borrowing DatabaseConnection
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
    }
    
    private func ensureStateRowExists(connection: borrowing DatabaseConnection) throws {
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
        return .init(
            lastEnqueuedHistoryPrimaryKey: row?["history_pk"] as? Int64 ?? 0,
            stateSerialization: stateSerialization,
            didBootstrapZone: (row?["did_bootstrap_zone"] as? Int64 ?? 0) != 0,
            lastErrorCode: row?["last_error_code"] as? String
        )
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
        let encodedStateSerialization: any Sendable = {
            guard let stateSerialization = stateSerialization ?? current.stateSerialization,
                  let data = try? JSONEncoder().encode(stateSerialization) else {
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
    }
    
    private func resetLocalState(deleteRecordMetadata: Bool = true) throws {
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
            try PreparedStatement(
                sql: """
                DELETE FROM \(PendingReferenceTable.tableName)
                WHERE \(PendingReferenceTable.storeIdentifier.rawValue) = ?
                """,
                bindings: [store.identifier],
                handle: connection.handle
            ).run()
            try PreparedStatement(
                sql: """
                DELETE FROM \(PendingRecordTable.tableName)
                WHERE \(PendingRecordTable.storeIdentifier.rawValue) = ?
                """,
                bindings: [store.identifier],
                handle: connection.handle
            ).run()
        }
    }
    
    public func eraseCloudKitData(recreateEmptyZone: Bool = false) async throws {
        logger.notice("Erasing CloudKit data...", metadata: [
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
}

// MARK: Outgoing to CloudKit

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    /// Prepares the snapshot payload to send out to CloudKit.
    ///
    /// - Parameter change: A single event operation.
    internal func makePendingRecordZoneChanges<T: PersistentModel>(for change: HistoryChange, as type: T.Type)
    throws -> [CKSyncEngine.PendingRecordZoneChange] {
        let existingMetadata = try loadOwnedRecordMetadata(for: change.changedPersistentIdentifier)
        switch change {
        case .insert(let insert as DatabaseHistoryInsert<T>):
            guard let currentSnapshot = try snapshot(for: change.changedPersistentIdentifier) else {
                logger.debug("Converted CloudKit insert without local snapshot to delete.", metadata: [
                    "identifier": "\(change.changedPersistentIdentifier)"
                ])
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
            logger.notice("Enqueueing CloudKit save for inserted model.", metadata: [
                "entity_name": "\(change.changedPersistentIdentifier.entityName)",
                "primary_key": "\(self.store.manager.primaryKey(for: change.changedPersistentIdentifier))"
            ])
            return try enqueueProjectedChanges(
                for: insert.changedPersistentIdentifier,
                snapshot: currentSnapshot,
                existingMetadata: existingMetadata
            )
        case .update(let update as DatabaseHistoryUpdate<T>):
            guard let currentSnapshot = try snapshot(for: change.changedPersistentIdentifier) else {
                logger.debug("Converted CloudKit update without local snapshot to delete.", metadata: [
                    "identifier": "\(change.changedPersistentIdentifier)"
                ])
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
            logger.notice("Enqueueing CloudKit save for updated model.", metadata: [
                "entity_name": "\(change.changedPersistentIdentifier.entityName)",
                "primary_key": "\(self.store.manager.primaryKey(for: change.changedPersistentIdentifier))",
                "changed_properties": "\(update.fields.joined(separator: ", "))"
            ])
            return try enqueueProjectedChanges(
                for: update.changedPersistentIdentifier,
                snapshot: currentSnapshot,
                existingMetadata: existingMetadata
            )
        case .delete(let delete as DatabaseHistoryDelete<T>):
            guard !existingMetadata.isEmpty else {
                return []
            }
            logger.notice("Enqueueing CloudKit delete for removed model.", metadata: [
                "entity_name": "\(change.changedPersistentIdentifier.entityName)",
                "primary_key": "\(self.store.manager.primaryKey(for: change.changedPersistentIdentifier))"
            ])
            var pending = try existingMetadata.map { metadata in
                let recordID = makeRecordID(recordName: metadata.recordName)
                enqueuedChangesByRecordID[recordID] = try .identifier(
                    for: store.identifier,
                    entityName: metadata.entityName,
                    primaryKey: metadata.primaryKey
                )
                return CKSyncEngine.PendingRecordZoneChange.deleteRecord(recordID)
            }
            let primaryKey = self.store.manager.primaryKey(for: delete.changedPersistentIdentifier)
            let relatedMetadata = try loadRelatedRecordMetadata(targetPrimaryKey: primaryKey)
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
        case .insert(_), .update(_), .delete(_):
            fallthrough
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
        let projectedRecordTypes = Set(projected.map(\.recordType))
        var pending = [CKSyncEngine.PendingRecordZoneChange]()
        for record in projected {
            cachedRecordsForBatch[record.recordID] = record
            enqueuedChangesByRecordID[record.recordID] = identifier
            pending.append(.saveRecord(record.recordID))
        }
        for metadata in existingMetadata {
            let recordID = makeRecordID(recordName: metadata.recordName)
            guard projectedRecordTypes.contains(metadata.recordType) else {
                continue
            }
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
    internal typealias PendingReferenceTable = DatabaseConfiguration.CloudKitDatabase.PendingReferenceTable
    internal typealias PendingRecordTable = DatabaseConfiguration.CloudKitDatabase.PendingRecordTable
    
    @discardableResult internal func saveRemoteChanges(
        inserted: [Store.Snapshot],
        updated: [Store.Snapshot],
        deleted: [Store.Snapshot]
    ) throws -> SyncSaveChangesResult<Store.Snapshot> {
        guard !inserted.isEmpty || !updated.isEmpty || !deleted.isEmpty else {
            return .init(for: store.identifier, remappedIdentifiers: [:], snapshotsToReregister: [:])
        }
        let request = SyncSaveChangesRequest(
            editingState: DatabaseEditingState(author: remoteAuthor),
            inserted: inserted,
            updated: updated,
            deleted: deleted
        )
        let result: SyncSaveChangesResult = try store.save(request)
        return result
    }
    
    internal func applyRemoteChanges(changedRecords: [CKRecord], deletedRecordIDs: [CKRecord.ID]) throws {
        guard !changedRecords.isEmpty || !deletedRecordIDs.isEmpty else {
            return
        }
        logger.notice("Applying remote CloudKit changes...", metadata: [
            "changed_count": "\(changedRecords.count)",
            "deleted_count": "\(deletedRecordIDs.count)"
        ])
        struct JoinTable {
            let joinTable: String
            let sourceColumn: String
            let destinationColumn: String
            let sourcePrimaryKey: String
            let destinationPrimaryKey: String
        }
        var remappedPrimaryKeys: [String: String] = [:]
        var batchUnresolvedReferences: [String: [UnresolvedToOneReference]] = [:]
        var changedRecordsToPersist: [CKRecord] = []
        var deletedRecordIDsToCleanup: [CKRecord.ID] = []
        var operations: [DataStoreOperation: [Store.Snapshot]] = [:]
        var recordNameToPrimaryKey: [String: String] = [:]
        var deferredJoinTableInserts: [JoinTable] = []
        for record in changedRecords {
            if let primaryKey = record[pk] as? String {
                recordNameToPrimaryKey[record.recordID.recordName] = primaryKey
            }
        }
        for record in changedRecords.lazy {
            if let root = try resolveRootRecordOwnership(for: record) {
                guard configuration.delegate.shouldSyncEntity(root.entityName) else {
                    continue
                }
                let existingSnapshot = try snapshot(for: .identifier(
                    for: store.identifier,
                    entityName: root.entityName,
                    primaryKey: root.primaryKey
                ))
                var incomingSnapshot: Store.Snapshot
                do {
                    incomingSnapshot = try Store.Snapshot(
                        existingSnapshot,
                        record: record,
                        store: store
                    ) { recordName, destinationEntityName, propertyName in
                        if let primaryKey = recordNameToPrimaryKey[recordName] {
                            return primaryKey
                        }
                        if let primaryKey = try self.loadRecordMetadata(recordName: recordName)?.primaryKey {
                            return primaryKey
                        }
                        batchUnresolvedReferences[recordName, default: []].append(.init(
                            entityName: root.entityName,
                            primaryKey: root.primaryKey,
                            propertyName: propertyName
                        ))
                        return nil
                    }
                } catch let unresolved as UnresolvedRequiredRelationshipError {
                    try storePendingRecord(record, awaitedRecordName: unresolved.recordName)
                    logger.debug("Unresolved CloudKit record awaiting required dependency.", metadata: [
                        "record_name": "\(record.recordID.recordName)",
                        "record_type": "\(record.recordType)",
                        "awaited_record_name": "\(unresolved.recordName)",
                        "property": "\(unresolved.propertyName)"
                    ])
                    continue
                }
                if existingSnapshot == nil, let missingProperty = missingRequiredAttribute(in: &incomingSnapshot) {
                    logger.error("Skipped CloudKit record with missing required attribute.", metadata: [
                        "record_name": "\(record.recordID.recordName)",
                        "record_type": "\(record.recordType)",
                        "property": "\(missingProperty.name)"
                    ])
                    continue
                }
                operations[existingSnapshot == nil ? .insert : .update, default: []].append(incomingSnapshot)
                changedRecordsToPersist.append(record)
            } else {
                if let descriptor = self.intermediaryRecordDescriptor(for: record.recordType) {
                    let sourceFieldName = descriptor.sourceFieldName
                    let destinationFieldName = descriptor.destinationFieldName
                    if let sourceRecordName = record[sourceFieldName] as? String,
                       let destinationRecordName = record[destinationFieldName] as? String {
                        let sourcePrimaryKey = recordNameToPrimaryKey[sourceRecordName]
                        ?? (try? loadRecordMetadata(recordName: sourceRecordName)?.primaryKey)
                        let destinationPrimaryKey = recordNameToPrimaryKey[destinationRecordName]
                        ?? (try? loadRecordMetadata(recordName: destinationRecordName)?.primaryKey)
                        if let sourcePrimaryKey, let destinationPrimaryKey {
                            let connection = try store.queue.connection(.writer)
                            try upsertRecordMetadata(
                                recordType: record.recordType,
                                recordName: record.recordID.recordName,
                                entityName: descriptor.ownerEntityName,
                                primaryKey: sourcePrimaryKey,
                                targetPrimaryKey: destinationPrimaryKey,
                                systemFields: record.systemFieldsData(),
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
                            changedRecordsToPersist.append(record)
                        }
                    }
                }
            }
        }
        for recordID in deletedRecordIDs.lazy {
            if let owner = try resolveRootRecordOwnership(for: recordID),
               let existingSnapshot = try snapshot(for: .identifier(
                for: store.identifier,
                entityName: owner.entityName,
                primaryKey: owner.primaryKey
               )) {
                logger.debug("Deleting local snapshot for remote tombstone...", metadata: [
                    "record_name": "\(recordID.recordName)",
                    "entity_name": "\(owner.entityName)",
                    "primary_key": "\(owner.primaryKey)"
                ])
                operations[.delete, default: []].append(existingSnapshot)
            }
            deletedRecordIDsToCleanup.append(recordID)
            try removePendingEntries(awaitedRecordName: recordID.recordName)
            try removePendingRecord(recordName: recordID.recordName)
        }
        let identityConflictRemaps = try reconcileRemoteIdentityConflicts(operations[.insert] ?? [])
        for (incomingPrimaryKey, existingPrimaryKey) in identityConflictRemaps {
            remappedPrimaryKeys[incomingPrimaryKey] = existingPrimaryKey
        }
        let orderedInsertPlan = orderedInsertPlan(inserted: operations[.insert] ?? [], updated: operations[.update] ?? [])
        logger.debug("Applying dispatch plan...", metadata: [
            "insert_count": "\((operations[.insert] ?? []).count)",
            "insert_keys": "\((operations[.insert] ?? []).map { "\($0.entityName):\($0.primaryKey)" })",
            "update_count": "\((operations[.update] ?? []).count)",
            "insert_layer_keys": "\(orderedInsertPlan.insertLayers.map { $0.map(\.primaryKey) })",
            "update_result_keys": "\(orderedInsertPlan.updateSnapshots.map(\.primaryKey))"
        ])
        for (awaitedRecordName, references) in orderedInsertPlan.deferredReferences {
            batchUnresolvedReferences[awaitedRecordName, default: []].append(contentsOf: references)
        }
        let flattenedInserts = orderedInsertPlan.insertLayers.flatMap { $0 }
        if !flattenedInserts.isEmpty || !orderedInsertPlan.updateSnapshots.isEmpty {
            let remappedInserts = try flattenedInserts.map {
                try remappedSnapshot($0, remappedPrimaryKeys: remappedPrimaryKeys)
            }
            let remappedUpdates = try orderedInsertPlan.updateSnapshots.map {
                try remappedSnapshot($0, remappedPrimaryKeys: remappedPrimaryKeys)
            }
            let result = try saveRemoteChanges(inserted: remappedInserts, updated: remappedUpdates, deleted: [])
            mergeRemappedPrimaryKeys(from: result, into: &remappedPrimaryKeys)
        }
        if let deleted = operations[.delete] {
            let result = try saveRemoteChanges(inserted: [], updated: [], deleted: deleted)
            mergeRemappedPrimaryKeys(from: result, into: &remappedPrimaryKeys)
        }
        if !deferredJoinTableInserts.isEmpty {
            for insert in deferredJoinTableInserts {
                let sourcePrimaryKey = remappedPrimaryKeys[insert.sourcePrimaryKey]
                ?? insert.sourcePrimaryKey
                let destinationPrimaryKey = remappedPrimaryKeys[insert.destinationPrimaryKey]
                ?? insert.destinationPrimaryKey
                do {
                    let connection = try store.queue.connection(.writer)
                    _ = try connection.query(
                        """
                        INSERT OR IGNORE INTO "\(insert.joinTable)" (
                            "\(insert.sourceColumn)",
                            "\(insert.destinationColumn)"
                        ) VALUES (?, ?)
                        """,
                        bindings: [sourcePrimaryKey, destinationPrimaryKey]
                    )
                } catch {
                    logger.error("Skipped join row after constraint failure: \(insert.joinTable) - \(error)", metadata: [
                        "source": "\(insert.sourceColumn) = \(sourcePrimaryKey)",
                        "destination": "\(insert.destinationColumn) = \(destinationPrimaryKey)",
                    ])
                }
            }
        }
        for record in changedRecordsToPersist {
            try persistSavedRecordMetadata(record, remappedPrimaryKeys: remappedPrimaryKeys)
            try removePendingRecord(recordName: record.recordID.recordName)
        }
        for recordID in deletedRecordIDsToCleanup {
            let connection = try store.queue.connection(.writer)
            try removeAppliedDeletedRecordMetadata(recordID, connection: connection)
        }
        if !batchUnresolvedReferences.isEmpty {
            try persistPendingReferences(batchUnresolvedReferences, remappedPrimaryKeys: remappedPrimaryKeys)
        }
        var resolvedRecordNames = Set(recordNameToPrimaryKey.keys)
        if !hasSweptPendingResolutions {
            hasSweptPendingResolutions = true
            resolvedRecordNames.formUnion(try resolvableAwaitedRecordNames())
        }
        try repairPendingReferences(for: resolvedRecordNames, remappedPrimaryKeys: remappedPrimaryKeys)
        try replayPendingRecords(awaiting: resolvedRecordNames)
        func reconcileRemoteIdentityConflicts(_ incomingInserts: [Store.Snapshot]) throws -> [String: String] {
            guard !incomingInserts.isEmpty else {
                return [:]
            }
            let connection = try store.queue.connection(.writer)
            var remaps = [String: String]()
            for snapshot in incomingInserts {
                let incomingPrimaryKey = snapshot.primaryKey
                let probe = try connection.fetchByUniqueness(snapshot, onNone: { _ in Optional<String>.none }) { existing, _ in
                    existing.primaryKey
                }
                guard let existingPrimaryKey = probe.flatMap(\.self), existingPrimaryKey != incomingPrimaryKey else {
                    continue
                }
                logger.notice("Resolving remote identity conflict.", metadata: [
                    "entity": "\(snapshot.entityName)",
                    "existing_pk": "\(existingPrimaryKey)",
                    "incoming_pk": "\(incomingPrimaryKey)"
                ])
                remaps[incomingPrimaryKey] = existingPrimaryKey
            }
            pendingResolvedConflicts += remaps.count
            return remaps
        }
        func mergeRemappedPrimaryKeys(
            from result: SyncSaveChangesResult<Store.Snapshot>,
            into remappedPrimaryKeys: inout [String: String]
        ) {
            for (sourceIdentifier, destinationIdentifier) in result.remappedIdentifiers {
                let sourcePrimaryKey = self.store.manager.primaryKey(for: sourceIdentifier)
                let destinationPrimaryKey = self.store.manager.primaryKey(for: destinationIdentifier)
                guard sourcePrimaryKey != destinationPrimaryKey else { continue }
                remappedPrimaryKeys[sourcePrimaryKey] = destinationPrimaryKey
            }
        }
        func remappedSnapshot(_ snapshot: Store.Snapshot, remappedPrimaryKeys: [String: String])
        throws -> Store.Snapshot {
            guard !remappedPrimaryKeys.isEmpty else { return snapshot }
            var remappedIdentifiers = [PersistentIdentifier: PersistentIdentifier]()
            for (property, value) in zip(snapshot.properties, snapshot.values) {
                guard property.metadata is Schema.Relationship else { continue }
                switch value {
                case let identifier as PersistentIdentifier:
                    try appendRemappedIdentifier(
                        identifier,
                        remappedPrimaryKeys: remappedPrimaryKeys,
                        into: &remappedIdentifiers
                    )
                case let identifiers as [PersistentIdentifier]:
                    for identifier in identifiers {
                        try appendRemappedIdentifier(
                            identifier,
                            remappedPrimaryKeys: remappedPrimaryKeys,
                            into: &remappedIdentifiers
                        )
                    }
                default:
                    continue
                }
            }
            let sourceIdentifier = snapshot.persistentIdentifier
            let sourcePrimaryKey = self.store.manager.primaryKey(for: sourceIdentifier)
            let destinationIdentifier: PersistentIdentifier
            if let destinationPrimaryKey = remappedPrimaryKeys[sourcePrimaryKey] {
                destinationIdentifier = try .identifier(
                    for: store.identifier,
                    entityName: sourceIdentifier.entityName,
                    primaryKey: destinationPrimaryKey
                )
            } else {
                destinationIdentifier = sourceIdentifier
            }
            guard !remappedIdentifiers.isEmpty || destinationIdentifier != sourceIdentifier else {
                return snapshot
            }
            return snapshot.copy(
                persistentIdentifier: destinationIdentifier,
                remappedIdentifiers: remappedIdentifiers
            )
        }
        // TODO: Revert back to `PersistentIdentifier` as the preferred identifier.
        func appendRemappedIdentifier(
            _ identifier: PersistentIdentifier,
            remappedPrimaryKeys: [String: String],
            into remappedIdentifiers: inout [PersistentIdentifier: PersistentIdentifier]
        ) throws {
            let primaryKey = self.store.manager.primaryKey(for: identifier)
            guard let destinationPrimaryKey = remappedPrimaryKeys[primaryKey] else { return }
            remappedIdentifiers[identifier] = try .identifier(
                for: store.identifier,
                entityName: identifier.entityName,
                primaryKey: destinationPrimaryKey
            )
        }
        func missingRequiredAttribute(in snapshot: inout Store.Snapshot) -> PropertyMetadata? {
            for property in snapshot.properties {
                guard let attribute = property.metadata as? Schema.Attribute else { continue }
                guard !property.isOptional else { continue }
                let value = snapshot.values[property.index]
                guard value is SQLNull || value is NSNull else { continue }
                guard let resolvedDefault = attribute.defaultValue as? (any DataStoreSnapshotValue)?,
                      let defaultValue = resolvedDefault,
                      snapshot.setValue(defaultValue, for: property) else {
                    return property
                }
            }
            return nil
        }
    }
}

// MARK: Applying Remote Changes

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func repairPendingReferences(
        for recordNames: Set<String>,
        remappedPrimaryKeys: [String: String]
    ) throws {
        guard !recordNames.isEmpty else { return }
        var repairedSnapshots = [Store.Snapshot]()
        var servedReferences = [(recordName: String, reference: UnresolvedToOneReference)]()
        for recordName in recordNames {
            let references = try loadPendingReferences(awaitedRecordName: recordName)
            guard !references.isEmpty else { continue }
            guard let metadata = try loadRecordMetadata(recordName: recordName) else { continue }
            let destinationPrimaryKey = remappedPrimaryKeys[metadata.primaryKey] ?? metadata.primaryKey
            for reference in references {
                let referencePrimaryKey = remappedPrimaryKeys[reference.primaryKey] ?? reference.primaryKey
                guard referencePrimaryKey != destinationPrimaryKey else {
                    logger.error("Skipped self-referential to-one relationship repair.", metadata: [
                        "record_name": "\(recordName)",
                        "entity_name": "\(reference.entityName)",
                        "property": "\(reference.propertyName)"
                    ])
                    servedReferences.append((recordName, reference))
                    continue
                }
                let identifier = try PersistentIdentifier.identifier(
                    for: store.identifier,
                    entityName: reference.entityName,
                    primaryKey: referencePrimaryKey
                )
                guard var snapshot = try self.snapshot(for: identifier) else {
                    servedReferences.append((recordName, reference))
                    continue
                }
                guard let property = snapshot.properties.first(where: { $0.name == reference.propertyName }),
                      let relationship = property.metadata as? Schema.Relationship else {
                    servedReferences.append((recordName, reference))
                    continue
                }
                do {
                    try snapshot.setValue(relationship, destinationPrimaryKey, at: property.index)
                    repairedSnapshots.append(snapshot)
                    servedReferences.append((recordName, reference))
                } catch {
                    logger.error("Failed to repair unresolved to-one relationship: \(error)", metadata: [
                        "record_name": "\(recordName)",
                        "entity_name": "\(reference.entityName)",
                        "property": "\(reference.propertyName)"
                    ])
                }
            }
        }
        if !repairedSnapshots.isEmpty {
            try saveRemoteChanges(inserted: [], updated: repairedSnapshots, deleted: [])
            logger.notice("Repaired unresolved to-one relationship references: \(repairedSnapshots.count)")
        }
        for served in servedReferences {
            try removePendingReference(awaitedRecordName: served.recordName, reference: served.reference)
        }
    }
    
    internal func replayPendingRecords(awaiting recordNames: Set<String>) throws {
        guard !recordNames.isEmpty else { return }
        let pendingRecords = try loadPendingRecords(awaiting: recordNames)
        guard !pendingRecords.isEmpty else { return }
        for record in pendingRecords {
            try removePendingRecord(recordName: record.recordID.recordName)
        }
        logger.notice("Replaying pending CloudKit records: \(pendingRecords.count)")
        try applyRemoteChanges(changedRecords: pendingRecords, deletedRecordIDs: [])
    }
    
    internal func persistPendingReferences(
        _ references: [String: [UnresolvedToOneReference]],
        remappedPrimaryKeys: [String: String]
    ) throws {
        let connection = try store.queue.connection(.writer)
        for (recordName, entries) in references {
            for entry in entries {
                _ = try connection.query(
                    """
                    INSERT OR REPLACE INTO \(PendingReferenceTable.tableName) (
                        \(PendingReferenceTable.storeIdentifier.rawValue),
                        \(PendingReferenceTable.awaitedRecordName.rawValue),
                        \(PendingReferenceTable.entityName.rawValue),
                        \(PendingReferenceTable.entityPrimaryKey.rawValue),
                        \(PendingReferenceTable.propertyName.rawValue)
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    bindings: [
                        store.identifier,
                        recordName,
                        entry.entityName,
                        remappedPrimaryKeys[entry.primaryKey] ?? entry.primaryKey,
                        entry.propertyName
                    ]
                )
            }
        }
    }
    
    internal func loadPendingReferences(awaitedRecordName: String) throws -> [UnresolvedToOneReference] {
        let rows = try store.queue.connection(.reader).query(
            """
            SELECT
                \(PendingReferenceTable.entityName.rawValue),
                \(PendingReferenceTable.entityPrimaryKey.rawValue),
                \(PendingReferenceTable.propertyName.rawValue)
            FROM \(PendingReferenceTable.tableName)
            WHERE \(PendingReferenceTable.storeIdentifier.rawValue) = ?
            AND \(PendingReferenceTable.awaitedRecordName.rawValue) = ?
            """,
            bindings: [store.identifier, awaitedRecordName]
        )
        return rows.compactMap { row in
            guard let entityName = row["entity_name"] as? String,
                  let primaryKey = row["entity_pk"] as? String,
                  let propertyName = row["property_name"] as? String else {
                return nil
            }
            return .init(entityName: entityName, primaryKey: primaryKey, propertyName: propertyName)
        }
    }
    
    internal func removePendingReference(
        awaitedRecordName: String,
        reference: UnresolvedToOneReference
    ) throws {
        _ = try store.queue.connection(.writer).query(
            """
            DELETE FROM \(PendingReferenceTable.tableName)
            WHERE \(PendingReferenceTable.storeIdentifier.rawValue) = ?
            AND \(PendingReferenceTable.awaitedRecordName.rawValue) = ?
            AND \(PendingReferenceTable.entityName.rawValue) = ?
            AND \(PendingReferenceTable.entityPrimaryKey.rawValue) = ?
            AND \(PendingReferenceTable.propertyName.rawValue) = ?
            """,
            bindings: [
                store.identifier,
                awaitedRecordName,
                reference.entityName,
                reference.primaryKey,
                reference.propertyName
            ]
        )
    }
    
    internal func removePendingEntries(awaitedRecordName: String) throws {
        let connection = try store.queue.connection(.writer)
        _ = try connection.query(
            """
            DELETE FROM \(PendingReferenceTable.tableName)
            WHERE \(PendingReferenceTable.storeIdentifier.rawValue) = ?
            AND \(PendingReferenceTable.awaitedRecordName.rawValue) = ?
            """,
            bindings: [store.identifier, awaitedRecordName]
        )
        _ = try connection.query(
            """
            DELETE FROM \(PendingRecordTable.tableName)
            WHERE \(PendingRecordTable.storeIdentifier.rawValue) = ?
            AND \(PendingRecordTable.awaitedRecordName.rawValue) = ?
            """,
            bindings: [store.identifier, awaitedRecordName]
        )
    }
    
    internal func storePendingRecord(_ record: CKRecord, awaitedRecordName: String) throws {
        let archive = try record.fullRecordData()
        _ = try store.queue.connection(.writer).query(
            """
            INSERT OR REPLACE INTO \(PendingRecordTable.tableName) (
                \(PendingRecordTable.storeIdentifier.rawValue),
                \(PendingRecordTable.recordName.rawValue),
                \(PendingRecordTable.awaitedRecordName.rawValue),
                \(PendingRecordTable.recordArchive.rawValue)
            ) VALUES (?, ?, ?, ?)
            """,
            bindings: [store.identifier, record.recordID.recordName, awaitedRecordName, archive]
        )
    }
    
    internal func loadPendingRecords(awaiting recordNames: Set<String>) throws -> [CKRecord] {
        guard !recordNames.isEmpty else { return [] }
        let names = Array(recordNames)
        let placeholders = Array(repeating: "?", count: names.count).joined(separator: ", ")
        let rows = try store.queue.connection(.reader).query(
            """
            SELECT \(PendingRecordTable.recordArchive.rawValue)
            FROM \(PendingRecordTable.tableName)
            WHERE \(PendingRecordTable.storeIdentifier.rawValue) = ?
            AND \(PendingRecordTable.awaitedRecordName.rawValue) IN (\(placeholders))
            """,
            bindings: [store.identifier] + (names as [any Sendable])
        )
        return rows.compactMap { row in
            guard let archive = row["record_archive"] as? Data else { return nil }
            return try? CKRecord.fromFullRecordData(archive)
        }
    }
    
    internal func removePendingRecord(recordName: String) throws {
        _ = try store.queue.connection(.writer).query(
            """
            DELETE FROM \(PendingRecordTable.tableName)
            WHERE \(PendingRecordTable.storeIdentifier.rawValue) = ?
            AND \(PendingRecordTable.recordName.rawValue) = ?
            """,
            bindings: [store.identifier, recordName]
        )
    }
    
    internal func resolvableAwaitedRecordNames() throws -> Set<String> {
        let rows = try store.queue.connection(.reader).query(
            """
            SELECT DISTINCT awaited FROM (
                SELECT \(PendingReferenceTable.awaitedRecordName.rawValue) AS awaited
                FROM \(PendingReferenceTable.tableName)
                WHERE \(PendingReferenceTable.storeIdentifier.rawValue) = ?
                UNION
                SELECT \(PendingRecordTable.awaitedRecordName.rawValue) AS awaited
                FROM \(PendingRecordTable.tableName)
                WHERE \(PendingRecordTable.storeIdentifier.rawValue) = ?
            )
            WHERE awaited IN (
                SELECT \(RecordMetadataTable.recordName.rawValue)
                FROM \(RecordMetadataTable.tableName)
                WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            )
            """,
            bindings: [store.identifier, store.identifier, store.identifier]
        )
        return .init(rows.compactMap { $0["awaited"] as? String })
    }
    
    internal func pendingUnresolvedCount() throws -> Int {
        let rows = try store.queue.connection(.reader).query(
            """
            SELECT (
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT
                        \(PendingReferenceTable.entityName.rawValue),
                        \(PendingReferenceTable.entityPrimaryKey.rawValue)
                    FROM \(PendingReferenceTable.tableName)
                    WHERE \(PendingReferenceTable.storeIdentifier.rawValue) = ?
                )
            ) + (
                SELECT COUNT(*)
                FROM \(PendingRecordTable.tableName)
                WHERE \(PendingRecordTable.storeIdentifier.rawValue) = ?
            ) AS unresolved_count
            """,
            bindings: [store.identifier, store.identifier]
        )
        return (rows.first?["unresolved_count"] as? Int64).map(Int.init) ?? 0
    }
    
    public func republishLocalRecords() async throws {
        try saveState(lastEnqueuedHistoryPrimaryKey: 0, clearErrorCode: true)
        try await sync()
        let unresolvedCount = (try? pendingUnresolvedCount()) ?? 0
        await store.reportPendingUnresolvedCount(for: self.id, count: unresolvedCount)
    }
    
    internal func orderedInsertPlan(inserted: [Store.Snapshot], updated: [Store.Snapshot]) -> OrderedInsertPlan {
        var deferredReferences = [String: [UnresolvedToOneReference]]()
        var insertLayers = [[Store.Snapshot]]()
        insertLayers.reserveCapacity(inserted.count)
        let insertSnapshots = Dictionary(inserted.map { ($0.primaryKey, $0) }, uniquingKeysWith: { $1 })
        var updateSnapshots = Dictionary(updated.map { ($0.primaryKey, $0) }, uniquingKeysWith: { $1 })
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
                        let relatedPrimaryKey = self.store.manager.primaryKey(for: relatedIdentifier)
                        if remainingInsertSnapshots[relatedPrimaryKey] != nil {
                            if property.isOptional {
                                insert.values[property.index] = SQLNull()
                                requiresFollowUpUpdate = true
                                continue
                            }
                            hasRequiredDependency = true
                            break
                        }
                        if insertSnapshots[relatedPrimaryKey] != nil {
                            continue
                        }
                        if updateSnapshots[relatedPrimaryKey] != nil {
                            continue
                        }
                        if (try? self.snapshot(for: relatedIdentifier)) != nil {
                            logger.debug("Satisfied dependency from local snapshot: \(property)")
                        } else {
                            logger.warning("Insert dependency is neither batched nor stored locally: \(property) = \(relatedPrimaryKey)")
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
                        if !relatedIdentifiers.isEmpty { requiresFollowUpUpdate = true }
                    }
                }
                guard !hasRequiredDependency else {
                    continue
                }
                layer.append(insert)
                if requiresFollowUpUpdate {
                    updateSnapshots[snapshot.primaryKey] = snapshot
                }
            }
            guard !layer.isEmpty else {
                insertLayers.append(Array(remainingInsertSnapshots.values))
                break
            }
            insertLayers.append(layer)
            for snapshot in layer {
                remainingInsertSnapshots[snapshot.primaryKey] = nil
            }
        }
        let batchedPrimaryKeys = Set(insertSnapshots.keys).union(updateSnapshots.keys)
        let sanitizedInsertLayers = insertLayers.map { $0.map(nullingDanglingOptionalReferences) }
        let insertedPrimaryKeys = Set(sanitizedInsertLayers.flatMap(\.self).map(\.primaryKey))
        let updateResult = (updated + inserted.compactMap { snapshot in
            guard insertedPrimaryKeys.contains(snapshot.primaryKey),
                  updateSnapshots[snapshot.primaryKey] != nil else {
                return nil
            }
            return snapshot
        }).map(nullingDanglingOptionalReferences)
        func nullingDanglingOptionalReferences(in snapshot: Store.Snapshot) -> Store.Snapshot {
            var sanitizedSnapshot = snapshot
            for property in snapshot.properties {
                guard let relationship = property.metadata as? Schema.Relationship,
                      relationship.isToOneRelationship,
                      property.isOptional else {
                    continue
                }
                guard let relatedIdentifier = snapshot.values[property.index] as? PersistentIdentifier else {
                    continue
                }
                let relatedPrimaryKey = self.store.manager.primaryKey(for: relatedIdentifier)
                if batchedPrimaryKeys.contains(relatedPrimaryKey) { continue }
                if (try? self.snapshot(for: relatedIdentifier)) != nil { continue }
                sanitizedSnapshot.values[property.index] = SQLNull()
                logger.warning("Nulled dangling optional reference: \(property) = \(relatedPrimaryKey)")
                if let awaitedRecordName = try? loadRecordMetadata(
                    recordType: makeRecordType(relationship.destination),
                    entityName: relationship.destination,
                    primaryKey: relatedPrimaryKey
                )?.recordName {
                    deferredReferences[awaitedRecordName, default: []].append(
                        UnresolvedToOneReference(
                            entityName: snapshot.entityName,
                            primaryKey: snapshot.primaryKey,
                            propertyName: property.name
                        )
                    )
                }
            }
            return sanitizedSnapshot
        }
        return .init(
            insertLayers: sanitizedInsertLayers,
            updateSnapshots: updateResult,
            deferredReferences: deferredReferences
        )
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
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func scheduleInitialUploadIfNeeded() throws {
        try initializeSyncEngineIfNeeded()
        let state = try loadState()
        guard !state.didBootstrapZone else {
            return
        }
        syncEngine?.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
        try saveState(clearErrorCode: true, didBootstrapZone: true)
    }
    
    internal func resetForAccountChange() throws {
        enqueuedChangesByRecordID.removeAll()
        try resetLocalState(deleteRecordMetadata: true)
        self.syncEngine = nil
        self.didPrepare = false
    }
}

#endif
