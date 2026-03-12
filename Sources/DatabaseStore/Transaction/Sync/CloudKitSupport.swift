//
//  CloudKitSupport.swift
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
import CloudKit
import SwiftData
#else
@preconcurrency import CloudKit
@preconcurrency import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

public enum CloudKitState: String {
    nonisolated public static let tableName: String = "_CloudKitState"
    case storeIdentifier = "store_identifier"
    case lastExportedHistoryPrimaryKey = "last_exported_history_pk"
    case lastExportedTransactionIdentifier = "last_exported_transaction_identifier"
    case serverChangeTokenBase64Encoded = "server_change_token_base64_encoded"
    case didBootstrapZone = "did_bootstrap_zone"
    case lastSyncAtMicroseconds = "last_sync_at_microseconds"
    case lastErrorCode = "last_error_code"
    
    nonisolated internal static var createTable: String {
        """
        CREATE TABLE IF NOT EXISTS \(Self.tableName) (
            \(Self.storeIdentifier.rawValue) TEXT PRIMARY KEY,
            \(Self.lastExportedHistoryPrimaryKey.rawValue) INTEGER NOT NULL DEFAULT 0,
            \(Self.lastExportedTransactionIdentifier.rawValue) INTEGER NOT NULL DEFAULT 0,
            \(Self.serverChangeTokenBase64Encoded.rawValue) TEXT,
            \(Self.didBootstrapZone.rawValue) INTEGER NOT NULL DEFAULT 0,
            \(Self.lastSyncAtMicroseconds.rawValue) INTEGER NOT NULL DEFAULT 0,
            \(Self.lastErrorCode.rawValue) TEXT
        )
        """
    }
    
    nonisolated internal static let requiredColumns: [(String, String)] = [
        (Self.lastExportedHistoryPrimaryKey.rawValue, "INTEGER NOT NULL DEFAULT 0"),
        (Self.lastExportedTransactionIdentifier.rawValue, "INTEGER NOT NULL DEFAULT 0"),
        (Self.serverChangeTokenBase64Encoded.rawValue, "TEXT"),
        (Self.didBootstrapZone.rawValue, "INTEGER NOT NULL DEFAULT 0"),
        (Self.lastSyncAtMicroseconds.rawValue, "INTEGER NOT NULL DEFAULT 0"),
        (Self.lastErrorCode.rawValue, "TEXT")
    ]
}

extension CloudKitState: CustomStringConvertible {
    nonisolated public var description: String { rawValue }
}

public enum CloudKitRecordMetadata: String {
    nonisolated public static let tableName: String = "_CloudKitRecordMetadata"
    case storeIdentifier = "store_identifier"
    case recordName = "record_name"
    case recordType = "record_type"
    case entityName = "entity_name"
    case entityPrimaryKey = "entity_pk"
    case systemFieldsBase64Encoded = "system_fields_base64_encoded"
    
    nonisolated internal static var createTable: String {
        """
        CREATE TABLE IF NOT EXISTS \(CloudKitRecordMetadata.tableName) (
            \(CloudKitRecordMetadata.storeIdentifier.rawValue) TEXT NOT NULL,
            \(CloudKitRecordMetadata.recordName.rawValue) TEXT NOT NULL,
            \(CloudKitRecordMetadata.recordType.rawValue) TEXT NOT NULL,
            \(CloudKitRecordMetadata.entityName.rawValue) TEXT NOT NULL,
            \(CloudKitRecordMetadata.entityPrimaryKey.rawValue) TEXT NOT NULL,
            \(CloudKitRecordMetadata.systemFieldsBase64Encoded.rawValue) TEXT,
            PRIMARY KEY (
                \(CloudKitRecordMetadata.storeIdentifier.rawValue),
                \(CloudKitRecordMetadata.recordName.rawValue)
            )
        )
        """
    }
    
    nonisolated internal static var createIndex: String {
        """
        CREATE INDEX IF NOT EXISTS CloudKit_Metadata_Entity_Index
        ON \(CloudKitRecordMetadata.tableName) (
            \(CloudKitRecordMetadata.storeIdentifier.rawValue),
            \(CloudKitRecordMetadata.entityName.rawValue),
            \(CloudKitRecordMetadata.entityPrimaryKey.rawValue)
        )
        """
    }
    
    nonisolated internal static let requiredColumns: [(String, String)] = [
        (Self.storeIdentifier.rawValue, "TEXT NOT NULL"),
        (Self.recordType.rawValue, "TEXT NOT NULL"),
        (Self.entityName.rawValue, "TEXT NOT NULL"),
        (Self.entityPrimaryKey.rawValue, "TEXT NOT NULL"),
        (Self.systemFieldsBase64Encoded.rawValue, "TEXT")
    ]
}

extension CloudKitRecordMetadata: CustomStringConvertible {
    nonisolated public var description: String { rawValue }
}

extension DatabaseStore {
    nonisolated internal var createCloudKitTablesSQL: [String] {
        [
            CloudKitState.createTable,
            CloudKitRecordMetadata.createTable,
            CloudKitRecordMetadata.createIndex
        ]
    }
}

public struct CloudKitSyncState: Sendable {
    nonisolated internal var task: Task<Void, Never>?
    nonisolated internal var pending: Bool
    
    nonisolated internal init(task: Task<Void, Never>? = nil, pending: Bool = false) {
        self.task = task
        self.pending = pending
    }
}

public protocol CloudKitSyncDelegate: Sendable {
    func shouldSyncEntity(_ entityName: String) -> Bool
    func shouldSyncColumn(_ columnName: String, entityName: String) -> Bool
    func recordType(for entityName: String) -> String
}

public extension CloudKitSyncDelegate {
    func shouldSyncEntity(_ entityName: String) -> Bool { true }
    func shouldSyncColumn(_ columnName: String, entityName: String) -> Bool { true }
    func recordType(for entityName: String) -> String {
        CloudKitIDs.recordType(for: entityName)
    }
}

public struct DefaultCloudKitSyncDelegate: CloudKitSyncDelegate, Sendable {
    public var excludedEntities: Set<String>
    public var excludedColumns: Set<String>
    
    public init(excludedEntities: Set<String> = [], excludedColumns: Set<String> = []) {
        self.excludedEntities = excludedEntities
        self.excludedColumns = excludedColumns
    }
    
    public func shouldSyncEntity(_ entityName: String) -> Bool {
        !excludedEntities.contains(entityName)
    }
    
    public func shouldSyncColumn(_ columnName: String, entityName: String) -> Bool {
        !excludedColumns.contains(columnName)
    }
    
    public func recordType(for entityName: String) -> String {
        CloudKitIDs.recordType(for: entityName)
    }
}

public struct CloudKitConfiguration: Sendable {
    nonisolated package let containerIdentifier: String?
    nonisolated package let remoteAuthor: String
    nonisolated package let zoneName: String
    nonisolated package let databaseScope: CKDatabase.Scope
    nonisolated package let delegate: any CloudKitSyncDelegate
    
    nonisolated public init(
        containerIdentifier: String?,
        remoteAuthor: String = "CloudKit",
        zoneName: String? = nil,
        databaseScope: CKDatabase.Scope = .private,
        delegate: (any CloudKitSyncDelegate)? = nil
    ) {
        self.containerIdentifier = containerIdentifier
        self.remoteAuthor = remoteAuthor
        self.zoneName = zoneName
        ?? "\((Bundle.main.bundleIdentifier ?? "Application")).DataStoreKit"
        self.databaseScope = databaseScope
        self.delegate = delegate ?? DefaultCloudKitSyncDelegate(
            excludedEntities: [
                CloudKitState.tableName,
                CloudKitRecordMetadata.tableName,
                HistoryTable.tableName,
                ArchiveTable.tableName,
                InternalTable.tableName
            ]
        )
    }
}

internal enum CloudKitMirrorField {
    nonisolated static let primaryKey = "pk"
    nonisolated static let schemaVersion = "schema_version"
    nonisolated static let updatedAtMicroseconds = "updated_at_us"
    nonisolated static let payloadEncoding = "payload_encoding"
    nonisolated static let payloadInline = "payload_inline"
    nonisolated static let payloadAsset = "payload_asset"
}

internal enum CloudKitPayloadEncoding: String, Codable, Sendable {
    case inlineData
    case asset
}

internal struct CloudKitSnapshotEnvelope: Codable, Sendable {
    nonisolated var schemaVersion: UInt16
    nonisolated var snapshot: DatabaseSnapshot
    
    nonisolated init(schemaVersion: UInt16 = 1, snapshot: DatabaseSnapshot) {
        self.schemaVersion = schemaVersion
        self.snapshot = snapshot
    }
}

internal enum CloudKitIDs {
    nonisolated internal static func recordType(for entityName: String) -> String {
        entityName.replacingOccurrences(of: ".", with: "_")
    }
    
    nonisolated internal static func recordName(
        storeIdentifier: String,
        entityName: String,
        primaryKey: String
    ) -> String {
        "\(storeIdentifier)|\(entityName)|\(primaryKey)"
    }
    
    nonisolated internal static func parseRecordName(_ recordName: String) -> (
        storeIdentifier: String,
        entityName: String,
        primaryKey: String
    )? {
        let parts = recordName.split(separator: "|", maxSplits: 2).map(String.init)
        guard parts.count == 3 else { return nil }
        guard !parts[0].isEmpty, !parts[1].isEmpty, !parts[2].isEmpty else {
            return nil
        }
        return (parts[0], parts[1], parts[2])
    }
}

internal extension CKRecord {
    nonisolated func systemFieldsData() -> Data {
        let archiver = NSKeyedArchiver(requiringSecureCoding: true)
        encodeSystemFields(with: archiver)
        archiver.finishEncoding()
        return archiver.encodedData
    }
    
    nonisolated static func fromSystemFields(_ data: Data) throws -> CKRecord {
        let unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        unarchiver.requiresSecureCoding = true
        guard let record = CKRecord(coder: unarchiver) else {
            throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Unable to decode CKRecord system fields."))
        }
        unarchiver.finishDecoding()
        return record
    }
}

internal struct CloudKitLocalMirrorState: Sendable {
    nonisolated var lastExportedHistoryPrimaryKey: Int64
    nonisolated var lastExportedTransactionIdentifier: Int64
    nonisolated var token: CKServerChangeToken?
    nonisolated var didBootstrapZone: Bool
    nonisolated var lastSyncAtMicroseconds: Int64
    nonisolated var lastErrorCode: String?
}

internal struct CloudKitHistoryRow: Sendable {
    nonisolated let historyPrimaryKey: Int64
    nonisolated let changeType: String
    nonisolated let transactionIdentifier: Int64
    nonisolated let entityName: String
    nonisolated let entityPrimaryKey: String
    nonisolated let author: String?
    nonisolated let context: String?
}

internal enum CloudKitPendingOperation: UInt8, Sendable {
    case upsert = 0
    case delete = 1
}

internal struct CloudKitPendingChangeKey: Hashable, Sendable {
    nonisolated let entityName: String
    nonisolated let primaryKey: String
}

internal struct CloudKitPendingChange: Sendable {
    nonisolated let key: CloudKitPendingChangeKey
    nonisolated var operation: CloudKitPendingOperation
    nonisolated var historyPrimaryKey: Int64
    nonisolated var transactionIdentifier: Int64
    nonisolated var changedPropertyNames: Set<String>?
    nonisolated var author: String?
    
    nonisolated var entityName: String { key.entityName }
    nonisolated var primaryKey: String { key.primaryKey }
}

internal struct CloudKitFetchResult: Sendable {
    nonisolated var changed: [CKRecord]
    nonisolated var deleted: [CKRecord.ID]
    nonisolated var newToken: CKServerChangeToken?
    nonisolated var changeTokenExpired: Bool
}

@DatabaseActor public final class CloudKitReplicator {
    nonisolated private unowned let store: DatabaseStore
    private let configuration: CloudKitConfiguration
    private let container: CKContainer
    private let database: CKDatabase
    private let zoneID: CKRecordZone.ID
    private let subscriptionID: String
    private let payloadInlineThresholdBytes: Int
    private let jsonEncoder: JSONEncoder
    private let jsonDecoder: JSONDecoder
    
    nonisolated internal init(
        store: DatabaseStore,
        configuration: CloudKitConfiguration,
        payloadInlineThresholdBytes: Int = 900_000
    ) {
        let container: CKContainer = {
            if let identifier = configuration.containerIdentifier {
                return CKContainer(identifier: identifier)
            }
            return CKContainer.default()
        }()
        let database: CKDatabase
        switch configuration.databaseScope {
        case .private: database = container.privateCloudDatabase
        case .public: database = container.publicCloudDatabase
        case .shared: database = container.sharedCloudDatabase
        @unknown default:
            fatalError("Unsupported CKDatabase.Scope: \(configuration.databaseScope)")
        }
        self.store = store
        self.configuration = configuration
        self.container = container
        self.database = database
        self.zoneID = CKRecordZone.ID(
            zoneName: configuration.zoneName,
            ownerName: CKCurrentUserDefaultName
        )
        self.subscriptionID = "\(configuration.zoneName).subscription"
        self.payloadInlineThresholdBytes = payloadInlineThresholdBytes
        self.jsonEncoder = JSONEncoder()
        self.jsonDecoder = JSONDecoder()
    }
    
    public func prepare() async throws {
        let _ = try await container.accountStatus()
        try createCloudKitTables()
        try await ensureZoneExists()
        try await ensureSubscriptionExists()
        try saveState(didBootstrapZone: true)
    }
    
    public func sync() async throws {
        try await pushLocalChanges()
        try await pullRemoteChanges()
    }
    
    public func resetRemoteZone() async throws {
        try await deleteAllRemoteData()
        try await ensureZoneExists()
        try await ensureSubscriptionExists()
        try resetLocalMirrorState()
        try saveState(didBootstrapZone: true)
    }
    
    public func deleteAllRemoteData() async throws {
        try await withCheckedThrowingContinuation { continuation in
            let operation = CKModifyRecordZonesOperation(
                recordZonesToSave: nil,
                recordZoneIDsToDelete: [zoneID]
            )
            operation.modifyRecordZonesResultBlock = { result in
                switch result {
                case .success:
                    continuation.resume(returning: ())
                case .failure(let error):
                    if let ckError = error as? CKError, ckError.code == .zoneNotFound {
                        continuation.resume(returning: ())
                    } else {
                        continuation.resume(throwing: error)
                    }
                }
            }
            database.add(operation)
        }
    }
    
    private func createCloudKitTables() throws {
        let connection = try store.queue.connection(.writer)
        for sql in store.createCloudKitTablesSQL {
            try connection.execute(sql)
        }
        try ensureCloudKitStateColumns(connection: connection)
        try ensureCloudKitMetadataColumns(connection: connection)
        try ensureStateRowExists(connection: connection)
    }
    
    private func ensureCloudKitStateColumns(
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        try ensureColumns(
            tableName: CloudKitState.tableName,
            columns: CloudKitState.requiredColumns,
            connection: connection
        )
    }
    
    private func ensureCloudKitMetadataColumns(
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        try ensureColumns(
            tableName: CloudKitRecordMetadata.tableName,
            columns: CloudKitRecordMetadata.requiredColumns,
            connection: connection
        )
    }
    
    private func ensureColumns(
        tableName: String,
        columns: [(String, String)],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        let existingColumns = Set(try connection.query(
            "PRAGMA table_info(\(tableName))"
        ).compactMap { $0["name"] as? String })
        for (name, definition) in columns where !existingColumns.contains(name) {
            try connection.execute("ALTER TABLE \(tableName) ADD COLUMN \(name) \(definition)")
        }
    }
    
    private func ensureStateRowExists(
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        let exists = (try connection.query(
            """
            SELECT 1
            FROM \(CloudKitState.tableName)
            WHERE \(CloudKitState.storeIdentifier.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier]
        ).first) != nil
        if !exists {
            try connection.execute.insert(into: CloudKitState.tableName, values: [
                CloudKitState.storeIdentifier.rawValue: store.identifier,
                CloudKitState.lastExportedHistoryPrimaryKey.rawValue: Int64(0),
                CloudKitState.lastExportedTransactionIdentifier.rawValue: Int64(0),
                CloudKitState.serverChangeTokenBase64Encoded.rawValue: "",
                CloudKitState.didBootstrapZone.rawValue: 0,
                CloudKitState.lastSyncAtMicroseconds.rawValue: Int64(0),
                CloudKitState.lastErrorCode.rawValue: NSNull()
            ])
        }
    }
    
    private func ensureZoneExists() async throws {
        let zone = CKRecordZone(zoneID: zoneID)
        try await withCheckedThrowingContinuation { continuation in
            let operation = CKModifyRecordZonesOperation(
                recordZonesToSave: [zone],
                recordZoneIDsToDelete: nil
            )
            operation.modifyRecordZonesResultBlock = { result in
                switch result {
                case .success:
                    continuation.resume(returning: ())
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            }
            database.add(operation)
        }
    }
    
    private func ensureSubscriptionExists() async throws {
        let subscription = CKRecordZoneSubscription(zoneID: zoneID, subscriptionID: subscriptionID)
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        try await withCheckedThrowingContinuation { continuation in
            let operation = CKModifySubscriptionsOperation(
                subscriptionsToSave: [subscription],
                subscriptionIDsToDelete: nil
            )
            operation.modifySubscriptionsResultBlock = { result in
                switch result {
                case .success:
                    continuation.resume(returning: ())
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            }
            database.add(operation)
        }
    }
    
    private func loadState() throws -> CloudKitLocalMirrorState {
        let connection = try store.queue.connection(nil)
        let row = try connection.query(
            """
            SELECT
                \(CloudKitState.lastExportedHistoryPrimaryKey.rawValue) AS history_pk,
                \(CloudKitState.lastExportedTransactionIdentifier.rawValue) AS transaction_id,
                \(CloudKitState.serverChangeTokenBase64Encoded.rawValue) AS token,
                \(CloudKitState.didBootstrapZone.rawValue) AS did_bootstrap_zone,
                \(CloudKitState.lastSyncAtMicroseconds.rawValue) AS last_sync_at,
                \(CloudKitState.lastErrorCode.rawValue) AS last_error_code
            FROM \(CloudKitState.tableName)
            WHERE \(CloudKitState.storeIdentifier.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier]
        ).first
        let encodedToken = (row?["token"] as? String) ?? ""
        let token: CKServerChangeToken? = {
            guard !encodedToken.isEmpty,
                  let data = Data(base64Encoded: encodedToken) else {
                return nil
            }
            return try? NSKeyedUnarchiver.unarchivedObject(
                ofClass: CKServerChangeToken.self,
                from: data
            )
        }()
        return .init(
            lastExportedHistoryPrimaryKey: row?["history_pk"] as? Int64 ?? 0,
            lastExportedTransactionIdentifier: row?["transaction_id"] as? Int64 ?? 0,
            token: token,
            didBootstrapZone: (row?["did_bootstrap_zone"] as? Int64 ?? 0) != 0,
            lastSyncAtMicroseconds: row?["last_sync_at"] as? Int64 ?? 0,
            lastErrorCode: row?["last_error_code"] as? String
        )
    }
    
    private func saveState(
        exportedHistoryPrimaryKey: Int64? = nil,
        exportedTransactionIdentifier: Int64? = nil,
        token: CKServerChangeToken? = nil,
        errorCode: String? = nil,
        didBootstrapZone: Bool? = nil
    ) throws {
        let connection = try store.queue.connection(.writer)
        let current = try loadState()
        let tokenString: String = {
            switch token {
            case nil:
                guard let currentToken = current.token else {
                    return ""
                }
                let data = try? NSKeyedArchiver.archivedData(
                    withRootObject: currentToken,
                    requiringSecureCoding: true
                )
                return data?.base64EncodedString() ?? ""
            case let token?:
                let data = try? NSKeyedArchiver.archivedData(
                    withRootObject: token,
                    requiringSecureCoding: true
                )
                return data?.base64EncodedString() ?? ""
            }
        }()
        let now = Int64(Date().timeIntervalSince1970 * 1_000_000)
        try PreparedStatement(
            sql: """
            UPDATE \(CloudKitState.tableName)
            SET
                \(CloudKitState.lastExportedHistoryPrimaryKey.rawValue) = ?,
                \(CloudKitState.lastExportedTransactionIdentifier.rawValue) = ?,
                \(CloudKitState.serverChangeTokenBase64Encoded.rawValue) = ?,
                \(CloudKitState.didBootstrapZone.rawValue) = ?,
                \(CloudKitState.lastSyncAtMicroseconds.rawValue) = ?,
                \(CloudKitState.lastErrorCode.rawValue) = ?
            WHERE \(CloudKitState.storeIdentifier.rawValue) = ?
            """,
            bindings: [
                sendable(cast: exportedHistoryPrimaryKey ?? current.lastExportedHistoryPrimaryKey),
                exportedTransactionIdentifier ?? current.lastExportedTransactionIdentifier,
                tokenString,
                (didBootstrapZone ?? current.didBootstrapZone) ? 1 : 0,
                now,
                errorCode ?? current.lastErrorCode ?? NSNull(),
                store.identifier
            ],
            handle: connection.handle
        ).run()
    }
    
    private func resetLocalMirrorState() throws {
        let connection = try store.queue.connection(.writer)
        try PreparedStatement(
            sql: """
            UPDATE \(CloudKitState.tableName)
            SET
                \(CloudKitState.lastExportedHistoryPrimaryKey.rawValue) = 0,
                \(CloudKitState.lastExportedTransactionIdentifier.rawValue) = 0,
                \(CloudKitState.serverChangeTokenBase64Encoded.rawValue) = '',
                \(CloudKitState.didBootstrapZone.rawValue) = 0,
                \(CloudKitState.lastSyncAtMicroseconds.rawValue) = 0,
                \(CloudKitState.lastErrorCode.rawValue) = NULL
            WHERE \(CloudKitState.storeIdentifier.rawValue) = ?
            """,
            bindings: [store.identifier],
            handle: connection.handle
        ).run()
        try PreparedStatement(
            sql: """
            DELETE FROM \(CloudKitRecordMetadata.tableName)
            WHERE \(CloudKitRecordMetadata.storeIdentifier.rawValue) = ?
            """,
            bindings: [store.identifier],
            handle: connection.handle
        ).run()
    }
    
    public func pushLocalChanges() async throws {
        let state = try loadState()
        let rows = try fetchHistoryRows(
            afterHistoryPrimaryKey: state.lastExportedHistoryPrimaryKey,
            excludingAuthor: configuration.remoteAuthor
        )
        guard !rows.isEmpty else { return }
        let changes = try coalescePendingChanges(rows)
        guard !changes.isEmpty else {
            if let last = rows.last {
                try saveState(
                    exportedHistoryPrimaryKey: last.historyPrimaryKey,
                    exportedTransactionIdentifier: last.transactionIdentifier,
                    errorCode: nil
                )
            }
            return
        }
        var lastExportedHistoryPrimaryKey = state.lastExportedHistoryPrimaryKey
        var lastExportedTransactionIdentifier = state.lastExportedTransactionIdentifier
        do {
            for change in changes {
                switch change.operation {
                case .delete:
                    try await pushDelete(change)
                case .upsert:
                    guard let snapshot = try fetchCurrentSnapshot(
                        entityName: change.entityName,
                        primaryKey: change.primaryKey
                    ) else {
                        try await pushDelete(change)
                        lastExportedHistoryPrimaryKey = max(lastExportedHistoryPrimaryKey, change.historyPrimaryKey)
                        lastExportedTransactionIdentifier = max(lastExportedTransactionIdentifier, change.transactionIdentifier)
                        try saveState(
                            exportedHistoryPrimaryKey: lastExportedHistoryPrimaryKey,
                            exportedTransactionIdentifier: lastExportedTransactionIdentifier,
                            errorCode: nil
                        )
                        continue
                    }
                    try await pushUpsert(change, snapshot: snapshot)
                }
                lastExportedHistoryPrimaryKey = max(lastExportedHistoryPrimaryKey, change.historyPrimaryKey)
                lastExportedTransactionIdentifier = max(lastExportedTransactionIdentifier, change.transactionIdentifier)
                try saveState(
                    exportedHistoryPrimaryKey: lastExportedHistoryPrimaryKey,
                    exportedTransactionIdentifier: lastExportedTransactionIdentifier,
                    errorCode: nil
                )
            }
        } catch {
            try? saveState(errorCode: cloudKitErrorCodeString(error))
            throw error
        }
    }
    
    public func pullRemoteChanges() async throws {
        let state = try loadState()
        do {
            let result = try await fetchZoneChanges(previousServerChangeToken: state.token)
            if result.changeTokenExpired {
                let full = try await fetchZoneChanges(previousServerChangeToken: nil)
                let remoteRecordNames = Set(full.changed.map { $0.recordID.recordName })
                try applyRemoteChanges(
                    changed: full.changed,
                    deleted: full.deleted,
                    fullResyncRemoteRecordNames: remoteRecordNames
                )
                try saveState(token: full.newToken, errorCode: nil)
            } else {
                try applyRemoteChanges(changed: result.changed, deleted: result.deleted)
                try saveState(token: result.newToken, errorCode: nil)
            }
        } catch {
            if let ckError = error as? CKError, ckError.code == .zoneNotFound {
                try await ensureZoneExists()
                try resetLocalMirrorState()
                try saveState(didBootstrapZone: true)
                return
            }
            try? saveState(errorCode: cloudKitErrorCodeString(error))
            throw error
        }
    }
    
    private func fetchHistoryRows(
        afterHistoryPrimaryKey historyPrimaryKey: Int64,
        excludingAuthor author: String
    ) throws -> [CloudKitHistoryRow] {
        let connection = try store.queue.connection(nil)
        var rows = try queryHistoryRows(
            databaseName: "main",
            afterHistoryPrimaryKey: historyPrimaryKey,
            excludingAuthor: author,
            connection: connection
        )
        if let mainURL = try connection.mainDatabaseURL() {
            let archiveURLs = archiveDatabaseURLs(mainURL: mainURL)
            for (index, archiveURL) in archiveURLs.enumerated() {
                let databaseName = "archive_\(index)"
                guard FileManager.default.fileExists(atPath: archiveURL.path) else { continue }
                do {
                    try connection.attachDatabase(at: archiveURL, as: databaseName)
                    defer { try? connection.detachDatabaseIfAttached(named: databaseName) }
                    rows.append(contentsOf: try queryHistoryRows(
                        databaseName: databaseName,
                        afterHistoryPrimaryKey: historyPrimaryKey,
                        excludingAuthor: author,
                        connection: connection
                    ))
                } catch {
                    try? connection.detachDatabaseIfAttached(named: databaseName)
                }
            }
        }
        rows.sort { lhs, rhs in
            if lhs.historyPrimaryKey != rhs.historyPrimaryKey {
                return lhs.historyPrimaryKey < rhs.historyPrimaryKey
            }
            return lhs.transactionIdentifier < rhs.transactionIdentifier
        }
        return rows
    }
    
    private func queryHistoryRows(
        databaseName: String,
        afterHistoryPrimaryKey historyPrimaryKey: Int64,
        excludingAuthor author: String,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws -> [CloudKitHistoryRow] {
        try connection.query(
            """
            SELECT
                \(HistoryTable.pk.rawValue) AS history_pk,
                \(HistoryTable.event.rawValue) AS change_type,
                \(HistoryTable.timestamp.rawValue) AS transaction_identifier,
                \(HistoryTable.recordTarget.rawValue) AS entity_name,
                \(HistoryTable.recordIdentifier.rawValue) AS entity_pk,
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
                historyPrimaryKey: historyPrimaryKey,
                changeType: changeType,
                transactionIdentifier: transactionIdentifier,
                entityName: entityName,
                entityPrimaryKey: entityPrimaryKey,
                author: row["author"] as? String,
                context: row["context"] as? String
            )
        }
    }
    
    private func archiveDatabaseURLs(mainURL: URL) -> [URL] {
        let directory = HistoryTable.archiveDirectoryURL(mainURL: mainURL)
        guard let urls = try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            return []
        }
        return urls
            .filter { $0.pathExtension == "archive" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
    }
    
    private func coalescePendingChanges(
        _ rows: [CloudKitHistoryRow]
    ) throws -> [CloudKitPendingChange] {
        var changes: [CloudKitPendingChangeKey: CloudKitPendingChange] = [:]
        changes.reserveCapacity(rows.count)
        for row in rows {
            guard configuration.delegate.shouldSyncEntity(row.entityName) else {
                continue
            }
            let key = CloudKitPendingChangeKey(
                entityName: row.entityName,
                primaryKey: row.entityPrimaryKey
            )
            switch row.changeType {
            case DataStoreOperation.delete.rawValue:
                changes[key] = .init(
                    key: key,
                    operation: .delete,
                    historyPrimaryKey: row.historyPrimaryKey,
                    transactionIdentifier: row.transactionIdentifier,
                    changedPropertyNames: nil,
                    author: row.author
                )
            case DataStoreOperation.insert.rawValue:
                changes[key] = .init(
                    key: key,
                    operation: .upsert,
                    historyPrimaryKey: row.historyPrimaryKey,
                    transactionIdentifier: row.transactionIdentifier,
                    changedPropertyNames: nil,
                    author: row.author
                )
            case DataStoreOperation.update.rawValue:
                var existing = changes[key] ?? .init(
                    key: key,
                    operation: .upsert,
                    historyPrimaryKey: row.historyPrimaryKey,
                    transactionIdentifier: row.transactionIdentifier,
                    changedPropertyNames: [],
                    author: row.author
                )
                existing.operation = .upsert
                existing.historyPrimaryKey = row.historyPrimaryKey
                existing.transactionIdentifier = row.transactionIdentifier
                existing.author = row.author
                if existing.changedPropertyNames != nil {
                    existing.changedPropertyNames?.formUnion(parseChangedPropertyNames(row.context))
                }
                changes[key] = existing
            default:
                continue
            }
        }
        return changes.values.sorted { lhs, rhs in
            lhs.historyPrimaryKey < rhs.historyPrimaryKey
        }
    }
    
    private func parseChangedPropertyNames(_ context: String?) -> Set<String> {
        guard let context, !context.isEmpty else { return [] }
        return Set(context.split(separator: ",").map(String.init))
    }
    
    private func fetchCurrentSnapshot(
        entityName: String,
        primaryKey: String
    ) throws -> DatabaseSnapshot? {
        guard let entity = store.schema.entitiesByName[entityName] else {
            return nil
        }
        var relatedSnapshots: [PersistentIdentifier: DatabaseSnapshot]? = nil
        return try store.fetch(
            for: primaryKey,
            entity: entity,
            relatedSnapshots: &relatedSnapshots
        )
    }
    
    private func pushDelete(_ change: CloudKitPendingChange) async throws {
        let recordID = CKRecord.ID(
            recordName: CloudKitIDs.recordName(
                storeIdentifier: store.identifier,
                entityName: change.entityName,
                primaryKey: change.primaryKey
            ),
            zoneID: zoneID
        )
        do {
            try await deleteRecord(recordID)
        } catch let ckError as CKError where ckError.code == .unknownItem {
        } catch let ckError as CKError where ckError.code == .zoneNotFound {
            try await ensureZoneExists()
            try await deleteRecord(recordID)
        }
        try removeRecordMetadata(recordName: recordID.recordName)
    }
    
    private func pushUpsert(
        _ change: CloudKitPendingChange,
        snapshot: DatabaseSnapshot
    ) async throws {
        let outgoingSnapshot = try redactOutgoingSnapshot(snapshot)
        do {
            let (record, temporaryAssetURL) = try makeRecord(
                entityName: change.entityName,
                primaryKey: change.primaryKey,
                snapshot: outgoingSnapshot,
                baseRecord: try loadSystemFieldsRecord(
                    entityName: change.entityName,
                    primaryKey: change.primaryKey
                )
            )
            defer {
                if let temporaryAssetURL {
                    try? FileManager.default.removeItem(at: temporaryAssetURL)
                }
            }
            let savedRecord = try await saveRecord(record)
            try persistSavedRecordMetadata(savedRecord)
        } catch let ckError as CKError where ckError.code == .serverRecordChanged {
            try await resolveConflictAndSave(
                change: change,
                localSnapshot: outgoingSnapshot,
                error: ckError
            )
        } catch let ckError as CKError where ckError.code == .zoneNotFound {
            try await ensureZoneExists()
            try await pushUpsert(change, snapshot: outgoingSnapshot)
        }
    }
    
    private func redactOutgoingSnapshot(_ snapshot: DatabaseSnapshot) throws -> DatabaseSnapshot {
        var copy = try normalize(snapshot: snapshot)
        for property in copy.properties {
            guard property.name != pk else { continue }
            guard configuration.delegate.shouldSyncColumn(property.name, entityName: copy.entityName) else {
                copy.values[property.index] = SQLNull()
                continue
            }
        }
        return copy
    }
    
    private func prepareIncomingSnapshot(
        _ snapshot: DatabaseSnapshot,
        existingSnapshot: DatabaseSnapshot?
    ) throws -> DatabaseSnapshot {
        var copy = try normalize(snapshot: snapshot)
        for property in copy.properties {
            guard property.name != pk else { continue }
            guard configuration.delegate.shouldSyncColumn(property.name, entityName: copy.entityName) else {
                if let existingSnapshot {
                    copy.values[property.index] = existingSnapshot.values[property.index]
                } else {
                    copy.values[property.index] = SQLNull()
                }
                continue
            }
        }
        return copy
    }
    
    private func makeRecord(
        entityName: String,
        primaryKey: String,
        snapshot: DatabaseSnapshot,
        baseRecord: CKRecord?
    ) throws -> (CKRecord, URL?) {
        let recordName = CloudKitIDs.recordName(
            storeIdentifier: store.identifier,
            entityName: entityName,
            primaryKey: primaryKey
        )
        let recordID = CKRecord.ID(recordName: recordName, zoneID: zoneID)
        let recordType = configuration.delegate.recordType(for: entityName)
        let record = baseRecord ?? CKRecord(recordType: recordType, recordID: recordID)
        record[CloudKitMirrorField.primaryKey] = primaryKey as NSString
        record[CloudKitMirrorField.schemaVersion] = NSNumber(value: 1)
        record[CloudKitMirrorField.updatedAtMicroseconds] = NSNumber(value: Int64(Date().timeIntervalSince1970 * 1_000_000))
        let payload = try jsonEncoder.encode(CloudKitSnapshotEnvelope(snapshot: snapshot))
        if payload.count > payloadInlineThresholdBytes {
            let temporaryAssetURL = try writeTemporaryPayloadAsset(payload, entityName: entityName, primaryKey: primaryKey)
            record[CloudKitMirrorField.payloadEncoding] = CloudKitPayloadEncoding.asset.rawValue as NSString
            record[CloudKitMirrorField.payloadInline] = nil
            record[CloudKitMirrorField.payloadAsset] = CKAsset(fileURL: temporaryAssetURL)
            return (record, temporaryAssetURL)
        } else {
            record[CloudKitMirrorField.payloadEncoding] = CloudKitPayloadEncoding.inlineData.rawValue as NSString
            record[CloudKitMirrorField.payloadInline] = payload as NSData
            record[CloudKitMirrorField.payloadAsset] = nil
            return (record, nil)
        }
    }
    
    private func writeTemporaryPayloadAsset(
        _ data: Data,
        entityName: String,
        primaryKey: String
    ) throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("DataStoreKit")
            .appendingPathComponent("CloudKitAssets")
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        let fileName = "\(entityName)-\(primaryKey)-\(UUID().uuidString).json"
        let url = directory.appendingPathComponent(fileName)
        try data.write(to: url, options: [.atomic])
        return url
    }
    
    private func loadSystemFieldsRecord(entityName: String, primaryKey: String) throws -> CKRecord? {
        let connection = try store.queue.connection(nil)
        let row = try connection.query(
            """
            SELECT \(CloudKitRecordMetadata.systemFieldsBase64Encoded.rawValue) AS system_fields
            FROM \(CloudKitRecordMetadata.tableName)
            WHERE \(CloudKitRecordMetadata.storeIdentifier.rawValue) = ?
            AND \(CloudKitRecordMetadata.entityName.rawValue) = ?
            AND \(CloudKitRecordMetadata.entityPrimaryKey.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier, entityName, primaryKey]
        ).first
        guard let base64 = row?["system_fields"] as? String,
              !base64.isEmpty,
              let data = Data(base64Encoded: base64) else {
            return nil
        }
        return try CKRecord.fromSystemFields(data)
    }
    
    private func saveRecord(_ record: CKRecord) async throws -> CKRecord {
        let (saveResults, deleteResults) = try await database.modifyRecords(
            saving: [record],
            deleting: [],
            savePolicy: .ifServerRecordUnchanged,
            atomically: true
        )
        for (_, result) in deleteResults {
            if case .failure(let error) = result {
                throw error
            }
        }
        for (_, result) in saveResults {
            switch result {
            case .success(let record): return record
            case .failure(let error): throw error
            }
        }
        throw CKError(.internalError)
    }
    
    private func deleteRecord(_ recordID: CKRecord.ID) async throws {
        let (_, deleteResults) = try await database.modifyRecords(
            saving: [],
            deleting: [recordID],
            savePolicy: .ifServerRecordUnchanged,
            atomically: true
        )
        for (_, result) in deleteResults {
            if case .failure(let error) = result {
                throw error
            }
        }
    }
    
    private func resolveConflictAndSave(
        change: CloudKitPendingChange,
        localSnapshot: DatabaseSnapshot,
        error: CKError
    ) async throws {
        let serverRecord = (error.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord)
        ?? (error.userInfo[CKRecordChangedErrorAncestorRecordKey] as? CKRecord)
        ?? (error.userInfo[CKRecordChangedErrorClientRecordKey] as? CKRecord)
        guard let serverRecord else {
            throw error
        }
        let serverSnapshot = try decodeSnapshot(from: serverRecord)
        let mergedSnapshot = try merge(
            serverSnapshot: serverSnapshot,
            localSnapshot: localSnapshot,
            changedPropertyNames: change.changedPropertyNames
        )
        let (record, temporaryAssetURL) = try makeRecord(
            entityName: change.entityName,
            primaryKey: change.primaryKey,
            snapshot: mergedSnapshot,
            baseRecord: serverRecord
        )
        defer {
            if let temporaryAssetURL {
                try? FileManager.default.removeItem(at: temporaryAssetURL)
            }
        }
        let savedRecord = try await saveRecord(record)
        try persistSavedRecordMetadata(savedRecord)
    }
    
    private func merge(
        serverSnapshot: DatabaseSnapshot,
        localSnapshot: DatabaseSnapshot,
        changedPropertyNames: Set<String>?
    ) throws -> DatabaseSnapshot {
        guard let changedPropertyNames else {
            return localSnapshot
        }
        var merged = try normalize(snapshot: serverSnapshot)
        for property in localSnapshot.properties where changedPropertyNames.contains(property.name) {
            merged.values[property.index] = localSnapshot.values[property.index]
        }
        return merged
    }
    
    private func normalize(snapshot: DatabaseSnapshot) throws -> DatabaseSnapshot {
        let normalizedIdentifier = try PersistentIdentifier.identifier(
            for: store.identifier,
            entityName: snapshot.entityName,
            primaryKey: snapshot.primaryKey
        )
        var normalized = snapshot.copy(persistentIdentifier: normalizedIdentifier)
        for property in normalized.properties where property.metadata is Schema.Relationship {
            switch normalized.values[property.index] {
            case let identifier as PersistentIdentifier:
                normalized.values[property.index] = try PersistentIdentifier.identifier(
                    for: store.identifier,
                    entityName: identifier.entityName,
                    primaryKey: identifier.primaryKey()
                )
            case let identifiers as [PersistentIdentifier]:
                normalized.values[property.index] = try identifiers.map {
                    try PersistentIdentifier.identifier(
                        for: store.identifier,
                        entityName: $0.entityName,
                        primaryKey: $0.primaryKey()
                    )
                }
            default:
                continue
            }
        }
        return normalized
    }
    
    private func decodeSnapshot(from record: CKRecord) throws -> DatabaseSnapshot {
        let encoding = (record[CloudKitMirrorField.payloadEncoding] as? String)
            .flatMap(CloudKitPayloadEncoding.init(rawValue:))
        let data: Data
        switch encoding {
        case .inlineData:
            if let inline = record[CloudKitMirrorField.payloadInline] as? Data {
                data = inline
            } else if let inline = record[CloudKitMirrorField.payloadInline] as? NSData {
                data = inline as Data
            } else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: [],
                    debugDescription: "Missing inline CloudKit payload."
                ))
            }
        case .asset:
            guard let asset = record[CloudKitMirrorField.payloadAsset] as? CKAsset,
                  let url = asset.fileURL else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: [],
                    debugDescription: "Missing CloudKit asset payload."
                ))
            }
            data = try Data(contentsOf: url)
        case nil:
            if let inline = record[CloudKitMirrorField.payloadInline] as? Data {
                data = inline
            } else if let inline = record[CloudKitMirrorField.payloadInline] as? NSData {
                data = inline as Data
            } else if let asset = record[CloudKitMirrorField.payloadAsset] as? CKAsset,
                      let url = asset.fileURL {
                data = try Data(contentsOf: url)
            } else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: [],
                    debugDescription: "Missing CloudKit snapshot payload."
                ))
            }
        }
        if let envelope = try? jsonDecoder.decode(CloudKitSnapshotEnvelope.self, from: data) {
            return try normalize(snapshot: envelope.snapshot)
        }
        let snapshot = try jsonDecoder.decode(DatabaseSnapshot.self, from: data)
        return try normalize(snapshot: snapshot)
    }
    
    private func fetchZoneChanges(
        previousServerChangeToken: CKServerChangeToken?
    ) async throws -> CloudKitFetchResult {
        try await withCheckedThrowingContinuation { continuation in
            struct State {
                var changed: [CKRecord] = []
                var deleted: [CKRecord.ID] = []
                var newToken: CKServerChangeToken?
                var changeTokenExpired: Bool = false
                var isFinished: Bool = false
            }
            let lock = Mutex(State())
            func finish(_ body: () -> Void) {
                let shouldFinish = lock.withLock { state in
                    if state.isFinished { return false }
                    state.isFinished = true
                    return true
                }
                if shouldFinish { body() }
            }
            let configuration = CKFetchRecordZoneChangesOperation.ZoneConfiguration()
            configuration.previousServerChangeToken = previousServerChangeToken
            let operation = CKFetchRecordZoneChangesOperation(
                recordZoneIDs: [zoneID],
                configurationsByRecordZoneID: [zoneID: configuration]
            )
            operation.fetchAllChanges = true
            operation.recordWasChangedBlock = { _, result in
                switch result {
                case .success(let record):
                    lock.withLock { state in state.changed.append(record) }
                case .failure(let error):
                    finish { continuation.resume(throwing: error) }
                }
            }
            operation.recordWithIDWasDeletedBlock = { recordID, _ in
                lock.withLock { state in state.deleted.append(recordID) }
            }
            operation.recordZoneFetchResultBlock = { _, result in
                switch result {
                case .success(let zoneResult):
                    lock.withLock { state in state.newToken = zoneResult.serverChangeToken }
                case .failure(let error):
                    if let ckError = error as? CKError, ckError.code == .changeTokenExpired {
                        lock.withLock { state in state.changeTokenExpired = true }
                        return
                    }
                    finish { continuation.resume(throwing: error) }
                }
            }
            operation.fetchRecordZoneChangesResultBlock = { result in
                switch result {
                case .success:
                    finish {
                        let state = lock.withLock { $0 }
                        continuation.resume(returning: .init(
                            changed: state.changed,
                            deleted: state.deleted,
                            newToken: state.newToken,
                            changeTokenExpired: state.changeTokenExpired
                        ))
                    }
                case .failure(let error):
                    if let ckError = error as? CKError, ckError.code == .changeTokenExpired {
                        finish {
                            let state = lock.withLock { $0 }
                            continuation.resume(returning: .init(
                                changed: state.changed,
                                deleted: state.deleted,
                                newToken: nil,
                                changeTokenExpired: true
                            ))
                        }
                    } else {
                        finish { continuation.resume(throwing: error) }
                    }
                }
            }
            database.add(operation)
        }
    }
    
    private func applyRemoteChanges(
        changed: [CKRecord],
        deleted: [CKRecord.ID],
        fullResyncRemoteRecordNames: Set<String>? = nil
    ) throws {
        guard !changed.isEmpty || !deleted.isEmpty || fullResyncRemoteRecordNames != nil else {
            return
        }
        let connection = try store.queue.connection(.writer, for: DatabaseEditingState(
            id: .init(),
            author: configuration.remoteAuthor
        ))
        try connection.withTransaction(nil) {
            if let fullResyncRemoteRecordNames {
                try deleteLocallyMissingRemoteRecords(
                    remoteRecordNames: fullResyncRemoteRecordNames,
                    connection: connection
                )
            }
            for record in changed {
                try applyChangedRecord(record, connection: connection)
            }
            for recordID in deleted {
                try applyDeletedRecord(recordID, connection: connection)
            }
        }
    }
    
    private func applyChangedRecord(
        _ record: CKRecord,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        let incomingSnapshot = try decodeSnapshot(from: record)
        guard configuration.delegate.shouldSyncEntity(incomingSnapshot.entityName) else {
            return
        }
        let existingSnapshot = try fetchCurrentSnapshot(
            entityName: incomingSnapshot.entityName,
            primaryKey: incomingSnapshot.primaryKey
        )
        var preparedSnapshot = try prepareIncomingSnapshot(
            incomingSnapshot,
            existingSnapshot: existingSnapshot
        )
        if let existingSnapshot {
            try connection.update(from: existingSnapshot, to: preparedSnapshot)
            let relationshipIndices = relationshipIndices(for: preparedSnapshot)
            if !relationshipIndices.isEmpty {
                _ = try preparedSnapshot.reconcileExternalReferences(
                    comparingTo: existingSnapshot,
                    indices: relationshipIndices,
                    shouldAddOnly: false,
                    connection: connection
                )
            }
        } else {
            try connection.insert(preparedSnapshot)
            let relationshipIndices = relationshipIndices(for: preparedSnapshot)
            if !relationshipIndices.isEmpty {
                _ = try preparedSnapshot.reconcileExternalReferences(
                    comparingTo: nil,
                    indices: relationshipIndices,
                    shouldAddOnly: false,
                    connection: connection
                )
            }
        }
        try upsertRecordMetadata(
            connection: connection,
            recordName: record.recordID.recordName,
            recordType: record.recordType,
            entityName: preparedSnapshot.entityName,
            primaryKey: preparedSnapshot.primaryKey,
            systemFieldsBase64: record.systemFieldsData().base64EncodedString()
        )
    }
    
    private func applyDeletedRecord(
        _ recordID: CKRecord.ID,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        guard let parsed = CloudKitIDs.parseRecordName(recordID.recordName) else {
            return
        }
        guard parsed.storeIdentifier == store.identifier else {
            return
        }
        guard configuration.delegate.shouldSyncEntity(parsed.entityName) else {
            return
        }
        guard let snapshot = try fetchCurrentSnapshot(
            entityName: parsed.entityName,
            primaryKey: parsed.primaryKey
        ) else {
            try deleteRecordMetadata(connection: connection, recordName: recordID.recordName)
            return
        }
        var visited = Set<PersistentIdentifier>()
        try deleteSnapshotRecursively(snapshot, connection: connection, visited: &visited)
        try deleteRecordMetadata(connection: connection, recordName: recordID.recordName)
    }
    
    private func deleteLocallyMissingRemoteRecords(
        remoteRecordNames: Set<String>,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        let rows = try connection.query(
            """
            SELECT
                \(CloudKitRecordMetadata.recordName.rawValue) AS record_name,
                \(CloudKitRecordMetadata.entityName.rawValue) AS entity_name,
                \(CloudKitRecordMetadata.entityPrimaryKey.rawValue) AS entity_pk
            FROM \(CloudKitRecordMetadata.tableName)
            WHERE \(CloudKitRecordMetadata.storeIdentifier.rawValue) = ?
            """,
            bindings: [store.identifier]
        )
        for row in rows {
            guard let recordName = row["record_name"] as? String,
                  let entityName = row["entity_name"] as? String,
                  let primaryKey = row["entity_pk"] as? String else {
                continue
            }
            if remoteRecordNames.contains(recordName) {
                continue
            }
            guard let snapshot = try fetchCurrentSnapshot(
                entityName: entityName,
                primaryKey: primaryKey
            ) else {
                try deleteRecordMetadata(connection: connection, recordName: recordName)
                continue
            }
            var visited = Set<PersistentIdentifier>()
            try deleteSnapshotRecursively(snapshot, connection: connection, visited: &visited)
            try deleteRecordMetadata(connection: connection, recordName: recordName)
        }
    }
    
    private func deleteSnapshotRecursively(
        _ snapshot: DatabaseSnapshot,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        visited: inout Set<PersistentIdentifier>
    ) throws {
        if !visited.insert(snapshot.persistentIdentifier).inserted {
            return
        }
        var mutableSnapshot = snapshot
        let relationshipIndices = relationshipIndices(for: mutableSnapshot)
        if !relationshipIndices.isEmpty {
            let results = try mutableSnapshot.reconcileExternalReferencesBeforeDelete(
                indices: relationshipIndices,
                connection: connection
            )
            for cascadedIdentifier in results.cascaded {
                guard let cascadedSnapshot = try fetchCurrentSnapshot(
                    entityName: cascadedIdentifier.entityName,
                    primaryKey: cascadedIdentifier.primaryKey()
                ) else {
                    continue
                }
                try deleteSnapshotRecursively(
                    cascadedSnapshot,
                    connection: connection,
                    visited: &visited
                )
                let recordName = CloudKitIDs.recordName(
                    storeIdentifier: store.identifier,
                    entityName: cascadedIdentifier.entityName,
                    primaryKey: cascadedIdentifier.primaryKey()
                )
                try deleteRecordMetadata(connection: connection, recordName: recordName)
            }
        }
        try connection.delete(mutableSnapshot)
        let recordName = CloudKitIDs.recordName(
            storeIdentifier: store.identifier,
            entityName: snapshot.entityName,
            primaryKey: snapshot.primaryKey
        )
        try deleteRecordMetadata(connection: connection, recordName: recordName)
    }
    
    private func relationshipIndices(for snapshot: DatabaseSnapshot) -> [Int] {
        snapshot.properties.compactMap { property in
            property.metadata is Schema.Relationship ? property.index : nil
        }
    }
    
    private func persistSavedRecordMetadata(_ savedRecord: CKRecord) throws {
        let connection = try store.queue.connection(.writer)
        guard let parsed = CloudKitIDs.parseRecordName(savedRecord.recordID.recordName) else {
            return
        }
        try upsertRecordMetadata(
            connection: connection,
            recordName: savedRecord.recordID.recordName,
            recordType: savedRecord.recordType,
            entityName: parsed.entityName,
            primaryKey: parsed.primaryKey,
            systemFieldsBase64: savedRecord.systemFieldsData().base64EncodedString()
        )
    }
    
    private func upsertRecordMetadata(
        connection: borrowing DatabaseConnection<DatabaseStore>,
        recordName: String,
        recordType: String,
        entityName: String,
        primaryKey: String,
        systemFieldsBase64: String
    ) throws {
        try PreparedStatement(
            sql: """
            INSERT INTO \(CloudKitRecordMetadata.tableName) (
                \(CloudKitRecordMetadata.storeIdentifier.rawValue),
                \(CloudKitRecordMetadata.recordName.rawValue),
                \(CloudKitRecordMetadata.recordType.rawValue),
                \(CloudKitRecordMetadata.entityName.rawValue),
                \(CloudKitRecordMetadata.entityPrimaryKey.rawValue),
                \(CloudKitRecordMetadata.systemFieldsBase64Encoded.rawValue)
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (
                \(CloudKitRecordMetadata.storeIdentifier.rawValue),
                \(CloudKitRecordMetadata.recordName.rawValue)
            ) DO UPDATE SET
                \(CloudKitRecordMetadata.recordType.rawValue) = excluded.\(CloudKitRecordMetadata.recordType.rawValue),
                \(CloudKitRecordMetadata.entityName.rawValue) = excluded.\(CloudKitRecordMetadata.entityName.rawValue),
                \(CloudKitRecordMetadata.entityPrimaryKey.rawValue) = excluded.\(CloudKitRecordMetadata.entityPrimaryKey.rawValue),
                \(CloudKitRecordMetadata.systemFieldsBase64Encoded.rawValue) = excluded.\(CloudKitRecordMetadata.systemFieldsBase64Encoded.rawValue)
            """,
            bindings: [
                store.identifier,
                recordName,
                recordType,
                entityName,
                primaryKey,
                systemFieldsBase64
            ],
            handle: connection.handle
        ).run()
    }
    
    private func removeRecordMetadata(recordName: String) throws {
        let connection = try store.queue.connection(.writer)
        try deleteRecordMetadata(connection: connection, recordName: recordName)
    }
    
    private func deleteRecordMetadata(
        connection: borrowing DatabaseConnection<DatabaseStore>,
        recordName: String
    ) throws {
        try PreparedStatement(
            sql: """
            DELETE FROM \(CloudKitRecordMetadata.tableName)
            WHERE \(CloudKitRecordMetadata.storeIdentifier.rawValue) = ?
            AND \(CloudKitRecordMetadata.recordName.rawValue) = ?
            """,
            bindings: [store.identifier, recordName],
            handle: connection.handle
        ).run()
    }
    
    private func cloudKitErrorCodeString(_ error: Swift.Error) -> String {
        if let ckError = error as? CKError {
            return "\(ckError.code.rawValue)"
        }
        return String(describing: error)
    }
}
