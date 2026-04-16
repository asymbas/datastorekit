//
//  CloudKitSupport.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
private import DataStoreRuntime
private import DataStoreSQL
private import DataStoreSupport
private import Logging
private import SQLiteHandle
private import SQLSupport
private import Synchronization
internal import Foundation

#if swift(>=6.2)
internal import SwiftData
#else
@preconcurrency internal import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

#if canImport(CloudKit)

internal import CloudKit

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
            throw DecodingError.dataCorrupted(.init(
                codingPath: [],
                debugDescription: "Unable to decode CKRecord system fields."
            ))
        }
        unarchiver.finishDecoding()
        return record
    }
}

extension DatabaseStore {
    nonisolated internal var createCloudKitTablesSQL: [String] {
        [
            Configuration.CloudKitDatabase.StateTable.createTable,
            Configuration.CloudKitDatabase.RecordMetadataTable.createTable,
            Configuration.CloudKitDatabase.RecordMetadataTable.createIndex
        ]
    }
}

extension DatabaseConfiguration.CloudKitDatabase {
    internal enum StateTable: String, CustomStringConvertible {
        nonisolated internal static let tableName: String = "_CloudKitState"
        case storeIdentifier = "store_identifier"
        case didBootstrapZone = "did_bootstrap_zone"
        case lastSyncAtMicroseconds = "last_sync"
        case lastErrorCode = "last_error_code"
        case lastEnqueuedHistoryPrimaryKey = "last_enqueued_history_pk"
        case stateSerialization = "state_serialization"
        
        nonisolated internal static var createTable: String {
            """
            CREATE TABLE IF NOT EXISTS \(Self.tableName) (
                \(Self.storeIdentifier.rawValue) TEXT PRIMARY KEY,
                \(Self.didBootstrapZone.rawValue) INTEGER NOT NULL DEFAULT 0,
                \(Self.lastSyncAtMicroseconds.rawValue) INTEGER NOT NULL DEFAULT 0,
                \(Self.lastErrorCode.rawValue) TEXT,
                \(Self.lastEnqueuedHistoryPrimaryKey.rawValue) INTEGER,
                \(Self.stateSerialization.rawValue) BLOB
            )
            """
        }
        
        nonisolated internal static let requiredColumns: [(String, String)] = [
            (Self.didBootstrapZone.rawValue, "INTEGER NOT NULL DEFAULT 0"),
            (Self.lastSyncAtMicroseconds.rawValue, "INTEGER NOT NULL DEFAULT 0")
        ]
        
        /// Inherited from `CustomStringConvertible.description`.
        nonisolated internal var description: String { rawValue }
    }
}

extension DatabaseConfiguration.CloudKitDatabase {
    /// A type that provides a lookup schema for SwiftData models and CloudKit records.
    internal enum RecordMetadataTable: String, CustomStringConvertible {
        nonisolated internal static let tableName: String = "_CloudKitRecordMetadata"
        nonisolated internal static let indexName: String = "_CloudKit_RecordMetadata_Entity_Index"
        case storeIdentifier = "store_identifier"
        case recordType = "record_type"
        case recordName = "record_name"
        case entityName = "entity_name"
        case entityPrimaryKey = "entity_pk"
        case entityTargetPrimaryKey = "entity_target_pk"
        case systemFields = "system_fields"
        
        nonisolated internal static var createTable: String {
            """
            CREATE TABLE IF NOT EXISTS \(Self.tableName) (
                \(Self.storeIdentifier.rawValue) TEXT NOT NULL,
                \(Self.recordType.rawValue) TEXT NOT NULL,
                \(Self.recordName.rawValue) TEXT NOT NULL,
                \(Self.entityName.rawValue) TEXT NOT NULL,
                \(Self.entityPrimaryKey.rawValue) TEXT NOT NULL,
                \(Self.entityTargetPrimaryKey.rawValue) TEXT,
                \(Self.systemFields.rawValue) BLOB,
                PRIMARY KEY (
                    \(Self.storeIdentifier.rawValue),
                    \(Self.recordName.rawValue)
                )
            )
            """
        }
        
        nonisolated internal static var createIndex: String {
            """
            CREATE INDEX IF NOT EXISTS \(indexName)
            ON \(Self.tableName) (
                \(Self.storeIdentifier.rawValue),
                \(Self.entityName.rawValue),
                \(Self.entityPrimaryKey.rawValue)
            )
            """
        }
        
        nonisolated internal static let requiredColumns: [(String, String)] = [
            (Self.storeIdentifier.rawValue, "TEXT NOT NULL"),
            (Self.recordType.rawValue, "TEXT NOT NULL"),
            (Self.entityName.rawValue, "TEXT NOT NULL"),
            (Self.entityPrimaryKey.rawValue, "TEXT NOT NULL"),
            (Self.entityTargetPrimaryKey.rawValue, "TEXT"),
            (Self.systemFields.rawValue, "BLOB")
        ]
        
        /// Inherited from `CustomStringConvertible.description`.
        nonisolated internal var description: String { rawValue }
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    public protocol Delegate: Sendable {
        func shouldSyncEntity(_ entityName: String) -> Bool
        func shouldSyncColumn(_ columnName: String, entityName: String) -> Bool
    }
}

internal extension DatabaseConfiguration.CloudKitDatabase.Replicator.Delegate {
    func shouldSyncEntity(_ entityName: String) -> Bool { true }
    func shouldSyncColumn(_ columnName: String, entityName: String) -> Bool { true }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal struct DefaultSyncDelegate: Delegate {
        internal var excludedEntities: Set<String> = []
        internal var excludedColumns: Set<String> = []
        
        internal func shouldSyncEntity(_ entityName: String) -> Bool {
            !excludedEntities.contains(entityName)
        }
        
        internal func shouldSyncColumn(_ columnName: String, entityName: String) -> Bool {
            !excludedColumns.contains(columnName)
        }
    }
}

#endif
