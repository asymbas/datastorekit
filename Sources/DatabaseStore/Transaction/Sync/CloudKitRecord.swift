//
//  CloudKitRecord.swift
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

nonisolated internal func validateCloudKitRecordName(_ value: consuming String) -> String {
    precondition(!value.isEmpty, "CloudKit record name must not be empty.")
    precondition(value.first != "_", "CloudKit record name must not start with an underscore.")
    precondition(value.utf8.count <= 255, "CloudKit record name must not exceed 255 ASCII characters.")
    precondition(value.allSatisfy(\.isASCII), "CloudKit record name must contain only ASCII characters.")
    return consume value
}

nonisolated internal func makeRecordType(_ name: String) -> String {
    "DSK_\(name)"
}

nonisolated internal func makeEntityName(fromRecordType recordType: String) -> String {
    if recordType.hasPrefix("DSK_") { return String(recordType.dropFirst(4)) }
    return recordType
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func snapshot(for recordIdentifier: RecordIdentifier) throws -> Store.Snapshot? {
        guard let type = Schema.type(for: recordIdentifier.tableName) else {
            throw SchemaError.entityNotRegistered
        }
        do {
            let snapshot = try store.queue.reader {
                try $0.fetch(for: recordIdentifier.primaryKey.description, as: type)
            }
            logger.trace("Loaded current local snapshot.", metadata: [
                "entity_name": "\(recordIdentifier.tableName)",
                "primary_key": "\(recordIdentifier.primaryKey)",
                "snapshot_found": "\(snapshot != nil)"
            ])
            return snapshot
        } catch {
            logger.trace("Failed to fetch current local snapshot: \(error)", metadata: [
                "entity_name": "\(recordIdentifier.tableName)",
                "primary_key": "\(recordIdentifier.primaryKey)"
            ])
            throw error
        }
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func makeRecordID(recordName: String) -> CKRecord.ID {
        .init(recordName: recordName, zoneID: zoneID)
    }
    
    internal func makeCloudKitRecordName() throws -> String {
        while true {
            let recordName = UUID().uuidString
            if try loadRecordMetadata(recordName: recordName) == nil {
                return recordName
            }
        }
    }
    
    internal func resolveRootRecordOwnership(for record: CKRecord) throws -> RecordIdentifier? {
        switch record[pk] as? String {
        case let primaryKey?:
            .init(
                for: store.identifier,
                tableName: makeEntityName(fromRecordType: record.recordType),
                primaryKey: primaryKey
            )
        case nil:
            try resolveRootRecordOwnership(for: record.recordID)
        }
    }
    
    internal func resolveRootRecordOwnership(for recordID: CKRecord.ID) throws -> RecordIdentifier? {
        switch try loadRecordMetadata(recordName: recordID.recordName) {
        case let metadata? where metadata.recordType == makeRecordType(metadata.entityName):
            .init(for: store.identifier, tableName: metadata.entityName, primaryKey: metadata.primaryKey)
        default:
            nil
        }
    }
    
    internal func resolveProjectedRecordOwnership(record: CKRecord) throws -> ProjectedRecordOwnership? {
        if let root = try resolveRootRecordOwnership(for: record) {
            return .root(entityName: root.tableName, primaryKey: root.primaryKey.description)
        }
        if let metadata = try loadRecordMetadata(recordName: record.recordID.recordName),
           metadata.recordType != makeRecordType(metadata.entityName),
           let targetPrimaryKey = metadata.targetPrimaryKey {
            return .reference(
                entityName: metadata.entityName,
                primaryKey: metadata.primaryKey,
                intermediaryTableName: makeEntityName(fromRecordType: record.recordType),
                targetPrimaryKey: targetPrimaryKey
            )
        }
        return try resolveProjectedRecordOwnership(recordType: record.recordType, recordID: record.recordID)
    }
    
    internal func resolveProjectedRecordOwnership(recordType: String? = nil, recordID: CKRecord.ID)
    throws -> ProjectedRecordOwnership? {
        if let metadata = try loadRecordMetadata(recordName: recordID.recordName) {
            let resolvedRecordType = recordType ?? metadata.recordType
            if resolvedRecordType == makeRecordType(metadata.entityName) {
                logger.trace("Resolved projected record ownership using persisted metadata as root record.", metadata: [
                    "record_name": "\(recordID.recordName)",
                    "entity_name": "\(metadata.entityName)",
                    "primary_key": "\(metadata.primaryKey)"
                ])
                return .root(entityName: metadata.entityName, primaryKey: metadata.primaryKey)
            }
            guard let targetPrimaryKey = metadata.targetPrimaryKey else {
                logger.trace(
                    "Persisted CloudKit metadata existed, but the intermediary record had no related primary key.",
                    metadata: [
                        "record_type": "\(resolvedRecordType)",
                        "record_name": "\(recordID.recordName)"
                    ]
                )
                return nil
            }
            logger.trace("Resolved projected record ownership using persisted metadata.", metadata: [
                "record_name": "\(recordID.recordName)",
                "entity_name": "\(metadata.entityName)",
                "primary_key": "\(metadata.primaryKey)",
                "intermediary_table_name": "\(resolvedRecordType)",
                "destination_primary_key": "\(targetPrimaryKey)"
            ])
            return .reference(
                entityName: metadata.entityName,
                primaryKey: metadata.primaryKey,
                intermediaryTableName: makeEntityName(fromRecordType: resolvedRecordType),
                targetPrimaryKey: targetPrimaryKey
            )
        }
        logger.trace("Failed to resolve projected record ownership.", metadata: [
            "record_type": "\(recordType ?? "nil")",
            "record_name": "\(recordID.recordName)"
        ])
        return nil
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func loadRecordMetadata(recordType: String, for recordIdentifier: RecordIdentifier)
    throws -> RecordMetadata? {
        if let metadata = self.identifiers[recordIdentifier] {
            return metadata
        }
        let rows = try store.queue.connection(.reader).fetch(
            """
            SELECT
                \(RecordMetadataTable.recordType.rawValue) AS record_type,
                \(RecordMetadataTable.recordName.rawValue) AS record_name,
                \(RecordMetadataTable.entityName.rawValue) AS entity_name,
                \(RecordMetadataTable.entityPrimaryKey.rawValue) AS entity_pk,
                \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) AS related_pk
            FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.entityName.rawValue) = ?
            AND \(RecordMetadataTable.entityPrimaryKey.rawValue) = ?
            AND \(RecordMetadataTable.recordType.rawValue) = ?
            LIMIT 1
            """,
            bindings: [
                store.identifier,
                recordIdentifier.tableName,
                recordIdentifier.primaryKey,
                recordType
            ]
        )
        guard let row = rows.first else {
            return nil
        }
        guard let recordType = row[0] as? String,
              let recordName = row[1] as? String,
              let entityName = row[2] as? String,
              let primaryKey = row[3] as? String else {
            return nil
        }
        assert(recordIdentifier.tableName == entityName)
        assert(recordIdentifier.primaryKey.description == primaryKey)
        let metadata = RecordMetadata(
            recordType: recordType,
            recordName: recordName,
            identifier: recordIdentifier,
            targetPrimaryKey: row[4] as? String
        )
        self.identifiers[recordIdentifier] = metadata
        return metadata
    }
    
    internal func loadRecordMetadata(recordName: String) throws -> RecordMetadata? {
        let rows = try store.queue.connection(.reader).query(
            """
            SELECT
                \(RecordMetadataTable.recordType.rawValue) AS record_type,
                \(RecordMetadataTable.recordName.rawValue) AS record_name,
                \(RecordMetadataTable.entityName.rawValue) AS entity_name,
                \(RecordMetadataTable.entityPrimaryKey.rawValue) AS entity_pk,
                \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) AS related_pk
            FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.recordName.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier, recordName]
        )
        guard let row = rows.first else {
            return nil
        }
        guard let recordType = row["record_type"] as? String,
              let recordName = row["record_name"] as? String,
              let entityName = row["entity_name"] as? String,
              let primaryKey = row["entity_pk"] as? String else {
            return nil
        }
        let identifier = RecordIdentifier(for: store.identifier, tableName: entityName, primaryKey: primaryKey)
        return RecordMetadata(
            recordType: recordType,
            recordName: recordName,
            identifier: identifier,
            targetPrimaryKey: row["related_pk"] as? String
        )
    }
    
    internal func loadRecordMetadata(recordType: String, entityName: String, primaryKey: String)
    throws -> RecordMetadata? {
        try loadRecordMetadata(
            recordType: recordType,
            for: .init(for: store.identifier, tableName: entityName, primaryKey: primaryKey)
        )
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func loadReferenceRecordMetadata(
        recordType: String,
        entityName: String,
        primaryKey: String,
        targetPrimaryKey: String
    ) throws -> RecordMetadata? {
        let rows = try store.queue.connection(.reader).fetch(
            """
            SELECT
                \(RecordMetadataTable.recordType.rawValue) AS record_type,
                \(RecordMetadataTable.recordName.rawValue) AS record_name,
                \(RecordMetadataTable.entityName.rawValue) AS entity_name,
                \(RecordMetadataTable.entityPrimaryKey.rawValue) AS entity_pk,
                \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) AS related_pk
            FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.entityName.rawValue) = ?
            AND \(RecordMetadataTable.entityPrimaryKey.rawValue) = ?
            AND \(RecordMetadataTable.recordType.rawValue) = ?
            AND \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier, entityName, primaryKey, recordType, targetPrimaryKey]
        )
        guard let row = rows.first else {
            return nil
        }
        guard let recordType = row[0] as? String,
              let recordName = row[1] as? String,
              let entityName = row[2] as? String,
              let primaryKey = row[3] as? String else {
            return nil
        }
        let identifier = RecordIdentifier(for: store.identifier, tableName: entityName, primaryKey: primaryKey)
        return RecordMetadata(
            recordType: recordType,
            recordName: recordName,
            identifier: identifier,
            targetPrimaryKey: row[4] as? String
        )
    }
    
    internal func loadRelatedRecordMetadata(targetPrimaryKey: String) throws -> [RecordMetadata] {
        try store.queue.connection(.reader).query(
            """
            SELECT
                \(RecordMetadataTable.recordType.rawValue) AS record_type,
                \(RecordMetadataTable.recordName.rawValue) AS record_name,
                \(RecordMetadataTable.entityName.rawValue) AS entity_name,
                \(RecordMetadataTable.entityPrimaryKey.rawValue) AS entity_pk,
                \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) AS related_pk
            FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) = ?
            """,
            bindings: [store.identifier, targetPrimaryKey]
        ).compactMap { row in
            guard let recordType = row["record_type"] as? String,
                  let recordName = row["record_name"] as? String,
                  let entityName = row["entity_name"] as? String,
                  let primaryKey = row["entity_pk"] as? String else {
                return nil
            }
            let identifier = RecordIdentifier(for: store.identifier, tableName: entityName, primaryKey: primaryKey)
            return RecordMetadata(
                recordType: recordType,
                recordName: recordName,
                identifier: identifier,
                targetPrimaryKey: row["related_pk"] as? String
            )
        }
    }
    
    internal func loadOwnedRecordMetadata(for recordIdentifier: RecordIdentifier) throws -> [RecordMetadata] {
        try store.queue.connection(.reader).query(
            """
            SELECT
                \(RecordMetadataTable.recordType.rawValue) AS record_type,
                \(RecordMetadataTable.recordName.rawValue) AS record_name,
                \(RecordMetadataTable.entityName.rawValue) AS entity_name,
                \(RecordMetadataTable.entityPrimaryKey.rawValue) AS entity_pk,
                \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) AS related_pk
            FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.entityName.rawValue) = ?
            AND \(RecordMetadataTable.entityPrimaryKey.rawValue) = ?
            ORDER BY \(RecordMetadataTable.recordName.rawValue) ASC
            """,
            bindings: [store.identifier, recordIdentifier.tableName, recordIdentifier.primaryKey]
        ).compactMap { row in
            guard let recordType = row["record_type"] as? String,
                  let recordName = row["record_name"] as? String,
                  let entityName = row["entity_name"] as? String,
                  let primaryKey = row["entity_pk"] as? String else {
                return nil
            }
            let identifier = RecordIdentifier(for: store.identifier, tableName: entityName, primaryKey: primaryKey)
            return RecordMetadata(
                recordType: recordType,
                recordName: recordName,
                identifier: identifier,
                targetPrimaryKey: row["related_pk"] as? String
            )
        }
    }
    
    internal func loadOwnedRecordMetadata(entityName: String, primaryKey: String) throws -> [RecordMetadata] {
        try loadOwnedRecordMetadata(for: .init(for: store.identifier, tableName: entityName, primaryKey: primaryKey))
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func provisionRootRecordName(recordType: String, entityName: String, primaryKey: String)
    throws -> String {
        if let metadata = try loadRecordMetadata(recordType: recordType, entityName: entityName, primaryKey: primaryKey) {
            return metadata.recordName
        }
        let recordName = try makeCloudKitRecordName()
        let connection = try store.queue.connection(.writer)
        try upsertRecordMetadata(
            recordType: makeRecordType(entityName),
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: nil,
            systemFields: nil,
            connection: connection
        )
        return recordName
    }
    
    internal func provisionReferenceRecordName(
        entityName: String,
        primaryKey: String,
        intermediaryTableName: String,
        targetPrimaryKey: String
    ) throws -> String {
        if let recordName = try loadReferenceRecordMetadata(
            recordType: makeRecordType(intermediaryTableName),
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: targetPrimaryKey
        )?.recordName {
            return recordName
        }
        let recordName = try makeCloudKitRecordName()
        let connection = try store.queue.connection(.writer)
        try upsertRecordMetadata(
            recordType: makeRecordType(intermediaryTableName),
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: targetPrimaryKey,
            systemFields: nil,
            connection: connection
        )
        return recordName
    }
    
    internal func projectedRecords(for snapshot: Store.Snapshot) throws -> [CKRecord] {
        let rootRecordName = try provisionRootRecordName(
            recordType: makeRecordType(snapshot.entityName),
            entityName: snapshot.entityName,
            primaryKey: snapshot.primaryKey
        )
        let systemFieldsData = try loadSystemFieldsData(recordName: rootRecordName)
        return try snapshot.records(
            rootRecordName: rootRecordName,
            systemFieldsData: systemFieldsData,
            zoneID: zoneID,
            store: store,
            loadSystemFieldsData: { recordName in
                try self.loadSystemFieldsData(recordName: recordName)
            },
            referenceRecordName: { intermediaryTableName, destinationPrimaryKey in
                try self.provisionReferenceRecordName(
                    entityName: snapshot.entityName,
                    primaryKey: snapshot.primaryKey,
                    intermediaryTableName: intermediaryTableName,
                    targetPrimaryKey: destinationPrimaryKey
                )
            },
            resolveRelationshipRecordName: { destinationEntityName, primaryKey in
                try self.provisionRootRecordName(
                    recordType: makeRecordType(destinationEntityName),
                    entityName: destinationEntityName,
                    primaryKey: primaryKey
                )
            }
        )
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func intermediaryRecordDescriptor(for recordType: String) -> IntermediaryRecordDescriptor? {
        for entity in store.schema.entities where configuration.delegate.shouldSyncEntity(entity.name) {
            guard let type = Schema.type(for: entity.name) else {
                preconditionFailure()
            }
            for property in type.databaseSchemaMetadata {
                guard let relationship = property.metadata as? Schema.Relationship,
                      relationship.isToOneRelationship == false,
                      let reference = property.reference,
                      reference.count == 2,
                      makeRecordType(reference[0].destinationTable) == recordType else {
                    continue
                }
                return .init(
                    ownerEntityName: entity.name,
                    property: property,
                    destinationEntityName: relationship.destination,
                    sourceFieldName: "key_" + reference[0].rhsColumn,
                    destinationFieldName: "key_" + reference[1].lhsColumn
                )
            }
        }
        return nil
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func persistSavedRecordMetadata(_ savedRecord: CKRecord) throws {
        let connection = try store.queue.connection(.writer)
        if let root = try resolveRootRecordOwnership(for: savedRecord) {
            try upsertRecordMetadata(
                recordType: savedRecord.recordType,
                recordName: savedRecord.recordID.recordName,
                entityName: root.tableName,
                primaryKey: root.primaryKey.description,
                targetPrimaryKey: nil,
                systemFields: try savedRecord.systemFieldsData(),
                connection: connection
            )
            return
        }
        guard let ownership = try resolveProjectedRecordOwnership(record: savedRecord) else {
            return
        }
        let entityName: String
        let primaryKey: String
        let targetPrimaryKey: String?
        switch ownership {
        case .root(let resolvedEntityName, let resolvedPrimaryKey):
            entityName = resolvedEntityName
            primaryKey = resolvedPrimaryKey
            targetPrimaryKey = nil
        case .reference(let resolvedEntityName, let resolvedPrimaryKey, _, let resolvedTargetPrimaryKey):
            entityName = resolvedEntityName
            primaryKey = resolvedPrimaryKey
            targetPrimaryKey = resolvedTargetPrimaryKey
        }
        try upsertRecordMetadata(
            recordType: savedRecord.recordType,
            recordName: savedRecord.recordID.recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: targetPrimaryKey,
            systemFields: savedRecord.systemFieldsData(),
            connection: connection
        )
    }
    
    internal func upsertRecordMetadata(
        recordType: String,
        recordName: String,
        entityName: String,
        primaryKey: String,
        targetPrimaryKey: String?,
        systemFields: Data?,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        try PreparedStatement(
            sql: """
            INSERT INTO \(RecordMetadataTable.tableName) (
                \(RecordMetadataTable.storeIdentifier.rawValue),
                \(RecordMetadataTable.recordType.rawValue),
                \(RecordMetadataTable.recordName.rawValue),
                \(RecordMetadataTable.entityName.rawValue),
                \(RecordMetadataTable.entityPrimaryKey.rawValue),
                \(RecordMetadataTable.entityTargetPrimaryKey.rawValue),
                \(RecordMetadataTable.systemFields.rawValue)
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (
                \(RecordMetadataTable.storeIdentifier.rawValue),
                \(RecordMetadataTable.recordName.rawValue)
            ) DO UPDATE SET
                \(RecordMetadataTable.recordType.rawValue) = excluded.\(RecordMetadataTable.recordType.rawValue),
                \(RecordMetadataTable.entityName.rawValue) = excluded.\(RecordMetadataTable.entityName.rawValue),
                \(RecordMetadataTable.entityPrimaryKey.rawValue) = excluded.\(RecordMetadataTable.entityPrimaryKey.rawValue),
                \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) = excluded.\(RecordMetadataTable.entityTargetPrimaryKey.rawValue),
                \(RecordMetadataTable.systemFields.rawValue) = excluded.\(RecordMetadataTable.systemFields.rawValue)
            """,
            bindings: [
                store.identifier,
                recordType,
                recordName,
                entityName,
                primaryKey,
                targetPrimaryKey ?? NSNull(),
                systemFields ?? NSNull()
            ],
            handle: connection.handle
        ).run()
    }
    
    internal func upsertRecordMetadata(
        recordType: String,
        recordName: String,
        entityName: String,
        primaryKey: String,
        systemFields: Data
    ) throws {
        let connection = try store.queue.connection(.writer)
        try upsertRecordMetadata(
            recordType: recordType,
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: nil,
            systemFields: systemFields,
            connection: connection
        )
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func deleteRecordMetadata(recordType: String, entityName: String, primaryKey: String) throws {
        let connection = try store.queue.connection(.writer)
        try PreparedStatement(
            sql: """
            DELETE FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.entityName.rawValue) = ?
            AND \(RecordMetadataTable.entityPrimaryKey.rawValue) = ?
            AND \(RecordMetadataTable.recordType.rawValue) = ?
            """,
            bindings: [store.identifier, entityName, primaryKey, recordType],
            handle: connection.handle
        ).run()
    }
    
    internal func deleteRootRecordMetadata(entityName: String, primaryKey: String) throws {
        try deleteRecordMetadata(
            recordType: makeRecordType(entityName),
            entityName: entityName,
            primaryKey: primaryKey
        )
    }
    
    internal func deleteRecordMetadata(recordName: String) throws {
        let connection = try store.queue.connection(.writer)
        try deleteRecordMetadata(recordName: recordName, connection: connection)
    }
    
    internal func deleteRecordMetadata(recordName: String, connection: borrowing DatabaseConnection<Store>) throws {
        try PreparedStatement(
            sql: """
            DELETE FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.recordName.rawValue) = ?
            """,
            bindings: [store.identifier, recordName],
            handle: connection.handle
        ).run()
        store.attachment?.storeDidSave(inserted: [], updated: [], deleted: [])
    }
    
    internal func deleteOwnedRecordMetadata(
        entityName: String,
        primaryKey: String,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        try PreparedStatement(
            sql: """
            DELETE FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.entityName.rawValue) = ?
            AND \(RecordMetadataTable.entityPrimaryKey.rawValue) = ?
            """,
            bindings: [store.identifier, entityName, primaryKey],
            handle: connection.handle
        ).run()
    }
    
    internal func removeAppliedDeletedRecordMetadata(_ recordID: CKRecord.ID) throws {
        let connection = try store.queue.connection(.writer)
        if let ownership = try resolveRootRecordOwnership(for: recordID) {
            let relatedMetadata = try loadRelatedRecordMetadata(targetPrimaryKey: ownership.primaryKey.description)
            for metadata in relatedMetadata {
                try deleteRecordMetadata(recordName: metadata.recordName, connection: connection)
            }
            try deleteOwnedRecordMetadata(
                entityName: ownership.tableName,
                primaryKey: ownership.primaryKey.description,
                connection: connection
            )
            return
        }
        try deleteRecordMetadata(recordName: recordID.recordName, connection: connection)
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func loadSystemFieldsData(recordName: String) throws -> Data? {
        try store.queue.connection(.reader).fetch(
            """
            SELECT \(RecordMetadataTable.systemFields.rawValue) AS system_fields
            FROM \(RecordMetadataTable.tableName)
            WHERE \(RecordMetadataTable.storeIdentifier.rawValue) = ?
            AND \(RecordMetadataTable.recordName.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier, recordName]
        ).first?[0] as? Data
    }
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func loadServerRecord(from error: CKError) -> CKRecord? {
        let record = (error.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord)
        ?? (error.userInfo[CKRecordChangedErrorAncestorRecordKey] as? CKRecord)
        ?? (error.userInfo[CKRecordChangedErrorClientRecordKey] as? CKRecord)
        logger.trace("Loaded server-side conflict record.", metadata: [
            "found_record": "\(record != nil)",
            "error_code": "\(error.code.rawValue)"
        ])
        return record
    }
}

#endif
