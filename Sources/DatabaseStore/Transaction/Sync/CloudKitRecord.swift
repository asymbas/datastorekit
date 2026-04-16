//
//  CloudKitRecord.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
private import DataStoreRuntime
private import DataStoreSupport
private import Foundation
private import Logging
private import SQLiteHandle
private import SQLSupport
private import Synchronization
internal import DataStoreSQL

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
    internal func snapshot(for persistentIdentifier: PersistentIdentifier) throws -> Store.Snapshot? {
        guard let type = Schema.type(for: persistentIdentifier.entityName) else {
            throw SchemaError.entityNotRegistered
        }
        let primaryKey = self.store.manager.primaryKey(for: persistentIdentifier)
        do {
            let snapshot = try store.queue.reader {
                try $0.fetch(for: store.manager.primaryKey(for: persistentIdentifier), as: type)
            }
            logger.trace("Loaded current local snapshot.", metadata: [
                "entity_name": "\(persistentIdentifier.entityName)",
                "primary_key": "\(primaryKey)",
                "snapshot_found": "\(snapshot != nil)"
            ])
            return snapshot
        } catch {
            logger.trace("Failed to fetch current local snapshot: \(error)", metadata: [
                "entity_name": "\(persistentIdentifier.entityName)",
                "primary_key": "\(primaryKey)"
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
    
    internal func resolveRootRecordOwnership(for record: CKRecord) throws -> (entityName: String, primaryKey: String)? {
        switch record[pk] as? String {
        case let primaryKey?:
            (
                entityName: makeEntityName(fromRecordType: record.recordType),
                primaryKey: primaryKey
            )
        case nil:
            try resolveRootRecordOwnership(for: record.recordID)
        }
    }
    
    internal func resolveRootRecordOwnership(for recordID: CKRecord.ID) throws -> (entityName: String, primaryKey: String)? {
        switch try loadRecordMetadata(recordName: recordID.recordName) {
        case let metadata? where metadata.recordType == makeRecordType(metadata.entityName):
            (entityName: metadata.entityName, primaryKey: metadata.primaryKey)
        default:
            nil
        }
    }
    
}

extension DatabaseConfiguration.CloudKitDatabase.Replicator {
    internal func loadRecordMetadata(recordType: String, for persistentIdentifier: PersistentIdentifier)
    throws -> RecordMetadata? {
        if let metadata = self.identifiers[persistentIdentifier] {
            return metadata
        }
        let entityName = persistentIdentifier.entityName
        let primaryKey = self.store.manager.primaryKey(for: persistentIdentifier)
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
            bindings: [store.identifier, entityName, primaryKey, recordType]
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
        assert(persistentIdentifier.entityName == entityName)
        assert(persistentIdentifier.primaryKey().description == primaryKey)
        let metadata = RecordMetadata(
            recordType: recordType,
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: row[4] as? String
        )
        self.identifiers[persistentIdentifier] = metadata
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
        let metadata = RecordMetadata(
            recordType: recordType,
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: row["related_pk"] as? String
        )
        let persistentIdentifier = try PersistentIdentifier.identifier(for: store.identifier, entityName: entityName, primaryKey: primaryKey)
        self.identifiers[persistentIdentifier] = metadata
        return metadata
    }
    
    internal func loadRecordMetadata(recordType: String, entityName: String, primaryKey: String)
    throws -> RecordMetadata? {
        try loadRecordMetadata(
            recordType: recordType,
            for: .identifier(for: store.identifier, entityName: entityName, primaryKey: primaryKey)
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
        let metadata = RecordMetadata(
            recordType: recordType,
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: row[4] as? String
        )
        let persistentIdentifier = try PersistentIdentifier.identifier(for: store.identifier, entityName: entityName, primaryKey: primaryKey)
        self.identifiers[persistentIdentifier] = metadata
        return metadata
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
            let metadata = RecordMetadata(
                recordType: recordType,
                recordName: recordName,
                entityName: entityName,
                primaryKey: primaryKey,
                targetPrimaryKey: row["related_pk"] as? String
            )
            let persistentIdentifier = try PersistentIdentifier.identifier(for: store.identifier, entityName: entityName, primaryKey: primaryKey)
            if identifiers[persistentIdentifier] == nil {
                identifiers[persistentIdentifier] = metadata
            }
            return metadata
        }
    }
    
    internal func loadOwnedRecordMetadata(for persistentIdentifier: PersistentIdentifier) throws -> [RecordMetadata] {
        let primaryKey = self.store.manager.primaryKey(for: persistentIdentifier)
        return try store.queue.connection(.reader).query(
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
            bindings: [store.identifier, persistentIdentifier.entityName, primaryKey]
        ).compactMap { row in
            guard let recordType = row["record_type"] as? String,
                  let recordName = row["record_name"] as? String,
                  let entityName = row["entity_name"] as? String,
                  let primaryKey = row["entity_pk"] as? String else {
                return Optional<RecordMetadata>.none
            }
            let metadata = RecordMetadata(
                recordType: recordType,
                recordName: recordName,
                entityName: entityName,
                primaryKey: primaryKey,
                targetPrimaryKey: row["related_pk"] as? String
            )
            if identifiers[persistentIdentifier] == nil {
                identifiers[persistentIdentifier] = metadata
            }
            return metadata
        }
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
        let recordType = makeRecordType(intermediaryTableName)
        if let recordName = try loadReferenceRecordMetadata(
            recordType: recordType,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: targetPrimaryKey
        )?.recordName {
            return recordName
        }
        if let existingRecordName = try loadReverseReferenceRecordMetadata(
            recordType: recordType,
            primaryKey: primaryKey,
            targetPrimaryKey: targetPrimaryKey
        )?.recordName {
            let connection = try store.queue.connection(.writer)
            try upsertRecordMetadata(
                recordType: recordType,
                recordName: try makeCloudKitRecordName(),
                entityName: entityName,
                primaryKey: primaryKey,
                targetPrimaryKey: targetPrimaryKey,
                systemFields: nil,
                connection: connection
            )
            return existingRecordName
        }
        let recordName = try makeCloudKitRecordName()
        let connection = try store.queue.connection(.writer)
        try upsertRecordMetadata(
            recordType: recordType,
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: targetPrimaryKey,
            systemFields: nil,
            connection: connection
        )
        return recordName
    }
    
    internal func loadReverseReferenceRecordMetadata(
        recordType: String,
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
            AND \(RecordMetadataTable.recordType.rawValue) = ?
            AND \(RecordMetadataTable.entityPrimaryKey.rawValue) = ?
            AND \(RecordMetadataTable.entityTargetPrimaryKey.rawValue) = ?
            LIMIT 1
            """,
            bindings: [store.identifier, recordType, targetPrimaryKey, primaryKey]
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
        return RecordMetadata(
            recordType: recordType,
            recordName: recordName,
            entityName: entityName,
            primaryKey: primaryKey,
            targetPrimaryKey: row[4] as? String
        )
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
        if let metadata = try loadRecordMetadata(recordName: savedRecord.recordID.recordName) {
            try upsertRecordMetadata(
                recordType: metadata.recordType,
                recordName: metadata.recordName,
                entityName: metadata.entityName,
                primaryKey: metadata.primaryKey,
                targetPrimaryKey: metadata.targetPrimaryKey,
                systemFields: savedRecord.systemFieldsData(),
                connection: connection
            )
            return
        }
        if let root = try resolveRootRecordOwnership(for: savedRecord) {
            try upsertRecordMetadata(
                recordType: savedRecord.recordType,
                recordName: savedRecord.recordID.recordName,
                entityName: root.entityName,
                primaryKey: root.primaryKey,
                targetPrimaryKey: nil,
                systemFields: savedRecord.systemFieldsData(),
                connection: connection
            )
        }
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
            let relatedMetadata = try loadRelatedRecordMetadata(targetPrimaryKey: ownership.primaryKey)
            for metadata in relatedMetadata {
                try deleteRecordMetadata(recordName: metadata.recordName, connection: connection)
            }
            try deleteOwnedRecordMetadata(
                entityName: ownership.entityName,
                primaryKey: ownership.primaryKey,
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
