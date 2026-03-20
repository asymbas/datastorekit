//
//  DataStoreSnapshot+CloudKit.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreRuntime
import DataStoreSQL
import DataStoreSupport
import Logging
import SQLSupport
import SwiftData

#if canImport(CloudKit)
import CloudKit
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.cloudkit")

nonisolated private func validateCloudKitRecordName(_ value: String) -> String {
    precondition(!value.isEmpty, "CloudKit record name must not be empty.")
    precondition(value.first != "_", "CloudKit record name must not start with an underscore.")
    precondition(value.utf8.count <= 255, "CloudKit record name must not exceed 255 ASCII characters.")
    precondition(value.allSatisfy(\.isASCII), "CloudKit record name must contain only ASCII characters.")
    return value
}

extension PersistentIdentifier {
    nonisolated internal func recordName(_ manager: ModelManager? = nil) -> String {
        guard let storeIdentifier = self.storeIdentifier else {
            preconditionFailure()
        }
        let primaryKey = manager?.primaryKey(for: self) ?? self.primaryKey()
        return validateCloudKitRecordName("\(storeIdentifier):\(self.entityName):\(primaryKey)")
    }
}

#if canImport(CloudKit)

extension DatabaseSnapshot {
    nonisolated internal init(record: CKRecord, store: Store) throws {
        guard let primaryKey = record[pk] as? String else {
            preconditionFailure("CKRecord primary key must be present.")
        }
        try self.init(
            primaryKey: primaryKey,
            storeIdentifier: store.identifier,
            type: nil,
            entityName: record.recordType
        )
        let externalStorageURL = store.configuration.externalStorageURL
        for (key, value) in record {
            guard let property = self.getProperty(name: key) else {
                logger.debug("PropertyMetadata not found with CKRecord field key: \(key) = \(value)")
                continue
            }
            guard let value: any Sendable = sendable(cast: value) else {
                preconditionFailure("Unsupported CKRecord value type: \(Swift.type(of: value))")
            }
            switch property.metadata {
            case let attribute as Schema.Attribute:
                try setValue(attribute, value, at: property.index, externalStorageURL: externalStorageURL)
            case _ as Schema.Relationship:
                fatalError("Relationship is not implemented yet.")
            default:
                continue
            }
        }
    }
    
    nonisolated internal func createCloudKitRecord(
        zoneID: CKRecordZone.ID,
        manager: ModelManager? = nil
    ) throws -> CKRecord {
        let sourcePrimaryKey = manager?.primaryKey(for: persistentIdentifier) ?? self.primaryKey
        let record = CKRecord(
            recordType: entityName,
            recordID: .init(recordName: persistentIdentifier.recordName(manager), zoneID: zoneID)
        )
        record[pk] = sourcePrimaryKey as CKRecordValue
        for (property, value) in zip(properties, values) {
            switch property.metadata {
            case is Schema.Attribute:
                guard let normalizedValue = SQLValue(any: value).base as? CKRecordValue else {
                    logger.error("Value is not a CKRecordValue: \(property) = \(value)")
                    continue
                }
                record[property.name] = normalizedValue
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                if let identifier = value as? PersistentIdentifier {
                    let primaryKey = manager?.primaryKey(for: identifier) ?? identifier.primaryKey()
                    record[property.name] = primaryKey as CKRecordValue
                } else if relationship.isOptional {
                    record.setObject(nil, forKey: property.name)
                } else {
                    preconditionFailure()
                }
            case is Schema.Relationship:
                continue
            default:
                continue
            }
        }
        return record
    }
}

extension DatabaseSnapshot {
    nonisolated private func intermediaryRecordName(
        reference: [TableReference],
        destination destinationIdentifier: PersistentIdentifier,
        manager: ModelManager? = nil
    ) -> String {
        let sourcePrimaryKey = manager?.primaryKey(for: persistentIdentifier) ?? persistentIdentifier.primaryKey()
        let destinationPrimaryKey = manager?.primaryKey(for: destinationIdentifier) ?? destinationIdentifier.primaryKey()
        return validateCloudKitRecordName("\(reference[0].lhsTable):\(sourcePrimaryKey):\(destinationPrimaryKey)")
    }
    
    nonisolated internal func createReferenceRecords(
        zoneID: CKRecordZone.ID,
        manager: ModelManager? = nil
    ) throws -> [CKRecord] {
        var records: [CKRecord] = []
        let sourcePrimaryKey = manager?.primaryKey(for: persistentIdentifier) ?? persistentIdentifier.primaryKey()
        for (property, value) in zip(properties, values) {
            guard let relationship = property.metadata as? Schema.Relationship else {
                continue
            }
            guard let reference = property.reference, reference.count == 2 else {
                continue
            }
            guard let identifiers = value as? [PersistentIdentifier] else {
                if relationship.isOptional {
                    continue
                }
                preconditionFailure()
            }
            for identifier in identifiers {
                let destinationPrimaryKey = manager?.primaryKey(for: identifier) ?? identifier.primaryKey()
                let record = CKRecord(
                    recordType: reference[0].lhsTable,
                    recordID: .init(
                        recordName: intermediaryRecordName(reference: reference, destination: identifier, manager: manager),
                        zoneID: zoneID
                    )
                )
                record[reference[0].lhsColumn] = sourcePrimaryKey as CKRecordValue
                record[reference[1].lhsColumn] = destinationPrimaryKey as CKRecordValue
                records.append(record)
            }
        }
        return records
    }
    
    nonisolated internal func records(
        zoneID: CKRecordZone.ID,
        manager: ModelManager? = nil
    ) throws -> [CKRecord] {
        [try createCloudKitRecord(zoneID: zoneID, manager: manager)] +
        (try createReferenceRecords(zoneID: zoneID, manager: manager))
    }
}

#endif
