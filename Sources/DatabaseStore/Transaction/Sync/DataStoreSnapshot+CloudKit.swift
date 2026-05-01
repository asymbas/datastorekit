//
//  DataStoreSnapshot+CloudKit.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreRuntime
private import DataStoreSQL
private import DataStoreSupport
private import Logging
private import SQLSupport
private import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.cloudkit")

#if canImport(CloudKit)

internal import CloudKit

extension DatabaseSnapshot {
    nonisolated internal init(
        _ existingSnapshot: Self? = nil,
        record: CKRecord,
        store: Store,
        resolveRelationshipPrimaryKey: (_ recordName: String, _ destinationEntityName: String) throws -> String?
    ) throws {
        guard let primaryKey = record[pk] as? String else {
            preconditionFailure("CKRecord primary key must be present.")
        }
        logger.debug("Creating DatabaseSnapshot for CKRecord with primary key: \(primaryKey)")
        try self.init(
            primaryKey: primaryKey,
            storeIdentifier: store.identifier,
            type: nil,
            entityName: makeEntityName(fromRecordType: record.recordType)
        )
        let externalStorageURL = store.configuration.externalStorageURL
        let allKeys = Set(record.allKeys())
        for property in self.properties {
            guard allKeys.contains(property.name) else {
                if let existingSnapshot {
                    self.values[property.index] = existingSnapshot.values[property.index]
                    logger.debug("Incoming snapshot copied existing value (non-existent): \(property)")
                } else if let relationship = property.metadata as? Schema.Relationship,
                          !relationship.isToOneRelationship && !relationship.isOptional {
                    self.values[property.index] = [PersistentIdentifier]()
                }
                continue
            }
            switch property.metadata {
            case let attribute as Schema.Attribute:
                let cloudKitValue: CKRecordValue
                if attribute.options.contains(.allowsCloudEncryption) {
                    guard let value = record.encryptedValues[property.name] else {
                        logger.warning("Attribute does not match to any CKRecord encrypted field key: \(property)")
                        continue
                    }
                    cloudKitValue = value
                } else {
                    guard let value = record[property.name] else {
                        logger.warning("Attribute does not match to any CKRecord field key: \(property)")
                        continue
                    }
                    cloudKitValue = value
                }
                guard let anyValue: any Sendable = sendable(cast: cloudKitValue) else {
                    preconditionFailure("Unsupported CKRecord value type: \(Swift.type(of: cloudKitValue))")
                }
                let value = try setValue(anyValue, attribute: attribute)
                try setValue(attribute, value, at: property.index, externalStorageURL: externalStorageURL)
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                guard let value = record[property.name] else {
                    logger.warning("Relationship does not match to any CKRecord field key: \(property)")
                    continue
                }
                guard let recordName = value as? String else {
                    preconditionFailure("Relationship field value is the record name and must be a String.")
                }
                guard let resolvedPrimaryKey = try resolveRelationshipPrimaryKey(recordName, relationship.destination) else {
                    logger.warning("Unresolved to-one relationship dependency: \(property) = \(recordName)")
                    continue
                }
                try setValue(relationship, resolvedPrimaryKey, at: property.index)
            case let relationship as Schema.Relationship:
                if let existingSnapshot {
                    self.values[property.index] = existingSnapshot.values[property.index]
                    logger.debug("Incoming snapshot copied existing to-many relationships: \(property)")
                } else if !relationship.isOptional && !relationship.isToOneRelationship {
                    self.values[property.index] = [PersistentIdentifier]()
                    logger.debug("Incoming snapshot initialized default empty to-many relationships: \(property)")
                }
            default:
                continue
            }
        }
    }
    
    nonisolated private func setValue(_ value: any Sendable, attribute: Schema.Attribute) throws -> any Sendable {
        let description = "\(entityName).\(attribute.name) as \(attribute.valueType).self"
        guard let valueType = unwrapOptionalMetatype(attribute.valueType) as? any DataStoreSnapshotValue.Type else {
            preconditionFailure("Attribute must conform to DataStoreSnapshotValue: \(description)")
        }
        if attribute.options.contains(.externalStorage), let value = value as? CKAsset {
            guard let fileURL = value.fileURL else {
                preconditionFailure("CKAsset file URL must be present for external storage attribute: \(description)")
            }
            if valueType is Data.Type {
                return try Data(contentsOf: fileURL)
            } else {
                return try JSONDecoder().decode(valueType, from: Data(contentsOf: fileURL))
            }
        }
        switch value {
        case is SQLNull, is NSNull:
            return SQLNull()
        case let value as NSNumber:
            if CFGetTypeID(value) == CFBooleanGetTypeID() { return Int64(value.boolValue ? 1 : 0) }
            if CFNumberIsFloatType(value) { return value.doubleValue }
            return value.int64Value
        case let value as NSDate: return value as Date
        case let value as NSString: return value as String
        case let value as NSData: return value as Data
        case let value as any DataStoreSnapshotValue:
            guard let convertedValue = SQLValue.convert(value, as: valueType) else {
                preconditionFailure(
                    """
                    CKRecord attribute value could not be converted:
                    \(description) = \(value) as \(Swift.type(of: value))
                    """
                )
            }
            return convertedValue
        case let value:
            preconditionFailure(
                """
                Unsupported CKRecord attribute value:
                \(description) = \(value) as \(Swift.type(of: value))
                """
            )
        }
    }
}

extension DatabaseSnapshot {
    nonisolated internal func records(
        rootRecordName: String,
        systemFieldsData: Data?,
        zoneID: CKRecordZone.ID,
        store: Store,
        loadSystemFieldsData: (_ recordName: String) throws -> Data?,
        referenceRecordName: (_ intermediaryTableName: String, _ primaryKey: String) throws -> String,
        resolveRelationshipRecordName: (_ destinationEntityName: String, _ primaryKey: String) throws -> String
    ) throws -> [CKRecord] {
        [try createCloudKitRecord(
            for: rootRecordName,
            systemFieldsData: systemFieldsData,
            zoneID: zoneID,
            store: store,
            resolveRelationshipRecordName: resolveRelationshipRecordName
        )] +
        (try createReferenceCloudKitRecords(
            zoneID: zoneID,
            store: store,
            loadSystemFieldsData: loadSystemFieldsData,
            referenceRecordName: referenceRecordName,
            resolveRelationshipRecordName: resolveRelationshipRecordName
        ))
    }
    
    nonisolated internal func createCloudKitRecord(
        for recordName: String,
        systemFieldsData: Data?,
        zoneID: CKRecordZone.ID,
        store: Store,
        resolveRelationshipRecordName: (_ destinationEntityName: String, _ primaryKey: String) throws -> String
    ) throws -> CKRecord {
        logger.debug("Creating CKRecord from snapshot with primary key: \(primaryKey)")
        let record: CKRecord
        if let systemFieldsData, let existingRecord = try? CKRecord.fromSystemFields(systemFieldsData) {
            record = existingRecord
        } else {
            record = CKRecord(
                recordType: makeRecordType(entityName),
                recordID: .init(recordName: validateCloudKitRecordName(recordName), zoneID: zoneID)
            )
        }
        let externalStorageURL = store.configuration.externalStorageURL
        record[pk] = primaryKey as CKRecordValue
        for (property, value) in zip(properties, values) {
            switch property.metadata {
            case let attribute as Schema.Attribute:
                if let recordValue = try getValue(value, attribute: attribute, externalStorageURL: externalStorageURL) {
                    switch attribute.options.contains(.allowsCloudEncryption) {
                    case true:
                        record.encryptedValues[property.name] = recordValue
                        logger.trace("CKRecord attribute encrypted value set: \(property) = \(recordValue)")
                    case false:
                        record[property.name] = recordValue
                        logger.trace("CKRecord attribute value set: \(property) = \(recordValue)")
                    }
                } else {
                    if attribute.options.contains(.allowsCloudEncryption) {
                        record.encryptedValues[property.name] = nil
                    }
                    record.setObject(nil, forKey: property.name)
                    logger.trace("CKRecord attribute value set: \(property) = nil")
                }
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                if let identifier = value as? PersistentIdentifier {
                    let relatedPrimaryKey = store.manager.primaryKey(for: identifier)
                    let relatedRecordName = try resolveRelationshipRecordName(relationship.destination, relatedPrimaryKey)
                    record[property.name] = relatedRecordName as CKRecordValue
                    logger.trace("CKRecord to-one relationship value set: \(property) = \(primaryKey)")
                } else if relationship.isOptional {
                    record.setObject(nil, forKey: property.name)
                    logger.trace("CKRecord to-one relationship value set: \(property) = nil")
                } else {
                    preconditionFailure()
                }
            case is Schema.Relationship:
                logger.debug("CKRecord to-many relationship value skipped: \(property)")
                continue
            default:
                logger.debug("CKRecord with unknown property skipped: \(property)")
                continue
            }
        }
        return record
    }
    
    nonisolated internal func createReferenceCloudKitRecords(
        zoneID: CKRecordZone.ID,
        store: Store,
        loadSystemFieldsData: (_ recordName: String) throws -> Data?,
        referenceRecordName: (_ intermediaryTableName: String, _ primaryKey: String) throws -> String,
        resolveRelationshipRecordName: (_ destinationEntityName: String, _ primaryKey: String) throws -> String
    ) throws -> [CKRecord] {
        var records = [CKRecord]()
        logger.debug("Creating intermediary CloudKit records from snapshot with primary key: \(primaryKey)")
        for (property, value) in zip(properties, values) {
            guard let relationship = property.metadata as? Schema.Relationship else {
                continue
            }
            guard !relationship.isToOneRelationship else {
                continue
            }
            guard let reference = property.reference, reference.count == 2 else {
                continue
            }
            guard let identifiers = value as? [PersistentIdentifier] else {
                if relationship.isOptional { continue }
                preconditionFailure()
            }
            let intermediaryTableName = reference[0].destinationTable
            let sourceFieldName = "key_" + reference[0].rhsColumn
            let destinationFieldName = "key_" + reference[1].lhsColumn
            for identifier in identifiers {
                let destinationPrimaryKey = store.manager.primaryKey(for: identifier)
                let recordName = try validateCloudKitRecordName(referenceRecordName(
                    intermediaryTableName,
                    destinationPrimaryKey
                ))
                let record: CKRecord
                if let systemFieldsData = try loadSystemFieldsData(recordName),
                   let existingRecord = try? CKRecord.fromSystemFields(systemFieldsData) {
                    record = existingRecord
                } else {
                    record = CKRecord(
                        recordType: makeRecordType(intermediaryTableName),
                        recordID: .init(recordName: recordName, zoneID: zoneID)
                    )
                }
                let sourceRecordName = try resolveRelationshipRecordName(entityName, primaryKey)
                let destinationRecordName = try resolveRelationshipRecordName(
                    relationship.destination,
                    destinationPrimaryKey
                )
                record[sourceFieldName] = sourceRecordName as CKRecordValue
                record[destinationFieldName] = destinationRecordName as CKRecordValue
                records.append(record)
            }
        }
        return records
    }
    
    nonisolated private func getValue(
        _ value: any Sendable,
        attribute: Schema.Attribute,
        externalStorageURL: URL
    ) throws -> CKRecordValue? {
        let description = "\(entityName).\(attribute.name) as \(attribute.valueType).self"
        let resolvedValue: any Sendable
        switch value {
        case is SQLNull, is NSNull:
            return nil
        case _ where attribute.options.contains(.externalStorage):
            let relativePath = "\(entityName)/\(attribute.name)/\(primaryKey)"
            let url = externalStorageURL.appending(path: relativePath)
            guard FileManager.default.fileExists(atPath: url.path) else {
                logger.notice("External storage data could not be found: \(description) = \(url.path)")
                return nil
            }
            return CKAsset(fileURL: url)
        default:
            resolvedValue = value
        }
        switch SQLValue(any: resolvedValue).base {
        case is SQLNull, is NSNull: return nil
        case let value as Int64: return NSNumber(value: value)
        case let value as Double: return NSNumber(value: value)
        case let value as Date: return value as NSDate
        case let value as String: return value as NSString
        case let value as Data: return value as NSData
        default:
            preconditionFailure("Attribute is not representable as CKRecordValue: \(description) = \(resolvedValue)")
        }
    }
}

#endif
