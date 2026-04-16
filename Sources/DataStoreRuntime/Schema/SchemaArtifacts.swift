//
//  SchemaArtifacts.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreSQL
private import DataStoreSupport
private import Logging
private import SQLSupport
package import SQLiteStatement

#if swift(>=6.2)
package import SwiftData
#else
@preconcurrency package import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.bootstrap")

nonisolated package func normalizedIndexName(_ tableName: String, _ columnGroup: [String]) -> String {
    ([tableName] + columnGroup + ["Index"]).joined(separator: "_")
}

/// Creates table indexes as described in `Schema.Index<T>`.
///
/// - Important:
///   Translating indices as an `rtree` type is not supported.
/// - Parameters:
///   - type: The model type to create artifacts for.
///   - schemaProperty: The `Schema.Index` instance from the model's schema metadata.
/// - Returns:
///   An array of table indexes associated with the model.
nonisolated package func createTableIndexes<T>(
    for type: T.Type,
    on schemaProperty: any SchemaProperty
) -> [SQLIndex] where T: PersistentModel {
    guard let schemaIndex = schemaProperty as? Schema.Index<T> else {
        preconditionFailure("Expected Schema.Index<\(T.self)>, but received: \(schemaProperty)")
    }
    let entityName = Schema.entityName(for: T.self)
    logger.trace("Translating Schema.Index<\(T.self)>: \(schemaIndex.indices)")
    var indexes = [SQLIndex]()
    for types in schemaIndex.indices {
        let keyPaths: [PartialKeyPath<T>]
        switch types {
        case .binary(let propertyGroups):
            keyPaths = propertyGroups
        case .rtree(let propertyGroups):
            fatalError("Translating indices as an rtree type is not supported: \(propertyGroups)")
        @unknown default:
            fatalError(DataStoreError.unsupportedFeature.localizedDescription)
        }
        var columnGroup = [String]()
        for keyPath in keyPaths {
            guard let keyPath: PartialKeyPath<T> & Sendable = sendable(cast: keyPath),
                  let property = T.schemaMetadata(for: keyPath) else {
                fatalError("No PropertyMetadata found at key path: \(keyPath)")
            }
            switch property.metadata {
            case let attribute as Schema.Attribute:
                columnGroup.append(attribute.name)
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                columnGroup.append(relationship.name + "_pk")
            default:
                logger.notice("Unknown property type (index): \(entityName).\(property)")
            }
        }
        guard !columnGroup.isEmpty else {
            continue
        }
        let name = normalizedIndexName(entityName, columnGroup)
        let index = SQLIndex(schema: nil, name: name, table: entityName) {
            for column in columnGroup {
                SQLIndexedColumn(name: column)
            }
        }
        indexes.append(index)
        logger.debug("Produced SQL index.", metadata: [
            "table": .string(entityName),
            "columns": .stringConvertible(columnGroup),
            "sql": .string(index.sql)
        ])
    }
    return indexes
}

/// Creates `UNIQUE` table constraints as described in `Schema.Unique<T>`.
///
/// - Parameters:
///   - type: The model type to create artifacts for.
///   - schemaProperty: The `Schema.Unique` instance from the model's schema metadata.
/// - Returns:
///   An array of `UNIQUE` table constraints associated with the model.
nonisolated package func createUniqueTableConstraints<T>(
    for type: T.Type,
    on schemaProperty: any SchemaProperty
) -> [TableConstraint] where T: PersistentModel {
    guard let schemaUnique = schemaProperty as? Schema.Unique<T> else {
        preconditionFailure("Expected Schema.Unique<\(T.self)>, but received: \(schemaProperty)")
    }
    let entityName = Schema.entityName(for: T.self)
    logger.trace("Translating Schema.Unique<\(T.self)>: \(schemaUnique.constraints)")
    var constraints = [TableConstraint]()
    for keyPaths in schemaUnique.constraints {
        var columnGroup = [String]()
        for keyPath in keyPaths {
            guard let keyPath: PartialKeyPath<T> & Sendable = sendable(cast: keyPath),
                  let property = T.schemaMetadata(for: keyPath) else {
                fatalError("No PropertyMetadata found at key path: \(keyPath)")
            }
            switch property.metadata {
            case let attribute as Schema.Attribute:
                columnGroup.append(attribute.name)
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                columnGroup.append(relationship.name + "_pk")
            default:
                logger.notice("Unknown property type (unique): \(entityName).\(property)")
            }
        }
        guard !columnGroup.isEmpty else {
            continue
        }
        let constraint = TableConstraint.unique(columnGroup)
        constraints.append(constraint)
        logger.debug("Produced SQL UNIQUE table constraint.", metadata: [
            "table": .string(entityName),
            "columns": .stringConvertible(columnGroup),
            "sql": .string(constraint.sql)
        ])
    }
    return constraints
}

/// Creates table indexes as described in `Schema.Entity`.
///
/// - Important:
///   Translating indices as an `rtree` type is not supported.
/// - Parameter entity:
///   The entity that defines its indices.
/// - Returns:
///   An array of table indexes derived from the entity.
nonisolated package func createTableIndexes(for entity: Schema.Entity)
-> [SQLIndex] {
    let entityName = entity.name
    logger.trace("Translating \(entityName) entity: \(entity.indices)")
    var indexes = [SQLIndex]()
    for propertyGroups in entity.indices {
        var columnGroup = [String]()
        var type: String?
        for propertyName in propertyGroups {
            switch propertyName {
            case "binary" where type == nil:
                type = "binary"
                continue
            case "rtree" where type == nil:
                type = "rtree"
                fatalError("Translating indices as an rtree type is not supported.")
            default:
                break
            }
            guard let property = entity.storedPropertiesByName[propertyName] else {
                fatalError("Property not found in entity: \(entityName).\(propertyName)")
            }
            switch property {
            case let attribute as Schema.Attribute:
                columnGroup.append(attribute.name)
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                columnGroup.append(relationship.name + "_pk")
            default:
                continue
            }
        }
        guard !columnGroup.isEmpty else {
            continue
        }
        let name = normalizedIndexName(entityName, columnGroup)
        let index = SQLIndex(schema: nil, name: name, table: entityName) {
            for column in columnGroup {
                SQLIndexedColumn(name: column)
            }
        }
        indexes.append(index)
        logger.debug("Produced SQL index.", metadata: [
            "table": .string(entityName),
            "columns": .stringConvertible(columnGroup),
            "sql": .string(index.sql)
        ])
    }
    return indexes
}

/// Creates `UNIQUE` table constraints as described in `Schema.Entity`.
///
/// - Parameter entity:
///   The entity that definies its uniqueness constraints.
/// - Returns:
///   An array of `UNIQUE` table constraints derived from the entity.
nonisolated package func createUniqueTableConstraints(for entity: Schema.Entity)
-> [TableConstraint] {
    let entityName = entity.name
    logger.trace("Translating \(entityName) entity: \(entity.uniquenessConstraints)")
    var constraints = [TableConstraint]()
    for propertyGroups in entity.uniquenessConstraints {
        var columnGroup = [String]()
        for propertyName in propertyGroups {
            guard let property = entity.storedPropertiesByName[propertyName] else {
                fatalError("Property not found in entity: \(entityName).\(propertyName)")
            }
            switch property {
            case let attribute as Schema.Attribute:
                columnGroup.append(attribute.name)
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                columnGroup.append(relationship.name + "_pk")
            default:
                continue
            }
        }
        guard !columnGroup.isEmpty else {
            continue
        }
        let constraint = TableConstraint.unique(columnGroup)
        constraints.append(constraint)
        logger.debug("Produced SQL UNIQUE table constraint.", metadata: [
            "table": .string(entityName),
            "columns": .stringConvertible(columnGroup),
            "sql": .string(constraint.sql)
        ])
    }
    return constraints
}

nonisolated package func createUniqueColumnGroups(for entity: Schema.Entity) -> [[String]] {
    let entityName = entity.name
    var groups = [[String]]()
    for propertyGroup in entity.uniquenessConstraints {
        var columnGroup = [String]()
        for propertyName in propertyGroup {
            guard let property = entity.storedPropertiesByName[propertyName] else {
                fatalError("Property not found in entity: \(entityName).\(propertyName)")
            }
            switch property {
            case let attribute as Schema.Attribute:
                columnGroup.append(attribute.name)
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                columnGroup.append(relationship.name + "_pk")
            default:
                continue
            }
        }
        guard !columnGroup.isEmpty else {
            continue
        }
        groups.append(columnGroup)
    }
    return groups
}
