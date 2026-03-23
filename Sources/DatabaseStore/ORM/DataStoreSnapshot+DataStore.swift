//
//  DataStoreSnapshot+DatabaseStore.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Collections
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

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension DatabaseSnapshot {
    /// Creates a snapshot from fetched data.
    ///
    /// The discriminator is a `PropertyMetadata` that is not part of the model's schema metadata.
    /// It is instantiated when the model is loaded during `SQLPredicateTranslator<T>`.
    /// This provides the model's metatype, the entity, and its primary key.
    ///
    /// - Parameters:
    ///   - store:
    ///     The data store this model will be associated to.
    ///   - registry:
    ///     The cache registry bound to one or more `ModelContext`,
    ///   - properties:
    ///     The model properties selected to be included in the result set.
    ///     It is required that the first property is the result set discriminator and primary key.
    ///   - values:
    ///     The result column values where it is expected to be aligned with the given properties.
    ///   - relatedSnapshots:
    ///     Snapshots that were referenced and included in the result set using the `JOIN` clause.
    ///     The purpose is to provide any prefetched relationships requested by `FetchDescriptor<T>`.
    nonisolated public init(
        store: DatabaseStore,
        registry: SnapshotRegistry? = nil,
        properties: consuming ArraySlice<PropertyMetadata>,
        values: consuming ArraySlice<any Sendable>,
        relatedSnapshots: inout [PersistentIdentifier: Self]
    ) throws {
        self = try Self(
            storeIdentifier: store.identifier,
            configuration: store.configuration,
            queue: store.queue,
            registry: registry,
            properties: properties,
            values: values,
            relatedSnapshots: &relatedSnapshots
        )
    }
    
    nonisolated public init(
        storeIdentifier: String,
        configuration: DatabaseConfiguration,
        queue: DatabaseQueue<Store>,
        registry: SnapshotRegistry? = nil,
        properties: consuming ArraySlice<PropertyMetadata>,
        values: consuming ArraySlice<any Sendable>,
        relatedSnapshots: inout [PersistentIdentifier: Self]
    ) throws {
        guard let discriminator = properties.first else {
            throw Self.Error.insufficientProperties
        }
        guard let primaryKey = values.first as? String else {
            throw SQLError(.columnNotFound(pk), message: String(describing: values))
        }
        guard let type = discriminator.valueType as? any PersistentModel.Type else {
            throw ModelMappingError.discriminatorKeyNotFound
        }
        guard let value = discriminator.defaultValue ?? configuration.schema?.entity(for: type),
              let entity = value as? Schema.Entity else {
            throw SchemaError.entityNotRegistered
        }
        var properties = properties
        logger.trace("Creating \(entity.name) snapshot: \(zip(properties.map(\.name), values))")
        let persistentIdentifier = try PersistentIdentifier.identifier(
            for: storeIdentifier,
            entityName: entity.name,
            primaryKey: primaryKey
        )
        var inheritedValues = [String: any DataStoreSnapshotValue]()
        if entity.superentity != nil || !entity.subentities.isEmpty {
            inheritedValues = try Self.fetchInheritanceDependencies(
                for: persistentIdentifier,
                on: entity,
                connection: queue.connection(nil),
                direction: .both,
                excludeExistingValues: true
            )
        }
        if let relatedSnapshot = relatedSnapshots[persistentIdentifier] {
            logger.trace("\(entity.name) snapshot found in related snapshots: \(primaryKey)")
            self = consume relatedSnapshot
            return
        }
        try self.init(
            primaryKey: primaryKey,
            storeIdentifier: storeIdentifier,
            type: type,
            entityName: entity.name,
            properties: .init(type.databaseSchemaMetadata)
        )
        let configuration = configuration
        var excludedProperties = [PropertyMetadata]()
        var cursor = values.index(after: values.startIndex)
        properties = properties.dropFirst()
        for (index, property) in properties.enumerated() {
            let offset = cursor
            let description = "\(primaryKey) \(discriminator.index)-\(entityName).\(property)"
            logger.trace("Assigning result set column \(offset) to property \(index): \(description)")
            if let inheritedValue = inheritedValues[property.name] {
                logger.trace("Property has an inherited value: \(description) = \(inheritedValue)")
                try setValue(
                    inheritedValue,
                    at: property,
                    storeIdentifier: storeIdentifier,
                    externalStorageURL: configuration.externalStorageURL
                )
                continue
            }
            if !property.isSelected {
                logger.debug("Property was not a selected result set column: \(description)")
                self.properties[property.index] = property
                excludedProperties.append(property)
                continue
            }
            guard offset < values.endIndex else { break }
            if discriminator.index != property.index && property.name == pk {
                do {
                    assert(
                        property.valueType is any (PersistentModel & SendableMetatype).Type,
                        "Unexpected type used as discriminator in result set: \(description)"
                    )
                    // Consider as EOL in a result set.
                    // A `LEFT JOIN` on an optional to-one relationship will include `NULL`.
                    if values[offset] is SQLNull {
                        break
                    }
                    // Check beforehand that the value is the primary key type.
                    guard values[offset] as? String != nil else {
                        break
                    }
                    let relatedSnapshot = try Self(
                        storeIdentifier: storeIdentifier,
                        configuration: configuration,
                        queue: queue,
                        registry: registry,
                        properties: properties[properties.index(properties.startIndex, offsetBy: index)...],
                        values: values[offset...],
                        relatedSnapshots: &relatedSnapshots
                    )
                    logger.debug("Created a related \(relatedSnapshot.entityName) snapshot: \(description)")
                    relatedSnapshots[relatedSnapshot.persistentIdentifier] = consume relatedSnapshot
                    break
                } catch ModelMappingError.discriminatorKeyNotFound {
                    logger.notice("Related snapshot for this property has no discriminator: \(description)")
                    break
                } catch {
                    logger.error("Related snapshot could not be created: \(description) \(error)")
                    throw error
                }
            }
            try setValue(
                values[offset],
                at: property,
                storeIdentifier: storeIdentifier,
                externalStorageURL: configuration.externalStorageURL
            )
            values.formIndex(after: &cursor)
        }
        if !excludedProperties.isEmpty {
            let count = excludedProperties.count
            #if swift(>=6.2)
            let connection = try queue.request(.reader)
            let results = Mutex<[Int: any DataStoreSnapshotValue]>(.init(minimumCapacity: count))
            let excludedPropertiesCopy = consume excludedProperties
            DispatchQueue.concurrentPerform(iterations: count) { index in
                let property = excludedPropertiesCopy[index]
                guard let relationship = property.metadata as? Schema.Relationship else {
                    return
                }
                do {
                    let value: any DataStoreSnapshotValue
                    if let graph = registry?.graph,
                       let cachedTargets = graph.cachedReferencesIfPresent(
                        for: persistentIdentifier,
                        at: property.name
                       ) {
                        value = try ensureRelationshipValue(cachedTargets, in: relationship)
                        logger.trace("Resolved excluded properties using graph: \(property) = \(cachedTargets)")
                    } else {
                        value = try fetchExternalReferences(
                            for: persistentIdentifier,
                            in: property,
                            graph: registry?.graph,
                            connection: connection
                        )
                        if let graph = registry?.graph,
                           let targets = ReferenceGraph.normalizeTargets(value) {
                            graph.setReferences(
                                for: persistentIdentifier,
                                at: property.name,
                                to: targets
                            )
                        }
                        logger.debug("Resolved excluded properties using store: \(property) = \(value)")
                    }
                    results.withLock { $0[property.index] = value }
                } catch {
                    logger.error("An error occurred fetching reference: \(property) -> \(error)")
                }
            }
            queue.release(consume connection)
            results.withLock {
                for (index, value) in $0 {
                    self.values[index] = value
                }
            }
            #else
            // FIXME: `for ... in` infinitely hangs attempting to compile.
            excludedProperties.forEach { property in
                guard property.metadata is Schema.Relationship else {
                    return
                }
                do {
                    let connection = try queue.request(.reader)
                    self.values[property.index] = try fetchExternalReferences(
                        for: persistentIdentifier,
                        in: property,
                        connection: connection
                    )
                } catch {
                    logger.error("An error occurred fetching reference: \(property) -> \(error)")
                }
            }
            #endif
        }
        logger.trace("Created snapshot for \(entityName): \(primaryKey) (\(contentDescriptions))")
    }
    
    nonisolated package init(
        store: DatabaseStore,
        entity: Schema.Entity,
        row: consuming [String: any Sendable],
        relatedSnapshots: inout [PersistentIdentifier: Self]
    ) throws {
        let (row, _) = try parseRowData(row: row, entity: entity, storeIdentifier: store.identifier)
        logger.debug("Creating \(entity.name) snapshot: \(row)")
        guard let primaryKey = row[pk] as? String else {
            throw SQLError(.columnNotFound(pk), message: "No primary key found in row.")
        }
        try self.init(
            primaryKey: primaryKey,
            storeIdentifier: store.identifier,
            type: nil,
            entityName: entity.name
        )
        self.properties = .init(type.databaseSchemaMetadata)
        self.values = .init(repeating: SQLNull(), count: properties.count)
        for property in properties {
            let description = "\(entity.name).\(property.name) as \(property.valueType)"
            switch row[property.name] {
            case let value?:
                try setValue(value, at: property, store: store)
                logger.debug("Assigned column to attribute from unordered row: \(description) = \(value)")
            case nil where property.metadata is Schema.Relationship:
                let connection = try store.queue.connection(.reader)
                let value = try fetchExternalReferences(
                    for: persistentIdentifier,
                    in: property,
                    graph: store.manager.graph,
                    connection: connection
                )
                try setValue(value, at: property, store: store)
            default:
                logger.warning("Unknown attribute value found in unordered row: \(description)")
                continue
            }
        }
    }
    
    nonisolated public func overwrite(
        persistentIdentifier: PersistentIdentifier,
        values row: [any Sendable],
        externalStorageURL: URL
    ) throws -> Self {
        var copy = copy(persistentIdentifier: persistentIdentifier)
        for (index, property) in self.properties.enumerated() {
            switch property.metadata {
            case let attribute as Schema.Attribute:
                try copy.setValue(attribute, row[index], at: index, externalStorageURL: externalStorageURL)
            case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                try copy.setValue(relationship, row[index], at: index, storeIdentifier: storeIdentifier.unsafelyUnwrapped)
            default:
                continue
            }
        }
        return copy
    }
    
    nonisolated private mutating func setValue(
        _ value: any Sendable,
        at property: PropertyMetadata,
        store: DatabaseStore
    ) throws {
        switch property.metadata {
        case let attribute as Schema.Attribute:
            let externalStorageURL = store.configuration.externalStorageURL
            try setValue(attribute, value, at: property.index, externalStorageURL: externalStorageURL)
        case let relationship as Schema.Relationship:
            let storeIdentifier = store.identifier
            try setValue(relationship, value, at: property.index, storeIdentifier: storeIdentifier)
        default:
            preconditionFailure("Property is not an attribute or a relationship: \(Swift.type(of: property.metadata))")
        }
    }
    
    nonisolated private mutating func setValue(
        _ value: any Sendable,
        at property: PropertyMetadata,
        storeIdentifier: String,
        externalStorageURL: URL
    ) throws {
        switch property.metadata {
        case let attribute as Schema.Attribute:
            try setValue(attribute, value, at: property.index, externalStorageURL: externalStorageURL)
        case let relationship as Schema.Relationship:
            try setValue(relationship, value, at: property.index, storeIdentifier: storeIdentifier)
        default:
            preconditionFailure("Property is not an attribute or a relationship: \(Swift.type(of: property.metadata))")
        }
    }
    
    /// Sets the attribute value indexed at the result row.
    nonisolated internal mutating func setValue(
        _ attribute: Schema.Attribute,
        _ value: any Sendable,
        at index: Int,
        externalStorageURL: URL
    ) throws {
        let description = "\(entityName).\(attribute.name) as \(attribute.valueType).self"
        let valueType = unwrapOptionalMetatype(attribute.valueType)
        guard let valueType = valueType as? any DataStoreSnapshotValue.Type else {
            preconditionFailure("Attribute must conform to DataStoreSnapshotValue: \(description)")
        }
        switch value {
        case let value where attribute.options.contains(.externalStorage):
            guard let valueType = unwrapOptionalMetatype(attribute.valueType) as? any DataStoreSnapshotValue.Type else {
                preconditionFailure("Only Codable types can be retrieved from external storage: \(description)")
            }
            let relativePath = "\(entityName)/\(attribute.name)/\(primaryKey)"
            let url = externalStorageURL.appending(path: relativePath)
            guard FileManager.default.fileExists(atPath: url.path) else {
                logger.notice("External storage data could not be found: \(description) = \(url.path)")
                self.values[index] = SQLNull()
                return
            }
            if valueType is Data.Type {
                self.values[index] = try Data(contentsOf: url)
                return
            }
            self.values[index] = try JSONDecoder().decode(valueType, from: Data(contentsOf: url))
            logger.trace("Assigned column to attribute: \(description) = \(value) external storage")
        case let value as any DataStoreSnapshotValue:
            guard let value = SQLValue.convert(value, as: valueType) else {
                fatalError("Column could not be assigned a value: \(description) = \(value) as \(Swift.type(of: value))")
            }
            self.values[index] = value
            logger.trace("Assigned column to attribute: \(description) = \(value)")
        case is NSNull where attribute.defaultValue != nil:
            guard let value = attribute.defaultValue as! (any DataStoreSnapshotValue)? else {
                logger.debug("Failed to cast entity default value: \(description)")
                fallthrough
            }
            self.values[index] = value
            logger.trace("Assigned column to attribute: \(description) = \(value) default")
        case is NSNull where attribute.isOptional:
            self.values[index] = SQLNull()
            logger.trace("Assigned column to attribute: \(description) = NULL")
        default:
            preconditionFailure("Attribute was not assigned a value: \(description) = \(value)")
        }
    }
    
    /// Sets the relationship value indexed at the result row.
    nonisolated internal mutating func setValue(
        _ relationship: Schema.Relationship,
        _ value: any Sendable,
        at index: Int,
        storeIdentifier: String
    ) throws {
        let description = "\(entityName).\(relationship.name) as \(relationship.valueType).self"
        if relationship.isToOneRelationship {
            switch value {
            case let relatedPersistentIdentifier as PersistentIdentifier:
                self.values[index] = relatedPersistentIdentifier
                logger.trace("Assigned column to one-to-one relationship: \(description) = primary key")
            case let referencedPrimaryKey as String:
                self.values[index] = try PersistentIdentifier.identifier(
                    for: storeIdentifier,
                    entityName: relationship.destination,
                    primaryKey: referencedPrimaryKey
                )
                logger.trace("Assigned column to one-to-one relationship: \(description) = primary key")
            case is NSNull where relationship.isOptional:
                self.values[index] = SQLNull()
                logger.trace("Assigned column to one-to-one relationship: \(description) = NULL")
            default:
                preconditionFailure("Relationship was not assigned a value: \(description) = \(value)")
            }
        } else {
            switch value {
            case let relatedPersistentIdentifiers as [PersistentIdentifier]:
                self.values[index] = relatedPersistentIdentifiers
                logger.trace("Assigned column to one-to-many relationship: \(description) = [primary key]")
            case let referencedPrimaryKeys as [String]:
                self.values[index] = try referencedPrimaryKeys.map { referencedPrimaryKey in
                    try PersistentIdentifier.identifier(
                        for: storeIdentifier,
                        entityName: relationship.destination,
                        primaryKey: referencedPrimaryKey
                    )
                }
                logger.trace("Assigned column to one-to-many relationship: \(description) = [primary key]")
            case is NSNull where relationship.isOptional:
                self.values[index] = SQLNull()
                logger.trace("Assigned column to one-to-many relationship: \(description) = NULL")
            default:
                preconditionFailure("Relationship was not assigned a value: \(description) = \(value)")
            }
        }
    }
}
