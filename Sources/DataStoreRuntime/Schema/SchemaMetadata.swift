//
//  SchemaMetadata.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SQLiteHandle
import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.bootstrap")

nonisolated package func makePropertyMetadataArray<T>(schema: Schema, for type: T.Type) -> (
    result: [PropertyMetadata],
    keyPathVariants: [AnyKeyPath & Sendable: AnyKeyPath & Sendable]
) where T: PersistentModel & SendableMetatype {
    makeSchemaMetadata(schema, for: type, into: [PropertyMetadata]()) { $0.append($1) }
}

nonisolated package func makePropertyMetadataDictionary<T>(schema: Schema, for type: T.Type) -> (
    result: [String: PropertyMetadata],
    keyPathVariants: [AnyKeyPath & Sendable: AnyKeyPath & Sendable]
) where T: PersistentModel & SendableMetatype {
    makeSchemaMetadata(schema, for: type, into: [String: PropertyMetadata]()) { $0[$1.name] = $1 }
}

nonisolated package func makePropertyMetadataDictionaryInversion<T>(schema: Schema, for type: T.Type) -> (
    result: [(AnyKeyPath & Sendable): PropertyMetadata],
    keyPathVariants: [AnyKeyPath & Sendable: AnyKeyPath & Sendable]
) where T: PersistentModel & SendableMetatype {
    makeSchemaMetadata(schema, for: type, into: [(AnyKeyPath & Sendable): PropertyMetadata]()) { $0[$1.keyPath] = $1 }
}

// TODO: Creating an entity without a schema is incomplete.

nonisolated package func makeSchemaMetadata<Model, Result>(
    _ schema: Schema?,
    for type: Model.Type,
    into result: consuming Result,
    accumulate: (inout Result, PropertyMetadata) throws -> Void
) rethrows -> (
    result: Result,
    keyPathVariants: [AnyKeyPath & Sendable: AnyKeyPath & Sendable]
) where Model: PersistentModel & SendableMetatype, Result: Collection {
    let entityName = Schema.entityName(for: Model.self)
    let entity = schema?.entitiesByName[entityName] ?? Schema.Entity(entityName)
    precondition(
        entity.name != InternalTable.tableName,
        "The entity name cannot be used. It is reserved for the internal table: \(entity.name)"
    )
    precondition(
        entity.name != HistoryTable.tableName,
        "The entity name cannot be used. It is reserved for the history table: \(entity.name)"
    )
    precondition(
        entity.name != ArchiveTable.tableName,
        "The entity name cannot be used. It is reserved for the archive table: \(entity.name)"
    )
    var result = consume result
    var keyPathVariants = [AnyKeyPath & Sendable: AnyKeyPath & Sendable]()
    var schemaMetadata = [Schema.Entity: [PropertyMetadata]]()
    var auxiliaryMetadata = [PropertyMetadata]()
    try accumulate(&result, .discriminator(for: Model.self))
    // Construct the property metadata generated from the `@Model` macro.
    reflectSchemaMetadata(for: Model.self) { index, name, keyPath, defaultValue, metadata in
        // Use placeholder schema property where macros were not explicit.
        schemaMetadata[entity, default: []].append(PropertyMetadata(
            index: index,
            name: name,
            keyPath: keyPath,
            defaultValue: defaultValue,
            metadata: metadata ?? Schema.Attribute(name: name, valueType: Void.self)
        ))
        if schema == nil {
            // Rebuild entity for empty schema.
            switch metadata {
            case let relationship as Schema.Relationship:
                let newRelationship = Schema.Relationship(
                    deleteRule: relationship.deleteRule,
                    minimumModelCount: relationship.minimumModelCount,
                    maximumModelCount: relationship.maximumModelCount,
                    originalName: relationship.originalName,
                    inverse: relationship.inverseKeyPath,
                    hashModifier: relationship.hashModifier
                )
                newRelationship.name = relationship.name
                newRelationship.keypath = relationship.keypath
                newRelationship.inverseName = relationship.inverseName
                newRelationship.inverseKeyPath = relationship.inverseKeyPath
                newRelationship.options = relationship.options
                newRelationship.valueType = relationship.valueType
                newRelationship.destination = relationship.destination
                entity.storedProperties.append(newRelationship)
            case let compositeAttribute as Schema.CompositeAttribute:
                let newCompositeAttribute = Schema.CompositeAttribute(
                    name: compositeAttribute.name,
                    originalName: compositeAttribute.originalName,
                    options: compositeAttribute.options,
                    valueType: compositeAttribute.valueType,
                    defaultValue: compositeAttribute.defaultValue,
                    hashModifier: compositeAttribute.hashModifier
                )
                newCompositeAttribute.properties = compositeAttribute.properties
                entity.storedProperties.append(newCompositeAttribute)
            case let attribute as Schema.Attribute:
                let newAttribute = Schema.Attribute(
                    name: attribute.name,
                    originalName: attribute.originalName,
                    options: attribute.options,
                    valueType: attribute.valueType,
                    defaultValue: attribute.defaultValue,
                    hashModifier: attribute.hashModifier
                )
                entity.storedProperties.append(newAttribute)
            default:
                break
            }
        }
    } onAuxiliaryMetadata: { name, metadata in
        auxiliaryMetadata.append(PropertyMetadata(
            index: -1,
            keyPath: \Schema.encodingVersion,
            metadata: metadata
        ))
    }
    for (index, property) in entity.storedProperties.enumerated() {
        let description = "\(entity.name).\(property.name) as \(property.valueType).self"
        precondition(
            property.name != pk,
            "The property name cannot be used. It is reserved for the primary key: \(description)"
        )
        precondition(
            property.name != "\(property.name)_pk" || property.name != "0_pk" || property.name != "1_pk",
            "The property name cannot be used. It is reserved for the foreign key: \(description)"
        )
        var isInherited = false
        var resolvedKeyPath: (AnyKeyPath & Sendable)?
        if let type = type as? any PredicateCodableKeyPathProviding.Type,
           let keyPath = getPredicateCodableKeyPath(for: type, property: property), registerKeyPath(keyPath) {
            logger.trace("Received supplied key path: \(description)", metadata: ["key_path": "\(keyPath)"])
        }
        if let keyPath = (property as? Schema.Relationship)?.keypath, registerKeyPath(keyPath) {
            logger.trace("Received relationship key path: \(description)", metadata: ["key_path": "\(keyPath)"])
        }
        switch entity.inheritedPropertiesByName[property.name] != nil {
        case true:
            guard #available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *) else {
                fallthrough
            }
            logger.trace("Property is inherited: \(description)")
            guard let property = findInheritedPropertyMetadata(
                for: property,
                startingAt: entity,
                schemaMetadata: &schemaMetadata
            ) else {
                fatalError("Unable to find inherited property metadata: \(description)")
            }
            isInherited = true
            _ = registerKeyPath(property.keyPath)
        case false:
            logger.trace("Property is not inherited: \(description)")
            guard let property = schemaMetadata[entity]?.first(where: { $0.name == property.name }) else {
                fatalError("Unable to find property metadata: \(description)")
            }
            _ = registerKeyPath(property.keyPath)
        }
        /// Compares to the previous `keyPath` to a preferred `KeyPath` that will be the canonical one.
        func registerKeyPath(_ otherKeyPath: AnyKeyPath?) -> Bool {
            guard let otherKeyPath: AnyKeyPath & Sendable = sendable(cast: otherKeyPath as Any) else {
                logger.debug("Attempted to register key path as nil for property: \(description)")
                return false
            }
            switch resolvedKeyPath {
            case let existingKeyPath?:
                if existingKeyPath != otherKeyPath {
                    keyPathVariants[otherKeyPath] = existingKeyPath
                    logger.trace(
                        "Registering key path as variant: \(description) \(existingKeyPath) != \(otherKeyPath)",
                        metadata: ["key_path": "\(existingKeyPath)", "other_key_path": "\(otherKeyPath)"]
                    )
                    return false
                } else {
                    logger.trace(
                        "Key path is the same and already registered: \(description) \(existingKeyPath)",
                        metadata: ["key_path": "\(existingKeyPath)", "other_key_path": "\(otherKeyPath)"]
                    )
                    return false
                }
            case nil:
                resolvedKeyPath = otherKeyPath
                logger.debug(
                    "Registered key path for property: \(description)",
                    metadata: ["key_path": "\(otherKeyPath)"]
                )
                return true
            }
        }
        guard let keyPath = resolvedKeyPath else {
            fatalError("No key path found for property: \(description)")
        }
        var canonicalProperty = PropertyMetadata(index: index, keyPath: keyPath, metadata: property)
        try makeTableReferences(&canonicalProperty)
        if isInherited {
            canonicalProperty.flags.insert(.isInherited)
        }
        try accumulate(&result, canonicalProperty)
        if let type = Model.self as? any SQLPassthrough.Type {
            var property = insertSQLQueryPassthrough(for: type)
            property.flags.insert(.isExternal)
            try accumulate(&result, property)
        }
        func makeTableReferences(_ property: inout PropertyMetadata) throws {
            switch property.metadata {
            case let relationship as Schema.Relationship where relationship.inverseName != nil:
                // Bidirectional relationship.
                let inverseName = relationship.inverseName.unsafelyUnwrapped
                var type: Any.Type = unwrapOptionalMetatype(relationship.valueType)
                if let relationshipType = type as? any RelationshipCollection.Type {
                    type = unwrapArrayMetatype(relationshipType)
                }
                if let type = type as? any (PersistentModel & SendableMetatype).Type {
                    guard let destinationEntity = Schema([type]).entity(for: type),
                          let inverseRelationship = destinationEntity.relationshipsByName[inverseName] else {
                        fatalError(SchemaError.relationshipTargetEntityNotRegistered.localizedDescription)
                    }
                    var array = [TableReference]()
                    switch true {
                    case !relationship.isToOneRelationship where !inverseRelationship.isToOneRelationship:
                        // Set many-to-many with a deterministic ordering for intermediary tables.
                        let intermediaryTable = IntermediaryTableReference(
                            lhsTable: entity.name,
                            lhsColumn: relationship.name,
                            rhsTable: destinationEntity.name,
                            rhsColumn: inverseRelationship.name
                        )
                        guard let lhsReference = intermediaryTable.join(
                            from: (nil, entity.name),
                            to: (nil, intermediaryTable.name)
                        ) else {
                            fatalError("From LHS table to intermediary table failed: \(intermediaryTable)")
                        }
                        guard let rhsReference = intermediaryTable.join(
                            from: (nil, intermediaryTable.name),
                            to: (nil, destinationEntity.name)
                        ) else {
                            fatalError("From intermediary table to RHS table failed: \(intermediaryTable)")
                        }
                        array = Array<TableReference>(unsafeUninitializedCapacity: 2) {
                            $0[0] = lhsReference; $0[1] = rhsReference; $1 = 2
                        }
                        logger.trace(
                            "Created reference for many-to-many relationship: \(array)",
                            metadata: [
                                "intermediary_table": "\(intermediaryTable)",
                                "lhs_table": "\(lhsReference)",
                                "rhs_table": "\(rhsReference)",
                                "reference": "\(array)"
                            ]
                        )
                    case relationship.isToOneRelationship:
                        // Set one-to-one or many-to-one relationships where `self` holds the foreign key.
                        let reference = TableReference(
                            sourceTable: entityName,
                            sourceColumn: relationship.name + "_pk",
                            destinationTable: destinationEntity.name,
                            destinationColumn: pk
                        )
                        array = Array<TableReference>(unsafeUninitializedCapacity: 1) {
                            $0[0] = reference; $1 = 1
                        }
                        let cardinality = inverseRelationship.isToOneRelationship ? "one-to-one" : "many-to-one"
                        let type = reference.isSelfReferencing ? "self-reference" : "reference"
                        let ownership = reference.isOwningReference() ? "owning" : "non-owning"
                        logger.trace(
                            "Created \(type) for \(ownership) \(cardinality) relationship: \(reference)",
                            metadata: [
                                "is_self_referencing": "\(type)",
                                "is_owning_reference": "\(ownership)",
                                "cardinality": "\(cardinality)",
                                "reference": "\(reference)"
                            ]
                        )
                    default:
                        // Access to relationship where `self` does not hold the foreign key.
                        let reference = TableReference(
                            sourceTable: entity.name,
                            sourceColumn: pk,
                            destinationTable: destinationEntity.name,
                            destinationColumn: inverseRelationship.name + "_pk"
                        )
                        array = Array<TableReference>(unsafeUninitializedCapacity: 1) {
                            $0[0] = reference
                            $1 = 1
                        }
                        let type = reference.isSelfReferencing ? "self-reference" : "reference"
                        let ownership = reference.isOwningReference() ? "owning" : "non-owning"
                        logger.trace(
                            "Created \(type) for \(ownership) one-to-many relationship.",
                            metadata: [
                                "is_self_referencing": "\(type)",
                                "is_owning_reference": "\(ownership)",
                                "reference": "\(reference)"
                            ]
                        )
                    }
                    property.reference = array
                }
            case let relationship as Schema.Relationship:
                // Unidirectional relationship.
                let reference = TableReference(
                    sourceTable: entityName,
                    sourceColumn: relationship.name + "_pk",
                    destinationTable: relationship.destination,
                    destinationColumn: pk
                )
                property.reference = [reference]
                let type = reference.isSelfReferencing ? "self-reference" : "reference"
                let ownership = reference.isOwningReference() ? "owning" : "non-owning"
                logger.trace(
                    "Created \(type) for \(ownership) unidirectional to-one relationship.",
                    metadata: [
                        "is_self_referencing": "\(type)",
                        "is_owning_reference": "\(ownership)",
                        "reference": "\(reference)"
                    ]
                )
            case _ where entity.inheritedPropertiesByName[property.name] is Schema.Attribute:
                guard let superentity = entity.superentity else {
                    fatalError("Missing superentity for inherited property: \(description)")
                }
                // Create an implicit reference between subclass and superclass tables.
                let reference = TableReference(
                    sourceTable: entity.name,
                    sourceColumn: "super_pk",
                    destinationTable: superentity.name,
                    destinationColumn: pk
                )
                property.reference = [reference]
                logger.trace(
                    "Created inheritance reference.",
                    metadata: [
                        "superentity": "\(superentity.name)",
                        "subentity": "\(entity.name)",
                        "reference": "\(reference)"
                    ]
                )
            default:
                break
            }
            if DataStoreDebugging.mode == .trace {
                logger.info(
                    """
                    PropertyMetadata
                    Type layout
                        size = \(MemoryLayout<PropertyMetadata>.size),
                        stride = \(MemoryLayout<PropertyMetadata>.stride),
                        alignment = \(MemoryLayout<PropertyMetadata>.alignment)
                    Instance layout \(entity.name).\(property.name)
                        size = \(MemoryLayout.size(ofValue: property)),
                        stride = \(MemoryLayout.stride(ofValue: property)),
                        alignment = \(MemoryLayout.alignment(ofValue: property))
                    """
                )
            }
        }
    }
    func getPredicateCodableKeyPath<Root>(for type: Root.Type, property: any SchemaProperty)
    -> (PartialKeyPath<Model> & Sendable)? where Root: PredicateCodableKeyPathProviding {
        sendable(cast: Root.predicateCodableKeyPaths[property.name] as? PartialKeyPath<Model> as Any)
    }
    func insertSQLQueryPassthrough<Root>(for type: Root.Type)
    -> PropertyMetadata where Root: SQLPassthrough {
        PropertyMetadata(
            index: result.count,
            keyPath: \Root.sql,
            metadata: Schema.Attribute(name: "_sql", valueType: String.self)
        )
    }
    for property in auxiliaryMetadata {
        try accumulate(&result, property)
    }
    logger.trace("\(Model.self).self key path property name mapping: \(result)")
    return (result, keyPathVariants)
}

nonisolated private func findInheritedPropertyMetadata(
    for property: any SchemaProperty,
    startingAt entity: Schema.Entity,
    schemaMetadata: inout [Schema.Entity: [PropertyMetadata]]
) -> PropertyMetadata? {
    guard let superentity = entity.superentity else {
        return nil
    }
    if #available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *) {
        logger.warning("Inheritance is not fully implemented: \(superentity.name).\(property.name)")
    }
    guard let superclass = Schema.type(for: superentity.name) else {
        fatalError("The superentity's associated superclass type is not registered: \(superentity.name)")
    }
    logger.trace(
        "Ascending the entity hierarchy to find PropertyMetadata.",
        metadata: [
            "entity": "\(entity.name).\(property.name)",
            "superentity": "\(superentity.name).\(property.name)"
        ]
    )
    let superentitySchemaMetadata: [PropertyMetadata]
    if let cachedSchemaMetadata = schemaMetadata[superentity] {
        superentitySchemaMetadata = cachedSchemaMetadata
    } else {
        let inheritedSchemaMetadata = reflectPropertyMetadata(for: superclass)
        schemaMetadata[superentity, default: []] = inheritedSchemaMetadata
        superentitySchemaMetadata = inheritedSchemaMetadata
    }
    if let property = superentitySchemaMetadata.first(where: { $0.name == property.name }) {
        logger.trace(
            "Found PropertyMetadata for inherited property.",
            metadata: [
                "entity": "\(entity.name).\(property.name)",
                "superentity": "\(superentity.name).\(property.name)"
            ]
        )
        return property
    } else {
        return findInheritedPropertyMetadata(
            for: property,
            startingAt: superentity,
            schemaMetadata: &schemaMetadata
        )
    }
}
