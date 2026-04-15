//
//  SchemaMapping.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import Logging
import SQLiteStatement
import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.bootstrap")

@SQLTableBuilder nonisolated package func makeTableDefinitions(
    schema: Schema,
    metadata makeSchemaMetadata: (Schema.Entity) -> (
        discriminator: PropertyMetadata,
        properties: [PropertyMetadata],
        uniqueTableConstraints: [TableConstraint]
    )?
) -> [any TableDefinition] {
    for entity in schema.entities {
        makeTableDefinition(schema: schema, entity: entity, metadata: makeSchemaMetadata)
    }
    for entity in schema.entities {
        let type = Schema.type(for: entity.name).unsafelyUnwrapped
        for property in type.databaseSchemaMetadata where property.isManyToManyRelationship {
            makeIntermediaryTableDefinition(schema: schema, entity: entity, property: property)
        }
    }
}

@SQLTableBuilder nonisolated package func makeTableDefinition(
    schema: Schema,
    entity: Schema.Entity,
    metadata makeSchemaMetadata: (Schema.Entity) -> (
        discriminator: PropertyMetadata,
        properties: [PropertyMetadata],
        uniqueTableConstraints: [TableConstraint]
    )?
) -> [any TableDefinition] {
    if let (discriminator, properties, uniqueTableConstraints) = makeSchemaMetadata(entity) {
        SQLTable(name: entity.name, constraints: uniqueTableConstraints) {
            SQLAttributeColumn(
                name: discriminator.name,
                valueType: String.self,
                constraints: .primaryKey, .notNull
            ) { _ in
                logger.trace("Creating primary key column: \(entity.name).\(discriminator.name)")
            }
            for property in properties where entity.inheritedPropertiesByName[property.name] == nil {
                let description = "\(entity.name).\(property.name)"
                switch property.metadata {
                case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                    let inverseRelationship = {
                        guard let inverseName = relationship.inverseName,
                              let destinationEntity = schema.entitiesByName[relationship.destination] else {
                            return Optional<Schema.Relationship>.none
                        }
                        return destinationEntity.relationshipsByName[inverseName]
                    }()
                    SQLRelationshipColumn(
                        name: property.reference.unsafelyUnwrapped[0].sourceColumn,
                        valueType: String.self,
                        constraints: [
                            relationship.isOptional ? nil : .notNull,
                            relationship.isUnique ? .unique : nil,
                            .references(
                                relationship.destination,
                                discriminator.name,
                                onDelete: referenceDeleteAction(
                                    for: relationship,
                                    inverse: inverseRelationship
                                ),
                                onUpdate: nil,
                                deferrable: .deferrable.initiallyDeferred
                            )
                        ]
                    ) { result in
                        guard case .success(_) = result else {
                            preconditionFailure("ColumnDefinition should never fail.")
                        }
                        if relationship.isUnique, !relationship.isToOneRelationship {
                            preconditionFailure("To-many relationship cannot be unique.")
                        }
                        logger.trace("Creating relationship column: \(description)")
                    }
                case let compositeAttribute as Schema.CompositeAttribute where !property.flags.contains(.isExternal):
                    SQLCompositeAttributeColumn(
                        name: compositeAttribute.name,
                        valueType: compositeAttribute.options.contains(.externalStorage)
                        ? String.self
                        : compositeAttribute.valueType,
                        constraints: [
                            compositeAttribute.isOptional ? nil : .notNull,
                            compositeAttribute.isUnique ? .unique : nil,
                            compositeAttribute.defaultValue == nil
                            ? nil
                            : .defaultValue(compositeAttribute.defaultValue!)
                        ]
                    ) { result in
                        guard case .success(let column) = result else {
                            preconditionFailure("ColumnDefinition should never fail.")
                        }
                        validatePropertyOptions(attribute: compositeAttribute, column: column)
                        logger.trace("Creating composite attribute column: \(description)")
                    } columns: {
                        for attribute in compositeAttribute.properties {
                            SQLAttributeColumn(
                                name: attribute.name,
                                valueType: attribute.valueType,
                                constraints: [
                                    attribute.isOptional ? nil : .notNull,
                                    attribute.isUnique ? .unique : nil
                                ]
                            ) { result in
                                guard case .success(let column) = result else {
                                    preconditionFailure("ColumnDefinition should never fail.")
                                }
                                validatePropertyOptions(attribute: attribute, column: column)
                                let description = "\(description).\(attribute.name)"
                                logger.trace("Creating sub-attribute column: \(description)")
                            }
                        }
                    }
                case let attribute as Schema.Attribute where !property.flags.contains(.isExternal):
                    SQLAttributeColumn(
                        name: attribute.name,
                        valueType: attribute.options.contains(.externalStorage)
                        ? String.self
                        : attribute.valueType,
                        constraints: [
                            attribute.isOptional ? nil : .notNull,
                            attribute.isUnique ? .unique : nil,
                            attribute.defaultValue == nil
                            ? nil
                            : .defaultValue(attribute.defaultValue!)
                        ]
                    ) { result in
                        guard case .success(let column) = result else {
                            preconditionFailure("ColumnDefinition should never fail.")
                        }
                        validatePropertyOptions(attribute: attribute, column: column)
                        logger.trace("Creating attribute column: \(description)")
                    }
                default:
                    SQLEmptyColumn()
                }
            }
        }
    }
}

@SQLTableBuilder nonisolated package func makeIntermediaryTableDefinition(
    schema: Schema,
    entity: Schema.Entity,
    property: PropertyMetadata
) -> [any TableDefinition] {
    if let relationship = property.metadata as? Schema.Relationship,
       let reference = property.reference,
       let destinationEntity = schema.entitiesByName[relationship.destination],
       let inverseName = relationship.inverseName,
       let inverseRelationship = destinationEntity.relationshipsByName[inverseName],
       reference[0].sourceTable < reference[1].destinationTable {
        SQLTable(
            name: reference[0].destinationTable,
            constraints: [
                .primaryKey(
                    reference[0].destinationColumn,
                    reference[1].sourceColumn
                ),
                .foreignKey(
                    reference[0].destinationColumn,
                    references: reference[0].sourceTable,
                    at: pk,
                    onDelete: joinTableDeleteAction(from: relationship.deleteRule),
                    deferrable: .deferrable.initiallyDeferred
                ),
                .foreignKey(
                    reference[1].sourceColumn,
                    references: reference[1].destinationTable,
                    at: pk,
                    onDelete: joinTableDeleteAction(from: inverseRelationship.deleteRule),
                    deferrable: .deferrable.initiallyDeferred
                )
            ]
        ) {
            SQLColumn(
                name: reference[0].destinationColumn,
                valueType: String.self,
                constraints: .notNull,
                references: reference[0]
            ) { _ in
            }
            SQLColumn(
                name: reference[1].sourceColumn,
                valueType: String.self,
                constraints: .notNull,
                references: reference[1]
            ) { _ in
            }
        }
    }
}

nonisolated internal func validatePropertyOptions(
    attribute: some Schema.Attribute,
    column: some ColumnDefinition
) {
    if attribute.isTransformable {
        logger.warning("Transformable is not supported: \(attribute)")
    }
    for option in attribute.options {
        switch option {
        case .allowsCloudEncryption:
            logger.warning("Option is not supported: \(attribute.name) -> \(option)")
        case .ephemeral:
            continue
        case .externalStorage:
            continue
        case .preserveValueOnDeletion:
            continue
        case .spotlight:
            logger.warning("Option is not supported: \(attribute.name) -> \(option)")
        case .unique:
            assert(column.constraints.contains(.unique), "Column expected to be unique.")
        default:
            logger.notice("Unknown attribute option: \(option)")
        }
    }
}
