//
//  ReferenceFetch.swift
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
import SQLiteHandle
import SQLiteStatement
import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

nonisolated package func ensureRelationshipValue<T>(
    _ value: T,
    in relationship: Schema.Relationship
) throws -> any DataStoreSnapshotValue where
T: Collection & DataStoreSnapshotValue,
T.Element: DataStoreSnapshotValue {
    switch relationship.isToOneRelationship {
    case true:
        if let relatedPersistentIdentifier = value.first {
            assert(value.count == 1, "To-one relationship should only accept one row.")
            return relatedPersistentIdentifier
        } else if !relationship.isOptional {
            throw ConstraintError(.referentialIntegrityViolated)
        }
    case false:
        if !value.isEmpty {
            return value
        } else if !relationship.isOptional {
            return [PersistentIdentifier]()
        }
    }
    return SQLNull()
}

/// Fetches rows that requires an intermediary table for a many-to-many relationship.
nonisolated package func fetchManyToManyReference<Result>(
    _ primaryKey: String,
    _ foreignKey: String,
    for property: PropertyMetadata,
    into initial: Result,
    connection: borrowing DatabaseConnection<DatabaseStore>,
    body: @escaping @Sendable (inout Result, ResultRows.Element) -> Void
) throws -> Result where Result: Collection {
    let reference = property.reference.unsafelyUnwrapped
    return try connection.fetch(
        """
        SELECT "\(reference[0].rhsColumn)", "\(reference[1].lhsColumn)"
        FROM "\(reference[0].rhsTable)"
        WHERE "\(reference[0].rhsColumn)" = ? AND "\(reference[1].lhsColumn)" = ?
        """,
        bindings: [primaryKey, foreignKey],
        into: initial,
        body: body
    )
}

/// Fetches rows that references from a non-owning to-many relationship (e.g. one-to-many).
nonisolated package func fetchToManyReference<Result>(
    _ childForeignKey: String,
    for property: PropertyMetadata,
    into initial: Result,
    connection: borrowing DatabaseConnection<DatabaseStore>,
    body: @escaping @Sendable (inout Result, ResultRows.Element) -> Void
) throws -> Result where Result: Collection {
    let reference = property.reference.unsafelyUnwrapped
    return try connection.fetch(
        """
        SELECT "\(reference[0].destinationColumn)"
        FROM "\(reference[0].destinationTable)"
        WHERE "\(reference[0].destinationColumn)" = ?
        """,
        bindings: [childForeignKey],
        into: initial,
        body: body
    )
}

/// Fetches rows that references from an owning to-one relationship (e.g. many-to one or one-to-one).
nonisolated package func fetchToOneReference<Result>(
    _ parentForeignKey: String,
    for property: PropertyMetadata,
    into initial: Result,
    connection: borrowing DatabaseConnection<DatabaseStore>,
    body: @escaping @Sendable (inout Result, ResultRows.Element) -> Void
) throws -> Result where Result: Collection {
    let reference = property.reference.unsafelyUnwrapped
    let orientation = reference[0].isOwningReference()
    return try connection.fetch(
        """
        SELECT "\(orientation ? reference[0].rhsColumn : reference[0].lhsColumn)"
        FROM "\(orientation ? reference[0].rhsTable : reference[0].lhsTable)"
        WHERE "\(pk)" = ?
        """,
        bindings: [parentForeignKey],
        into: initial,
        body: body
    )
}

nonisolated package func fetchExternalReferences(
    for persistentIdentifier: PersistentIdentifier,
    in property: PropertyMetadata,
    connection: borrowing DatabaseConnection<DatabaseStore>
) throws -> any DataStoreSnapshotValue {
    guard let relationship = property.metadata as? Schema.Relationship else {
        preconditionFailure("Property should have been a relationship: \(property)")
    }
    guard let storeIdentifier = persistentIdentifier.storeIdentifier else {
        preconditionFailure("References cannot be fetched for a temporary identifier.")
    }
    let description = "\(persistentIdentifier.entityName).\(property)"
    if let graph = connection.context?.graph,
       let cachedTargets = graph.cachedReferencesIfPresent(
        for: persistentIdentifier,
        at: property.name
       ) {
        logger.debug("Using cached references: \(description) -> \(cachedTargets)")
        return try ensureRelationshipValue(cachedTargets, in: relationship)
    }
    let primaryKey = persistentIdentifier.primaryKey()
    let relationshipAlias = "relationship"
    let results: [[String: any Sendable]]
    switch property.reference {
    case let reference? where reference.count == 2:
        results = try connection.query {
            "SELECT \(quote(reference[1].lhsColumn)) AS \(quote(relationshipAlias))"
            From(reference[1].lhsTable)
            Where(.equals(column: reference[0].rhsColumn, value: primaryKey))
        }
    case let reference? where !relationship.isToOneRelationship:
        results = try connection.query {
            "SELECT \(quote(reference[0].sourceColumn)) AS \(quote(relationshipAlias))"
            From(reference[0].destinationTable)
            Where(.equals(column: reference[0].destinationColumn, value: primaryKey))
        }
    case let reference? where relationship.isToOneRelationship:
        results = try connection.query {
            "SELECT \(quote(reference[0].destinationColumn)) AS \(quote(relationshipAlias))"
            From(reference[0].sourceTable)
            Where(.equals(column: reference[0].sourceColumn, value: primaryKey))
            if relationship.isToOneRelationship { Limit(1) }
        }
    default:
        preconditionFailure("Invalid table reference and relationship combination.")
    }
    let relatedIdentifiers = try results.compactMap { result -> PersistentIdentifier? in
        try (result[relationshipAlias] as? String).flatMap { foreignKey -> PersistentIdentifier? in
            try PersistentIdentifier.identifier(
                for: storeIdentifier,
                entityName: relationship.destination,
                primaryKey: foreignKey
            )
        }
    }
    if let graph = connection.context?.graph {
        graph.setReferences(for: persistentIdentifier, at: property.name, to: relatedIdentifiers)
    }
    logger.trace(
        "Fetched external references: \(description)",
        metadata: ["results": "\(relatedIdentifiers)"]
    )
    return try ensureRelationshipValue(relatedIdentifiers, in: relationship)
}

nonisolated package func fetchExternalRows(
    for persistentIdentifier: PersistentIdentifier,
    in property: PropertyMetadata,
    connection: borrowing DatabaseConnection<DatabaseStore>
) throws -> any Sendable {
    guard let relationship = property.metadata as? Schema.Relationship else {
        preconditionFailure("Property should have been a relationship: \(property)")
    }
    var type = relationship.valueType
    if !relationship.isToOneRelationship { type = unwrapArrayMetatype(type) }
    if !relationship.isOptional { type = unwrapOptionalMetatype(type) }
    guard let type = type as? any PersistentModel.Type else {
        preconditionFailure("The relationship value type should be a PersistentModel.Type: \(property)")
    }
    let primaryKey = persistentIdentifier.primaryKey()
    let destinationColumns = type.databaseSchemaMetadata.columns
    let destinationTable = relationship.destination
    let results: [[any Sendable]]
    switch property.reference {
    case let reference? where reference.count == 2:
        let linkAlias = "link"
        let destinationAlias = "destination"
        let selectList = destinationColumns
            .map { "\(quote(destinationAlias)).\(quote($0))" }
            .joined(separator: ", ")
        results = try connection.fetch {
            "SELECT \(selectList)"
            From(reference[1].lhsTable, as: linkAlias)
            Join(
                destinationTable,
                as: destinationAlias,
                on: "\(linkAlias).\(reference[1].lhsColumn) = \(destinationAlias).\(pk)"
            )
            Where(.equals(column: reference[0].rhsColumn, value: primaryKey))
        }
    case let reference? where !relationship.isToOneRelationship:
        let selectList = destinationColumns
            .map { quote($0) }
            .joined(separator: ", ")
        results = try connection.fetch {
            "SELECT \(selectList)"
            From(reference[0].destinationTable)
            Where(.equals(column: reference[0].destinationColumn, value: primaryKey))
        }
    case let reference? where relationship.isToOneRelationship:
        let sourceAlias = "source"
        let destinationAlias = "destination"
        let selectList = destinationColumns
            .map { "\(quote(destinationAlias)).\(quote($0))" }
            .joined(separator: ", ")
        results = try connection.fetch {
            "SELECT \(selectList)"
            From(reference[0].sourceTable, as: sourceAlias)
            Join(
                destinationTable,
                as: destinationAlias,
                on: "\(sourceAlias).\(reference[0].destinationColumn) = \(destinationAlias).\(pk)"
            )
            Where(.equals(column: reference[0].sourceColumn, value: primaryKey))
            Limit(1)
        }
    default:
        preconditionFailure("The relationship must have a reference: \(property)")
    }
    return results
}

nonisolated package func fetchExternalRowsBatched(
    for ownerPrimaryKeys: [String],
    in property: PropertyMetadata,
    connection: borrowing DatabaseConnection<DatabaseStore>,
    chunkSize: Int = 400
) throws -> [String: [[any Sendable]]] {
    guard !ownerPrimaryKeys.isEmpty else { return [:] }
    guard let relationship = property.metadata as? Schema.Relationship else {
        preconditionFailure("Property should have been a relationship: \(property)")
    }
    guard !relationship.isToOneRelationship else {
        preconditionFailure("This batched function is intended for to-many/many-to-many.")
    }
    var type = unwrapArrayMetatype(relationship.valueType)
    if !relationship.isOptional { type = unwrapOptionalMetatype(type) }
    guard let type = type as? any PersistentModel.Type else {
        preconditionFailure("The relationship value type should be a PersistentModel.Type: \(property)")
    }
    let destinationColumns = type.databaseSchemaMetadata.columns
    let ownerAlias = "owner_pk"
    let destinationAlias = "destination"
    let linkAlias = "link"
    let destinationSelectList = destinationColumns
        .map { "\(quote(destinationAlias)).\(quote($0))" }
        .joined(separator: ", ")
    var result = [String: [[any Sendable]]](minimumCapacity: ownerPrimaryKeys.count)
    var start = 0
    while start < ownerPrimaryKeys.count {
        let end = min(start + chunkSize, ownerPrimaryKeys.count)
        let slice = ownerPrimaryKeys[start..<end]
        start = end
        let placeholders = Array(repeating: "?", count: slice.count).joined(separator: ", ")
        let bindings = slice.map(\.self) as [any Sendable]
        let sql: String
        switch property.reference {
        case let reference? where reference.count == 2:
            let linkTable = reference[1].lhsTable
            let ownerForeignKeyInLink = reference[0].rhsColumn
            let destinationTable = relationship.destination
            let destinationForeignKeyInLink = reference[1].lhsColumn
            sql = """
                SELECT "\(linkAlias)"."\(ownerForeignKeyInLink)"
                AS "\(ownerAlias)", \(destinationSelectList)
                FROM "\(linkTable)" AS "\(linkAlias)"
                JOIN "\(destinationTable)" AS "\(destinationAlias)"
                ON "\(linkAlias)"."\(destinationForeignKeyInLink)" = "\(destinationAlias)"."\(pk)"
                WHERE "\(linkAlias)"."\(ownerForeignKeyInLink)" IN (\(placeholders))
                """
        case let reference?:
            let destinationTable = reference[0].destinationTable
            let ownerForeignKeyInDestination = reference[0].destinationColumn
            sql = """
                SELECT "\(destinationAlias)"."\(ownerForeignKeyInDestination)"
                AS "\(ownerAlias)", \(destinationSelectList)
                FROM "\(destinationTable)" AS "\(destinationAlias)"
                WHERE "\(destinationAlias)"."\(ownerForeignKeyInDestination)" IN (\(placeholders))
                """
        default:
            preconditionFailure()
        }
        let rows = try connection.fetch(sql, bindings: bindings)
        for row in rows {
            guard let ownerPrimaryKey = row.first as? String else {
                continue
            }
            var destinationRow = [any Sendable]()
            destinationRow.reserveCapacity(row.count - 1)
            destinationRow.append(contentsOf: row[1...])
            result[ownerPrimaryKey, default: []].append(destinationRow)
        }
    }
    return result
}

nonisolated package func fetchExternalReferenceKeysBatched(
    ownerPrimaryKeys: [String],
    ownerPersistentIdentifiers: [PersistentIdentifier],
    ownerIndexByPrimaryKey: [String: Int],
    in property: PropertyMetadata,
    graph: ReferenceGraph? = nil,
    connection: borrowing DatabaseConnection<DatabaseStore>,
    chunkSize: Int = 400
) throws -> [PersistentIdentifier: [PersistentIdentifier]] {
    guard !ownerPrimaryKeys.isEmpty else { return [:] }
    guard let relationship = property.metadata as? Schema.Relationship else {
        preconditionFailure("Property should have been a relationship: \(property)")
    }
    // TODO: Use store identifier from `DatabaseQueue`.
    guard let storeIdentifier = ownerPersistentIdentifiers.first?.storeIdentifier else {
        preconditionFailure()
    }
    var result = [PersistentIdentifier: [PersistentIdentifier]]()
    result.reserveCapacity(ownerPersistentIdentifiers.count)
    var missingOwnerPrimaryKeys = [String]()
    missingOwnerPrimaryKeys.reserveCapacity(ownerPrimaryKeys.count)
    if let graph {
        for (index, ownerIdentifier) in ownerPersistentIdentifiers.enumerated() {
            let primaryKey = ownerPrimaryKeys[index]
            if let cachedIdentifiers = graph.cachedReferencesIfPresent(
                for: ownerIdentifier,
                at: property.name
            ) {
                result[ownerIdentifier] = cachedIdentifiers
            } else {
                missingOwnerPrimaryKeys.append(primaryKey)
            }
        }
    } else {
        missingOwnerPrimaryKeys = ownerPrimaryKeys
    }
    guard !missingOwnerPrimaryKeys.isEmpty else {
        return result
    }
    let ownerAlias = "owner_pk"
    let relatedAlias = "related_pk"
    let destinationAlias = "destination"
    let linkAlias = "link"
    var foundOwnerPrimaryKeys = Set<String>()
    foundOwnerPrimaryKeys.reserveCapacity(missingOwnerPrimaryKeys.count)
    var start = 0
    while start < missingOwnerPrimaryKeys.count {
        let end = min(start + chunkSize, missingOwnerPrimaryKeys.count)
        let slice = missingOwnerPrimaryKeys[start..<end]
        start = end
        let inList = Array(repeating: "?", count: slice.count).joined(separator: ",")
        let bindings: [any Sendable] = slice.map { $0 }
        let sql: String
        switch property.reference {
        case let reference? where reference.count == 2:
            let linkTable = reference[1].lhsTable
            let ownerForeignKeyInLink = reference[0].rhsColumn
            let destinationForeignKeyInLink = reference[1].lhsColumn
            sql = """
                SELECT
                    "\(linkAlias)"."\(ownerForeignKeyInLink)" AS "\(ownerAlias)",
                    "\(linkAlias)"."\(destinationForeignKeyInLink)" AS "\(relatedAlias)"
                FROM "\(linkTable)" AS "\(linkAlias)"
                WHERE "\(linkAlias)"."\(ownerForeignKeyInLink)" IN (\(inList))
                """
        case let reference? where !relationship.isToOneRelationship:
            let destinationTable = reference[0].destinationTable
            let ownerForeignKeyInDestination = reference[0].destinationColumn
            sql = """
                SELECT
                    "\(destinationAlias)"."\(ownerForeignKeyInDestination)" AS "\(ownerAlias)",
                    "\(destinationAlias)"."\(pk)" AS "\(relatedAlias)"
                FROM "\(destinationTable)" AS "\(destinationAlias)"
                WHERE "\(destinationAlias)"."\(ownerForeignKeyInDestination)" IN (\(inList))
                """
        case let reference? where relationship.isToOneRelationship:
            let sourceTable = reference[0].sourceTable
            let ownerPrimaryKeyColumnInSource = reference[0].sourceColumn
            let foreignKeyColumnInSource = reference[0].destinationColumn
            sql = """
                SELECT
                    "\(sourceTable)"."\(ownerPrimaryKeyColumnInSource)" AS "\(ownerAlias)",
                    "\(sourceTable)"."\(foreignKeyColumnInSource)" AS "\(relatedAlias)"
                FROM "\(sourceTable)"
                WHERE "\(sourceTable)"."\(ownerPrimaryKeyColumnInSource)" IN (\(inList))
                """
        default:
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        for row in try connection.fetch(sql, bindings: bindings) {
            guard let ownerPrimaryKey = row[0] as? String else {
                continue
            }
            foundOwnerPrimaryKeys.insert(ownerPrimaryKey)
            guard let ownerIndex = ownerIndexByPrimaryKey[ownerPrimaryKey] else {
                continue
            }
            let ownerIdentifier = ownerPersistentIdentifiers[ownerIndex]
            guard let relatedPrimaryKey = row[1] as? String else {
                continue
            }
            let concreteEntityName = try resolveConcreteEntityName(
                for: relatedPrimaryKey,
                destination: relationship.destination,
                storeIdentifier: storeIdentifier,
                connection: connection
            )
            let relatedIdentifier = try PersistentIdentifier.identifier(
                for: storeIdentifier,
                entityName: concreteEntityName /*relationship.destination*/,
                primaryKey: relatedPrimaryKey
            )
            result[ownerIdentifier, default: []].append(relatedIdentifier)
        }
    }
    if let graph {
        for primaryKey in missingOwnerPrimaryKeys {
            guard let ownerIndex = ownerIndexByPrimaryKey[primaryKey] else {
                continue
            }
            let ownerIdentifier = ownerPersistentIdentifiers[ownerIndex]
            let targets = result[ownerIdentifier] ?? []
            graph.setReferences(for: ownerIdentifier, at: property.name, to: targets)
        }
    }
    return result
}

nonisolated package func resolveConcreteEntityName(
    for primaryKey: String,
    destination: String,
    storeIdentifier: String,
    connection: borrowing DatabaseConnection<DatabaseStore>
) throws -> String {
    guard let schema = connection.context?.schema,
          let destinationEntity = schema.entitiesByName[destination],
          !destinationEntity.subentities.isEmpty else {
        return destination
    }
    let temporaryIdentifier = try PersistentIdentifier.identifier(
        for: storeIdentifier,
        entityName: destination,
        primaryKey: primaryKey
    )
    return try fetchInheritanceEntity(
        for: temporaryIdentifier,
        on: destinationEntity,
        connection: connection
    ).name
}

nonisolated package func fetchInheritanceEntity(
    for persistentIdentifier: PersistentIdentifier,
    on entity: Schema.Entity,
    connection: borrowing DatabaseConnection<DatabaseStore>
) throws -> Schema.Entity {
    for subentity in entity.subentities {
        let rows = try connection.query(
            """
            SELECT 1 FROM "\(subentity.name)"
            WHERE "\(pk)" = ?
            LIMIT 1
            """,
            bindings: [persistentIdentifier.primaryKey()]
        )
        guard !rows.isEmpty else { continue }
        return try fetchInheritanceEntity(for: persistentIdentifier, on: subentity, connection: connection)
    }
    return entity
}

nonisolated package func fetchRowsByPrimaryKeys(
    entityName: String,
    columns: [String],
    primaryKeys: [String],
    connection: borrowing DatabaseConnection<DatabaseStore>,
    chunkSize: Int = 400
) throws -> [[any Sendable]] {
    guard !primaryKeys.isEmpty else { return [] }
    let selectList = columns.map { quote($0) }.joined(separator: ", ")
    var rows: [[any Sendable]] = []
    rows.reserveCapacity(primaryKeys.count)
    var start = 0
    while start < primaryKeys.count {
        let end = min(start + chunkSize, primaryKeys.count)
        let slice = primaryKeys[start..<end]
        start = end
        let placeholders = Array(repeating: "?", count: slice.count).joined(separator: ",")
        let bindings: [any Sendable] = slice.map(\.self)
        let sql = """
            SELECT \(selectList)
            FROM "\(entityName)"
            WHERE "\(pk)" IN (\(placeholders))
            """
        rows.append(contentsOf: try connection.fetch(sql, bindings: bindings))
    }
    return rows
}
