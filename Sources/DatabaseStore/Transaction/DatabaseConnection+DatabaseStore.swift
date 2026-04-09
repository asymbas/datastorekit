//
//  DatabaseConnection+DatabaseStore.swift
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
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension DatabaseConnection where Store == DatabaseStore {
    /// Tracks relationships between models by their `PersistentIdentifier`.
    nonisolated internal var graph: ReferenceGraph? {
        context?.graph ?? attachment?.graph
    }
    
    nonisolated internal var schema: Schema? {
        attachment?.schema
    }
    
    nonisolated internal var storeIdentifier: String? {
        attachment?.store?.identifier
    }
    
    nonisolated package func resolveEntity(
        _ entity: Schema.Entity? = nil,
        for persistentIdentifier: PersistentIdentifier
    ) throws -> Schema.Entity {
        guard let entity = entity ?? self.schema?.entitiesByName[persistentIdentifier.entityName] else {
            throw SchemaError.entityNotRegistered
        }
        assert(persistentIdentifier.entityName == entity.name)
        let resolvedPersistentIdentifier = try resolvePersistentIdentifier(for: persistentIdentifier)
        guard persistentIdentifier != resolvedPersistentIdentifier else {
            logger.debug("\(entity.name) does not resolve any further: \(persistentIdentifier)")
            return entity
        }
        guard let resolvedEntity = self.schema?.entitiesByName[resolvedPersistentIdentifier.entityName] else {
            throw SchemaError.entityNotRegistered
        }
        logger.debug(
            "\(persistentIdentifier.entityName) resolves to \(resolvedEntity.name): \(persistentIdentifier)",
            metadata: ["id": "\(persistentIdentifier.id)", "id_resolved": "\(resolvedPersistentIdentifier.id)"]
        )
        return resolvedEntity
    }
    
    /// Resolves the given persistent identifier with a variant that has a concrete entity name.
    ///
    /// - Parameter persistentIdentifier:
    ///   The persistent identifier to resolve and use for lookup.
    /// - Returns:
    ///   The resolved persistent identifier that has a concrete entity name, which may be identical to the given one.
    nonisolated package func resolvePersistentIdentifier(for persistentIdentifier: PersistentIdentifier)
    throws -> PersistentIdentifier {
        #if DEBUG
        guard let attachment = self.attachment else {
            preconditionFailure()
        }
        return try attachment.inheritance.resolvePersistentIdentifier(for: persistentIdentifier, connection: self)
        #else
        return try attachment!.inheritance.resolvePersistentIdentifier(for: persistentIdentifier, connection: self)
        #endif
    }
}

extension DatabaseConnection where Store == DatabaseStore {
    /// Fetches snapshots for the given model type, optionally filtered by SQL predicates.
    ///
    /// - Parameters:
    ///   - type: The model type to fetch.
    ///   - keyPaths: Properties to include. Defaults to all properties.
    ///   - sql: SQL clauses appended after the `FROM` clause.
    ///   - bindings: Bound parameter values for the SQL clauses.
    /// - Returns: An array of snapshots matching the query.
    nonisolated public func fetch<Model>(
        _ type: Model.Type,
        properties keyPaths: [PartialKeyPath<Model> & Sendable] = [],
        predicate sql: String...,
        bindings: [any Sendable] = []
    ) throws -> [Store.Snapshot] where Model: PersistentModel {
        guard let queue = self.queue else {
            preconditionFailure("The queue was unexpectedly nil.")
        }
        let entityName = Schema.entityName(for: Model.self)
        let propertiesCollected = try keyPaths.reduce(into: [PropertyMetadata]()) { partialResult, keyPath in
            guard let property = Model.schemaMetadata(for: keyPath) else {
                throw SchemaError.propertyMetadataNotFound
            }
            partialResult.append(property)
        }
        var properties = [PropertyMetadata.discriminator(for: Model.self)]
        + (propertiesCollected.isEmpty ? Model.databaseSchemaMetadata : propertiesCollected)
        for index in properties.indices {
            let property = properties[index]
            let hasColumn = property.column != nil
            let isInheritedColumn = hasColumn && property.reference != nil && property.flags.contains(.isInherited)
            properties[index].isSelected = hasColumn && !isInheritedColumn
        }
        let result = try fetch(
              """
              SELECT \(properties.filter(\.isSelected).compactMap(\.column).map(quote).joined(separator: ", ")) 
              FROM "\(entityName)"
              \(sql.joined(separator: "\n"))
              """,
              bindings: bindings
        )
        var relatedSnapshots = [PersistentIdentifier: Store.Snapshot]()
        let snapshots = try result.map { row in
            try Store.Snapshot(
                storeIdentifier: attachment!.store!.identifier,
                configuration: attachment!.configuration,
                connection: self,
                properties: properties[...],
                values: row[...],
                relatedSnapshots: &relatedSnapshots
            )
        }
        logger.debug("Fetched \(snapshots.count) \(entityName) snapshots.", metadata: [
            "sql": "\(sql.joined(separator: "\n"))",
            "bindings": "\(bindings)"
        ])
        return snapshots
    }
    
    /// Fetches a single snapshot by primary key.
    ///
    /// - Parameters:
    ///   - primaryKey: The primary key to look up.
    ///   - type: The model type to fetch.
    ///   - keyPaths: Properties to include. Defaults to all properties.
    /// - Returns: The matching snapshot, or `nil` if not found.
    nonisolated public func fetch<Model>(
        for primaryKey: String,
        as type: Model.Type,
        properties keyPaths: [PartialKeyPath<Model> & Sendable] = []
    ) throws -> Store.Snapshot? where Model: PersistentModel {
        try fetch(type, properties: keyPaths, predicate: "WHERE \(quote(pk)) = ?", "LIMIT 1", bindings: [primaryKey]).first
    }
    
    /// Fetches a single snapshot by primary key.
    ///
    /// - Parameters:
    ///   - primaryKey: The primary key to look up.
    ///   - type: The model type to fetch.
    /// - Returns: The matching snapshot, or `nil` if not found.
    nonisolated public func fetch<Model>(
        for primaryKey: String,
        as type: Model.Type
    ) throws -> Store.Snapshot? where Model: PersistentModel {
        try fetch(for: primaryKey, as: type, properties: [])
    }
    
    nonisolated public func fetch(for persistentIdentifier: PersistentIdentifier) throws -> Store.Snapshot? {
        guard let type = Schema.type(for: persistentIdentifier.entityName) else {
            preconditionFailure()
        }
        return try fetch(for: primaryKey(for: persistentIdentifier), as: type)
    }
}

// TODO: Immediately handle to-many references in non-transaction mutations.

extension DatabaseConnection where Store == DatabaseStore {
    /// Inserts the model's backing data into the data store.
    /// - Parameter snapshot: The model snapshot.
    nonisolated public func insert(_ snapshot: consuming Store.Snapshot, orReplace: Bool = false) throws {
        guard let transaction = self.transaction else {
            preconditionFailure("Inserting backing data is only allowed during a transaction.")
        }
        let export = snapshot.export
        try execute.insert(
            into: snapshot.entityName,
            orReplace: orReplace,
            columns: export.columns,
            values: export.values
        )
        try transaction.externalStorageTransaction.apply(export.externalStorageData)
        transaction.informDidInsertRow(
            for: snapshot.primaryKey,
            in: snapshot.entityName,
            columns: export.columns,
            values: export.values
        )
    }
    
    /// Updates the model's backing data into the data store.
    ///
    /// - Parameters:
    ///   - oldSnapshot: The previous model snapshot.
    ///   - newSnapshot: The current model snapshot.
    nonisolated public func update(
        from oldSnapshot: consuming Store.Snapshot? = nil,
        to newSnapshot: consuming Store.Snapshot
    ) throws {
        let entityName = newSnapshot.entityName
        let primaryKey = newSnapshot.primaryKey
        let oldSnapshot: Store.Snapshot? = oldSnapshot ?? {
            guard self.attachment != nil else {
                return nil
            }
            if let snapshot = self.context?.snapshot(for: newSnapshot.persistentIdentifier) {
                return snapshot
            }
            do {
                guard let result = try fetch(for: primaryKey, as: newSnapshot.type) else {
                    return nil
                }
                return result
            } catch {
                logger.error("Failed to fetch the row to update: \(error) - \(entityName) \(primaryKey)")
                return nil
            }
        }()
        let inheritedSnapshots = try inheritedSnapshots(for: newSnapshot)
        let inheritedOldSnapshotsByEntity = try Dictionary(
            uniqueKeysWithValues: (oldSnapshot.map { try self.inheritedSnapshots(for: $0) } ?? []).map {
                ($0.entityName, $0)
            }
        )
        try updateRow(from: oldSnapshot, to: newSnapshot)
        for inheritedSnapshot in inheritedSnapshots {
            try updateRow(
                from: inheritedOldSnapshotsByEntity[inheritedSnapshot.entityName],
                to: inheritedSnapshot
            )
        }
    }
    
    nonisolated private func updateRow(
        from oldSnapshot: consuming Store.Snapshot? = nil,
        to newSnapshot: consuming Store.Snapshot
    ) throws {
        guard let transaction = self.transaction else {
            preconditionFailure("Updating backing data is only allowed during a transaction.")
        }
        let entityName = newSnapshot.entityName
        let primaryKey = newSnapshot.primaryKey
        let export = newSnapshot.export
        let count = newSnapshot.properties.count
        var columnsToUpdate = [String]()
        var valuesToUpdate = [any Sendable]()
        var propertiesChanges = [String]()
        propertiesChanges.reserveCapacity(count)
        var oldValues = [any Sendable]()
        oldValues.reserveCapacity(count)
        var newValues = [any Sendable]()
        newValues.reserveCapacity(count)
        _ = oldSnapshot?.diff(from: newSnapshot) { property, lhs, rhs in
            if let column = property.column,
               let index = export.columns.firstIndex(of: column) {
                columnsToUpdate.append(export.columns[index])
                valuesToUpdate.append(export.values[index])
            }
            propertiesChanges.append(property.name)
            oldValues.append(lhs)
            newValues.append(rhs)
        }
        if !columnsToUpdate.isEmpty {
            let _ = try execute.update(
                table: newSnapshot.entityName,
                columns: columnsToUpdate,
                values: valuesToUpdate,
                where: "\(pk) = ?",
                bindings: [primaryKey]
            )
        }
        try transaction.externalStorageTransaction.apply(export.externalStorageData)
        if !propertiesChanges.isEmpty {
            transaction.informDidUpdateRow(
                for: primaryKey,
                in: entityName,
                columns: propertiesChanges,
                oldValues: oldValues,
                newValues: newValues
            )
        }
    }
    
    /// Deletes the model's backing data from the data store.
    /// - Parameter snapshot: The model snapshot.
    nonisolated public func delete(_ snapshot: consuming Store.Snapshot) throws {
        let inheritedSnapshots = try inheritedSnapshots(for: snapshot)
        try deleteRow(snapshot)
        for inheritedSnapshot in inheritedSnapshots {
            try deleteRow(inheritedSnapshot)
        }
    }
    
    nonisolated private func deleteRow(_ snapshot: consuming Store.Snapshot) throws {
        guard let transaction = self.transaction else {
            preconditionFailure("Deleting backing data is only allowed during a transaction.")
        }
        let entityName = snapshot.entityName
        let primaryKey = snapshot.primaryKey
        let delete = snapshot.delete
        let _ = try execute.delete(from: entityName, where: "\(pk) = ?", bindings: [primaryKey])
        try transaction.externalStorageTransaction.apply(delete.externalStorageData)
        transaction.informDidDeleteRow(
            primaryKey,
            in: entityName,
            preservedColumns: delete.columns.isEmpty ? nil : delete.columns,
            preservedValues: delete.values.isEmpty ? nil : delete.values
        )
    }
    
    nonisolated public consuming func upsert(
        _ snapshot: consuming Store.Snapshot,
        uniquenessConstraints: [[String]]
    ) throws {
        let temporaryIdentifier = snapshot.persistentIdentifier
        guard !uniquenessConstraints.isEmpty else {
            try self.insert(snapshot)
            return
        }
        let remappedIdentifiers = try fetchByUniqueness(
            snapshot,
            uniquenessConstraints: uniquenessConstraints
        ) { snapshot in
            var remappedIdentifiers = self.remappedIdentifiers
            remappedIdentifiers[temporaryIdentifier] = snapshot.persistentIdentifier
            try self.insert(snapshot)
            return remappedIdentifiers
        } onExisting: { existing, candidate in
            var remappedIdentifiers = self.remappedIdentifiers
            remappedIdentifiers[temporaryIdentifier] = existing.persistentIdentifier
            let candidate = candidate.copy(
                persistentIdentifier: existing.persistentIdentifier,
                remappedIdentifiers: remappedIdentifiers
            )
            try self.update(from: existing, to: candidate)
            return remappedIdentifiers
        } onConflict: { existing, candidate in
            var remappedIdentifiers = self.remappedIdentifiers
            remappedIdentifiers[temporaryIdentifier] = existing.persistentIdentifier
            let candidate = candidate.copy(
                persistentIdentifier: existing.persistentIdentifier,
                remappedIdentifiers: remappedIdentifiers
            )
            try self.update(from: existing, to: candidate)
            return remappedIdentifiers
        }
        if let remappedIdentifiers {
            self.remappedIdentifiers = remappedIdentifiers
        }
    }
    
    nonisolated public nonmutating func fetchByUniqueness<Result>(
        _ snapshot: Store.Snapshot,
        uniquenessConstraints: [[String]]? = nil,
        onNone: ((Store.Snapshot) throws -> Result)?,
        onConflict: (_ existing: Store.Snapshot, _ candidate: Store.Snapshot) throws -> Result
    ) throws -> Result? {
        try fetchByUniqueness(
            snapshot,
            uniquenessConstraints: uniquenessConstraints,
            onNone: onNone,
            onExisting: onConflict,
            onConflict: onConflict
        )
    }
    
    nonisolated public nonmutating func fetchByUniqueness<Result>(
        _ snapshot: Store.Snapshot,
        uniquenessConstraints: [[String]]? = nil,
        onNone: ((Store.Snapshot) throws -> Result)?,
        onExisting: (_ existing: Store.Snapshot, _ candidate: Store.Snapshot) throws -> Result,
        onConflict: (_ existing: Store.Snapshot, _ candidate: Store.Snapshot) throws -> Result
    ) throws -> Result? {
        let permanentIdentifier = snapshot.persistentIdentifier
        var snapshot = snapshot
        let export = snapshot.export
        guard let configuration = self.attachment?.configuration else {
            preconditionFailure("\(Store.Attachment.self) must have a configuration.")
        }
        guard let queue = self.queue else {
            preconditionFailure("The queue was unexpectedly nil.")
        }
        switch uniquenessConstraints ?? configuration.constraints[permanentIdentifier.entityName] {
        case let uniquenessConstraints? where !uniquenessConstraints.isEmpty:
            var existingRow: [any Sendable]?
            for uniquenessConstraint in uniquenessConstraints {
                do {
                    guard let matchedRow = try execute.fetchByUniqueness(
                        from: snapshot.entityName,
                        columns: export.columns,
                        values: export.values,
                        onConflict: uniquenessConstraint
                    ) else {
                        continue
                    }
                    guard let primaryKey = matchedRow.first as? String else {
                        throw SQLError(.columnNotFound(pk))
                    }
                    if existingRow == nil {
                        existingRow = matchedRow
                    } else if existingRow?.first as? String != primaryKey {
                        throw ConstraintError(.unique)
                    }
                } catch let error as SQLError where {
                    // Inherited properties will have missing columns.
                    if case .columnNotFound? = error.code { return true }
                    return false
                }() {
                    continue
                } catch {
                    throw error
                }
            }
            if let existingPrimaryKey = existingRow?.first as? String {
                logger.debug("Found a conflict.")
                if snapshot.primaryKey != existingPrimaryKey {
                    logger.debug("Resolving upsert conflict...", metadata: [
                        "entity": "\(snapshot.entityName)",
                        "diff": "\(snapshot.primaryKey) != \(existingPrimaryKey)"
                    ])
                    // Inheriting values then overwriting.
                    var relatedSnapshots = [PersistentIdentifier: Store.Snapshot]()
                    let existingSnapshot = try Store.Snapshot(
                        queue: queue,
                        properties: [.discriminator(for: snapshot.type)] + snapshot.properties[...],
                        values: (existingRow ?? [])[...],
                        relatedSnapshots: &relatedSnapshots
                    )
                    return try onConflict(existingSnapshot, snapshot)
                } else {
                    guard let existingSnapshot = try fetch(for: snapshot.primaryKey, as: snapshot.type) else {
                        preconditionFailure("Expected to find an existing snapshot with the same primary key.")
                    }
                    // Same primary key.
                    return try onExisting(existingSnapshot, snapshot)
                }
            } else {
                // No match.
                return try onNone?(snapshot)
            }
        default:
            // No unique constraint to check.
            return nil
        }
    }
    
    nonisolated public mutating func match(snapshot: consuming Store.Snapshot) throws -> Store.Snapshot? {
        var remappedIdentifiers = self.remappedIdentifiers
        let result = try fetchByUniqueness(snapshot, onNone: nil) { existing, candidate in
            remappedIdentifiers[candidate.persistentIdentifier] = existing.persistentIdentifier
            return existing
        }
        self.remappedIdentifiers = remappedIdentifiers
        return result
    }
}

extension DatabaseConnection where Store == DatabaseStore {
    nonisolated package func inheritedSnapshots(for snapshot: Store.Snapshot) throws -> [Store.Snapshot] {
        guard let entity = self.schema?.entitiesByName[snapshot.entityName],
              let superentity = entity.superentity else {
            return []
        }
        var snapshot = snapshot
        let indices = snapshot.export.inheritedDependencies
        guard !indices.isEmpty else { return [] }
        var inheritedSnapshots = [Store.Snapshot]()
        try snapshot.recursiveExportChain(
            on: superentity,
            indices: indices,
            inheritedTraversalSnapshots: &inheritedSnapshots
        )
        return inheritedSnapshots
    }
}
