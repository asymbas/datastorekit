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
    nonisolated public func fetch<Result>(
        _ type: Result.Type,
        properties keyPaths: [PartialKeyPath<Result> & Sendable] = [],
        sql: String...,
        bindings: [any Sendable] = []
    ) throws -> [DatabaseSnapshot] where Result: PersistentModel {
        guard let storeIdentifier = self.attachment?.store?.identifier else {
            fatalError()
        }
        guard let configuration = self.attachment?.configuration else {
            fatalError()
        }
        guard let queue = self.queue else {
            fatalError()
        }
        let entityName = Schema.entityName(for: Result.self)
        let propertiesCollected = keyPaths.reduce(into: [PropertyMetadata]()) { partialResult, keyPath in
            partialResult.append(Result.schemaMetadata(for: keyPath).unsafelyUnwrapped)
        }
        var properties = [PropertyMetadata.discriminator(for: Result.self)]
        + (propertiesCollected.isEmpty ? Result.databaseSchemaMetadata : propertiesCollected)
        for index in properties.indices {
            properties[index].isSelected = (properties[index].column != nil)
        }
        let columns = properties
            .compactMap { $0.column == nil ? nil : quote($0.column!) }
            .joined(separator: ", ")
        let result = try fetch(
                """
                SELECT \(columns) FROM "\(entityName)"
                \(sql.joined(separator: "\n"))
                """,
                bindings: bindings
        )
        var relatedSnapshots = [PersistentIdentifier: DatabaseSnapshot]()
        return try result.map { row in
            try DatabaseSnapshot(
                storeIdentifier: storeIdentifier,
                configuration: configuration,
                queue: queue,
                registry: context,
                properties: properties[...],
                values: row[...],
                relatedSnapshots: &relatedSnapshots
            )
        }
    }
    
    nonisolated public func fetch<Result>(
        for primaryKey: String,
        as type: Result.Type,
        properties keyPaths: [PartialKeyPath<Result> & Sendable] = []
    ) throws -> DatabaseSnapshot? where Result: PersistentModel {
        try fetch(
            type,
            properties: keyPaths,
            sql: "WHERE \(quote(pk)) = ?", "LIMIT 1",
            bindings: [primaryKey]
        ).first
    }
    
    nonisolated public func fetch<Result>(
        for primaryKey: String,
        as type: Result.Type
    ) throws -> DatabaseSnapshot? where Result: PersistentModel {
        try fetch(for: primaryKey, as: type, properties: [])
    }
}

// TODO: Immediately handle to-many references in non-transaction mutations.

extension DatabaseConnection where Store == DatabaseStore {
    /// Inserts the model's backing data into the data store.
    /// - Parameter snapshot: The model snapshot.
    nonisolated public func insert(
        _ snapshot: consuming Store.Snapshot,
        orReplace: Bool = false
    ) throws {
        guard let transaction = self.transaction else {
            fatalError("Inserting backing data is only allowed during a transaction.")
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
    nonisolated public /*mutating*/ func update(
        from oldSnapshot: consuming Store.Snapshot? = nil,
        to newSnapshot: consuming Store.Snapshot
    ) throws {
        guard let transaction = self.transaction else {
            fatalError("Updating backing data is only allowed during a transaction.")
        }
        let entityName = newSnapshot.entityName
        let primaryKey = newSnapshot.primaryKey
        let oldSnapshot: DatabaseSnapshot? = oldSnapshot ?? {
            guard self.attachment != nil else {
                return nil
            }
            if let snapshot = self.context?.snapshot(for: newSnapshot.persistentIdentifier) {
                logger.debug("Using snapshot from cache object: \(snapshot)")
                return snapshot
            }
            do {
                guard let result = try fetch(for: primaryKey, as: newSnapshot.type) else {
                    return nil
                }
                logger.notice("Fetching previous snapshot from store - \(context.debugDescription)")
                return result
            } catch {
                logger.error("Failed to fetch the row to update: \(error) - \(entityName) \(primaryKey)")
                return nil
            }
        }()
        let export = newSnapshot.export
//        self.externalDependencies[newSnapshot.persistentIdentifier] = export.toManyDependencies
        var columnsToUpdate = [String]()
        var valuesToUpdate = [any Sendable]()
        var propertiesChanges = [String]()
        var oldValues = [any Sendable]()
        var newValues = [any Sendable]()
        _ = try oldSnapshot?.diff(from: newSnapshot) { property, lhs, rhs in
            if let index = export.columns.firstIndex(of: property.name) {
                columnsToUpdate.append(export.columns[index])
                valuesToUpdate.append(export.values[index])
            }
            propertiesChanges.append(property.name)
            oldValues.append(lhs)
            newValues.append(rhs)
        }
        if !columnsToUpdate.isEmpty {
            logger.debug(
                "Updated snapshot.",
                metadata: [
                    "columns_to_update": "\(columnsToUpdate)",
                    "values_to_update": "\(valuesToUpdate)",
                ]
            )
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
        guard let transaction = self.transaction else {
            fatalError("Deleting backing data is only allowed during a transaction.")
        }
        let entityName = snapshot.entityName
        let primaryKey = snapshot.primaryKey
        let delete = snapshot.delete
        let _ = try execute.delete(
            from: entityName,
            as: nil,
            where: "\(pk) = ?",
            bindings: [primaryKey]
        )
        try transaction.externalStorageTransaction.apply(delete.externalStorageData)
        transaction.informDidDeleteRow(
            primaryKey,
            in: entityName,
            preservedColumns: delete.columns.isEmpty ? nil : delete.columns,
            preservedValues: delete.values.isEmpty ? nil : delete.values
        )
    }
    
    nonisolated public mutating func match(snapshot: consuming Store.Snapshot) throws -> Store.Snapshot? {
        guard let storeIdentifier = self.attachment?.store?.identifier else {
            fatalError()
        }
        guard let configuration = self.attachment?.configuration else {
            fatalError()
        }
        guard let queue = self.queue else {
            fatalError()
        }
        let persistentIdentifier = snapshot.persistentIdentifier
        var export = snapshot.export
        switch configuration.constraints[persistentIdentifier.entityName] {
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
                var targetPrimaryKey = snapshot.primaryKey
                if targetPrimaryKey != existingPrimaryKey {
                    logger.debug(
                        "Resolving upsert conflict...",
                        metadata: [
                            "entity": "\(snapshot.entityName)",
                            "diff": "\(snapshot.primaryKey) != \(existingPrimaryKey)"
                        ]
                    )
                    let existingIdentifier = try PersistentIdentifier.identifier(
                        for: persistentIdentifier.storeIdentifier!,
                        entityName: snapshot.entityName,
                        primaryKey: existingPrimaryKey
                    )
                    remappedIdentifiers[persistentIdentifier] = existingIdentifier
                    snapshot = snapshot.copy(
                        persistentIdentifier: existingIdentifier,
                        remappedIdentifiers: remappedIdentifiers
                    )
                    export = snapshot.export
                    targetPrimaryKey = existingPrimaryKey
                }
                var relatedSnapshots = [PersistentIdentifier: DatabaseSnapshot]()
                return try .init(
                    storeIdentifier: storeIdentifier,
                    configuration: configuration,
                    queue: queue,
                    registry: context,
                    properties: [.discriminator(for: snapshot.type)] + snapshot.properties[...],
                    values: (existingRow ?? [])[...],
                    relatedSnapshots: &relatedSnapshots
                )
            } else {
                logger.info("Inserted snapshot with no conflict: \(snapshot.persistentIdentifier)")
                return nil
            }
        default:
            throw SQLError("Unknown")
        }
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
        let remappedIdentifiers = try fetchByUniqueness(snapshot, uniquenessConstraints: uniquenessConstraints) { snapshot in
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
        guard let storeIdentifier = permanentIdentifier.storeIdentifier else {
            fatalError()
        }
        guard let configuration = self.attachment?.configuration else {
            fatalError()
        }
        guard let queue = self.queue else {
            fatalError()
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
                    logger.debug(
                        "Resolving upsert conflict...",
                        metadata: [
                            "entity": "\(snapshot.entityName)",
                            "diff": "\(snapshot.primaryKey) != \(existingPrimaryKey)"
                        ]
                    )
                    logger.debug("Inheriting values then overwriting.")
                    var relatedSnapshots = [PersistentIdentifier: DatabaseSnapshot]()
                    let existingSnapshot = try DatabaseSnapshot(
                        storeIdentifier: storeIdentifier,
                        configuration: configuration,
                        queue: queue,
                        registry: context,
                        properties: [.discriminator(for: snapshot.type)] + snapshot.properties[...],
                        values: (existingRow ?? [])[...],
                        relatedSnapshots: &relatedSnapshots
                    )
                    return try onConflict(existingSnapshot, snapshot)
                } else {
                    guard let existingSnapshot = try fetch(for: snapshot.primaryKey, as: snapshot.type) else {
                        fatalError("Expected to find an existing snapshot with the same primary key.")
                    }
                    logger.debug("Same primary key.")
                    return try onExisting(existingSnapshot, snapshot)
                }
            } else {
                logger.debug("No match.")
                return try onNone?(snapshot)
            }
        default:
            logger.debug("No unique constraint to check.")
            return nil
        }
    }
}
