//
//  InheritanceResolver.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreRuntime
import DataStoreSQL
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

package final class InheritanceResolver: Sendable {
    nonisolated private let storage: Mutex<[PersistentIdentifier: PersistentIdentifier]> = .init([:])
    nonisolated private let _manager: AtomicLazyReference<ModelManager> = .init()
    
    nonisolated private var manager: ModelManager {
        #if DEBUG
        guard let manager = self._manager.load() else {
            preconditionFailure()
        }
        return manager
        #else
        _manager.load().unsafelyUnwrapped
        #endif
    }
    
    nonisolated internal func bootstrap(manager: ModelManager) {
        let insert = _manager.storeIfNil(manager)
        assert(manager === insert)
    }
    
    /// Returns the resolved persistent identifier with the concrete entity name.
    ///
    /// - Parameter persistentIdentifier: The persistent identifier with a static entity name.
    /// - Returns: The resolved persistent identifier or `nil` if not found.
    nonisolated internal func resolvedPersistentIdentifier(for persistentIdentifier: PersistentIdentifier)
    -> PersistentIdentifier? { storage.withLock { $0[persistentIdentifier] } }
    
    /// Assigns the persistent identifier with the concrete entity name to the one with the dynamic entity name.
    ///
    /// - Parameters:
    ///   - persistentIdentifier: The persistent identifier with a static entity name.
    ///   - concretePersistentIdentifier: The persistent identifier with a concrete entity name
    nonisolated internal func set(
        persistentIdentifier: PersistentIdentifier,
        resolvingTo concretePersistentIdentifier: PersistentIdentifier
    ) {
        assert(persistentIdentifier.entityName != concretePersistentIdentifier.entityName)
        storage.withLock { $0[persistentIdentifier] = concretePersistentIdentifier }
    }
    
    nonisolated internal func remove(persistentIdentifier: PersistentIdentifier) {
        storage.withLock { $0[persistentIdentifier] = nil }
    }
}

extension InheritanceResolver {
    /// Resolves the given persistent identifier with a variant that has a concrete entity name.
    ///
    /// - Parameters:
    ///   - persistentIdentifier:
    ///     The persistent identifier to resolve and use for lookup.
    ///   - connection:
    ///     The database connection to resolve recursively.
    /// - Returns:
    ///   The resolved persistent identifier that has a concrete entity name, which may be identical to the given one.
    nonisolated internal func resolvePersistentIdentifier(
        for persistentIdentifier: PersistentIdentifier,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws -> PersistentIdentifier {
        if let resolvedPersistentIdentifier = try resolvedPersistentIdentifier(for: persistentIdentifier) {
            return resolvedPersistentIdentifier
        }
        guard let destinationEntity = self.manager.schema.entitiesByName[persistentIdentifier.entityName] else {
            throw SchemaError.relationshipTargetEntityNotRegistered
        }
        guard !destinationEntity.subentities.isEmpty else {
            return persistentIdentifier
        }
        let resolvedConcreteEntity = try fetchConcreteEntity(
            for: persistentIdentifier,
            on: destinationEntity,
            connection: connection
        )
        guard let storeIdentifier = connection.storeIdentifier else {
            preconditionFailure()
        }
        let resolvedPersistentIdentifier = try PersistentIdentifier.identifier(
            for: storeIdentifier,
            entityName: resolvedConcreteEntity.name,
            primaryKey: manager.primaryKey(for: persistentIdentifier)
        )
        try set(persistentIdentifier: persistentIdentifier, resolvingTo: resolvedPersistentIdentifier)
        return resolvedPersistentIdentifier
    }
    
    /// Recursively performs a fetch on each entity's subentities to find the concrete entity.
    ///
    /// - Parameters:
    ///   - persistentIdentifier:
    ///     The persistent identifier whose primary key is shared across the inheritance chain.
    ///   - entity:
    ///     The current entity to inspect.
    ///   - connection:
    ///     The database connection to resolve recursively.
    /// - Returns:
    ///   The concrete entity.
    nonisolated internal func fetchConcreteEntity(
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
                bindings: manager.primaryKey(for: persistentIdentifier)
            )
            guard !rows.isEmpty else { continue }
            return try fetchConcreteEntity(for: persistentIdentifier, on: subentity, connection: connection)
        }
        return entity
    }
}

extension InheritanceResolver {
    nonisolated internal func prepare(
        entityName: String,
        subentities: [Schema.Entity],
        persistentIdentifiers: [PersistentIdentifier],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        guard !persistentIdentifiers.isEmpty, !subentities.isEmpty else { return }
        guard let storeIdentifier = connection.storeIdentifier else {
            preconditionFailure()
        }
        let requestedIdentifiers = persistentIdentifiers.filter {
            $0.entityName == entityName && $0.storeIdentifier == storeIdentifier
        }
        guard !requestedIdentifiers.isEmpty else { return }
        let primaryKeysByIdentifier: [PersistentIdentifier: String] = manager.primaryKeys(
            for: requestedIdentifiers,
            as: String.self
        )
        let identifiersByPrimaryKey = Dictionary(grouping: requestedIdentifiers) {
            primaryKeysByIdentifier[$0]!
        }
        let primaryKeys = Array(identifiersByPrimaryKey.keys)
        let inList = Array(repeating: "?", count: primaryKeys.count).joined(separator: ",")
        let bindings: [any Sendable] = primaryKeys
        var resolved = [(PersistentIdentifier, PersistentIdentifier)]()
        for subentity in subentities {
            let rows = try connection.fetch(
                """
                SELECT "\(pk)" FROM "\(subentity.name)"
                WHERE "\(pk)" IN (\(inList))
                """,
                bindings: bindings
            )
            for row in rows {
                guard let resolvedPrimaryKey = row[0] as? String,
                      let requestedIdentifiers = identifiersByPrimaryKey[resolvedPrimaryKey] else {
                    continue
                }
                let resolvedPersistentIdentifier = try PersistentIdentifier.identifier(
                    for: storeIdentifier,
                    entityName: subentity.name,
                    primaryKey: resolvedPrimaryKey
                )
                for requestedPersistentIdentifier in requestedIdentifiers {
                    resolved.append((requestedPersistentIdentifier, resolvedPersistentIdentifier))
                }
            }
        }
        storage.withLock { storage in
            for (requestedPersistentIdentifier, resolvedPersistentIdentifier) in resolved {
                storage[requestedPersistentIdentifier] = resolvedPersistentIdentifier
            }
        }
    }
    
    nonisolated internal func prepare(
        destination: String,
        subentities: [Schema.Entity],
        persistentIdentifiers: [PersistentIdentifier],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        try prepare(
            entityName: destination,
            subentities: subentities,
            persistentIdentifiers: persistentIdentifiers,
            connection: connection
        )
    }
    
    nonisolated internal func prepare(
        entity: Schema.Entity,
        persistentIdentifiers: [PersistentIdentifier],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        guard !persistentIdentifiers.isEmpty else { return }
        guard let storeIdentifier = connection.storeIdentifier else {
            preconditionFailure()
        }
        let subentities = descendants(of: entity)
        guard !subentities.isEmpty else { return }
        let requestedIdentifiers = persistentIdentifiers.filter {
            $0.entityName == entity.name && $0.storeIdentifier == storeIdentifier
        }
        guard !requestedIdentifiers.isEmpty else { return }
        let primaryKeysByIdentifier: [PersistentIdentifier: String] = manager.primaryKeys(
            for: requestedIdentifiers,
            as: String.self
        )
        let identifiersByPrimaryKey = Dictionary(grouping: requestedIdentifiers) {
            primaryKeysByIdentifier[$0]!
        }
        let primaryKeys = Array(identifiersByPrimaryKey.keys)
        let inList = Array(repeating: "?", count: primaryKeys.count).joined(separator: ",")
        let bindings: [any Sendable] = primaryKeys
        var resolved = [(PersistentIdentifier, PersistentIdentifier)]()
        for subentity in subentities {
            let rows = try connection.fetch(
                 """
                 SELECT "\(pk)" FROM "\(subentity.name)"
                 WHERE "\(pk)" IN (\(inList))
                 """,
                 bindings: bindings
            )
            for row in rows {
                guard let resolvedPrimaryKey = row[0] as? String,
                      let requestedIdentifiers = identifiersByPrimaryKey[resolvedPrimaryKey] else {
                    continue
                }
                let resolvedPersistentIdentifier = try PersistentIdentifier.identifier(
                    for: storeIdentifier,
                    entityName: subentity.name,
                    primaryKey: resolvedPrimaryKey
                )
                for requestedPersistentIdentifier in requestedIdentifiers {
                    resolved.append((requestedPersistentIdentifier, resolvedPersistentIdentifier))
                }
            }
        }
        storage.withLock { storage in
            for (requestedPersistentIdentifier, resolvedPersistentIdentifier) in resolved {
                storage[requestedPersistentIdentifier] = resolvedPersistentIdentifier
            }
        }
    }
    
    /// Returns a flattened list of all descendant subentities of the given entity.
    ///
    /// - Parameter entity: The root entity whose descendant subentities are returned.
    /// - Returns: A flattened list of all nested subentities.
    nonisolated private func descendants(of entity: Schema.Entity) -> [Schema.Entity] {
        guard !entity.subentities.isEmpty else { return [] }
        var descendants = [Schema.Entity]()
        var queue = Array(entity.subentities)
        var index = 0
        while index < queue.count {
            let subentity = queue[index]
            index += 1
            descendants.append(subentity)
            queue.append(contentsOf: subentity.subentities)
        }
        return descendants
    }
}
