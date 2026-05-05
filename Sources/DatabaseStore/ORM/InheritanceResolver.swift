//
//  InheritanceResolver.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreRuntime
private import Logging
private import Synchronization
internal import DataStoreSQL
internal import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

package final class InheritanceResolver: Sendable {
    nonisolated private let storage: Mutex<[PersistentIdentifier: PersistentIdentifier]> = .init([:])
    nonisolated private let topology: AtomicLazyReference<TopologyCache> = .init()
    nonisolated private let _manager: AtomicLazyReference<ModelManager> = .init()
    
    nonisolated private var manager: ModelManager {
        #if DEBUG
        guard let manager = self._manager.load() else {
            preconditionFailure("No ModelManager has been set.")
        }
        return manager
        #else
        _manager.load().unsafelyUnwrapped
        #endif
    }
    
    nonisolated internal func bootstrap(manager: ModelManager) {
        let insert = _manager.storeIfNil(manager)
        assert(manager === insert)
        var absoluteDepth = [String: Int]()
        for entity in manager.schema.entities {
            var depth = 0
            var current = entity.superentity
            while let entity = current {
                depth += 1
                current = entity.superentity
            }
            absoluteDepth[entity.name] = depth
        }
        var descendants = [String: [Descendant]]()
        for entity in manager.schema.entities {
            var list = [Descendant]()
            var queue = Array(entity.subentities)
            var index = 0
            while index < queue.count {
                let subentity = queue[index]
                index += 1
                list.append(Descendant(name: subentity.name, depth: absoluteDepth[subentity.name] ?? 0))
                queue.append(contentsOf: subentity.subentities)
            }
            list.sort { $0.depth > $1.depth }
            descendants[entity.name] = list
        }
        _ = topology.storeIfNil(TopologyCache(descendants: descendants, absoluteDepth: absoluteDepth))
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
    nonisolated private struct Descendant: Sendable {
        nonisolated fileprivate let name: String
        nonisolated fileprivate let depth: Int
    }
    
    nonisolated private final class TopologyCache: Sendable {
        nonisolated fileprivate let descendants: [String: [Descendant]]
        nonisolated fileprivate let absoluteDepth: [String: Int]
        
        nonisolated fileprivate init(
            descendants: [String: [Descendant]],
            absoluteDepth: [String: Int]
        ) {
            self.descendants = descendants
            self.absoluteDepth = absoluteDepth
        }
    }
    
    nonisolated private func descendants(ofEntityNamed entityName: String) -> [Descendant] {
        topology.load()?.descendants[entityName] ?? []
    }
    
    nonisolated private func absoluteDepth(ofEntityNamed entityName: String) -> Int {
        topology.load()?.absoluteDepth[entityName] ?? 0
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
        if let resolvedPersistentIdentifier = resolvedPersistentIdentifier(for: persistentIdentifier) {
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
        if resolvedConcreteEntity.name != persistentIdentifier.entityName {
            set(persistentIdentifier: persistentIdentifier, resolvingTo: resolvedPersistentIdentifier)
        }
        return resolvedPersistentIdentifier
    }
    
    nonisolated internal func fetchConcreteEntity(
        for persistentIdentifier: PersistentIdentifier,
        on entity: Schema.Entity,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws -> Schema.Entity {
        let descendants = self.descendants(ofEntityNamed: entity.name)
        guard !descendants.isEmpty else { return entity }
        let primaryKey: String = manager.primaryKey(for: persistentIdentifier)
        var clauses = [String]()
        clauses.reserveCapacity(descendants.count)
        for descendant in descendants {
            clauses.append(
                """
                SELECT '\(descendant.name)' AS entity, \(descendant.depth) AS depth
                WHERE EXISTS (SELECT 1 FROM "\(descendant.name)" WHERE "\(pk)" = ?)
                """
            )
        }
        let sql = """
            SELECT entity
            FROM (\(clauses.joined(separator: "\nUNION ALL ")))
            ORDER BY depth DESC LIMIT 1
            """
        var bindings = [any Sendable]()
        bindings.reserveCapacity(descendants.count)
        for _ in 0..<descendants.count {
            bindings.append(primaryKey)
        }
        let rows = try connection.fetch(sql, bindings: bindings)
        guard let resolvedName = rows.first?.first as? String,
              let resolvedEntity = manager.schema.entitiesByName[resolvedName] else {
            return entity
        }
        return resolvedEntity
    }
}

extension InheritanceResolver {
    nonisolated internal func prepare(
        entityName: String,
        subentities: [Schema.Entity],
        persistentIdentifiers: [PersistentIdentifier],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        let descendants = subentities.map {
            Descendant(name: $0.name, depth: absoluteDepth(ofEntityNamed: $0.name))
        }
        try prepare(
            entityName: entityName,
            descendants: descendants,
            persistentIdentifiers: persistentIdentifiers,
            connection: connection
        )
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
        try prepare(
            entityName: entity.name,
            descendants: descendants(ofEntityNamed: entity.name),
            persistentIdentifiers: persistentIdentifiers,
            connection: connection
        )
    }
    
    nonisolated private func prepare(
        entityName: String,
        descendants: [Descendant],
        persistentIdentifiers: [PersistentIdentifier],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        guard !persistentIdentifiers.isEmpty, !descendants.isEmpty else { return }
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
        let placeholders = Array(repeating: "?", count: primaryKeys.count).joined(separator: ",")
        var clauses = [String]()
        clauses.reserveCapacity(descendants.count)
        for descendant in descendants {
            clauses.append(
                """
                SELECT "\(pk)" AS pk, '\(descendant.name)' AS entity, \(descendant.depth) AS depth
                FROM "\(descendant.name)"
                WHERE "\(pk)" IN (\(placeholders))
                """
            )
        }
        let sql = """
            SELECT pk, entity, MAX(depth)
            FROM (\(clauses.joined(separator: "\nUNION ALL ")))
            GROUP BY pk
            """
        var bindings = [any Sendable]()
        bindings.reserveCapacity(primaryKeys.count * descendants.count)
        for _ in 0..<descendants.count {
            for primaryKey in primaryKeys {
                bindings.append(primaryKey)
            }
        }
        let rows = try connection.fetch(sql, bindings: bindings)
        var resolved = [(PersistentIdentifier, PersistentIdentifier)]()
        for row in rows {
            guard let resolvedPrimaryKey = row[0] as? String,
                  let resolvedName = row[1] as? String,
                  let requestedIdentifiers = identifiersByPrimaryKey[resolvedPrimaryKey] else {
                continue
            }
            let resolvedPersistentIdentifier = try PersistentIdentifier.identifier(
                for: storeIdentifier,
                entityName: resolvedName,
                primaryKey: resolvedPrimaryKey
            )
            for requestedPersistentIdentifier in requestedIdentifiers {
                resolved.append((requestedPersistentIdentifier, resolvedPersistentIdentifier))
            }
        }
        storage.withLock { storage in
            for (requestedPersistentIdentifier, resolvedPersistentIdentifier) in resolved {
                storage[requestedPersistentIdentifier] = resolvedPersistentIdentifier
            }
        }
    }
    
    #if false
    
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
    
    #endif
}
