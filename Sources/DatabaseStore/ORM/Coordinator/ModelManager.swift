//
//  ModelManager.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Collections
import DataStoreCore
import Logging
import SQLiteHandle
import SQLSupport
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

public final class ModelManager: Sendable {
    internal typealias Snapshot = DatabaseSnapshot
    nonisolated private let storage: Mutex<[PersistentIdentifier: DatabaseBackingData]> = .init([:])
    nonisolated private let entityCacheRevisions: Mutex<[String: UInt64]> = .init([:])
    nonisolated internal let globalCacheRevision: Atomic<UInt64> = .init(0)
    nonisolated internal let isCachingSnapshots: Bool
    nonisolated internal let state: Atomic<State> = .init(.idle)
    nonisolated internal let editingStates: Mutex<[PersistentIdentifier: OrderedSet<EditingState.ID>]> = .init([:])
    nonisolated internal let registries: Mutex<[EditingState.ID: SnapshotRegistry]> = .init([:])
    nonisolated internal let configuration: DatabaseConfiguration
    nonisolated internal let broadcaster: EventBroadcaster = .init()
    nonisolated public let graph: ReferenceGraph = .init()
    
    nonisolated internal var store: DatabaseStore? {
        configuration.store
    }
    
    nonisolated public var persistentIdentifiers: [PersistentIdentifier] {
        .init(editingStates.withLock(\.keys))
    }
    
    nonisolated internal init(configuration: DatabaseConfiguration) {
        self.configuration = configuration
        self.isCachingSnapshots = !configuration.options.contains(.disableSnapshotCaching)
    }
    
    deinit {
        logger.debug("ModelManager deinit.")
        if !registries.withLock(\.isEmpty) {
            logger.debug(
                "SnapshotRegistry instances are still alive.",
                metadata: ["registries": "\(registries.withLock(\.keys))"]
            )
        }
    }
    
    internal enum State: UInt8, AtomicRepresentable {
        case idle = 0
        case transaction
    }
}

extension ModelManager {
    nonisolated internal func registry<T>(for editingState: T, isTransaction: Bool) -> SnapshotRegistry?
    where T: EditingStateProviding {
        guard isCachingSnapshots else {
            return nil
        }
        guard state.load(ordering: .acquiring) != .transaction else {
            logger.notice("SnapshotRegistry is currently in a transaction: \(editingState.id)")
            return nil
        }
        return registries.withLockIfAvailable { registries in
            guard let registry = registries[editingState.id] else {
                logger.warning("SnapshotRegistry not found: \(editingState.id)")
                return nil
            }
            let state = registry.state.load(ordering: .relaxed)
            guard state == .idle else {
                logger.notice("SnapshotRegistry is busy: \(editingState.id) = \(state)")
                if isTransaction {
                    return registry
                }
                return nil
            }
            logger.debug("SnapshotRegistry is found for ModelManager: \(editingState.id)")
            return registry
        } ?? nil
    }
    
    /// Retrieves a `SnapshotRegistry` instance that is associated with a `ModelContext`.
    nonisolated package func registry<T>(for editingState: T) -> SnapshotRegistry?
    where T: EditingStateProviding {
        registry(for: editingState, isTransaction: false)
    }
    
    /// Creates or replaces a `SnapshotRegistry` instance for the `ModelContext` it will be associated to.
    /// - Parameter editingState: The editing state that owns the registry.
    nonisolated internal func initializeState(for editingState: EditingState) {
        guard isCachingSnapshots else { return }
        registries.withLock { $0[editingState.id] = .init(manager: self, id: editingState.id) }
    }
    
    /// Invalidates the editing state and removes its associated registry after cleanup.
    /// - Parameter editingState: The `EditingState` to remove.
    nonisolated internal func invalidateState(for editingState: EditingState) {
        guard isCachingSnapshots else { return }
        guard let registry = self.registries.withLock({ $0[editingState.id] }) else {
            logger.warning("Unable to invalidate SnapshotRegistry: \(editingState)")
            return
        }
        let scopedIdentifiers = registry.persistentIdentifiers
        var pruneIdentifiers = Set<PersistentIdentifier>()
        editingStates.withLock { editingStates in
            for scopedIdentifier in scopedIdentifiers {
                if var editingStatesAssociatedWithScopedIdentifier = editingStates[scopedIdentifier] {
                    editingStatesAssociatedWithScopedIdentifier.remove(editingState.id)
                    if editingStatesAssociatedWithScopedIdentifier.isEmpty {
                        _ = editingStates.removeValue(forKey: scopedIdentifier)
                        pruneIdentifiers.insert(scopedIdentifier)
                        logger.debug("Invalidated and pruning identifier with no remaining ModelContext: \(editingState)")
                    } else {
                        editingStates[scopedIdentifier] = consume editingStatesAssociatedWithScopedIdentifier
                        logger.debug("Invalidated and updated identifier: \(editingState)")
                    }
                }
            }
        }
        for staleIdentifier in pruneIdentifiers {
            cleanup(persistentIdentifier: staleIdentifier)
        }
        _ = registries.withLock { $0.removeValue(forKey: editingState.id) }
    }
}

extension ModelManager {
    /// Fetches the snapshot from the internal storage.
    ///
    /// - Parameters:
    ///   - persistentIdentifier: The identifier of the snapshot being requested.
    ///   - remappedIdentifiers: A dictionary for updating the snapshot's references.
    /// - Returns: The snapshot can only return if a registered `PersistentModel` is still instantiated.
    nonisolated internal func snapshot(
        for persistentIdentifier: PersistentIdentifier,
        remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
    ) -> Snapshot? {
        guard let backingData = self.storage.withLock({ $0[persistentIdentifier] }) else {
            logger.notice("Snapshot not found in cache: \(persistentIdentifier)")
            return nil
        }
        backingData.accessedTimestamp = .now()
        guard let snapshot = try? Snapshot(backingData: backingData) else {
            logger.notice("Snapshot could not be instantiated from backing data: \(persistentIdentifier)")
            return nil
        }
        guard persistentIdentifier.storeIdentifier != nil && snapshot.storeIdentifier != nil else {
            let description = "\(persistentIdentifier) or \(snapshot.persistentIdentifier)"
            logger.notice("Fetched snapshot used a temporary identifier: \(description)")
            return nil
        }
        guard persistentIdentifier.entityName == snapshot.entityName else {
            let description = "\(persistentIdentifier.entityName) != \(snapshot.entityName)"
            logger.notice("Fetched snapshot has mismatching entity name: \(description)")
            return nil
        }
        if !remappedIdentifiers.isEmpty {
            return snapshot.copy(
                persistentIdentifier: remappedIdentifiers[persistentIdentifier] ?? persistentIdentifier,
                remappedIdentifiers: remappedIdentifiers
            )
        }
        return snapshot
    }
    
    nonisolated internal func snapshots(
        for identifiers: [PersistentIdentifier],
        remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
    ) -> [PersistentIdentifier: Snapshot] {
        guard !identifiers.isEmpty else { return [:] }
        let backingDataMapping = self.storage.withLock { storage in
            var result = [PersistentIdentifier: DatabaseBackingData](minimumCapacity: identifiers.count)
            for identifier in identifiers {
                if let backingData = storage[identifier] {
                    backingData.accessedTimestamp = .now()
                    result[identifier] = backingData
                }
            }
            return result
        }
        var snapshots = [PersistentIdentifier: Snapshot](minimumCapacity: backingDataMapping.count)
        for (identifier, backingData) in backingDataMapping {
            if let snapshot = try? Snapshot(backingData: backingData) {
                let identifier = remappedIdentifiers[identifier] ?? identifier
                snapshots[identifier] = remappedIdentifiers.isEmpty
                ? snapshot
                : snapshot.copy(
                    persistentIdentifier: identifier,
                    remappedIdentifiers: remappedIdentifiers
                )
            }
        }
        return snapshots
    }
    
    nonisolated internal func upsert(
        snapshot: Snapshot,
        from registry: SnapshotRegistry
    ) throws -> DatabaseBackingData? {
        let persistentIdentifier = snapshot.persistentIdentifier
        let backingData: DatabaseBackingData
        switch storage.withLock({ $0 [persistentIdentifier] }) {
        case let existingSnapshot? where existingSnapshot.createdTimestamp < snapshot.timestamp:
            if let id = existingSnapshot.subscription.withLock({ $0?.id }) {
                logger.trace("Found subscription identifier: \(id)")
                self.broadcaster.broadcast(for: id, value: .init(snapshot.values))
            }
            existingSnapshot.createdTimestamp = snapshot.timestamp
            existingSnapshot.values.withLock { values in values = snapshot.values }
            logger.debug("Updated existing cached snapshot: \(persistentIdentifier)")
            backingData = existingSnapshot
        default:
            backingData = .init(
                registry: registry,
                persistentIdentifier: persistentIdentifier,
                values: snapshot.values
            )
            storage.withLock { storage in
                storage[persistentIdentifier] = backingData
                logger.debug("Cached new snapshot: \(persistentIdentifier)")
            }
            try self.initialize(for: persistentIdentifier, from: registry.id)
        }
        try Task.checkCancellation()
        graph.set(owner: persistentIdentifier, mapping: extractRelationshipReferences(from: snapshot))
        return backingData
    }
    
    /// Invalidates tracked identifiers that are no longer present in the active set.
    /// - Parameter persistentIdentifiers: The identifiers that should remain active.
    nonisolated internal func validation(persistentIdentifiers: Set<PersistentIdentifier>) throws {
        let trackedIdentifiers = Set(self.editingStates.withLock(\.keys))
        let staleIdentifiers = trackedIdentifiers.subtracting(persistentIdentifiers)
        let remainingIdentifiers = persistentIdentifiers.subtracting(trackedIdentifiers)
        if staleIdentifiers.isEmpty && remainingIdentifiers.isEmpty { return }
        logger.debug("Stale identifiers to invalidate: -\(staleIdentifiers.count) +\(remainingIdentifiers.count)")
        for staleIdentifier in staleIdentifiers {
            invalidate(persistentIdentifier: staleIdentifier)
        }
    }
    
    nonisolated internal func initialize(
        for persistentIdentifier: PersistentIdentifier,
        from editingStateID: EditingState.ID
    ) throws {
        guard persistentIdentifier.storeIdentifier != nil else {
            logger.error("PersistentIdentifier is not associated to a store: \(persistentIdentifier)")
            return
        }
        try editingStates.withLock { editingStates in
            try Task.checkCancellation()
            if var associatedEditingStates = editingStates[persistentIdentifier] {
                let (index, _) = associatedEditingStates.append(editingStateID)
                editingStates[persistentIdentifier] = consume associatedEditingStates
                logger.trace("Registered into an existing identifier: \(persistentIdentifier) \(editingStateID) \(index)")
            } else {
                editingStates[persistentIdentifier] = [editingStateID]
                logger.trace("Registered into a new identifier: \(persistentIdentifier) \(editingStateID)")
            }
        }
    }
    
    /// Removes the `PersistentIdentifier` from all registries and cached state.
    /// - Parameter persistentIdentifier: The model's identifier to clean up.
    nonisolated internal func invalidate(persistentIdentifier: PersistentIdentifier) {
        if var associatedEditingStates = self.editingStates.withLock({ $0[persistentIdentifier] }) {
            for editingState in associatedEditingStates {
                guard let registry = self.registries.withLock({ $0[editingState] }) else {
                    continue
                }
                registry.invalidate(for: persistentIdentifier)
                associatedEditingStates.remove(editingState)
                logger.debug("Invalidated associated registry from identifier: \(persistentIdentifier)")
            }
            _ = editingStates.withLock { $0.removeValue(forKey: persistentIdentifier) }
        }
        cleanup(persistentIdentifier: persistentIdentifier)
    }
    
    /// Removes all cached state associated to the `PersistentModel`.
    /// - Parameter persistentIdentifier: The model's identifier to clean up.
    nonisolated private func cleanup(persistentIdentifier: PersistentIdentifier) {
        graph.removeAll(for: persistentIdentifier)
        graph.removeIncomingEdges(to: persistentIdentifier)
        storage.withLock { storage in
            if let backingData = storage.removeValue(forKey: persistentIdentifier) {
                backingData.stopListening()
            }
        }
    }
}

extension ModelManager {
    nonisolated internal func backingData(for persistentIdentifier: PersistentIdentifier)
    -> DatabaseBackingData? {
        storage.withLock { $0[persistentIdentifier] }
    }
}

extension ModelManager {
    /// Returns the primary key from the model's backing data.
    ///
    /// - Parameter persistentIdentifier: The identifier assigned to the model's backing data.
    /// - Returns: The primary key.
    nonisolated internal func _primaryKey(for persistentIdentifier: PersistentIdentifier)
    -> (any LosslessStringConvertible & Sendable)? {
        storage.withLock { $0[persistentIdentifier]?.primaryKey }
    }
    
    /// Returns the typed primary key from the model's backing data.
    ///
    /// - Parameters:
    ///   - persistentIdentifier: The identifier assigned to the model's backing data.
    ///   - type: The original type to cast the primary key to, otherwise it will be converted.
    /// - Returns: The typed primary key.
    nonisolated internal func _primaryKey<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifier: PersistentIdentifier,
        as type: PrimaryKey.Type = String.self
    ) -> PrimaryKey? {
        switch _primaryKey(for: persistentIdentifier) {
        case let cachedPrimaryKey as PrimaryKey: cachedPrimaryKey
        case let cachedPrimaryKey?: PrimaryKey(cachedPrimaryKey.description)
        default: nil
        }
    }
    
    /// Returns the typed primary key from the model's backing data before resorting to decoding.
    ///
    /// - Parameters:
    ///   - persistentIdentifier: The identifier assigned to the model's backing data.
    ///   - type: The original type to cast the primary key to, otherwise it will be converted.
    /// - Returns: The typed primary key found in the backing data or derived from the `PersistentIdentifier`.
    nonisolated internal func primaryKey<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifier: PersistentIdentifier,
        as type: PrimaryKey.Type = String.self
    ) -> PrimaryKey {
        switch _primaryKey(for: persistentIdentifier, as: type) {
        case let cachedPrimaryKey?: cachedPrimaryKey
        case nil: persistentIdentifier.primaryKey(as: type)
        }
    }
    
    nonisolated internal func _primaryKeys(
        for persistentIdentifiers: [PersistentIdentifier]
    ) -> [PersistentIdentifier: any LosslessStringConvertible & Sendable] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        return storage.withLock { storage in
            var output = [PersistentIdentifier: any LosslessStringConvertible & Sendable](minimumCapacity: persistentIdentifiers.count)
            for identifier in persistentIdentifiers {
                if let backingData = storage[identifier] {
                    output[identifier] = backingData.primaryKey
                }
            }
            return output
        }
    }
    
    nonisolated internal func _primaryKeys<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifiers: [PersistentIdentifier],
        as type: PrimaryKey.Type = String.self
    ) -> [PersistentIdentifier: PrimaryKey] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        return storage.withLock { storage in
            var output = [PersistentIdentifier: PrimaryKey](minimumCapacity: persistentIdentifiers.count)
            for identifier in persistentIdentifiers {
                guard let backingData = storage[identifier] else { continue }
                if let typedPrimaryKey = backingData.primaryKey as? PrimaryKey {
                    output[identifier] = typedPrimaryKey
                } else {
                    output[identifier] = PrimaryKey(backingData.primaryKey.description)
                }
            }
            return output
        }
    }
    
    nonisolated internal func primaryKeys<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifiers: [PersistentIdentifier],
        as type: PrimaryKey.Type = String.self
    ) -> [PersistentIdentifier: PrimaryKey] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        var output = [PersistentIdentifier: PrimaryKey](minimumCapacity: persistentIdentifiers.count)
        var missing = [PersistentIdentifier]()
        missing.reserveCapacity(persistentIdentifiers.count)
        storage.withLock { storage in
            for identifier in persistentIdentifiers {
                if let backingData = storage[identifier] {
                    if let typedPrimaryKey = backingData.primaryKey as? PrimaryKey {
                        output[identifier] = typedPrimaryKey
                    } else {
                        output[identifier] = PrimaryKey(backingData.primaryKey.description)
                    }
                } else {
                    missing.append(identifier)
                }
            }
        }
        for identifier in missing {
            output[identifier] = identifier.primaryKey(as: type)
        }
        return output
    }
    
    nonisolated internal func primaryKeys<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifiers: [PersistentIdentifier],
        as type: PrimaryKey.Type = String.self
    ) -> [PrimaryKey] {
        guard !persistentIdentifiers.isEmpty else { return [] }
        var output = Array<PrimaryKey?>(repeating: nil, count: persistentIdentifiers.count)
        var missing = [(index: Int, identifier: PersistentIdentifier)]()
        missing.reserveCapacity(persistentIdentifiers.count)
        storage.withLock { storage in
            for (index, identifier) in persistentIdentifiers.enumerated() {
                if let backingData = storage[identifier] {
                    if let typedPrimaryKey = backingData.primaryKey as? PrimaryKey {
                        output[index] = typedPrimaryKey
                    } else {
                        output[index] = PrimaryKey(backingData.primaryKey.description)
                    }
                } else {
                    missing.append((index, identifier))
                }
            }
        }
        for (index, identifier) in missing {
            output[index] = identifier.primaryKey(as: type)
        }
        return output.map(\.unsafelyUnwrapped)
    }
}

extension ModelManager {
    nonisolated internal func currentGlobalGeneration() -> UInt64 {
        globalCacheRevision.load(ordering: .relaxed)
    }
    
    nonisolated internal func currentEntityGeneration(for entityName: String) -> UInt64 {
        entityCacheRevisions.withLock { $0[entityName] ?? 0 }
    }
    
    nonisolated internal func currentEntityGenerations(for entities: Set<String>) -> [String: UInt64] {
        guard !entities.isEmpty else { return [:] }
        return entityCacheRevisions.withLock { generations in
            var output = [String: UInt64](minimumCapacity: entities.count)
            for entity in entities {
                output[entity] = generations[entity] ?? 0
            }
            return output
        }
    }
    
    nonisolated internal func advanceCacheRevisions(for entities: Set<String>) {
        guard !entities.isEmpty else { return }
        _ = globalCacheRevision.wrappingAdd(1, ordering: .relaxed)
        entityCacheRevisions.withLock { generations in
            for entity in entities {
                generations[entity] = (generations[entity] ?? 0) &+ 1
            }
        }
    }
}

extension ModelManager {
    internal enum IndexingMode: Sendable {
        case replaceOnly
        case replaceAndClear
    }
    
    nonisolated internal func indexRelationshipReferences(
        for snapshot: Snapshot,
        mode: IndexingMode = .replaceAndClear
    ) {
        let owner = snapshot.persistentIdentifier
        let mapping = extractRelationshipReferences(from: snapshot)
        switch mode {
        case .replaceOnly: graph.set(owner: owner, mapping: mapping)
        case .replaceAndClear: graph.setAuthoritative(owner: owner, mapping: mapping)
        }
    }
    
    nonisolated internal func extractRelationshipReferences(from snapshot: Snapshot)
    -> [String: [PersistentIdentifier]] {
        var mapping = [String: [PersistentIdentifier]]()
        mapping.reserveCapacity(snapshot.properties.count)
        for property in snapshot.properties where property.metadata is Schema.Relationship {
            switch snapshot.values[property.index] {
            case let relatedIdentifier as PersistentIdentifier:
                mapping[property.name] = [relatedIdentifier]
            case let relatedIdentifiers as [PersistentIdentifier]:
                mapping[property.name] = relatedIdentifiers
            case is SQLNull:
                mapping[property.name] = []
            default:
                continue
            }
        }
        return mapping
    }
}

extension ModelManager {
    nonisolated internal func debugDetailedLogging(listAll: Bool = false) {
        let snapshot = self.editingStates.withLock { $0 }
        let total = snapshot.count
        var byEntity = [String: [(key: PersistentIdentifier, value: Int)]]()
        byEntity.reserveCapacity(snapshot.count)
        for (persistentIdentifier, states) in snapshot {
            byEntity[persistentIdentifier.entityName, default: []]
                .append((persistentIdentifier, states.count))
        }
        let entitySummaries = byEntity
            .sorted { $0.key < $1.key }
            .map { key, items in "\(key): \(items.count)" }
            .joined(separator: ", ")
        let _0 = "total: \(total)"
        let _1 = "entities: \(byEntity.count)"
        let _2 = "[\(entitySummaries)]"
        logger.info("PersistentIdentifiers — \(_0), \(_1) \(_2)")
        graph.debugDetailedLogging(listAll: true)
        graph.verifyIntegrity()
        guard listAll else { return }
        for (entity, items) in byEntity.sorted(by: { ($0.value.count, $0.key) > ($1.value.count, $1.key) }) {
            let line = items
                .sorted { ($0.value, "\($0.key)") > ($1.value, "\($1.key)") }
                .map { "\($0.key) [\($0.value)]" }
                .joined(separator: ", ")
            logger.info("[\(entity)] \(items.count) identifiers: \(line)")
        }
    }
}
