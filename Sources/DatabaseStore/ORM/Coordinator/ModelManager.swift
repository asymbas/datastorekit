//
//  ModelManager.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import SQLiteHandle
private import SQLSupport
internal import Collections
internal import Logging
internal import Synchronization
public import DataStoreCore
public import DataStoreSQL
public import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

// TODO: Rename `ModelManager`.

public final class ModelManager: DatabaseAttachment, DataStoreSnapshotProvider {
    public typealias Context = SnapshotRegistry
    public typealias Snapshot = DatabaseSnapshot
    nonisolated private let storage: Mutex<[PersistentIdentifier: DatabaseBackingData]> = .init([:])
    nonisolated private let entityCacheRevisions: Mutex<[String: UInt64]> = .init([:])
    nonisolated internal let globalCacheRevision: Atomic<UInt64> = .init(0)
    nonisolated internal let isCachingSnapshots: Bool
    nonisolated internal let state: Atomic<State> = .init(.idle)
    nonisolated internal let editingStates: Mutex<[PersistentIdentifier: OrderedSet<EditingState.ID>]> = .init([:])
    nonisolated internal let registries: Mutex<[EditingState.ID: Context]> = .init([:])
    nonisolated internal let configuration: DatabaseConfiguration
    nonisolated internal let schema: Schema
    nonisolated internal let broadcaster: EventBroadcaster = .init()
    nonisolated internal let inheritance: InheritanceResolver = .init()
    /// Tracks relationships between models by their `PersistentIdentifier`.
    nonisolated public let graph: ReferenceGraph = .init()
    
    nonisolated internal var store: DatabaseStore? {
        configuration.store
    }
    
    nonisolated public var persistentIdentifiers: [PersistentIdentifier] {
        .init(editingStates.withLock(\.keys))
    }
    
    nonisolated internal init(configuration: DatabaseConfiguration, schema: Schema) {
        self.configuration = configuration
        self.schema = schema
        self.isCachingSnapshots = !configuration.options.contains(.disableSnapshotCaching)
        inheritance.bootstrap(manager: self)
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
    
    internal enum Error: Swift.Error {
        case primaryKeyNotFound(PersistentIdentifier)
    }
}

// MARK: Fetching `SnapshotRegistry`

extension ModelManager {
    /// Inherited from `DatabaseAttachment.makeObjectContext(editingState:)`.
    nonisolated public func makeObjectContext(editingState: some EditingStateProviding) -> Context? {
        registry(for: editingState)
    }
    
    /// Retrieves a `SnapshotRegistry` instance that is associated with a `ModelContext`.
    nonisolated package func registry<T>(for editingState: T) -> Context?
    where T: EditingStateProviding {
        registry(for: editingState, isTransaction: false)
    }
    
    nonisolated internal func registry<T>(for editingState: T, isTransaction: Bool) -> Context?
    where T: EditingStateProviding {
        guard isCachingSnapshots else { return nil }
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
                if isTransaction { return registry }
                return nil
            }
            logger.debug("SnapshotRegistry is found for ModelManager: \(editingState.id)")
            return registry
        } ?? nil
    }
}

// MARK: Link `EditingState`

extension ModelManager {
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

// MARK: Inheritance Resolution

extension ModelManager {
    /// Inherited from `DataStoreSnapshotProvider.resolvedPersistentIdentifier(for:)`.
    nonisolated public func resolvedPersistentIdentifier(for persistentIdentifier: PersistentIdentifier) -> PersistentIdentifier? {
        inheritance.resolvedPersistentIdentifier(for: persistentIdentifier)
    }
    
    nonisolated internal func setResolvedEntityName(_ resolvedEntityName: String, for persistentIdentifier: PersistentIdentifier) {
        inheritance.set(persistentIdentifier: persistentIdentifier, resolvingTo: persistentIdentifier)
    }
}

// MARK: Primary Key Resolution

extension ModelManager {
    /// Inherited from `DataStoreSnapshotProvider.primaryKey(for:as:)`.
    ///
    /// Returns the typed primary key from the model's backing data before resorting to decoding.
    ///
    /// - Parameters:
    ///   - persistentIdentifier: The identifier assigned to the model's backing data.
    ///   - type: The original type to cast the primary key to, otherwise it will be converted.
    /// - Returns:
    ///   The typed primary key found in the backing data or derived from the `PersistentIdentifier`.
    nonisolated public func primaryKey<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifier: PersistentIdentifier,
        as type: PrimaryKey.Type = String.self
    ) -> PrimaryKey {
        switch _primaryKey(for: persistentIdentifier, as: type) {
        case let cachedPrimaryKey?: cachedPrimaryKey
        case nil: persistentIdentifier.primaryKey(as: type)
        }
    }
    
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
    
    nonisolated internal func _primaryKey(for persistentIdentifier: PersistentIdentifier)
    -> (any LosslessStringConvertible & Sendable)? {
        storage.withLock { $0[persistentIdentifier]?.primaryKey }
    }
    
    nonisolated internal func primaryKeys<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifiers: [PersistentIdentifier],
        as type: PrimaryKey.Type = String.self
    ) -> [PrimaryKey] {
        guard !persistentIdentifiers.isEmpty else { return [] }
        let count = persistentIdentifiers.count
        var output = Array<PrimaryKey?>(repeating: nil, count: count)
        var missingIdentifiers = [(index: Int, identifier: PersistentIdentifier)]()
        missingIdentifiers.reserveCapacity(count)
        storage.withLock { storage in
            for (index, identifier) in persistentIdentifiers.enumerated() {
                if let backingData = storage[identifier] {
                    if let typedPrimaryKey = backingData.primaryKey as? PrimaryKey {
                        output[index] = typedPrimaryKey
                    } else {
                        output[index] = PrimaryKey(backingData.primaryKey.description)
                    }
                } else {
                    missingIdentifiers.append((index, identifier))
                }
            }
        }
        for (index, identifier) in missingIdentifiers {
            output[index] = identifier.primaryKey(as: type)
        }
        return output.map(\.unsafelyUnwrapped)
    }
    
    nonisolated internal func primaryKeys<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifiers: [PersistentIdentifier],
        as type: PrimaryKey.Type = String.self
    ) -> [PersistentIdentifier: PrimaryKey] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        let count = persistentIdentifiers.count
        var output = [PersistentIdentifier: PrimaryKey](minimumCapacity: count)
        var missingIdentifiers = [PersistentIdentifier]()
        missingIdentifiers.reserveCapacity(count)
        storage.withLock { storage in
            for identifier in persistentIdentifiers {
                if let backingData = storage[identifier] {
                    if let typedPrimaryKey = backingData.primaryKey as? PrimaryKey {
                        output[identifier] = typedPrimaryKey
                    } else {
                        output[identifier] = PrimaryKey(backingData.primaryKey.description)
                    }
                } else {
                    missingIdentifiers.append(identifier)
                }
            }
        }
        for identifier in missingIdentifiers {
            output[identifier] = identifier.primaryKey(as: type)
        }
        return output
    }
    
    nonisolated internal func _primaryKeys<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifiers: [PersistentIdentifier],
        as type: PrimaryKey.Type = String.self
    ) -> [PersistentIdentifier: PrimaryKey] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        let count = persistentIdentifiers.count
        return storage.withLock { storage in
            var output = [PersistentIdentifier: PrimaryKey](minimumCapacity: count)
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
    
    nonisolated internal func _primaryKeys(for persistentIdentifiers: [PersistentIdentifier])
    -> [PersistentIdentifier: any LosslessStringConvertible & Sendable] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        let count = persistentIdentifiers.count
        return storage.withLock { storage in
            var output = [PersistentIdentifier: any LosslessStringConvertible & Sendable](minimumCapacity: count)
            for identifier in persistentIdentifiers {
                if let backingData = storage[identifier] {
                    output[identifier] = backingData.primaryKey
                }
            }
            return output
        }
    }
}

// MARK: Fetching `DatabaseBackingData`

extension ModelManager {
    nonisolated internal func backingData(for persistentIdentifier: PersistentIdentifier)
    -> DatabaseBackingData? {
        if let backingData = self.storage.withLock({ $0[persistentIdentifier] }) {
            return backingData
        }
        let primaryKey = primaryKey(for: persistentIdentifier)
        guard let resolvedPersistentIdentifier = resolvedPersistentIdentifier(for: persistentIdentifier),
              resolvedPersistentIdentifier.entityName != persistentIdentifier.entityName else {
            return nil
        }
        guard let storeIdentifier = persistentIdentifier.storeIdentifier else {
            preconditionFailure("PersistentIdentifier cannot have a backing data with a nil store identifier.")
        }
        do {
            let resolvedIdentifier = try PersistentIdentifier.identifier(
                for: storeIdentifier,
                entityName: resolvedPersistentIdentifier.entityName,
                primaryKey: primaryKey
            )
            return storage.withLock { $0[resolvedIdentifier] }
        } catch {
            preconditionFailure("Unable to resolve identifier for backing data: \(error)")
        }
    }
    
    nonisolated internal func backingDatas(of entityName: String) -> [DatabaseBackingData] {
        storage.withLock { storage in
            var result = [DatabaseBackingData]()
            for (identifier, backingData) in storage where identifier.entityName == entityName {
                result.append(backingData)
            }
            return result
        }
    }
}

// MARK: Fetching `DatabaseSnapshot`

extension ModelManager {
    /// Inherited from `DataStoreSnapshotProvider.snapshot(for:)`.
    nonisolated public func snapshot(for persistentIdentifier: PersistentIdentifier) -> Snapshot? {
        snapshot(for: persistentIdentifier, remappedIdentifiers: [:])
    }
    
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
        for persistentIdentifiers: [PersistentIdentifier],
        remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
    ) -> [PersistentIdentifier: Snapshot] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        let backingDatas = self.storage.withLock { storage in
            var result = [PersistentIdentifier: DatabaseBackingData](minimumCapacity: persistentIdentifiers.count)
            for persistentIdentifier in persistentIdentifiers {
                if let backingData = storage[persistentIdentifier] {
                    backingData.accessedTimestamp = .now()
                    result[persistentIdentifier] = backingData
                }
            }
            return result
        }
        var snapshots = [PersistentIdentifier: Snapshot](minimumCapacity: backingDatas.count)
        for (persistentIdentifier, backingData) in backingDatas {
            if let snapshot = try? Snapshot(backingData: backingData) {
                let persistentIdentifier = remappedIdentifiers[persistentIdentifier] ?? persistentIdentifier
                snapshots[persistentIdentifier] = remappedIdentifiers.isEmpty
                ? snapshot
                : snapshot.copy(persistentIdentifier: persistentIdentifier, remappedIdentifiers: remappedIdentifiers)
            }
        }
        return snapshots
    }
}

// MARK: Managing

extension ModelManager {
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
    
    nonisolated internal func upsert(snapshot: Snapshot, from registry: Context) throws -> DatabaseBackingData? {
        let persistentIdentifier = snapshot.persistentIdentifier
        guard !snapshot.isPartial else {
            logger.debug("Skipping cache for partial snapshot: \(persistentIdentifier)")
            return nil
        }
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
            // Walk up hierarchy.
            var inheritanceChain = Set<String>()
            if let entity = self.schema.entitiesByName[persistentIdentifier.entityName] {
                var current = entity.superentity
                while let superentity = current {
                    inheritanceChain.insert(superentity.name)
                    current = superentity.superentity
                }
            }
            backingData = .init(
                registry: registry,
                inheritanceChain: inheritanceChain,
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
    
    /// Removes all cached state associated to the `PersistentModel`.
    /// - Parameter persistentIdentifier: The model's identifier to clean up.
    nonisolated private func cleanup(persistentIdentifier: PersistentIdentifier) {
        graph.removeAll(for: persistentIdentifier)
        graph.removeIncomingEdges(to: persistentIdentifier)
        inheritance.remove(persistentIdentifier: persistentIdentifier)
        storage.withLock { storage in
            if let backingData = storage.removeValue(forKey: persistentIdentifier) {
                backingData.stopListening()
            }
        }
    }
    
    @available(*, deprecated, message: "")
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
}

// MARK: Cache

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
            for entity in entities { output[entity] = generations[entity] ?? 0 }
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

// MARK: ReferenceGraph

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

// MARK: Debug

extension ModelManager {
    nonisolated internal func debugDetailedLogging(level: Logger.Level = .info, listAll: Bool = false) {
        let snapshot = self.editingStates.withLock(\.self)
        let total = snapshot.count
        var byEntity = [String: [(key: PersistentIdentifier, value: Int)]]()
        byEntity.reserveCapacity(snapshot.count)
        for (persistentIdentifier, states) in snapshot {
            byEntity[persistentIdentifier.entityName, default: []].append((persistentIdentifier, states.count))
        }
        let entitySummaries = byEntity
            .sorted { $0.key < $1.key }
            .map { key, items in "\(key): \(items.count)" }
            .joined(separator: ", ")
        let _0 = "total: \(total)"; let _1 = "entities: \(byEntity.count)"; let _2 = "[\(entitySummaries)]"
        logger.log(level: level, "PersistentIdentifiers — \(_0), \(_1) \(_2)")
        graph.debugDetailedLogging(level: level, listAll: true)
        graph.verifyIntegrity()
        guard listAll else { return }
        for (entity, items) in byEntity.sorted(by: { ($0.value.count, $0.key) > ($1.value.count, $1.key) }) {
            let line = items
                .sorted { ($0.value, "\($0.key)") > ($1.value, "\($1.key)") }
                .map { "\($0.key) [\($0.value)]" }
                .joined(separator: ", ")
            logger.log(level: level, "[\(entity)] \(items.count) identifiers: \(line)")
        }
    }
}
