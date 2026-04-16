//
//  SnapshotRegistry.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreSupport
private import Logging
internal import DataStoreCore
internal import Dispatch
internal import Synchronization
public import DataStoreSQL
public import Foundation
public import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

internal struct DataStoreFetchResultMap: Identifiable, Sendable {
    /// Inherited from `Identifiable.id`.
    nonisolated internal let id: Int
    nonisolated internal let timestamp: DispatchTime = .now()
    nonisolated internal var lastAccessed: DispatchTime = .now()
    nonisolated internal var hitCount: UInt32 = 0
    nonisolated internal var globalCacheRevision: UInt64 = 0
    nonisolated internal var entityCacheRevisions: [String: UInt64] = [:]
    nonisolated internal var fetchedIdentifiers: [PersistentIdentifier]
    nonisolated internal var relatedIdentifiers: [PersistentIdentifier]
}

/// A cached object with a lifecycle bound to a `ModelContext` via a linked `EditingState`.
public final class SnapshotRegistry: DatabaseContext, DataStoreSnapshotProvider {
    public typealias Snapshot = DatabaseSnapshot
    nonisolated private let manager: ModelManager
    /// Inherited from `Identifiable.id`.
    nonisolated public let id: EditingState.ID
    nonisolated private let key: Int
    nonisolated private let storage: Mutex<[PersistentIdentifier: DatabaseBackingData]> = .init([:])
    nonisolated private let entityIndex: Mutex<[String: Set<PersistentIdentifier>]> = .init([:])
    nonisolated private let trackedIdentifiers: Mutex<Set<PersistentIdentifier>> = .init([])
    nonisolated private let pendingIdentifiers: Mutex<Set<PersistentIdentifier>> = .init([])
    nonisolated private let invalidatingIdentifiers: Mutex<Set<PersistentIdentifier>> = .init([])
    nonisolated private let preloadedFetches: Mutex<[PreloadFetchKey: any Sendable]> = .init([:])
    nonisolated private let cachedFetchResultMapping: Mutex<[Int: DataStoreFetchResultMap]> = .init([:])
    nonisolated private let cachedFetchResultKeyOrder: Mutex<[Int]> = .init([])
    @DatabaseActor private var cachedFetchResultTotalCost: UInt64 = 0
    @DatabaseActor private var cacheTasksByKey: [Int: Task<Void, any Swift.Error>] = [:]
    @DatabaseActor private var evictionScheduled: Bool = false
    @DatabaseActor private var task: Task<Void, any Swift.Error>?
    nonisolated private let shouldDebugOperations: Bool
    nonisolated private let independentlyManaged: Bool
    nonisolated private let request: Atomic<Int> = .init(0)
    nonisolated internal let state: Atomic<State> = .init(.idle)
    
    nonisolated private var schema: Schema {
        manager.schema
    }
    
    /// Tracks relationships between models by their `PersistentIdentifier`.
    nonisolated public var graph: ReferenceGraph {
        manager.graph
    }
    
    /// All `PersistentIdentifier` bound to this registry.
    nonisolated internal var persistentIdentifiers: [PersistentIdentifier] {
        .init(trackedIdentifiers.withLock(\.self))
    }
    
    nonisolated internal init(manager: ModelManager, id: EditingState.ID) {
        self.manager = manager
        self.id = id
        self.key = Int.random(in: Int.min..<Int.max)
        self.independentlyManaged = !manager.configuration.options.contains(.centralizedSnapshotCaching)
        self.shouldDebugOperations = DataStoreDebugging.mode == .trace
    }
    
    deinit {
        logger.debug("SnapshotRegistry deinit: \(id) \(key)")
    }
    
    internal enum State: UInt8, AtomicRepresentable {
        case idle = 0
        case running
    }
    
    internal enum Error: Swift.Error {
        /// Unable to use the existing result map due to a missing snapshot.
        case fetchResultMapInconsistency
        /// `ModelManager` and `SnapshotRegistry` are no longer in lockstep.
        case storageInconsistency
    }
}

// MARK: Inheritance Resolution

extension SnapshotRegistry {
    /// Inherited from `DataStoreSnapshotProvider.resolvedPersistentIdentifier(for:)`.
    nonisolated public func resolvedPersistentIdentifier(for persistentIdentifier: PersistentIdentifier) -> PersistentIdentifier? {
        manager.resolvedPersistentIdentifier(for: persistentIdentifier)
    }
    
    nonisolated internal func setResolvedEntityName(_ resolvedEntityName: String, for persistentIdentifier: PersistentIdentifier) {
        manager.setResolvedEntityName(resolvedEntityName, for: persistentIdentifier)
    }
}

// MARK: Primary Key Resolution

extension SnapshotRegistry {
    /// Inherited from `DataStoreSnapshotProvider.primaryKey(for:as:)`.
    public func primaryKey<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifier: PersistentIdentifier,
        as type: PrimaryKey.Type = String.self
    ) -> PrimaryKey {
        manager.primaryKey(for: persistentIdentifier, as: type)
    }
}

// MARK: Fetching `DatabaseBackingData`

extension SnapshotRegistry {
    nonisolated private func backingData(for persistentIdentifier: PersistentIdentifier) -> DatabaseBackingData? {
        if independentlyManaged {
            if let backingData = self.storage.withLock({ $0[persistentIdentifier] }) {
                return backingData
            }
            let primaryKey = primaryKey(for: persistentIdentifier)
            guard let resolvedPersistentIdentifier = self.manager.resolvedPersistentIdentifier(for: persistentIdentifier),
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
        } else {
            return manager.backingData(for: persistentIdentifier)
        }
    }
}

// MARK: Fetching `DatabaseSnapshot`

extension SnapshotRegistry {
    /// Inherited from `DataStoreSnapshotProvider.snapshot(for:)`.
    public func snapshot(for persistentIdentifier: PersistentIdentifier) -> Snapshot? {
        if self.state.load(ordering: .relaxed) == .running {
            logger.debug("Current SnapshotRegistry is busy.", metadata: [
                "editing_state": "\(id)",
                "persistent_identifier": "\(persistentIdentifier)"
            ])
            return nil
        }
        switch independentlyManaged {
        case true:
            guard let backingData = self.storage.withLock({ $0[persistentIdentifier] }) else {
                if trackedIdentifiers.withLock({ $0.contains(persistentIdentifier) }) {
                    logger.error("Tracked identifier missing backing data: \(id) \(persistentIdentifier)")
                } else {
                    logger.trace("Snapshot not found in SnapshotRegistry: \(id) \(persistentIdentifier) ")
                }
                return nil
            }
            backingData.accessedTimestamp = .now()
            guard let snapshot = try? Snapshot(backingData: backingData) else {
                fatalError("Snapshot could not be instantiated from the backing data: \(backingData)")
            }
            return snapshot
        case false:
            return self.manager.snapshot(for: persistentIdentifier)
        }
    }
    
    package func snapshots(
        for persistentIdentifiers: [PersistentIdentifier],
        remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
    ) -> [PersistentIdentifier: Snapshot] {
        guard !persistentIdentifiers.isEmpty else { return [:] }
        let state = self.state.load(ordering: .relaxed)
        if state == .running { return [:] }
        if independentlyManaged {
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
        } else {
            return manager.snapshots(for: persistentIdentifiers, remappedIdentifiers: remappedIdentifiers)
        }
    }
}

// MARK: Managing

extension SnapshotRegistry {
    nonisolated package func synchronize(
        snapshots: [PersistentIdentifier: Snapshot],
        invalidateIdentifiers: Set<PersistentIdentifier>
    ) {
        state.store(.running, ordering: .sequentiallyConsistent)
        if shouldDebugOperations {
            // Ensure every `PersistentIdentifier` has a store identifier.
            for (persistentIdentifier, snapshot) in snapshots {
                ensureRemappedIdentifiers(for: persistentIdentifier, snapshot: snapshot, includeToManyRelationships: true)
            }
        }
        pendingIdentifiers.withLock { $0.formUnion(Set(snapshots.keys)) }
        Task { @concurrent in
            await DatabaseActor.run {
                task?.cancel()
                self.task = Task { @DatabaseActor in
                    defer {
                        pendingIdentifiers.withLock { $0.removeAll() }
                        manager.debugDetailedLogging(listAll: false)
                        state.store(.idle, ordering: .sequentiallyConsistent)
                        logger.debug("Exiting SnapshotRegistry synchronization: \(id)")
                        scheduleEvictionIfNeeded()
                    }
                    do {
                        logger.debug("Starting SnapshotRegistry synchronization: \(id)")
                        let touchedEntities: Set<String> = {
                            var entities = Set<String>()
                            entities.reserveCapacity(snapshots.count &+ invalidateIdentifiers.count)
                            for (_, snapshot) in snapshots { entities.insert(snapshot.entityName) }
                            for identifier in invalidateIdentifiers { entities.insert(identifier.entityName) }
                            return entities
                        }()
                        manager.advanceCacheRevisions(for: touchedEntities)
                        try await withThrowingDiscardingTaskGroup { group in
                            let requests = self.request.add(1, ordering: .sequentiallyConsistent)
                            logger.debug("Updating SnapshotRegistry (request count: \(requests)")
                            defer { request.subtract(1, ordering: .sequentiallyConsistent) }
                            _ = group.addTaskUnlessCancelled { [weak self] in
                                guard let self else { return }
                                for (_, snapshot) in snapshots where !snapshot.isTemporary && !snapshot.isPartial {
                                    _ = try register(snapshot: snapshot)
                                }
                            }
                            _ = group.addTaskUnlessCancelled { [weak self] in
                                guard let self else { return }
                                for persistentIdentifier in invalidateIdentifiers {
                                    manager.invalidate(persistentIdentifier: persistentIdentifier)
                                }
                            }
                        }
                    } catch {
                        logger.error("An error occurred while synchronizing: \(error)")
                    }
                }
            }
        }
    }
    
    nonisolated internal func register(snapshot: Snapshot) throws -> DatabaseBackingData? {
        let persistentIdentifier = snapshot.persistentIdentifier
        let backingData = try manager.upsert(snapshot: snapshot, from: self)
        _ = trackedIdentifiers.withLock { $0.insert(persistentIdentifier) }
        try manager.initialize(for: persistentIdentifier, from: self.id)
        if independentlyManaged, let backingData {
            storage.withLock { $0[snapshot.persistentIdentifier] = backingData }
            _ = entityIndex.withLock { $0[snapshot.entityName, default: []].insert(snapshot.persistentIdentifier) }
        }
        return backingData
    }
    
    /// Removes a snapshot from storage and invalidates any cached fetch results that reference it.
    /// This method updates tracked state immediately and schedules cache cleanup asynchronously.
    ///
    /// - Important:
    ///   This method should only be called from the `ModelManager`.
    /// - Parameter persistentIdentifier: The identifier of the snapshot to invalidate.
    nonisolated internal func invalidate(for persistentIdentifier: PersistentIdentifier) {
        guard !invalidatingIdentifiers.withLock({ $0.contains(persistentIdentifier) }) else {
            logger.notice("Snapshot is already being invalidated: \(id) \(persistentIdentifier)")
            return
        }
        trackedIdentifiers.withLock { trackedIdentifiers in
            let removedIdentifier = trackedIdentifiers.remove(persistentIdentifier)
            let count = trackedIdentifiers.count
            check: if independentlyManaged {
                let removedBackingData = self.storage.withLock { $0.removeValue(forKey: persistentIdentifier) }
                _ = entityIndex.withLock { $0[persistentIdentifier.entityName]?.remove(persistentIdentifier) }
                if removedIdentifier != nil && removedBackingData == nil {
                    logger.error("Tracked removal without backing data: \(id) \(persistentIdentifier)")
                }
                guard let backingData = removedBackingData else {
                    break check
                }
                _ = invalidatingIdentifiers.withLock { $0.insert(persistentIdentifier) }
                Task { @DatabaseActor in
                    let keysSnapshot = backingData.cachedFetchResults
                    for key in keysSnapshot {
                        if let result = removeCachedFetchResultClearingBacklinks(forKey: key) {
                            #if DEBUG
                            logger.trace("Removed cached fetch result: \(result)")
                            #endif
                        } else {
                            continue
                        }
                    }
                    _ = invalidatingIdentifiers.withLock { $0.remove(persistentIdentifier) }
                }
            }
            logger.debug("PersistentIdentifier invalidated (total: \(count))", metadata: [
                "persistent_identifier": "\(persistentIdentifier)",
                "editing_state_id": "\(id)"
            ])
        }
        if !independentlyManaged {
            let cachedFetchResults = self.cachedFetchResultMapping.withLock(\.self)
            _ = invalidatingIdentifiers.withLock { $0.insert(persistentIdentifier) }
            Task(priority: .utility) { @DatabaseActor in
                defer { _ = invalidatingIdentifiers.withLock { $0.remove(persistentIdentifier) } }
                try await withThrowingDiscardingTaskGroup { group in
                    for (key, result) in cachedFetchResults {
                        _ = group.addTaskUnlessCancelled { [weak self] in
                            try Task.checkCancellation()
                            async let fetchedFlag = result
                                .fetchedIdentifiers
                                .contains(persistentIdentifier)
                            async let relatedFlag = result
                                .relatedIdentifiers
                                .contains(persistentIdentifier)
                            let shouldInvalidateCache = await (fetchedFlag, relatedFlag)
                            if shouldInvalidateCache.0 || shouldInvalidateCache.1 {
                                await DatabaseActor.run { [weak self] in
                                    _ = self?.removeCachedFetchResultClearingBacklinks(forKey: key)
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// MARK: In-Memory Querying

extension SnapshotRegistry {
    package func step(from entityName: String, predicate: @escaping (DatabaseBackingData) -> Bool) -> [Snapshot] {
        let candidates: [DatabaseBackingData]
        if independentlyManaged {
            let identifiers = self.entityIndex.withLock { $0[entityName] ?? [] }
            candidates = self.storage.withLock { storage in
                var result = [DatabaseBackingData]()
                result.reserveCapacity(identifiers.count)
                for identifier in identifiers {
                    if let backingData = storage[identifier] {
                        result.append(backingData)
                    }
                }
                return result
            }
        } else {
            candidates = self.manager.backingDatas(of: entityName)
        }
        var result = [Snapshot]()
        result.reserveCapacity(candidates.count)
        for backingData in candidates where predicate(backingData) {
            if let snapshot = try? Snapshot(backingData: backingData) {
                result.append(snapshot)
            }
        }
        return result
    }
}

// MARK: `PreloadFetchRequest` Support

extension SnapshotRegistry {
    nonisolated internal func preload<Result: FetchResult>(
        for editingState: some EditingStateProviding,
        as resultType: Result.Type = Result.self
    ) -> PreloadFetchResult<Result.ModelType, Result.SnapshotType>? {
        let key = PreloadFetchKey(
            editingStateID: editingState.id,
            modifier: editingState.author?.hasPrefix("\(key)") == true ? editingState.author : nil,
            key: nil
        )
        return preloadedFetches.withLock { $0[key].take() }
        as? PreloadFetchResult<Result.ModelType, Result.SnapshotType>
    }
    
    @concurrent internal func preload<T, Snapshot>(
        _ result: PreloadFetchResult<T, Snapshot>,
        for request: PreloadFetchRequest<T>
    ) async -> PreloadFetchKey {
        let key = PreloadFetchKey(
            editingStateID: request.editingState.id,
            modifier: request.modifier == nil ? nil : "\(key)-\(request.modifier!)",
            key: result.key
        )
        preloadedFetches.withLock { $0[key] = result }
        return key
    }
}

// MARK: Cached Predicate Results

extension SnapshotRegistry {
    // TODO: Use the batched variant when rebuilding a fetch result.
    
    nonisolated private func rebuildFetchResult(
        forKey key: Int,
        on entityName: String,
        result: DataStoreFetchResultMap
    ) throws -> (
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    )? {
        let initialFetchedCount = result.fetchedIdentifiers.count
        let fetchedSnapshots = Mutex<[Snapshot]>([])
        fetchedSnapshots.withLock { $0.reserveCapacity(initialFetchedCount) }
        let initialRelatedCount = result.relatedIdentifiers.count
        let relatedSnapshots = Mutex<[PersistentIdentifier: Snapshot]>([:])
        relatedSnapshots.withLock { $0.reserveCapacity(initialRelatedCount) }
        let group = DispatchGroup()
        let error = Mutex<Error?>(nil)
        group.enter()
        DispatchQueue.global(qos: .userInitiated).async {
            var local = fetchedSnapshots.withLock(\.self)
            for persistentIdentifier in result.fetchedIdentifiers {
                let resultEntityName = persistentIdentifier.entityName
                if entityName != resultEntityName {
                    logger.notice("Detected a mismatch in entity name: \(entityName) != \(resultEntityName)")
                }
                guard let snapshot = self.snapshot(for: persistentIdentifier) else {
                    error.withLock { $0 = Error.fetchResultMapInconsistency }
                    break
                }
                local.append(snapshot)
            }
            fetchedSnapshots.withLock { $0 = local }
            group.leave()
        }
        group.enter()
        DispatchQueue.global(qos: .userInitiated).async {
            var local = relatedSnapshots.withLock(\.self)
            for persistentIdentifier in result.relatedIdentifiers {
                guard let snapshot = self.snapshot(for: persistentIdentifier) else {
                    break
                }
                local[snapshot.persistentIdentifier] = snapshot
            }
            relatedSnapshots.withLock { $0 = local }
            group.leave()
        }
        group.wait()
        if let error = error.withLock(\.self) {
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: key) }
            throw error
        }
        let rebuiltFetchedSnapshots = fetchedSnapshots.withLock(\.self)
        let rebuiltRelatedSnapshots = relatedSnapshots.withLock(\.self)
        let count = (rebuiltFetchedSnapshots.count, rebuiltRelatedSnapshots.count)
        if (initialFetchedCount != count.0) || (initialRelatedCount != count.1) {
            logger.warning("Invalidating cache due to inconsistent counts.")
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: key) }
            return nil
        }
        logger.debug("Rebuilt cached result from references: \(count.0), \(count.1)")
        return (rebuiltFetchedSnapshots, rebuiltRelatedSnapshots)
    }
    
    nonisolated package func cachedFetchResult(
        forKey key: Int,
        on entityName: String
    ) throws -> (
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    )? {
        guard manager.isCachingSnapshots else { return nil }
        let requests = self.request.wrappingAdd(1, ordering: .relaxed)
        defer { request.wrappingSubtract(1, ordering: .relaxed) }
        logger.trace("Number of requests for retrieving caches: \(requests) < 10")
        guard requests.newValue < 10 else {
            return nil
        }
        guard var entry = self.cachedFetchResultMapping.withLock({ $0[key] }) else {
            logger.trace("Not found in cached fetch results: \(id)")
            return nil
        }
        let policy = self.manager.configuration.cachePolicy.predicateResults
        let now = DispatchTime.now()
        if isExpired(entry, policy: policy, now: now) || !isValid(entry, policy: policy) {
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: key) }
            return nil
        }
        let rebuilt = try rebuildFetchResult(forKey: key, on: entityName, result: entry)
        guard rebuilt != nil else { return nil }
        entry.lastAccessed = now
        entry.hitCount &+= 1
        cachedFetchResultMapping.withLock { $0[key] = entry }
        touchFetchResultKey(key)
        scheduleEvictionIfNeeded()
        return rebuilt
    }
    
    nonisolated package func cachedFetchIdentifiersResult(forKey key: Int) -> (
        fetchedSnapshots: [PersistentIdentifier],
        relatedSnapshots: [PersistentIdentifier]
    )? {
        guard manager.isCachingSnapshots else { return nil }
        let requests = self.request.wrappingAdd(1, ordering: .relaxed)
        defer { request.wrappingSubtract(1, ordering: .relaxed) }
        logger.trace("Number of requests for retrieving caches: \(requests) < 10")
        guard requests.newValue < 10 else {
            return nil
        }
        guard var entry = self.cachedFetchResultMapping.withLock({ $0[key] }) else {
            logger.trace("Not found in cached fetch results: \(id)")
            return nil
        }
        let policy = self.manager.configuration.cachePolicy.predicateResults
        let now = DispatchTime.now()
        if isExpired(entry, policy: policy, now: now) || !isValid(entry, policy: policy) {
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: key) }
            return nil
        }
        entry.lastAccessed = now
        entry.hitCount &+= 1
        cachedFetchResultMapping.withLock { $0[key] = entry }
        touchFetchResultKey(key)
        scheduleEvictionIfNeeded()
        return (entry.fetchedIdentifiers, entry.relatedIdentifiers)
    }
    
    nonisolated package func cachedResult(forKey key: Int) -> (any Sendable)? {
        let requests = self.request.wrappingAdd(1, ordering: .relaxed)
        defer { request.wrappingSubtract(1, ordering: .relaxed) }
        logger.trace("Number of requests for retrieving caches: \(requests) < 10")
        guard requests.newValue < 10 else {
            return nil
        }
        guard var entry = self.cachedFetchResultMapping.withLock({ $0[key] }) else {
            logger.trace("Not found in cached fetch results: \(id)")
            return nil
        }
        let policy = self.manager.configuration.cachePolicy.predicateResults
        let now = DispatchTime.now()
        if isExpired(entry, policy: policy, now: now) || !isValid(entry, policy: policy) {
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: key) }
            return nil
        }
        entry.lastAccessed = now
        entry.hitCount &+= 1
        cachedFetchResultMapping.withLock { $0[key] = entry }
        touchFetchResultKey(key)
        scheduleEvictionIfNeeded()
        return entry
    }
    
    nonisolated package func scheduleCacheFetchResult(
        forKey key: Int,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    ) {
        guard manager.isCachingSnapshots else { return }
        if fetchedSnapshots.isEmpty { return }
        Task(priority: .utility) { @DatabaseActor [weak self] in
            guard let self else { return }
            if self.cachedFetchResultMapping.withLock({ $0[key] != nil }) {
                self.touchFetchResultKey(key)
                self.scheduleEvictionIfNeeded()
                return
            }
            if self.cacheTasksByKey[key] != nil { return }
            let task = Task { @DatabaseActor [weak self] in
                defer { self?.cacheTasksByKey[key] = nil }
                try Task.checkCancellation()
                try await self?.cacheFetchResult(
                    forKey: key,
                    fetchedSnapshots: fetchedSnapshots,
                    relatedSnapshots: relatedSnapshots
                )
            }
            self.cacheTasksByKey[key] = task
        }
    }
    
    nonisolated package func cacheFetchResult(
        forKey key: Int,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    ) async throws {
        guard manager.isCachingSnapshots else { return }
        if fetchedSnapshots.isEmpty { return }
        if cachedFetchResultMapping.withLock({ $0[key] != nil }) {
            touchFetchResultKey(key); scheduleEvictionIfNeeded()
            return
        }
        let requests = self.request.add(1, ordering: .sequentiallyConsistent)
        logger.trace("Caching fetch result: \(requests) \(self.id) (\(key))")
        defer { request.subtract(1, ordering: .sequentiallyConsistent) }
        let fetchedIdentifiers = try await registerFetchResult(fetchedSnapshots, forKey: key)
        let relatedIdentifiers = try await registerFetchResult(Array(relatedSnapshots.values), forKey: key)
        try Task.checkCancellation()
        let policy = self.manager.configuration.cachePolicy.predicateResults
        var dependencyEntities = Set<String>()
        if policy.validation == .entityGeneration {
            dependencyEntities.reserveCapacity(fetchedSnapshots.count &+ relatedSnapshots.count &+ 8)
            for snapshot in fetchedSnapshots {
                dependencyEntities.insert(snapshot.entityName)
                for property in snapshot.properties where property.metadata is Schema.Relationship {
                    switch snapshot.values[property.index] {
                    case let relatedIdentifier as PersistentIdentifier:
                        dependencyEntities.insert(relatedIdentifier.entityName)
                    case let relatedIdentifiers as [PersistentIdentifier]:
                        if let relatedIdentifier = relatedIdentifiers.first {
                            dependencyEntities.insert(relatedIdentifier.entityName)
                        }
                    default:
                        break
                    }
                }
            }
            for snapshot in relatedSnapshots.values {
                dependencyEntities.insert(snapshot.entityName)
            }
        }
        let globalGeneration = self.manager.currentGlobalGeneration()
        let entityGenerations = (policy.validation == .entityGeneration)
        ? manager.currentEntityGenerations(for: dependencyEntities)
        : [:]
        let entry = DataStoreFetchResultMap(
            id: key,
            lastAccessed: .now(),
            hitCount: 0,
            globalCacheRevision: globalGeneration,
            entityCacheRevisions: entityGenerations,
            fetchedIdentifiers: fetchedIdentifiers,
            relatedIdentifiers: relatedIdentifiers
        )
        cachedFetchResultMapping.withLock { $0[key] = entry }
        touchFetchResultKey(key)
        scheduleEvictionIfNeeded()
    }
    
    nonisolated(nonsending) private func registerFetchResult(
        _ snapshots: [Snapshot],
        forKey key: Int
    ) async throws -> [PersistentIdentifier] {
        try await withThrowingTaskGroup(of: (Int, PersistentIdentifier)?.self) { group in
            var batch = [PersistentIdentifier?](repeating: nil, count: snapshots.count)
            batch.reserveCapacity(snapshots.count)
            for (index, snapshot) in snapshots.enumerated() {
                _ = group.addTaskUnlessCancelled { [weak self] in
                    guard let self, let backingData = try register(snapshot: snapshot)
                            ?? self.backingData(for: snapshot.persistentIdentifier) else {
                        logger.debug("Collecting identifiers returned nil when registering.", metadata: [
                            "snapshot": "\(snapshot)"
                        ])
                        return nil
                    }
                    precondition(
                        snapshot.entityName == backingData.tableName ||
                        backingData.inheritanceChain.contains(snapshot.entityName)
                    )
                    _ = await DatabaseActor.run { backingData.cachedFetchResults.insert(key) }
                    return (index, snapshot.persistentIdentifier)
                }
            }
            try Task.checkCancellation()
            batch = try await group.reduce(into: batch) { partialResult, tuple in
                if let (index, persistentIdentifier) = tuple {
                    partialResult[index] = persistentIdentifier
                }
            }
            return batch.compactMap(\.self)
        }
    }
    
    private func isExpired(_ entry: DataStoreFetchResultMap, policy: CacheLayerPolicy, now: DispatchTime) -> Bool {
        switch policy.expiry {
        case let .expireAfterWrite(seconds):
            let ttl = seconds &* 1_000_000_000
            let age = now.uptimeNanoseconds &- entry.timestamp.uptimeNanoseconds
            return age > ttl
        case let .expireAfterAccess(seconds):
            let ttl = seconds &* 1_000_000_000
            let age = now.uptimeNanoseconds &- entry.lastAccessed.uptimeNanoseconds
            return age > ttl
        case .none:
            return false
        }
    }
    
    private func isValid(_ entry: DataStoreFetchResultMap, policy: CacheLayerPolicy) -> Bool {
        switch policy.validation {
        case .globalGeneration:
            return entry.globalCacheRevision == manager.currentGlobalGeneration()
        case .entityGeneration:
            if entry.entityCacheRevisions.isEmpty {
                return entry.globalCacheRevision == manager.currentGlobalGeneration()
            }
            for (entity, generation) in entry.entityCacheRevisions {
                if manager.currentEntityGeneration(for: entity) != generation {
                    return false
                }
            }
            return true
        case .none:
            return true
        }
    }
    
    private func touchFetchResultKey(_ key: Int) {
        let policy = self.manager.configuration.cachePolicy.predicateResults
        cachedFetchResultKeyOrder.withLock { order in
            switch policy.eviction {
            case .leastRecentlyUsed:
                order.removeAll(where: { $0 == key })
                order.append(key)
            case .firstInFirstOut:
                if !order.contains(key) { order.append(key) }
            case .leastFrequentlyUsed:
                if !order.contains(key) { order.append(key) }
            }
        }
    }
    
    @DatabaseActor private func evictPredicateResultsIfNeeded() {
        let policy = self.manager.configuration.cachePolicy.predicateResults
        func overEntryLimit() -> Bool {
            switch policy.limit {
            case let .bounded(maxCount): cachedFetchResultMapping.withLock(\.count) > maxCount
            case .unbounded: false
            }
        }
        func overCostLimit() -> Bool {
            switch policy.costLimit {
            case let .bounded(maxTotal): cachedFetchResultTotalCost > maxTotal
            case .unbounded: false
            }
        }
        guard overEntryLimit() || overCostLimit() else { return }
        while overEntryLimit() || overCostLimit() {
            guard let key = evictOnePredicateResultIfNeeded() else { return }
            _ = removeCachedFetchResultClearingBacklinks(forKey: key)
        }
    }
    
    @DatabaseActor private func evictOnePredicateResultIfNeeded() -> Int? {
        let policy = self.manager.configuration.cachePolicy.predicateResults
        switch policy.eviction {
        case .leastRecentlyUsed, .firstInFirstOut:
            return cachedFetchResultKeyOrder.withLock { order in
                order.isEmpty ? nil : order.removeFirst()
            }
        case .leastFrequentlyUsed:
            let order = self.cachedFetchResultKeyOrder.withLock(\.self)
            let mapping = self.cachedFetchResultMapping.withLock(\.self)
            guard !order.isEmpty, !mapping.isEmpty else { return nil }
            var bestKey: Int?
            var bestHits: UInt32 = .max
            for key in order {
                guard let entry = mapping[key] else { continue }
                if entry.hitCount < bestHits {
                    bestHits = entry.hitCount
                    bestKey = key
                    if bestHits == 0 { break }
                }
            }
            if let bestKey {
                cachedFetchResultKeyOrder.withLock { order in
                    order.removeAll(where: { $0 == bestKey })
                }
            }
            return bestKey
        }
    }
    
    @discardableResult @DatabaseActor
    private func removeCachedFetchResultClearingBacklinks(forKey key: Int) -> DataStoreFetchResultMap? {
        if let task = self.cacheTasksByKey[key] {
            task.cancel(); self.cacheTasksByKey[key] = nil
        }
        let removedResult = self.cachedFetchResultMapping.withLock { $0.removeValue(forKey: key) }
        cachedFetchResultKeyOrder.withLock { order in order.removeAll(where: { $0 == key }) }
        guard let removedResult else { return nil }
        for identifier in Set(removedResult.fetchedIdentifiers).union(removedResult.relatedIdentifiers) {
            self.backingData(for: identifier)?.cachedFetchResults.remove(key)
        }
        return removedResult
    }
    
    package func scheduleEvictionIfNeeded() {
        guard manager.isCachingSnapshots else { return }
        Task(priority: .utility) { @DatabaseActor in
            guard !evictionScheduled else { return }
            self.evictionScheduled = true
            defer { self.evictionScheduled = false }
            evictPredicateResultsIfNeeded()
        }
    }
}
