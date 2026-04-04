//
//  SnapshotRegistry.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreSQL
import DataStoreSupport
import Dispatch
import Foundation
import Logging
import SwiftData
import Synchronization

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
public final class SnapshotRegistry: ObjectContextProtocol {
    public typealias Snapshot = DatabaseSnapshot
    nonisolated private /*unowned*/ let manager: ModelManager
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
    nonisolated internal let state: Atomic<State> = .init(.idle)
    nonisolated private let request: Atomic<Int> = .init(0)
    
    nonisolated internal var schema: Schema {
        manager.store?.schema ?? .init()
    }
    
    /// All `PersistentIdentifier` bound to this registry.
    nonisolated internal var persistentIdentifiers: [PersistentIdentifier] {
        .init(trackedIdentifiers.withLock(\.self))
    }
    
    /// The reference graph for tracking relationships between models using their `PersistentIdentifier`.
    nonisolated public var graph: ReferenceGraph {
        manager.graph
    }
    
    nonisolated internal init(manager: ModelManager, id: EditingState.ID) {
        self.manager = manager
        self.id = id
        self.key = Int.random(in: Int.min..<Int.max)
        self.independentlyManaged = manager.configuration.options.contains(.centralizedSnapshotCaching) == false
        self.shouldDebugOperations = DataStoreDebugging.mode == .trace
    }
    
    deinit {
        logger.debug("SnapshotRegistry deinit: \(id)")
    }
    
    internal enum State: UInt8, AtomicRepresentable {
        case idle = 0
        case running
    }
    
    internal enum Error: Swift.Error {
        /// Unable to use the existing result map due to a missing snapshot.
        case resultMapInconsistency
        /// `ModelManager` and `SnapshotRegistry` are no longer in lockstep.
        case storageInconsistency
    }
}

extension SnapshotRegistry {
    nonisolated private func backingData(for persistentIdentifier: PersistentIdentifier) -> DatabaseBackingData? {
        if independentlyManaged {
            storage.withLock { $0[persistentIdentifier] }
        } else {
            manager.backingData(for: persistentIdentifier)
        }
    }
}

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
        return preloadedFetches.withLock {
            $0[key].take()
        } as? PreloadFetchResult<Result.ModelType, Result.SnapshotType>
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

extension SnapshotRegistry {
    package func primaryKey<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifier: PersistentIdentifier,
        as type: PrimaryKey.Type = String.self
    ) -> PrimaryKey {
        manager.primaryKey(for: persistentIdentifier, as: type)
    }
}

extension SnapshotRegistry {
    package func step(from entityName: String, predicate: @escaping (DatabaseBackingData) -> Bool) -> [Snapshot] {
        let candidates: [(PersistentIdentifier, DatabaseBackingData)]
        if independentlyManaged {
            let identifiers = entityIndex.withLock { $0[entityName] ?? [] }
            candidates = storage.withLock { storage in
                var result = [(PersistentIdentifier, DatabaseBackingData)]()
                result.reserveCapacity(identifiers.count)
                for identifier in identifiers {
                    if let backingData = storage[identifier] {
                        result.append((identifier, backingData))
                    }
                }
                return result
            }
        } else {
            candidates = self.manager.backingData(from: entityName)
        }
        var result = [Snapshot]()
        result.reserveCapacity(candidates.count)
        for (_, backingData) in candidates where predicate(backingData) {
            if let snapshot = try? Snapshot(backingData: backingData) {
                result.append(snapshot)
            }
        }
        return result
    }
}

extension SnapshotRegistry {
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
        for identifiers: [PersistentIdentifier],
        remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
    ) -> [PersistentIdentifier: Snapshot] {
        guard !identifiers.isEmpty else { return [:] }
        let state = self.state.load(ordering: .relaxed)
        if state == .running { return [:] }
        if independentlyManaged {
            let backing = self.storage.withLock { storage in
                var result = [PersistentIdentifier: DatabaseBackingData](minimumCapacity: identifiers.count)
                for identifier in identifiers {
                    if let data = storage[identifier] {
                        data.accessedTimestamp = .now()
                        result[identifier] = data
                    }
                }
                return result
            }
            var snapshots = [PersistentIdentifier: Snapshot](minimumCapacity: backing.count)
            for (identifier, data) in backing {
                if let snapshot = try? Snapshot(backingData: data) {
                    let key = remappedIdentifiers[identifier] ?? identifier
                    snapshots[key] = remappedIdentifiers.isEmpty
                    ? snapshot
                    : snapshot.copy(
                        persistentIdentifier: key,
                        remappedIdentifiers: remappedIdentifiers
                    )
                }
            }
            return snapshots
        } else {
            return manager.snapshots(for: identifiers, remappedIdentifiers: remappedIdentifiers)
        }
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
                let removedBacking = self.storage.withLock { $0.removeValue(forKey: persistentIdentifier) }
                _ = entityIndex.withLock { $0[persistentIdentifier.entityName]?.remove(persistentIdentifier) }
                if removedIdentifier != nil && removedBacking == nil {
                    logger.error("Tracked removal without backing data: \(id) \(persistentIdentifier)")
                }
                guard let backingData = removedBacking else {
                    break check
                }
                _ = invalidatingIdentifiers.withLock { $0.insert(persistentIdentifier) }
                Task { @DatabaseActor in
                    let keysSnapshot = backingData.cachedFetchResults
                    for hashKey in keysSnapshot {
                        if let _ = removeCachedFetchResultClearingBacklinks(forKey: hashKey) {
                        } else {
                            continue
                        }
                    }
                    _ = invalidatingIdentifiers.withLock { $0.remove(persistentIdentifier) }
                }
            }
            logger.debug(
                """
                Identifier invalidated: \(persistentIdentifier)
                EditingState: \(id) (total: \(count))
                """
            )
        }
        if !independentlyManaged {
            let cachedFetchResults = self.cachedFetchResultMapping.withLock(\.self)
            _ = invalidatingIdentifiers.withLock { $0.insert(persistentIdentifier) }
            Task(priority: .utility) { @DatabaseActor in
                defer {
                    _ = invalidatingIdentifiers.withLock { $0.remove(persistentIdentifier) }
                }
                try await withThrowingDiscardingTaskGroup { group in
                    for (hashKey, resultReference) in cachedFetchResults {
                        _ = group.addTaskUnlessCancelled { [weak self] in
                            try Task.checkCancellation()
                            async let fetchedFlag = resultReference
                                .fetchedIdentifiers
                                .contains(persistentIdentifier)
                            async let relatedFlag = resultReference
                                .relatedIdentifiers
                                .contains(persistentIdentifier)
                            let shouldInvalidateCache = await (fetchedFlag, relatedFlag)
                            if shouldInvalidateCache.0 || shouldInvalidateCache.1 {
                                await DatabaseActor.run { [weak self] in
                                    _ = self?.removeCachedFetchResultClearingBacklinks(forKey: hashKey)
                                }
                            }
                        }
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
    
    nonisolated package func synchronize(
        snapshots: [PersistentIdentifier: Snapshot],
        invalidateIdentifiers: Set<PersistentIdentifier>
    ) {
        state.store(.running, ordering: .sequentiallyConsistent)
        if shouldDebugOperations {
            // Ensure every `PersistentIdentifier` has a store identifier.
            for (persistentIdentifier, snapshot) in snapshots {
                ensureRemappedIdentifiers(
                    for: persistentIdentifier,
                    snapshot: snapshot,
                    includeToManyRelationships: true
                )
            }
        }
        pendingIdentifiers.withLock { pendingIdentifiers in
            pendingIdentifiers.formUnion(Set(snapshots.keys))
        }
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
                            for (_, snapshot) in snapshots {
                                entities.insert(snapshot.entityName)
                            }
                            for identifier in invalidateIdentifiers {
                                entities.insert(identifier.entityName)
                            }
                            return entities
                        }()
                        manager.advanceCacheRevisions(for: touchedEntities)
                        try await withThrowingDiscardingTaskGroup { group in
                            let requests = self.request.add(1, ordering: .sequentiallyConsistent)
                            logger.debug("Updating SnapshotRegistry (request count: \(requests)")
                            defer { request.subtract(1, ordering: .sequentiallyConsistent) }
                            _ = group.addTaskUnlessCancelled { [weak self] in
                                guard let self else { return }
                                for (_, snapshot) in snapshots where !snapshot.isTemporary {
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
}

extension SnapshotRegistry {
    nonisolated private func rebuildResults(
        forKey key: Int,
        on entityName: String,
        results: DataStoreFetchResultMap
    ) throws -> (
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    )? {
        let initialFetchedCount = results.fetchedIdentifiers.count
        let fetchedSnapshots = Mutex<[DatabaseSnapshot]>([])
        fetchedSnapshots.withLock { $0.reserveCapacity(initialFetchedCount) }
        let initialRelatedCount = results.relatedIdentifiers.count
        let relatedSnapshots = Mutex<[PersistentIdentifier: DatabaseSnapshot]>([:])
        relatedSnapshots.withLock { $0.reserveCapacity(initialRelatedCount) }
        let group = DispatchGroup()
        let error = Mutex<Error?>(nil)
        group.enter()
        DispatchQueue.global(qos: .userInitiated).async {
            var local = fetchedSnapshots.withLock(\.self)
            for persistentIdentifier in results.fetchedIdentifiers {
                if entityName != persistentIdentifier.entityName {
                    logger.notice("Detected a mismatch in entity name: \(entityName) != \(persistentIdentifier.entityName) \(persistentIdentifier)")
                }
                guard let snapshot = self.snapshot(for: persistentIdentifier) else {
                    error.withLock { $0 = Error.resultMapInconsistency }
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
            for persistentIdentifier in results.relatedIdentifiers {
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
        let count = (fetchedSnapshots.withLock(\.count), relatedSnapshots.withLock(\.count))
        if (initialFetchedCount != count.0) || (initialRelatedCount != count.1) {
            logger.warning("Invalidating cache due to inconsistent counts.")
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: key) }
            return nil
        }
        logger.debug("Rebuilt cached result from references: \(count.0) \(count.1)")
        return (fetchedSnapshots.withLock(\.self), relatedSnapshots.withLock(\.self))
    }
    
    nonisolated package func cachedResult(forKey hashKey: Int) throws -> (any Sendable)? {
        let requests = request.wrappingAdd(1, ordering: .relaxed)
        defer { request.wrappingSubtract(1, ordering: .relaxed) }
        logger.trace("Number of requests for retrieving caches: \(requests) < 10")
        guard requests.newValue < 10 else {
            return nil
        }
        guard var entry = self.cachedFetchResultMapping.withLock({ $0[hashKey] }) else {
            logger.trace("Not found in cached fetch results: \(id)")
            return nil
        }
        let policy = self.manager.configuration.cachePolicy.predicateResults
        let now = DispatchTime.now()
        if isExpired(entry, policy: policy, now: now) || !isValid(entry, policy: policy) {
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: hashKey) }
            return nil
        }
        entry.lastAccessed = now
        entry.hitCount &+= 1
        cachedFetchResultMapping.withLock { $0[hashKey] = entry }
        touchFetchResultKey(hashKey)
        scheduleEvictionIfNeeded()
        return entry
    }
}

extension SnapshotRegistry {
    nonisolated package func cachedFetchResult(
        forKey hashKey: Int,
        on entityName: String
    ) throws -> (
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    )? {
        guard manager.isCachingSnapshots else {
            return nil
        }
        let requests = self.request.wrappingAdd(1, ordering: .relaxed)
        defer { request.wrappingSubtract(1, ordering: .relaxed) }
        logger.trace("Number of requests for retrieving caches: \(requests) < 10")
        guard requests.newValue < 10 else {
            return nil
        }
        guard var entry = self.cachedFetchResultMapping.withLock({ $0[hashKey] }) else {
            logger.trace("Not found in cached fetch results: \(id)")
            return nil
        }
        let policy = self.manager.configuration.cachePolicy.predicateResults
        let now = DispatchTime.now()
        if isExpired(entry, policy: policy, now: now) || !isValid(entry, policy: policy) {
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: hashKey) }
            return nil
        }
        let rebuilt = try rebuildResults(forKey: hashKey, on: entityName, results: entry)
        guard rebuilt != nil else { return nil }
        entry.lastAccessed = now
        entry.hitCount &+= 1
        cachedFetchResultMapping.withLock { $0[hashKey] = entry }
        touchFetchResultKey(hashKey)
        scheduleEvictionIfNeeded()
        return rebuilt
    }
    
    nonisolated package func cachedFetchIdentifiersResult(forKey hashKey: Int) throws -> (
        fetchedSnapshots: [PersistentIdentifier],
        relatedSnapshots: [PersistentIdentifier]
    )? {
        guard manager.isCachingSnapshots else {
            return nil
        }
        let requests = self.request.wrappingAdd(1, ordering: .relaxed)
        defer { request.wrappingSubtract(1, ordering: .relaxed) }
        logger.trace("Number of requests for retrieving caches: \(requests) < 10")
        guard requests.newValue < 10 else {
            return nil
        }
        guard var entry = self.cachedFetchResultMapping.withLock({ $0[hashKey] }) else {
            logger.trace("Not found in cached fetch results: \(id)")
            return nil
        }
        let policy = self.manager.configuration.cachePolicy.predicateResults
        let now = DispatchTime.now()
        if isExpired(entry, policy: policy, now: now) || !isValid(entry, policy: policy) {
            Task { @DatabaseActor in _ = removeCachedFetchResultClearingBacklinks(forKey: hashKey) }
            return nil
        }
        entry.lastAccessed = now
        entry.hitCount &+= 1
        cachedFetchResultMapping.withLock { $0[hashKey] = entry }
        touchFetchResultKey(hashKey)
        scheduleEvictionIfNeeded()
        return (entry.fetchedIdentifiers, entry.relatedIdentifiers)
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
            if self.cacheTasksByKey[key] != nil {
                return
            }
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
        guard manager.isCachingSnapshots else {
            return
        }
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
        dependencyEntities.reserveCapacity(fetchedSnapshots.count &+ relatedSnapshots.count &+ 8)
        func insertDependencies(from snapshot: Snapshot) {
            dependencyEntities.insert(snapshot.entityName)
            for property in snapshot.properties where property.metadata is Schema.Relationship {
                switch snapshot.values[property.index] {
                case let related as PersistentIdentifier:
                    dependencyEntities.insert(related.entityName)
                case let related as [PersistentIdentifier]:
                    if let first = related.first {
                        dependencyEntities.insert(first.entityName)
                    }
                default:
                    break
                }
            }
        }
        for snapshot in fetchedSnapshots { insertDependencies(from: snapshot) }
        for snapshot in relatedSnapshots.values { insertDependencies(from: snapshot) }
        let globalGeneratoon = manager.currentGlobalGeneration()
        let entityGenerations = (policy.validation == .entityGeneration)
        ? manager.currentEntityGenerations(for: dependencyEntities)
        : [:]
        let entry = DataStoreFetchResultMap(
            id: key,
            lastAccessed: .now(),
            hitCount: 0,
            globalCacheRevision: globalGeneratoon,
            entityCacheRevisions: entityGenerations,
            fetchedIdentifiers: fetchedIdentifiers,
            relatedIdentifiers: relatedIdentifiers
        )
        cachedFetchResultMapping.withLock { cachedFetchResults in
            cachedFetchResults[key] = entry
        }
        touchFetchResultKey(key)
        scheduleEvictionIfNeeded()
    }
    
    nonisolated(nonsending) private func registerFetchResult(
        _ snapshots: [Snapshot],
        forKey key: Int
    ) async throws -> [PersistentIdentifier] {
        try await withThrowingTaskGroup(of: (Int, PersistentIdentifier)?.self) { scalarGroup in
            var batch = [PersistentIdentifier?](repeating: nil, count: snapshots.count)
            batch.reserveCapacity(snapshots.count)
            for (index, snapshot) in snapshots.enumerated() {
                _ = scalarGroup.addTaskUnlessCancelled { [weak self] in
                    guard let self,
                          let backingData = try register(snapshot: snapshot)
                            ?? self.backingData(for: snapshot.persistentIdentifier) else {
                        logger.debug(
                            "Collecting identifiers returned nil when registering.",
                            metadata: ["snapshot": "\(snapshot)"]
                        )
                        return nil
                    }
                    precondition(snapshot.entityName == backingData.tableName)
                    _ = await DatabaseActor.run {
                        backingData.cachedFetchResults.insert(key)
                    }
                    return (index, snapshot.persistentIdentifier)
                }
            }
            try Task.checkCancellation()
            batch = try await scalarGroup.reduce(into: batch) { partialResult, tuple in
                if let (index, persistentIdentifier) = tuple {
                    partialResult[index] = persistentIdentifier
                }
            }
            return batch.compactMap(\.self)
        }
    }
}

extension SnapshotRegistry {
    private func isExpired(
        _ entry: DataStoreFetchResultMap,
        policy: CacheLayerPolicy,
        now: DispatchTime
    ) -> Bool {
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
}

extension SnapshotRegistry {
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
        removeFetchResultKeyFromOrder(forKey: key)
        guard let removedResult else { return nil }
        for identifier in Set(removedResult.fetchedIdentifiers).union(removedResult.relatedIdentifiers) {
            self.backingData(for: identifier)?.cachedFetchResults.remove(key)
        }
        return removedResult
    }
    
    nonisolated private func removeFetchResultKeyFromOrder(forKey key: Int) {
        cachedFetchResultKeyOrder.withLock { order in
            order.removeAll(where: { $0 == key })
        }
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
