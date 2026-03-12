//
//  SnapshotRegistry.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import AsyncAlgorithms
import DataStoreCore
import DataStoreSQL
import DataStoreSupport
import Dispatch
import Foundation
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

package struct DataStoreFetchResultMap: Sendable {
    nonisolated package let key: Int
    nonisolated package let timestamp: DispatchTime = .now()
    nonisolated package var lastAccessed: DispatchTime = .now()
    nonisolated package var hitCount: UInt32 = 0
    nonisolated package var globalCacheRevision: UInt64 = 0
    nonisolated package var entityCacheRevisions: [String: UInt64] = [:]
    nonisolated package var fetchedIdentifiers: [PersistentIdentifier]
    nonisolated package var relatedIdentifiers: [PersistentIdentifier]
}

/// A cached object with a lifecycle bound to a `ModelContext` via a linked `EditingState`.
public final class SnapshotRegistry: ObjectContextProtocol {
    public typealias Snapshot = DatabaseSnapshot
    nonisolated private unowned let manager: ModelManager
    /// Inherited from `Identifiable.id`.
    nonisolated public let id: EditingState.ID
    nonisolated private let storage: Mutex<[PersistentIdentifier: DatabaseBackingData]> = .init([:])
    nonisolated private let trackedIdentifiers: Mutex<Set<PersistentIdentifier>> = .init([])
    nonisolated private let pendingIdentifiers: Mutex<Set<PersistentIdentifier>> = .init([])
    nonisolated private let invalidatingIdentifiers: Mutex<Set<PersistentIdentifier>> = .init([])
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
    
    /// All `PersistentIdentifier` bound to this registry.
    nonisolated internal var persistentIdentifiers: [PersistentIdentifier] {
        Array(trackedIdentifiers.withLock(\.self))
    }
    
    /// The reference graph for tracking relationships between models using their `PersistentIdentifier`.
    nonisolated public var graph: ReferenceGraph {
        manager.graph
    }
    
    nonisolated internal init(manager: ModelManager, id: EditingState.ID) {
        self.manager = manager
        self.id = id
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
    
    /// Removes the snapshot from global or local storage and invalidates any results containing it.
    ///
    /// - Important:
    ///   This method should only be called from the `ModelManager`.
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
                                .async
                                .contains(persistentIdentifier)
                            async let relatedFlag = resultReference
                                .relatedIdentifiers
                                .async
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
    
    /// Updates the storage to persist snapshots in memory and adds it to the centralized manager.
    nonisolated internal func register(snapshot: Snapshot) throws -> DatabaseBackingData? {
        let persistentIdentifier = snapshot.persistentIdentifier
        let backingData = try manager.upsert(snapshot: snapshot, from: self)
        _ = trackedIdentifiers.withLock { $0.insert(persistentIdentifier) }
        try manager.initialize(for: persistentIdentifier, from: self.id)
        if independentlyManaged, let backingData {
            storage.withLock { $0[snapshot.persistentIdentifier] = backingData }
        }
        return backingData
    }
    
    /// Processes the snapshots given to `DataStore.save(_:)` for caching.
    ///
    /// - Parameters:
    ///   - snapshots:
    ///     The snapshots that were inserted or updated.
    ///   - invalidateIdentifiers:
    ///     A `PersistentIdentifier` array to invalidate any backing data or query results.
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
                        // FIXME: Save requests do not include all registered models of a `ModelContext`.
                        #if false
                        try manager.validation(persistentIdentifiers: pendingIdentifiers.withLock(\.self))
                        #endif
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
        forKey hashValue: Int,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    ) {
        guard manager.isCachingSnapshots else { return }
        if fetchedSnapshots.isEmpty { return }
        Task(priority: .utility) { @DatabaseActor [weak self] in
            guard let self else { return }
            if self.cachedFetchResultMapping.withLock({ $0[hashValue] != nil }) {
                self.touchFetchResultKey(hashValue)
                self.scheduleEvictionIfNeeded()
                return
            }
            if self.cacheTasksByKey[hashValue] != nil {
                return
            }
            let task = Task { @DatabaseActor [weak self] in
                defer { self?.cacheTasksByKey[hashValue] = nil }
                try Task.checkCancellation()
                try await self?.cacheFetchResult(
                    forKey: hashValue,
                    fetchedSnapshots: fetchedSnapshots,
                    relatedSnapshots: relatedSnapshots
                )
            }
            self.cacheTasksByKey[hashValue] = task
        }
    }
    
    nonisolated package func cacheFetchResult(
        forKey hashValue: Int,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier: Snapshot]
    ) async throws {
        if fetchedSnapshots.isEmpty { return }
        guard manager.isCachingSnapshots else {
            return
        }
        if cachedFetchResultMapping.withLock({ $0[hashValue] != nil }) {
            touchFetchResultKey(hashValue)
            scheduleEvictionIfNeeded()
            return
        }
        let requests = self.request.add(1, ordering: .sequentiallyConsistent)
        logger.trace("Caching fetch result: \(requests) \(self.id) (\(hashValue))")
        defer { request.subtract(1, ordering: .sequentiallyConsistent) }
        let fetchedIdentifiers = try await collectPersistentIdentifiers(
            from: fetchedSnapshots,
            hashValue: hashValue
        )
        let relatedIdentifiers = try await collectPersistentIdentifiers(
            from: Array(relatedSnapshots.values),
            hashValue: hashValue
        )
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
            key: hashValue,
            lastAccessed: .now(),
            hitCount: 0,
            globalCacheRevision: globalGeneratoon,
            entityCacheRevisions: entityGenerations,
            fetchedIdentifiers: fetchedIdentifiers,
            relatedIdentifiers: relatedIdentifiers
        )
        #if false
        var replacedCost: UInt64 = 0
        cachedFetchResultMapping.withLock { cachedFetchResults in
            if let existing = cachedFetchResults[hashValue] {
                replacedCost = existing.estimatedCost
            }
            cachedFetchResults[hashValue] = entry
        }
        await DatabaseActor.run {
            self.cachedFetchResultTotalCost = cachedFetchResultTotalCost &+ cost
            if replacedCost > 0 {
                self.cachedFetchResultTotalCost = cachedFetchResultTotalCost &- replacedCost
            }
        }
        #endif
        touchFetchResultKey(hashValue)
        scheduleEvictionIfNeeded()
    }
    
    nonisolated private func collectPersistentIdentifiers(
        from snapshots: [Snapshot],
        hashValue: Int
    ) async throws -> [PersistentIdentifier] {
        try await withThrowingTaskGroup(of: (Int, PersistentIdentifier)?.self) { scalarGroup in
            var batch = [PersistentIdentifier?](repeating: nil, count: snapshots.count)
            batch.reserveCapacity(snapshots.count)
            for (index, snapshot) in snapshots.enumerated() {
                _ = scalarGroup.addTaskUnlessCancelled { [weak self] in
                    guard let self,
                          let backingData = try register(snapshot: snapshot)
                            ?? self.backingData(for: snapshot.persistentIdentifier) else {
                        logger.debug("Collecting identifiers returned nil when registering: \(snapshot.persistentIdentifier)")
                        return nil
                    }
                    precondition(snapshot.entityName == backingData.tableName)
                    _ = await DatabaseActor.run {
                        backingData.cachedFetchResults.insert(hashValue)
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
            return batch.compactMap { $0 }
        }
    }
}

extension SnapshotRegistry {
    nonisolated private func isExpired(
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
    
    nonisolated private func isValid(_ entry: DataStoreFetchResultMap, policy: CacheLayerPolicy) -> Bool {
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
    
    nonisolated private func touchFetchResultKey(_ key: Int) {
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
    
    nonisolated private func removeFetchResultKeyFromOrder(_ key: Int) {
        cachedFetchResultKeyOrder.withLock { order in
            order.removeAll(where: { $0 == key })
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
}

extension SnapshotRegistry {
    nonisolated package func scheduleEvictionIfNeeded() {
        guard manager.isCachingSnapshots else { return }
        Task(priority: .utility) { @DatabaseActor in
            guard !evictionScheduled else { return }
            self.evictionScheduled = true
            defer { self.evictionScheduled = false }
            evictPredicateResultsIfNeeded()
        }
    }
    
    @DatabaseActor private func evictPredicateResultsIfNeeded() {
        let policy = self.manager.configuration.cachePolicy.predicateResults
        func overEntryLimit() -> Bool {
            switch policy.limit {
            case let .bounded(maxCount):
                return cachedFetchResultMapping.withLock(\.count) > maxCount
            case .unbounded:
                return false
            }
        }
        func overCostLimit() -> Bool {
            switch policy.costLimit {
            case let .bounded(maxTotal):
                return cachedFetchResultTotalCost > maxTotal
            case .unbounded:
                return false
            }
        }
        guard overEntryLimit() || overCostLimit() else { return }
        while overEntryLimit() || overCostLimit() {
            guard let key = evictOnePredicateResultIfNeeded() else { return }
            _ = removeCachedFetchResultClearingBacklinks(forKey: key)
        }
    }
    
    @discardableResult @DatabaseActor
    private func removeCachedFetchResultClearingBacklinks(forKey key: Int) -> DataStoreFetchResultMap? {
        if let task = cacheTasksByKey[key] {
            task.cancel()
            cacheTasksByKey[key] = nil
        }
        let removed = self.cachedFetchResultMapping.withLock { $0.removeValue(forKey: key) }
        removeFetchResultKeyFromOrder(key)
        guard let removed else { return nil }
        for identifier in Set(removed.fetchedIdentifiers).union(removed.relatedIdentifiers) {
            if let backingData = self.backingData(for: identifier) {
                backingData.cachedFetchResults.remove(key)
            }
        }
        return removed
    }
}

extension SnapshotRegistry {
    /// Returns the in-memory backing data stored in this registry's storage or from the centralized storage.
    ///
    /// - Parameter persistentIdentifier: The unique identifier assigned to the backing data.
    /// - Returns: The in-memory backing data used for persistence.
    nonisolated private func backingData(for persistentIdentifier: PersistentIdentifier)
    -> DatabaseBackingData? {
        if independentlyManaged {
            storage.withLock { $0[persistentIdentifier] }
        } else {
            manager.backingData(for: persistentIdentifier)
        }
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
