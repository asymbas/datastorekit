//
//  ReferenceGraph.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Collections
private import DataStoreCore
private import DataStoreSQL
private import SQLiteHandle
private import SQLSupport
private import Synchronization
public import Logging
public import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.referencegraph")

public final class ReferenceGraph: Sendable {
    private typealias PropertyID = UInt32
    nonisolated private let storage: Mutex<Storage> = .init(.init())
    nonisolated public init() {}
    
    /// The backing storage for forward and reverse indices.
    private struct Storage: Equatable, Hashable, Sendable {
        nonisolated internal var properties: PropertyTable = .init()
        nonisolated internal var forward:
        [PersistentIdentifier: [PropertyID: OrderedSet<PersistentIdentifier>]] = [:]
        nonisolated internal var reverse:
        [PersistentIdentifier: Set<ReverseKey>] = [:]
    }
    
    private struct PropertyTable: Equatable, Hashable, Sendable {
        nonisolated private(set) var nameByID: [String] = []
        nonisolated private(set) var idByName: [String: PropertyID] = [:]
        
        nonisolated internal var count: Int { nameByID.count }
        
        nonisolated internal func name(for id: PropertyID) -> String { nameByID[Int(id)] }
        
        nonisolated internal func idIfPresent(_ name: String) -> PropertyID? { idByName[name] }
        
        nonisolated internal mutating func intern(_ name: String) -> PropertyID {
            if let existing = self.idByName[name] { return existing }
            let next = self.nameByID.count
            precondition(next <= Int(UInt32.max))
            let id = PropertyID(next)
            nameByID.append(name)
            self.idByName[name] = id
            return id
        }
    }
    
    public struct IncomingEdge: Equatable, Hashable, Sendable {
        nonisolated public let owner: PersistentIdentifier
        nonisolated public let property: String
        
        nonisolated public init(owner: PersistentIdentifier, property: String) {
            self.owner = owner
            self.property = property
        }
    }
    
    public struct Edge: Equatable, Hashable, Sendable {
        nonisolated public let owner: PersistentIdentifier
        nonisolated public let property: String
        nonisolated public let target: PersistentIdentifier
        
        nonisolated public init(
            owner: PersistentIdentifier,
            property: String,
            target: PersistentIdentifier
        ) {
            self.owner = owner
            self.property = property
            self.target = target
        }
    }
    
    private struct ReverseKey: Equatable, Hashable, Sendable {
        nonisolated internal let owner: PersistentIdentifier
        nonisolated internal let propertyID: PropertyID
    }
    
    public enum TraversalDirection: Sendable {
        case outgoing
        case incoming
        case both
    }
}

extension ReferenceGraph {
    nonisolated public func outgoing(from owner: PersistentIdentifier, property: String? = nil)
    -> [PersistentIdentifier] {
        storage.withLock { storage in
            guard let byProperty = storage.forward[owner] else {
                return []
            }
            if let property {
                guard let propertyID = storage.properties.idIfPresent(property),
                      let targets = byProperty[propertyID] else {
                    return []
                }
                return .init(targets)
            }
            var result = OrderedSet<PersistentIdentifier>()
            let sortedPropertyIDs = byProperty.keys.sorted {
                storage.properties.name(for: $0) < storage.properties.name(for: $1)
            }
            for propertyID in sortedPropertyIDs {
                if let targets = byProperty[propertyID] { result.formUnion(targets) }
            }
            return .init(result)
        }
    }
    
    nonisolated public func incoming(to target: PersistentIdentifier) -> [IncomingEdge] {
        storage.withLock { storage in
            guard let edges = storage.reverse[target] else {
                return []
            }
            return edges.map { key in
                    .init(
                        owner: key.owner,
                        property: storage.properties.name(for: key.propertyID)
                    )
            }
        }
    }
    
    nonisolated public func edges(from owner: PersistentIdentifier) -> [Edge] {
        storage.withLock { storage in
            guard let byProperty = storage.forward[owner] else {
                return []
            }
            var result = [Edge]()
            result.reserveCapacity(byProperty.values.reduce(0) { $0 + $1.count })
            for (propertyID, targets) in byProperty {
                let propertyName = storage.properties.name(for: propertyID)
                for target in targets {
                    result.append(.init(owner: owner, property: propertyName, target: target))
                }
            }
            return result
        }
    }
    
    nonisolated public func edges(to target: PersistentIdentifier) -> [Edge] {
        storage.withLock { storage in
            guard let incoming = storage.reverse[target] else {
                return []
            }
            return incoming.map { key in
                    .init(
                        owner: key.owner,
                        property: storage.properties.name(for: key.propertyID),
                        target: target
                    )
            }
        }
    }
}

extension ReferenceGraph {
    public enum EmptyPropertyMode: Sendable {
        case storeKnownEmpty
        case removeProperty
    }
    
    nonisolated private static func _set(
        in storage: inout Storage,
        owner: PersistentIdentifier,
        propertyID: PropertyID,
        targets newTargets: [PersistentIdentifier],
        emptyMode: EmptyPropertyMode
    ) {
        var targetsByProperty = storage.forward[owner, default: [:]]
        let oldTargets = targetsByProperty[propertyID] ?? []
        let newTargets = OrderedSet(newTargets)
        let removedDelta = oldTargets.subtracting(newTargets)
        let addedDelta = newTargets.subtracting(oldTargets)
        if newTargets.isEmpty {
            switch emptyMode {
            case .storeKnownEmpty: targetsByProperty[propertyID] = []
            case .removeProperty: targetsByProperty.removeValue(forKey: propertyID)
            }
        } else {
            targetsByProperty[propertyID] = consume newTargets
        }
        if targetsByProperty.isEmpty {
            storage.forward.removeValue(forKey: owner)
        } else {
            storage.forward[owner] = consume targetsByProperty
        }
        if !removedDelta.isEmpty {
            for target in consume removedDelta {
                if var edges = storage.reverse[target] {
                    edges.remove(.init(owner: owner, propertyID: propertyID))
                    if edges.isEmpty {
                        storage.reverse.removeValue(forKey: target)
                    } else {
                        storage.reverse[target] = consume edges
                    }
                }
            }
        }
        if !addedDelta.isEmpty {
            for target in consume addedDelta {
                var edges = storage.reverse[target, default: []]
                edges.insert(.init(owner: owner, propertyID: propertyID))
                storage.reverse[target] = consume edges
            }
        }
    }
    
    nonisolated public func set(
        owner: PersistentIdentifier,
        property: String,
        targets newTargets: [PersistentIdentifier]
    ) {
        storage.withLock { storage in
            let propertyID = storage.properties.intern(property)
            Self._set(
                in: &storage,
                owner: owner,
                propertyID: propertyID,
                targets: newTargets,
                emptyMode: .storeKnownEmpty
            )
        }
    }
    
    nonisolated internal func set(
        owner: PersistentIdentifier,
        mapping: [String: [PersistentIdentifier]]
    ) {
        storage.withLock { storage in
            for (property, targets) in consume mapping {
                let propertyID = storage.properties.intern(property)
                Self._set(
                    in: &storage,
                    owner: owner,
                    propertyID: propertyID,
                    targets: targets,
                    emptyMode: .storeKnownEmpty
                )
            }
        }
    }
    
    nonisolated public func setAuthoritative(
        owner: PersistentIdentifier,
        mapping: [String: [PersistentIdentifier]]
    ) {
        set(owner: owner, mapping: mapping)
        clearMissingProperties(owner: owner, preserve: Set(mapping.keys))
    }
    
    nonisolated public func removeAll(for owner: PersistentIdentifier) {
        storage.withLock { storage in
            guard let targetsByProperty = storage.forward.removeValue(forKey: owner) else {
                return
            }
            for (propertyID, targets) in consume targetsByProperty {
                for target in consume targets {
                    if var incomingEdges = storage.reverse[target] {
                        incomingEdges.remove(.init(owner: owner, propertyID: propertyID))
                        if incomingEdges.isEmpty {
                            storage.reverse.removeValue(forKey: target)
                        } else {
                            storage.reverse[target] = consume incomingEdges
                        }
                    }
                }
            }
        }
    }
    
    nonisolated public func removeIncomingEdges(
        to target: PersistentIdentifier,
        emptyMode: EmptyPropertyMode = .removeProperty
    ) {
        storage.withLock { storage in
            guard let incomingEdges = storage.reverse.removeValue(forKey: target) else {
                return
            }
            for key in consume incomingEdges {
                guard var targetsByProperty = storage.forward[key.owner] else {
                    continue
                }
                guard var ownerTargets = targetsByProperty[key.propertyID] else {
                    continue
                }
                ownerTargets.remove(target)
                if ownerTargets.isEmpty {
                    switch emptyMode {
                    case .removeProperty: targetsByProperty.removeValue(forKey: key.propertyID)
                    case .storeKnownEmpty: targetsByProperty[key.propertyID] = []
                    }
                } else {
                    targetsByProperty[key.propertyID] = consume ownerTargets
                }
                if targetsByProperty.isEmpty {
                    storage.forward.removeValue(forKey: key.owner)
                } else {
                    storage.forward[key.owner] = consume targetsByProperty
                }
            }
        }
    }
    
    nonisolated public func clearMissingProperties(
        owner: PersistentIdentifier,
        preserve properties: Set<String>,
        emptyMode: EmptyPropertyMode = .removeProperty
    ) {
        storage.withLock { storage in
            guard let existing = storage.forward[owner] else {
                return
            }
            var preserveIDs = Set<PropertyID>()
            preserveIDs.reserveCapacity(properties.count)
            for name in properties {
                if let id = storage.properties.idIfPresent(name) {
                    preserveIDs.insert(id)
                }
            }
            let propertiesToClear = existing.keys.filter {
                preserveIDs.contains($0) == false
            }
            for propertyID in propertiesToClear {
                Self._set(
                    in: &storage,
                    owner: owner,
                    propertyID: propertyID,
                    targets: [],
                    emptyMode: emptyMode
                )
            }
        }
    }
}

extension ReferenceGraph {
    public enum RemapReverseMode: Sendable {
        case pruneIfForwardMissing
        case keepEdges
    }
    
    nonisolated private static func _remap(
        in storage: inout Storage,
        from oldIdentifier: PersistentIdentifier,
        to newIdentifier: PersistentIdentifier,
        reverseMode: RemapReverseMode
    ) {
        if let oldTargetsByProperty = storage.forward.removeValue(forKey: oldIdentifier) {
            var mergedTargetsByProperty = storage.forward[newIdentifier] ?? [:]
            for (propertyID, oldTargets) in consume oldTargetsByProperty {
                var updatedTargets = mergedTargetsByProperty[propertyID] ?? []
                updatedTargets.formUnion(oldTargets)
                mergedTargetsByProperty[propertyID] = consume updatedTargets
                for target in consume oldTargets {
                    if var edges = storage.reverse[target] {
                        edges.remove(.init(owner: oldIdentifier, propertyID: propertyID))
                        edges.insert(.init(owner: newIdentifier, propertyID: propertyID))
                        storage.reverse[target] = consume edges
                    }
                }
            }
            storage.forward[newIdentifier] = consume mergedTargetsByProperty
        }
        if let oldIncomingEdges = storage.reverse.removeValue(forKey: oldIdentifier) {
            var mergedIncomingEdges = storage.reverse[newIdentifier] ?? []
            for key in consume oldIncomingEdges {
                var shouldInsertEdge = (reverseMode == .keepEdges)
                if var targetsByProperty = storage.forward[key.owner],
                   var targets = targetsByProperty[key.propertyID] {
                    if let oldIndex = targets.firstIndex(of: oldIdentifier) {
                        targets.remove(at: oldIndex)
                        if let existingIndex = targets.firstIndex(of: newIdentifier) {
                            targets.remove(at: existingIndex)
                            let insertionIndex = existingIndex < oldIndex ? oldIndex - 1 : oldIndex
                            targets.insert(newIdentifier, at: insertionIndex)
                        } else {
                            targets.insert(newIdentifier, at: oldIndex)
                        }
                    }
                    let forwardStillHasNew = targets.contains(newIdentifier)
                    if reverseMode == .pruneIfForwardMissing {
                        shouldInsertEdge = forwardStillHasNew
                    }
                    targetsByProperty[key.propertyID] = consume targets
                    storage.forward[key.owner] = consume targetsByProperty
                }
                if shouldInsertEdge {
                    mergedIncomingEdges.insert(key)
                }
            }
            if mergedIncomingEdges.isEmpty {
                storage.reverse.removeValue(forKey: newIdentifier)
            } else {
                storage.reverse[newIdentifier] = consume mergedIncomingEdges
            }
        }
    }
    
    nonisolated public func remap(
        from oldIdentifier: PersistentIdentifier,
        to newIdentifier: PersistentIdentifier,
        reverseMode: RemapReverseMode = .pruneIfForwardMissing
    ) {
        if oldIdentifier == newIdentifier { return }
        storage.withLock { storage in
            Self._remap(
                in: &storage,
                from: oldIdentifier,
                to: newIdentifier,
                reverseMode: reverseMode
            )
        }
    }
    
    nonisolated public func remap(
        using pairs: [PersistentIdentifier: PersistentIdentifier],
        reverseMode: RemapReverseMode = .pruneIfForwardMissing
    ) {
        storage.withLock { storage in
            for (oldIdentifier, newIdentifier) in consume pairs {
                if oldIdentifier == newIdentifier { continue }
                Self._remap(
                    in: &storage,
                    from: oldIdentifier,
                    to: newIdentifier,
                    reverseMode: reverseMode
                )
            }
        }
    }
}

extension ReferenceGraph {
    nonisolated package static func normalizeTargets(_ value: some DataStoreSnapshotValue)
    -> [PersistentIdentifier]? {
        switch value {
        case let value as [PersistentIdentifier]: value
        case let value as PersistentIdentifier: [value]
        case is SQLNull: []
        default: nil
        }
    }
    
    nonisolated public func cachedReferencesIfPresent(
        for owners: [PersistentIdentifier],
        at property: String
    ) -> (
        hits: [PersistentIdentifier: [PersistentIdentifier]],
        misses: [PersistentIdentifier]
    ) {
        storage.withLock { storage in
            var hits = [PersistentIdentifier: [PersistentIdentifier]]()
            hits.reserveCapacity(owners.count)
            var misses = [PersistentIdentifier]()
            misses.reserveCapacity(owners.count)
            guard let propertyID = storage.properties.idIfPresent(property) else {
                misses.append(contentsOf: owners)
                return (hits, misses)
            }
            for owner in owners {
                if let byProperty = storage.forward[owner],
                   let targets = byProperty[propertyID] {
                    hits[owner] = Array(targets)
                } else {
                    misses.append(owner)
                }
            }
            return (hits, misses)
        }
    }
    
    nonisolated public func cachedReferencesIfPresent(
        for owner: PersistentIdentifier,
        at property: String
    ) -> [PersistentIdentifier]? {
        storage.withLock { storage in
            guard let propertyID = storage.properties.idIfPresent(property),
                  let byProperty = storage.forward[owner],
                  let targets = byProperty[propertyID] else {
                logger.debug(
                    "Unable to find any references for \(owner.entityName).\(property).",
                    metadata: ["primary_key": "\(owner.primaryKey())"]
                )
                return nil
            }
            return Array(targets)
        }
    }
    
    nonisolated public func references(
        for owner: PersistentIdentifier,
        at property: String
    ) -> [PersistentIdentifier] {
        outgoing(from: owner, property: property)
    }
    
    nonisolated public func setReferences(
        for owner: PersistentIdentifier,
        at property: String,
        to targets: [PersistentIdentifier]
    ) {
        set(owner: owner, property: property, targets: targets)
    }
}

extension ReferenceGraph {
    nonisolated public func canonicalRemaps(
        _ pairs: [PersistentIdentifier: PersistentIdentifier],
        maxSteps: Int = 32
    ) -> [PersistentIdentifier: PersistentIdentifier] {
        var result: [PersistentIdentifier: PersistentIdentifier] = [:]
        result.reserveCapacity(pairs.count)
        for (old, start) in pairs {
            var current = start
            var steps = 0
            while let next = pairs[current], next != current, steps < maxSteps {
                current = next
                steps += 1
            }
            if old != current {
                result[old] = current
            }
        }
        return result
    }
}

extension ReferenceGraph {
    nonisolated public func reachable(
        from roots: [PersistentIdentifier],
        direction: TraversalDirection = .outgoing,
        propertyFilter: Set<String>? = nil,
        maxDepth: Int = .max,
        includeRoots: Bool = false
    ) -> Set<PersistentIdentifier> {
        storage.withLock { storage in
            var filterIDs: Set<PropertyID>? = nil
            if let propertyFilter {
                var ids = Set<PropertyID>()
                ids.reserveCapacity(propertyFilter.count)
                for name in propertyFilter {
                    if let id = storage.properties.idIfPresent(name) {
                        ids.insert(id)
                    }
                }
                filterIDs = ids.isEmpty ? [] : ids
            }
            var visited = Set<PersistentIdentifier>()
            visited.reserveCapacity(roots.count * 2)
            var frontier: [PersistentIdentifier] = roots
            frontier.reserveCapacity(roots.count)
            if includeRoots {
                for root in roots {
                    visited.insert(root)
                }
            }
            var depth = 0
            while !frontier.isEmpty, depth < maxDepth {
                var next = [PersistentIdentifier]()
                next.reserveCapacity(frontier.count * 2)
                switch direction {
                case .outgoing:
                    for node in frontier {
                        guard let byProperty = storage.forward[node] else {
                            continue
                        }
                        for (propertyID, targets) in byProperty {
                            if let filterIDs, filterIDs.contains(propertyID) == false {
                                continue
                            }
                            for target in targets {
                                if visited.insert(target).inserted {
                                    next.append(target)
                                }
                            }
                        }
                    }
                case .incoming:
                    for node in frontier {
                        guard let incoming = storage.reverse[node] else {
                            continue
                        }
                        for key in incoming {
                            if let filterIDs, filterIDs.contains(key.propertyID) == false {
                                continue
                            }
                            if visited.insert(key.owner).inserted {
                                next.append(key.owner)
                            }
                        }
                    }
                case .both:
                    for node in frontier {
                        if let byProperty = storage.forward[node] {
                            for (propertyID, targets) in byProperty {
                                if let filterIDs, filterIDs.contains(propertyID) == false {
                                    continue
                                }
                                for target in targets {
                                    if visited.insert(target).inserted {
                                        next.append(target)
                                    }
                                }
                            }
                        }
                        if let incoming = storage.reverse[node] {
                            for key in incoming {
                                if let filterIDs, filterIDs.contains(key.propertyID) == false {
                                    continue
                                }
                                if visited.insert(key.owner).inserted {
                                    next.append(key.owner)
                                }
                            }
                        }
                    }
                }
                frontier = next
                depth += 1
            }
            return visited
        }
    }
    
    nonisolated public func hasPath(
        from source: PersistentIdentifier,
        to destination: PersistentIdentifier,
        direction: TraversalDirection = .outgoing,
        propertyFilter: Set<String>? = nil,
        maxDepth: Int = .max
    ) -> Bool {
        if source == destination { return true }
        let found = reachable(
            from: [source],
            direction: direction,
            propertyFilter: propertyFilter,
            maxDepth: maxDepth,
            includeRoots: false
        )
        return found.contains(destination)
    }
}

extension ReferenceGraph {
    public struct Snapshot: Equatable, Hashable, Sendable {
        nonisolated public let forward:
        [PersistentIdentifier: [String: [PersistentIdentifier]]]
        nonisolated public let reverse:
        [PersistentIdentifier: [IncomingEdge]]
        nonisolated public let totalOwners: Int
        nonisolated public let totalTargets: Int
        nonisolated public let totalEdges: Int
        nonisolated public let totalProperties: Int
        
        nonisolated public init(
            forward: [PersistentIdentifier: [String: [PersistentIdentifier]]],
            reverse: [PersistentIdentifier: [IncomingEdge]],
            totalOwners: Int,
            totalTargets: Int,
            totalEdges: Int,
            totalProperties: Int
        ) {
            self.forward = forward
            self.reverse = reverse
            self.totalOwners = totalOwners
            self.totalTargets = totalTargets
            self.totalEdges = totalEdges
            self.totalProperties = totalProperties
        }
    }
    
    nonisolated public func snapshot() -> Snapshot {
        storage.withLock { storage in
            let forward: [PersistentIdentifier: [String: [PersistentIdentifier]]] =
            storage.forward.mapValues { byProperty in
                var mapped = [String: [PersistentIdentifier]]()
                mapped.reserveCapacity(byProperty.count)
                for (propertyID, targets) in byProperty {
                    mapped[storage.properties.name(for: propertyID)] = Array(targets)
                }
                return mapped
            }
            let reverse: [PersistentIdentifier: [IncomingEdge]] =
            storage.reverse.mapValues { edges in
                edges.map { edge in
                        .init(
                            owner: edge.owner,
                            property: storage.properties.name(for: edge.propertyID)
                        )
                }
            }
            let totalOwners = storage.forward.count
            let totalTargets = storage.reverse.count
            let totalEdges = storage.forward.values.reduce(into: 0) { sum, byProperty in
                for (_, targets) in byProperty { sum += targets.count }
            }
            return .init(
                forward: forward,
                reverse: reverse,
                totalOwners: totalOwners,
                totalTargets: totalTargets,
                totalEdges: totalEdges,
                totalProperties: storage.properties.count
            )
        }
    }
}

extension ReferenceGraph {
    @discardableResult nonisolated
    public func verifyIntegrity(logLevel: Logger.Level = .info) -> Bool {
        let snapshot = self.storage.withLock(\.self)
        let forward = snapshot.forward
        let reverse = snapshot.reverse
        var problems = 0
        for (owner, byProperties) in forward {
            for (propertyID, targets) in byProperties {
                for target in targets {
                    let key = ReverseKey(owner: owner, propertyID: propertyID)
                    if reverse[target]?.contains(key) != true {
                        problems += 1
                        let property = snapshot.properties.name(for: propertyID)
                        logger.log(
                            level: logLevel,
                            "Missing reverse edge: \(owner).\(property) -> \(target)"
                        )
                    }
                }
            }
        }
        for (target, keys) in reverse {
            for key in keys {
                if forward[key.owner]?[key.propertyID]?.contains(target) != true {
                    problems += 1
                    let property = snapshot.properties.name(for: key.propertyID)
                    let qualifiedKey = "\(key.owner).\(property)"
                    logger.log(
                        level: logLevel,
                        "Missing forward edge: \(qualifiedKey) -> \(target)"
                    )
                }
            }
        }
        if problems == 0 {
            logger.log(
                level: logLevel,
                "Reference invariants OK (\(forward.count) owners, \(reverse.count) targets)."
            )
            return true
        } else {
            logger.warning("Reference invariants FAILED (\(problems) issues).")
            return false
        }
    }
    
    nonisolated internal func debugDetailedLogging(
        level: Logger.Level = .info,
        listAll: Bool = false,
        topCount: Int = 10
    ) {
        let snapshot = self.storage.withLock(\.self)
        let forward = snapshot.forward
        let reverse = snapshot.reverse
        let totalOwners = forward.count
        let totalTargets = reverse.count
        var totalEdges = 0
        var targetCountByProperty = [String: Int]()
        totalEdges = forward.values.reduce(into: 0) { sum, byProperties in
            for (propertyID, targets) in byProperties {
                let property = snapshot.properties.name(for: propertyID)
                for target in targets {
                    sum += 1
                    let qualifiedKey = "\(property) -> \(target.entityName)"
                    targetCountByProperty[qualifiedKey, default: 0] += 1
                }
            }
        }
        var ownerEntities = [String: Int]()
        for (owner, _) in forward { ownerEntities[owner.entityName, default: 0] += 1 }
        var targetEntities = [String: Int]()
        for (target, _) in reverse { targetEntities[target.entityName, default: 0] += 1 }
        let ownerEntityCountByName = forward
            .map { (owner, byProperties) in (owner, byProperties.values.reduce(0) { $0 + $1.count }) }
            .sorted { ($0.1, "\($0.0)") > ($1.1, "\($1.0)") }
        let targetEntityCountByName = reverse
            .map { (target, edges) in (target, edges.count) }
            .sorted { ($0.1, "\($0.0)") > ($1.1, "\($1.0)") }
        let _0 = "owners: \(totalOwners)"
        let _1 = "targets: \(totalTargets)"
        let _2 = "edges: \(totalEdges)"
        let _3 = "property-target-entity pairs: \(targetCountByProperty.count)"
        logger.log(level: level, "References - \(_0), \(_1), \(_2), \(_3)")
        if !ownerEntities.isEmpty {
            let summary = ownerEntities
                .sorted { $0.key < $1.key }
                .map { "\($0.key): \($0.value)" }
                .joined(separator: ", ")
            logger.log(level: level, "Owner entities: [\(summary)]")
        }
        if !targetEntities.isEmpty {
            let summary = targetEntities
                .sorted { $0.key < $1.key }
                .map { "\($0.key): \($0.value)" }
                .joined(separator: ", ")
            logger.log(level: level, "Target entities: [\(summary)]")
        }
        if !targetEntityCountByName.isEmpty {
            let topProperties = targetCountByProperty
                .sorted { ($0.value, $0.key) > ($1.value, $1.key) }
                .prefix(topCount)
                .map { "\($0.key): \($0.value)" }
                .joined(separator: ", ")
            logger.log(level: level, "Top properties: [\(topProperties)]")
        }
        if !ownerEntityCountByName.isEmpty {
            let topOwners = ownerEntityCountByName.prefix(topCount)
                .map { "\($0.0) [\($0.1)]" }
                .joined(separator: ", ")
            logger.log(level: level, "Top owners by outgoing edges: \(topOwners)")
        }
        if !targetEntityCountByName.isEmpty {
            let topTargets = targetEntityCountByName.prefix(topCount)
                .map { "\($0.0) [\($0.1)]" }
                .joined(separator: ", ")
            logger.log(level: level, "Top targets by incoming edges: \(topTargets)")
        }
        guard listAll else { return }
        let grouped: [
            String: [(
                PersistentIdentifier,
                [PropertyID: OrderedSet<PersistentIdentifier>]
            )]
        ] = forward.reduce(into: .init()) {
            $0[$1.key.entityName, default: []].append(($1.key, $1.value))
        }
        for (entity, items) in grouped.sorted(by: { $0.key < $1.key }) {
            logger.log(level: level, "[\(entity)] \(items.count) owners")
            for (owner, byProperties) in items.sorted(by: { "\($0.0)" < "\($1.0)" }) {
                let parts = byProperties
                    .sorted {
                        let lhs = snapshot.properties.name(for: $0.key)
                        let rhs = snapshot.properties.name(for: $1.key)
                        return lhs < rhs
                    }
                    .map { propertyID, targets in
                        let property = snapshot.properties.name(for: propertyID)
                        let indent = String(repeating: " ", count: 4)
                        let targetsLog = targets
                            .map { "\(indent)\($0.entityName)(pk: \($0.primaryKey()))" }
                            .joined(separator: ",\n")
                        let description = "\(owner.entityName).\(property)"
                        if !targetsLog.isEmpty {
                            return "\(description): [\(targets.count)] {\n\(targetsLog)\n}"
                        } else {
                            return "\(description): [\(targets.count)]"
                        }
                    }
                    .joined(separator: "\n")
                let ownerEntityName = owner.entityName
                let ownerPrimaryKey = owner.primaryKey()
                let ownerLine = "Outgoing references grouped by property for \(ownerEntityName) entity."
                logger.log(
                    level: level,
                    "\(ownerLine)\n\(parts)",
                    metadata: [
                        "entity": "\(ownerEntityName)",
                        "primary_key": "\(ownerPrimaryKey)"
                    ]
                )
            }
        }
    }
    
    nonisolated internal func debugOutgoingOrdering(
        owner: PersistentIdentifier,
        logLevel: Logger.Level = .info
    ) {
        var logger = logger
        logger.logLevel = logLevel
        let snapshot = self.storage.withLock(\.self)
        guard let byProperty = snapshot.forward[owner] else {
            logger.log(level: logLevel, "Ordering - owner has no forward buckets: \(owner)")
            return
        }
        let rawPropertyIDs = Array(byProperty.keys)
        let sortedPropertyIDs = rawPropertyIDs.sorted {
            snapshot.properties.name(for: $0) < snapshot.properties.name(for: $1)
        }
        logger.log(level: logLevel, "Ordering - owner: \(owner)")
        logger.log(level: logLevel, "Ordering - raw property iteration: \(rawPropertyIDs.map { snapshot.properties.name(for: $0) })")
        logger.log(level: logLevel, "Ordering - sorted property names: \(sortedPropertyIDs.map { snapshot.properties.name(for: $0) })")
        for propertyID in sortedPropertyIDs {
            let name = snapshot.properties.name(for: propertyID)
            let targets = Array(byProperty[propertyID] ?? [])
            let indexed = targets.enumerated().map { "\($0.offset): \($0.element)" }.joined(separator: ", ")
            logger.log(level: logLevel, "Ordering - \(name) targets: [\(targets.count)] \(indexed)")
        }
        let merged = outgoing(from: owner, property: nil)
        let mergedIndexed = merged.enumerated().map { "\($0.offset): \($0.element)" }.joined(separator: ", ")
        logger.log(level: logLevel, "Ordering - merged outgoing (sorted-by-property-name + union): [\(merged.count)] \(mergedIndexed)")
    }
    
    nonisolated internal func debugIncomingOrdering(
        target: PersistentIdentifier,
        logLevel: Logger.Level = .info
    ) {
        var logger = logger
        logger.logLevel = logLevel
        let snapshot = self.storage.withLock(\.self)
        let keys = snapshot.reverse[target] ?? []
        if keys.isEmpty {
            logger.log(level: logLevel, "Ordering - target has no incoming edges: \(target)")
            return
        }
        let raw = keys.map { "\($0.owner).\(snapshot.properties.name(for: $0.propertyID))" }
        logger.log(level: logLevel, "Ordering - incoming raw (Set order unspecified): \(raw)")
        let sorted = keys.sorted {
            let a = "\($0.owner)-\(snapshot.properties.name(for: $0.propertyID))"
            let b = "\($1.owner)-\(snapshot.properties.name(for: $1.propertyID))"
            return a < b
        }.map { "\($0.owner).\(snapshot.properties.name(for: $0.propertyID))" }
        logger.log(level: logLevel, "Ordering - incoming sorted (stable): \(sorted)")
    }
    
    @available(*, unavailable, message: "")
    nonisolated internal func debugEdgesForOwner(
        id owner: PersistentIdentifier,
        logLevel: Logger.Level = .info
    ) {
        var logger = logger
        logger.logLevel = logLevel
        let snapshot = self.storage.withLock(\.self)
        let byProperties = snapshot.forward[owner] ?? [:]
        if byProperties.isEmpty {
            logger.log(level: logLevel, "Owner has no outgoing edges: \(owner)")
        } else {
            let parts = byProperties
                .sorted {
                    let lhs = snapshot.properties.name(for: $0.key)
                    let rhs = snapshot.properties.name(for: $1.key)
                    return lhs < rhs
                }
                .map { propertyID, targets in
                    let property = snapshot.properties.name(for: propertyID)
                    return "\(property): [\(targets.count)] \(Array(targets))"
                }
                .joined(separator: " | ")
            logger.log(level: logLevel, "Owner outgoing - \(owner): \(parts)")
        }
        let incomingKeys = snapshot.reverse[owner] ?? []
        if incomingKeys.isEmpty {
            logger.log(level: logLevel, "Owner incoming - none -> \(owner)")
        } else {
            let parts = incomingKeys
                .sorted {
                    let a = "\( $0.owner )-\( snapshot.properties.name(for: $0.propertyID) )"
                    let b = "\( $1.owner )-\( snapshot.properties.name(for: $1.propertyID) )"
                    return a < b
                }
                .map { key in
                    let property = snapshot.properties.name(for: key.propertyID)
                    return "\(key.owner).\(property)"
                }
                .joined(separator: ", ")
            logger.log(level: logLevel, "Owner incoming - \(parts) -> \(owner)")
        }
    }
}
