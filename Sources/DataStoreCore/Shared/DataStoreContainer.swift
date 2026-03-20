//
//  DataStoreContainer.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.shared")

extension DataStore where Self: Sendable {
    @discardableResult nonisolated package func initialize() -> DataStoreContainer {
        let container = DataStoreContainer(store: self)
        DataStoreAggregate.add(container, storeIdentifier: self.identifier)
        return container
    }
    
    nonisolated package func deinitialize() {
        DataStoreAggregate.remove(storeIdentifier: self.identifier)
    }
    
    nonisolated public static func load(for storeIdentifier: String) throws -> Self? {
        try DataStoreAggregate.load(for: storeIdentifier) as? Self
    }
}

extension DataStoreAggregate {
    nonisolated fileprivate static func add(
        _ container: DataStoreContainer,
        storeIdentifier: String
    ) {
        Self.withLock { aggregate in
            guard container.isInitialized else {
                logger.notice("Skipping registration for deallocated DataStoreContainer: \(storeIdentifier)")
                return
            }
            if let container = aggregate.containers.updateValue(container, forKey: storeIdentifier) {
                aggregate.invalidate(in: container)
            }
            #if DEBUG
            logger.debug(
                "Added DataStoreContainer: \(storeIdentifier)",
                metadata: ["active": "\(aggregate.containers.keys.joined(separator: ", "))"]
            )
            #endif
        }
    }
    
    nonisolated fileprivate static func remove(storeIdentifier: String) {
        Self.withLock { aggregate in
            let container = aggregate.containers.removeValue(forKey: storeIdentifier)
            switch container {
            case let container?:
                aggregate.invalidate(in: container)
                #if DEBUG
                let active = aggregate.containers.keys.joined(separator: ", ")
                logger.debug(
                    "Removed DataStoreContainer: \(storeIdentifier)",
                    metadata: ["active": "\(active)"]
                )
                #endif
            case nil:
                #if DEBUG
                let active = aggregate.containers.keys.joined(separator: ", ")
                logger.notice(
                    "Unable to remove missing DataStoreContainer: \(storeIdentifier)",
                    metadata: ["active": "\(active)"]
                )
                #else
                logger.notice("Unable to remove missing DataStoreContainer: \(storeIdentifier)")
                #endif
            }
        }
    }
    
    nonisolated public static func load(for storeIdentifier: String) throws -> (any DataStore & Sendable)? {
        switch Self.withLock({ $0.containers[storeIdentifier] }) {
        case let container?:
            do {
                let store = try container.load()
                #if DEBUG
                logger.debug("Loaded DataStoreContainer: \(storeIdentifier)")
                #endif
                return store
            } catch {
                logger.notice("DataStoreContainer could not be loaded: \(storeIdentifier)")
                Self.withLock { $0.invalidateAndRemoveIfCurrent(container) }
                throw error
            }
        case nil:
            #if DEBUG
            logger.debug("Unable to find DataStoreContainer: \(storeIdentifier)")
            #endif
            return nil
        }
    }
}

extension DataStoreAggregate {
    package static func initializeState(for editingState: EditingState, store: some DataStore & Sendable) {
        Self.withLock { aggregate in
            guard let container = aggregate.containers[store.identifier] else {
                preconditionFailure("Do not initialize state before the DataStore was registered.")
            }
            do {
                if try !container.insert(editingState.id) {
                    assertionFailure("The EditingState was connected to the DataStoreContainer more than once.")
                }
                aggregate.connectedStores[editingState.id] = container
                #if DEBUG
                logger.debug(
                    "Connected EditingState to DataStoreContainer.",
                    metadata: ["editing_state_id": "\(editingState.id)"]
                )
                #endif
            } catch {
                logger.error("Unable to connect EditingState to DataStoreContainer: \(error)")
            }
        }
    }
    
    package static func invalidateState(for editingState: EditingState) {
        Self.withLock { aggregate in
            switch aggregate.connectedStores.removeValue(forKey: editingState.id) {
            case let container?:
                if container.remove(editingState.id) {
                    #if DEBUG
                    logger.debug(
                        "Disconnected EditingState from DataStoreContainer.",
                        metadata: ["editing_state_id": "\(editingState.id)"]
                    )
                    #endif
                } else {
                    logger.debug(
                        "EditingState was not found in DataStoreContainer during invalidation.",
                        metadata: ["editing_state_id": "\(editingState.id)"]
                    )
                }
            case nil:
                logger.debug(
                    "EditingState was not connected to a DataStoreContainer.",
                    metadata: ["editing_state_id": "\(editingState.id)"]
                )
            }
        }
    }
    
    package static func load(editingState: EditingState) throws(Error) -> (any DataStore & Sendable)? {
        switch Self.withLock({ $0.connectedStores[editingState.id] }) {
        case let container?:
            do {
                return try container.load()
            } catch {
                Self.withLock { $0.invalidateAndRemoveIfCurrent(container) }
                throw error
            }
        case nil:
            return nil
        }
    }
}

package struct DataStoreAggregate: ~Copyable, Sendable {
    nonisolated private static let shared: Mutex<Self> = .init(.init())
    nonisolated fileprivate var containers: [String: DataStoreContainer] = [:]
    nonisolated fileprivate var connectedStores: [EditingState.ID: DataStoreContainer] = [:]
    
    nonisolated internal static func snapshot() -> Self {
        .shared.withLock { .init(containers: $0.containers, connectedStores: $0.connectedStores) }
    }
    
    nonisolated internal static func withLock<Result>(_ body: (inout Self) throws -> Result)
    rethrows -> Result where Result: ~Copyable {
        try Self.shared.withLock { try body(&$0) }
    }
    
    nonisolated fileprivate mutating func invalidate(in container: DataStoreContainer) {
        let identifiers = container.disconnectAll()
        if !identifiers.isEmpty {
            for editingStateID in identifiers {
                connectedStores[editingStateID] = nil
                #if DEBUG
                logger.debug(
                    "Disconnected EditingState from DataStoreContainer.",
                    metadata: [
                        "editing_state_id": "\(editingStateID)",
                        "store_identifier": "\(container.storeIdentifier ?? "nil")"
                    ]
                )
                #endif
            }
        }
    }
    
    nonisolated fileprivate mutating func invalidateAndRemoveIfCurrent(_ container: DataStoreContainer) {
        invalidate(in: container)
        guard let storeIdentifier = container.storeIdentifier else { return }
        guard containers[storeIdentifier] === container else { return }
        containers[storeIdentifier] = nil
    }
}

package final class DataStoreContainer: Sendable {
    #if swift(>=6.2)
    nonisolated private weak let store: (any DataStore & Sendable)?
    #else
    nonisolated(unsafe) private weak var store: (any DataStore & Sendable)?
    #endif
    nonisolated fileprivate let storeIdentifier: String?
    nonisolated fileprivate let editingStateIDs: Mutex<Set<EditingState.ID>> = .init([])
    nonisolated fileprivate let isActive: Atomic<Bool> = .init(true)
    
    nonisolated package init(store: (any DataStore & Sendable)?) {
        self.store = store
        self.storeIdentifier = store?.identifier
    }
    
    nonisolated package var isInitialized: Bool {
        store != nil
    }
    
    nonisolated package func load() throws(Error) -> any DataStore & Sendable {
        guard isActive.load(ordering: .relaxed) else {
            throw Self.Error.notActive
        }
        guard let store = self.store else {
            throw Self.Error.storeHasBeenDeallocated
        }
        return store
    }
    
    nonisolated fileprivate func insert(_ id: EditingState.ID) throws(Error) -> Bool {
        let result: Bool? = self.editingStateIDs.withLock { editingStateIDs in
            guard isActive.load(ordering: .relaxed) else {
                logger.debug(
                    "Attempted to connect EditingState to an inactive DataStoreContainer.",
                    metadata: ["editing_state_id": "\(id)"]
                )
                return nil
            }
            return editingStateIDs.insert(id).inserted
        }
        guard let result else { throw Self.Error.notActive }
        return result
    }
    
    nonisolated fileprivate func remove(_ id: EditingState.ID) -> Bool {
        editingStateIDs.withLock { editingStateIDs in
            editingStateIDs.remove(id) != nil
        }
    }
    
    nonisolated fileprivate func disconnectAll() -> Set<EditingState.ID> {
        editingStateIDs.withLock { editingStateIDs in
            isActive.store(false, ordering: .relaxed)
            let identifiers = editingStateIDs
            editingStateIDs.removeAll(keepingCapacity: true)
            return identifiers
        }
    }
    
    nonisolated package func isConnected(to editingState: some EditingStateProviding) -> Bool {
        editingStateIDs.withLock { $0.contains(editingState.id) }
    }
    
    deinit {
        #if DEBUG
        logger.debug("Deinitialized DataStoreContainer.")
        #endif
    }
    
    package enum Error: Swift.Error {
        case notActive
        case storeHasBeenDeallocated
    }
}
