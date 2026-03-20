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
        DataStoreContainer.add(container, storeIdentifier: self.identifier)
        return container
    }
    
    nonisolated package func deinitialize() {
        DataStoreContainer.remove(storeIdentifier: self.identifier)
    }
    
    nonisolated public static func load(for storeIdentifier: String) -> Self? {
        DataStoreContainer.load(for: storeIdentifier) as? Self
    }
}

extension DataStoreContainer {
    nonisolated fileprivate static let instances: Mutex<[String: DataStoreContainer]> = .init([:])
    
    nonisolated fileprivate static func add(_ container: DataStoreContainer, storeIdentifier: String) {
        Self.instances.withLock { containers in
            guard container.store != nil else {
                logger.notice("DataStoreContainer store is already nil: \(storeIdentifier)")
                return
            }
            containers[storeIdentifier] = container
            #if DEBUG
            logger.debug(
                "Added DataStoreContainer: \(storeIdentifier)",
                metadata: ["active": "\(containers.keys.joined(separator: ", "))"]
            )
            #endif
        }
    }
    
    nonisolated fileprivate static func remove(storeIdentifier: String) {
        let container = Self.instances.withLock { containers in
            containers.removeValue(forKey: storeIdentifier)
        }
        switch container {
        case let container?:
            let identifiers = container.disconnectAll()
            if !identifiers.isEmpty {
                Self.connectedStores.withLock { containers in
                    for editingStateID in identifiers {
                        containers[editingStateID] = nil
                        #if DEBUG
                        logger.debug(
                            "Disconnected EditingState from DataStoreContainer.",
                            metadata: [
                                "editing_state_id": "\(editingStateID)",
                                "store_identifier": "\(storeIdentifier)"
                            ]
                        )
                        #endif
                    }
                }
            }
            #if DEBUG
            let active = Self.instances.withLock { $0.keys.joined(separator: ", ") }
            logger.debug(
                "Removed DataStoreContainer: \(storeIdentifier)",
                metadata: ["active": "\(active)"]
            )
            #endif
        case nil:
            #if DEBUG
            let active = Self.instances.withLock { $0.keys.joined(separator: ", ") }
            logger.notice(
                "Unable to find and remove DataStoreContainer: \(storeIdentifier)",
                metadata: ["active": "\(active)"]
            )
            #else
            logger.notice("Unable to find and remove DataStoreContainer: \(storeIdentifier)")
            #endif
        }
    }
    
    nonisolated public static func load(for storeIdentifier: String) -> (any DataStore & Sendable)? {
        switch Self.instances.withLock({ $0[storeIdentifier] }) {
        case let container?:
            guard let store = container.store else {
                logger.notice("DataStoreContainer store is nil and cannot be loaded: \(storeIdentifier)")
                Self.remove(storeIdentifier: storeIdentifier)
                fallthrough
            }
            #if DEBUG
            logger.debug("Loaded DataStoreContainer: \(storeIdentifier)")
            #endif
            return store
        case nil:
            #if DEBUG
            logger.debug("Unable to find and load DataStoreContainer: \(storeIdentifier)")
            #endif
            return nil
        }
    }
}

extension DataStoreContainer {
    nonisolated private static let connectedStores: Mutex<[EditingState.ID: DataStoreContainer]> = .init([:])
    
    package static func initializeState(for editingState: EditingState, store: some DataStore & Sendable) {
        guard let container = Self.instances.withLock({ $0[store.identifier] }) else {
            fatalError()
        }
        guard container.insert(editingState.id) else {
            fatalError()
        }
        Self.connectedStores.withLock {
            $0[editingState.id] = container
        }
        guard container.isConnected(to: editingState) else {
            fatalError()
        }
        #if DEBUG
        logger.debug(
            "Connected DataStoreContainer for initialized EditingState.",
            metadata: ["editing_state_id": "\(editingState.id)"]
        )
        #endif
    }
    
    package static func invalidateState(for editingState: EditingState) {
        let container = Self.connectedStores.withLock { containers in
            containers.removeValue(forKey: editingState.id)
        }
        
        switch container {
        case let container?:
            if container.remove(editingState.id) {
                #if DEBUG
                logger.debug(
                    "Disconnected DataStoreContainer from invalidated EditingState.",
                    metadata: ["editing_state_id": "\(editingState.id)"]
                )
                #endif
            } else {
                logger.debug(
                    "Invalidated EditingState not found in DataStoreContainer.",
                    metadata: ["editing_state_id": "\(editingState.id)"]
                )
            }
        case nil:
            logger.debug(
                "Invalidated EditingState is not referencing a DataStoreContainer.",
                metadata: ["editing_state_id": "\(editingState.id)"]
            )
        }
    }
    
    package static func load(editingState: some EditingStateProviding) -> (any DataStore & Sendable)? {
        let container = Self.connectedStores.withLock { $0[editingState.id] }
        return container?.store
    }
}

extension DataStoreContainer {
    nonisolated fileprivate func insert(_ id: EditingState.ID) -> Bool {
        editingStateIDs.withLock { editingStateIDs in
            guard isActive.load(ordering: .relaxed) else {
                return false
            }
            return editingStateIDs.insert(id).inserted
        }
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
        editingStateIDs.withLock { editingStateIDs in
            editingStateIDs.contains(editingState.id)
        }
    }
}

package final class DataStoreContainer: Sendable {
    #if swift(>=6.2)
    nonisolated package weak let store: (any DataStore & Sendable)?
    #else
    nonisolated(unsafe) package weak var store: (any DataStore & Sendable)?
    #endif
    nonisolated fileprivate let editingStateIDs: Mutex<Set<EditingState.ID>> = .init([])
    nonisolated fileprivate let isActive: Atomic<Bool> = .init(true)
    
    nonisolated package init(store: (any DataStore & Sendable)?) {
        self.store = store
    }
    
    deinit {
        #if DEBUG
        logger.debug("Deinitialized DataStoreContainer.")
        #endif
        
        nonisolated fileprivate init(store: (any DataStore & Sendable)? = nil) {
            self.store = store
        }
    }
    
    #endif
}
