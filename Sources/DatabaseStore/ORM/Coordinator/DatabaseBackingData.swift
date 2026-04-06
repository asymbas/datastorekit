//
//  DatabaseBackingData.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import Foundation
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

package final class DatabaseBackingData: Sendable {
    /// Stored keys of `DataStoreFetchResultMap` that contains this backing data in its result.
    @DatabaseActor internal var cachedFetchResults: Set<Int> = []
    nonisolated private let _createdTimestamp: Atomic<UInt64> = .init(DispatchTime.now().uptimeNanoseconds)
    nonisolated private let _accessedTimestamp: Atomic<UInt64> = .init(DispatchTime.now().uptimeNanoseconds)
    nonisolated internal let recordIdentifier: RecordIdentifier
    nonisolated internal let values: Mutex<ContiguousArray<any DataStoreSnapshotValue>>
    nonisolated internal let subscription: Mutex<Subscription?>
    
    #if swift(>=6.2.3) && !SwiftPlaygrounds
    nonisolated internal weak let registry: SnapshotRegistry?
    #else
    nonisolated(unsafe) internal weak var registry: SnapshotRegistry?
    #endif
    
    nonisolated internal final var createdTimestamp: DispatchTime {
        get { DispatchTime(uptimeNanoseconds: _createdTimestamp.load(ordering: .relaxed)) }
        set { _createdTimestamp.store(newValue.uptimeNanoseconds, ordering: .relaxed) }
    }
    
    nonisolated internal final var accessedTimestamp: DispatchTime {
        get { DispatchTime(uptimeNanoseconds: _accessedTimestamp.load(ordering: .relaxed)) }
        set { _accessedTimestamp.store(newValue.uptimeNanoseconds, ordering: .relaxed) }
    }
    
    nonisolated internal final var storeIdentifier: String {
        recordIdentifier.storeIdentifier
    }
    
    nonisolated internal final var tableName: String {
        recordIdentifier.tableName
    }
    
    nonisolated internal final var primaryKey: any LosslessStringConvertible & Sendable {
        recordIdentifier.primaryKey
    }
    
    nonisolated internal init(
        registry: SnapshotRegistry? = nil,
        storeIdentifier: String,
        tableName: String,
        primaryKey: any LosslessStringConvertible & Sendable,
        values: ContiguousArray<any DataStoreSnapshotValue>
    ) {
        self.registry = registry
        self.recordIdentifier = .init(for: storeIdentifier, tableName: tableName, primaryKey: primaryKey)
        self.values = .init(values)
        self.subscription = .init(nil)
    }
    
    nonisolated internal convenience init(
        registry: SnapshotRegistry? = nil,
        persistentIdentifier: PersistentIdentifier,
        values: ContiguousArray<any DataStoreSnapshotValue>
    ) {
        let tableName = persistentIdentifier.entityName
        let primaryKey = persistentIdentifier.primaryKey()
        guard let storeIdentifier = persistentIdentifier.storeIdentifier else {
            preconditionFailure("Backing data must have a store identifier: \(primaryKey)")
        }
        self.init(
            registry: registry,
            storeIdentifier: storeIdentifier,
            tableName: tableName,
            primaryKey: primaryKey,
            values: values
        )
    }
    
    deinit {
        logger.debug("DatabaseBackingData deinit: \(recordIdentifier)")
    }
    
    package func compareField(_ rhs: any Sendable, at index: Int) -> Bool {
        let lhs = self.values.withLock { values in
            values[index]
        }
        guard let lhs = lhs as? any Equatable else {
            logger.error("LHS value at index \(index) is not Equatable.", metadata: [
                "lhs": "\(lhs)",
                "lhs type": "\(type(of: lhs))",
                "rhs": "\(rhs)",
                "rhs type": "\(type(of: rhs))"
            ])
            return false
        }
        func open<T: Equatable>(_ lhs: T) -> Bool {
            guard let rhs = rhs as? T else {
                logger.error("RHS value at index \(index) is not Equatable.", metadata: [
                    "lhs": "\(lhs)",
                    "lhs type": "\(type(of: lhs))",
                    "rhs": "\(rhs)",
                    "rhs type": "\(type(of: rhs))"
                ])
                return false
            }
            return lhs == rhs
        }
        return open(lhs)
    }
    
    package final class Export: Sendable {
        nonisolated package let columns: [String]
        nonisolated package let values: [any Sendable]
        nonisolated package let inheritedDependencies: [Int]
        nonisolated package let toOneDependencies: [Int]
        nonisolated package let toManyDependencies: [Int]
        nonisolated package let externalStorageData: [ExternalStoragePath]
        
        nonisolated package init(
            columns: [String],
            values: [any Sendable],
            inheritedDependencies: [Int],
            toOneDependencies: [Int],
            toManyDependencies: [Int],
            externalStorageData: [ExternalStoragePath]
        ) {
            self.columns = columns
            self.values = values
            self.inheritedDependencies = inheritedDependencies
            self.toOneDependencies = toOneDependencies
            self.toManyDependencies = toManyDependencies
            self.externalStorageData = externalStorageData
        }
    }
}

extension DatabaseBackingData {
    nonisolated internal func startListening() {
        Task(priority: .utility) { @DatabaseActor in
            guard let subscription = self.subscription.withLock(\.self) else {
                return
            }
            for try await event in subscription.stream {
                react(to: event)
            }
        }
    }
    
    nonisolated internal func stopListening() {
        subscription.withLock { $0?.cancel() }
    }
    
    nonisolated private func react(to event: ContiguousArray<any DataStoreSnapshotValue>) {
        values.withLock { $0 = event }
    }
}

extension DatabaseBackingData: CustomStringConvertible {
    nonisolated public var description: String {
        "DatabaseBackingData(\(recordIdentifier))"
    }
}
