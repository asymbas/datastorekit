//
//  DatabaseConnection.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import Foundation
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

/// Represents a leased database connection provided by `DatabaseQueue`.
public struct DatabaseConnection<Store>: ~Copyable, Sendable where Store: DatabaseProtocol {
    nonisolated private let _transaction: AtomicLazyReference<Storage> = .init()
    nonisolated package var onTransactionFailure: @Sendable (borrowing Self) -> Void = { _ in }
    nonisolated package weak var queue: DatabaseQueue<Store>?
    nonisolated package let handle: Store.Handle
    nonisolated package let context: Store.Context?
    nonisolated package let storeIdentifier: String
    nonisolated package var externalDependencies: [PersistentIdentifier: [Int]] = [:]
    nonisolated package var snapshots: [PersistentIdentifier: Store.Snapshot] = [:]
    nonisolated package var remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
    nonisolated package var snapshotsToReregister: [PersistentIdentifier: Store.Snapshot] = [:]
    nonisolated public let editingState: (any EditingStateProviding)?
    
    nonisolated package var transaction: Store.Transaction? {
        get { _transaction.load()?.transaction }
        nonmutating set {
            guard let newValue else { return }
            _ = _transaction.storeIfNil(.init(transaction: newValue))
        }
    }
    
    nonisolated package var attachment: Store.Attachment? {
        queue?.attachment
    }
    
    nonisolated package var id: any Hashable {
        handle.id
    }
    
    nonisolated package init(
        for editingState: (any EditingStateProviding)? = nil,
        storeIdentifier: String,
        queue: DatabaseQueue<Store>? = nil,
        handle: Store.Handle,
        context: Store.Context? = nil,
        transaction: Store.Transaction? = nil
    ) {
        self.editingState = editingState
        self.storeIdentifier = storeIdentifier
        self.queue = queue
        self.handle = handle
        self.context = context
        if let transaction { _ = self._transaction.storeIfNil(.init(transaction: transaction)) }
        if let queue { self.onTransactionFailure = queue.onTransactionFailure }
    }
    
    nonisolated internal init(connection: consuming Self) {
        self.editingState = connection.editingState
        self.storeIdentifier = connection.storeIdentifier
        self.queue = connection.queue
        self.handle = connection.handle
        self.context = connection.context
        if let transaction = connection.transaction {
            _ = self._transaction.storeIfNil(.init(transaction: transaction))
        }
    }
    
    deinit {
        if let queue = self.queue {
            queue.release(handle, transaction: transaction)
        }
    }
    
    /// Detaches this connection from its queue and returns the underlying handle.
    ///
    /// - Returns: The underlying database handle.
    nonisolated public consuming func release() -> Store.Handle {
        logger.trace(
            "DatabaseConnection<\(Store.self)> released.",
            metadata: ["id": "\(handle.id)", "queue_reference": "\(queue != nil)"]
        )
        if queue != nil { self.queue = nil }
        return handle
    }
    
    /// A shortcut for executing precomposed SQL statements.
    nonisolated public var execute: SQLStatementShortcut<Store.Handle> {
        .init(handle: handle, transaction: transaction)
    }
    
    @discardableResult
    nonisolated public func execute(_ sql: String) throws -> Store.Handle.Result {
        try handle.execute(sql)
    }
    
    /// Executes a query and returns rows as positional values.
    nonisolated public func fetch(_ sql: String, bindings: [any Sendable] = [])
    throws -> [[any Sendable]] {
        try handle.fetch(sql, bindings: bindings)
    }
    
    /// Executes a query and returns rows keyed by column name.
    nonisolated public func query(_ sql: String, bindings: [any Sendable] = [])
    throws -> [[String: any Sendable]] {
        try handle.query(sql, bindings: bindings)
    }
}

extension DatabaseConnection {
    private final class Storage: Sendable {
        nonisolated fileprivate let transaction: Store.Transaction
        
        nonisolated fileprivate init(transaction: Store.Transaction) {
            self.transaction = transaction
        }
    }
    
    public enum Error: Swift.Error {
        case noEditingStateProvided
    }
}
