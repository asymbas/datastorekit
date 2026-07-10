//
//  DatabaseConnection.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Foundation
private import Logging
private import Synchronization
package import SwiftData
public import DataStoreCore

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

/// Represents a leased database connection provided by `DatabaseQueue`.
public struct DatabaseConnection: ~Copyable, Sendable {
    nonisolated private let _transaction: AtomicLazyReference<Storage> = .init()
    nonisolated package var onTransactionFailure: @Sendable (borrowing Self) -> Void = { _ in }
    nonisolated package weak var queue: DatabaseQueue?
    nonisolated package let handle: SQLite
    nonisolated package let context: (any DatabaseContext)?
    nonisolated public let editingState: (any EditingStateProviding)?
    
    nonisolated package var provider: (any PersistentIdentifierProviding)? {
        context ?? attachment
    }
    
    nonisolated package var transaction: (any DatabaseTransaction)? {
        get { _transaction.load()?.transaction }
        nonmutating set {
            guard let newValue else { return }
            _ = _transaction.storeIfNil(.init(transaction: newValue))
        }
    }
    
    nonisolated package var attachment: (any DatabaseAttachment)? {
        queue?.attachment
    }
    
    nonisolated package var id: any Hashable {
        handle.id
    }
    
    nonisolated package var remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
    
    nonisolated package init(
        for editingState: (any EditingStateProviding)? = nil,
        queue: DatabaseQueue? = nil,
        handle: SQLite,
        context: (any DatabaseContext)? = nil,
        transaction: (any DatabaseTransaction)? = nil
    ) {
        self.editingState = editingState
        self.queue = queue
        self.handle = handle
        self.context = context
        if let transaction { _ = self._transaction.storeIfNil(.init(transaction: transaction)) }
        if let queue { self.onTransactionFailure = queue.onTransactionFailure }
    }
    
    nonisolated internal init(connection: consuming Self) {
        self.editingState = connection.editingState
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
    nonisolated public consuming func release() -> SQLite {
        logger.trace(
            "DatabaseConnection released.",
            metadata: ["id": "\(handle.id)", "queue_reference": "\(queue != nil)"]
        )
        if queue != nil { self.queue = nil }
        return handle
    }
    
    /// A shortcut for executing precomposed SQL statements.
    nonisolated public var execute: SQLStatementShortcut {
        .init(handle: handle, transaction: transaction)
    }
    
//    @discardableResult
//    nonisolated public func execute(_ sql: String) throws -> SQLite.Result {
//        try handle.execute(sql)
//    }
//
//    /// Executes a query and returns rows as positional values.
//    nonisolated public func fetch(_ sql: String, bindings: [any Sendable] = [])
//    throws -> [[any Sendable]] {
//        try handle.fetch(sql, bindings: bindings)
//    }
//
//    /// Executes a query and returns rows keyed by column name.
//    nonisolated public func query(_ sql: String, bindings: [any Sendable] = [])
//    throws -> [[String: any Sendable]] {
//        try handle.query(sql, bindings: bindings)
//    }
}

extension DatabaseConnection {
    nonisolated package func primaryKey<PrimaryKey: LosslessStringConvertible & Sendable>(
        for persistentIdentifier: PersistentIdentifier,
        as type: PrimaryKey.Type = String.self
    ) -> PrimaryKey {
        if let primaryKey = self.context?.primaryKey(for: persistentIdentifier, as: PrimaryKey.self) {
            return primaryKey
        }
        if let primaryKey = self.attachment?.primaryKey(for: persistentIdentifier, as: PrimaryKey.self) {
            return primaryKey
        }
        preconditionFailure()
    }
    
    nonisolated package func resolvedPersistentIdentifier(for persistentIdentifier: PersistentIdentifier)
    -> PersistentIdentifier? {
        context?.resolvedPersistentIdentifier(for: persistentIdentifier) ??
        attachment?.resolvedPersistentIdentifier(for: persistentIdentifier)
    }
}

extension DatabaseConnection {
    private final class Storage: Sendable {
        nonisolated fileprivate let transaction: any DatabaseTransaction
        
        nonisolated fileprivate init(transaction: any DatabaseTransaction) {
            self.transaction = transaction
        }
    }
    
    public enum Error: Swift.Error {
        case noEditingStateProvided
        case transactionUnavailable
    }
}
