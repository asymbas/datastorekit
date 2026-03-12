//
//  DatabaseConnection+SQLite.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import Logging
import SQLiteStatement

nonisolated private let logger: Logger = .init(label: "com.asymbas.sqlite")

extension DatabaseConnection where Store.Handle == SQLite {
    /// The most recent SQLite error for this connection.
    nonisolated public var error: any Swift.Error {
        handle.error
    }
    
    /// The row ID for the last insert.
    nonisolated public var lastInsertedRowID: Int64 {
        handle.lastInsertedRowID
    }
    
    /// The number of rows changed by the last operation.
    nonisolated public var rowChanges: Int32 {
        handle.rowChanges
    }
    
    /// The total number of rows changed since the database connection was opened.
    nonisolated public var totalRowChanges: Int32 {
        handle.totalRowChanges
    }
    
    /// Indicates whether this connection has been interrupted.
    nonisolated public var isInterrupted: Bool {
        handle.isInterrupted
    }
    
    /// Interrupts any running operation on this connection.
    nonisolated public func interrupt() {
        handle.interrupt()
    }
    
    /// Closes the underlying SQLite handle.
    nonisolated public consuming func close() throws {
        try handle.close()
    }
    
    /// Executes an SQL statement and returns the underlying SQLite result.
    @discardableResult nonisolated public func execute(_ sql: String)
    throws -> Store.Handle.Result {
        try handle.execute(sql)
    }
    
    nonisolated public func withPreparedStatement<Result>(
        _ sql: String,
        bindings: [any Sendable] = [],
        body: (borrowing PreparedStatement) throws -> sending Result
    ) throws -> Result {
        try handle.withPreparedStatement(sql, bindings: bindings, body: body)
    }
    
    nonisolated public func fetch<Result>(
        _ sql: String,
        bindings: [any Sendable] = [],
        into result: consuming Result,
        body: @Sendable (inout Result, ResultRows.Element) -> Void
    ) throws -> Result where Result: Collection {
        try handle.fetch(sql, bindings: bindings, into: result, body: body)
    }
    
    /// Executes a query and returns rows as positional values.
    nonisolated public func fetch(_ sql: String, bindings: [any Sendable] = [])
    throws -> [[any Sendable]] {
        try handle.fetch(sql, bindings: bindings)
    }
    
    /// Executes a query and returns rows as positional values.
    nonisolated public func fetch(_ sql: String, bindings: (any Sendable)...)
    throws -> [[any Sendable]] {
        try handle.fetch(sql, bindings: bindings)
    }
    
    /// Executes a query and returns rows keyed by column name.
    nonisolated public func query(_ sql: String, bindings: [any Sendable] = [])
    throws -> [[String: any Sendable]] {
        try handle.query(sql, bindings: bindings)
    }
    
    /// Executes a query and returns rows keyed by column name.
    nonisolated public func query(_ sql: String, bindings: (any Sendable)...)
    throws -> [[String: any Sendable]] {
        try handle.query(sql, bindings: bindings)
    }
}

extension DatabaseConnection where Store.Handle == SQLite {
    nonisolated public func fetch(_ statement: SQL)
    throws -> [[any Sendable]] {
        try handle.fetch(statement.sql, bindings: statement.bindings)
    }
    
    nonisolated public func fetch(@SQLBuilder _ statement: () throws -> [any SQLFragment])
    throws -> [[any Sendable]] {
        try fetch(SQL(statement()))
    }
    
    nonisolated public func query(_ statement: SQL)
    throws -> [[String: any Sendable]] {
        try handle.query(statement.sql, bindings: statement.bindings)
    }
    
    nonisolated public func query(@SQLBuilder _ statement: () throws -> [any SQLFragment])
    throws -> [[String: any Sendable]] {
        try query(SQL(statement()))
    }
}

extension DatabaseConnection where Store.Handle == SQLite {
    nonisolated public func checkCancellation() throws {
        if Task.isCancelled {
            handle.interrupt()
            logger.debug("Task cancelled data store transaction.")
            throw CancellationError()
        }
    }
    
    nonisolated public nonmutating func withTransaction(
        _ transactionMode: Store.Handle.TransactionMode?,
        _ operation: () throws -> Void
    ) throws {
        try transaction(transactionMode)
        do {
            try operation()
            try commit()
        } catch {
            onTransactionFailure(self)
            logger.error("\(transactionMode) was rolled back by an error: \(error)")
            try rollback()
            throw error
        }
    }
    
    nonisolated package mutating func _withTransaction(
        _ transactionMode: Store.Handle.TransactionMode?,
        _ operation: (inout sending Self) throws -> Void
    ) throws {
        try transaction(transactionMode)
        do {
            try operation(&self)
            try commit()
        } catch {
            onTransactionFailure(self)
            logger.error("\(transactionMode) was rolled back by an error: \(error)")
            try rollback()
            throw error
        }
    }
    
    nonisolated public nonmutating func withTransaction(_ operation: () throws -> Void) throws {
        try withTransaction(nil, operation)
    }
    
    nonisolated public nonmutating func withImmediateTransaction(_ operation: () throws -> Void) throws {
        try withTransaction(.immediate, operation)
    }
    
    nonisolated public nonmutating func withExclusiveTransaction(_ operation: () throws -> Void) throws {
        try withTransaction(.exclusive, operation)
    }
    
    nonisolated public func transaction(_ transactionMode: Store.Handle.TransactionMode? = nil) throws {
        guard let editingState = self.editingState else { fatalError() }
        guard transaction == nil else { return }
        self.transaction = queue?.makeTransaction(editingState, handle)
        do {
            try self.transaction?.transactionWillBegin()
            if let transactionMode {
                try execute("BEGIN \(transactionMode.rawValue) TRANSACTION")
            } else {
                try execute("BEGIN TRANSACTION")
            }
        } catch {
            self.transaction?.transactionDidRollback()
            self.transaction = nil
            throw error
        }
    }
    
    /// Commits the active transaction.
    nonisolated public func commit() throws {
        try execute("COMMIT TRANSACTION")
        self.transaction?.transactionDidCommit()
        self.transaction = nil
    }
    
    /// Rolls back the active transaction.
    nonisolated public func rollback() throws {
        try execute("ROLLBACK TRANSACTION")
        self.transaction?.transactionDidRollback()
        self.transaction = nil
    }
}
