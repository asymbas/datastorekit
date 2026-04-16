//
//  DatabaseTransaction.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Logging
public import DataStoreCore
public import Foundation

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

public protocol DatabaseTransaction: AnyObject & Sendable {
    associatedtype Store: DatabaseProtocol
    typealias Handle = Store.Handle
    nonisolated var handle: Handle? { get }
    nonisolated var timestamp: Date { get }
    nonisolated var storeIdentifier: String { get }
    nonisolated var editingState: any EditingStateProviding { get }
    nonisolated var transactionIdentifier: Int64 { get }
    nonisolated var startingTotalRowChanges: Handle.Count { get }
    nonisolated var lastObservedTotalRowChanges: Handle.Count { get set }
    nonisolated func transactionWillBegin() throws
    nonisolated func transactionDidCommit()
    nonisolated func transactionDidRollback()
    
    nonisolated func didInsertRow(
        columns: [String],
        values: [any Sendable],
        in table: String,
        primaryKey: some LosslessStringConvertible & Sendable
    )
    
    nonisolated func didUpdateRow(
        for primaryKey: some LosslessStringConvertible & Sendable,
        in table: String,
        columns: [String],
        oldValues: [any Sendable]?,
        newValues: [any Sendable]
    )
    
    nonisolated func didDeleteRow(
        _ primaryKey: some LosslessStringConvertible & Sendable,
        in table: String,
        preservedColumns: [String]?,
        preservedValues: [any Sendable]?
    )
}

extension DatabaseTransaction {
    nonisolated public var hasChanges: Bool {
        startingTotalRowChanges != lastObservedTotalRowChanges
    }
    
    nonisolated package func informDidInsertRow(
        for primaryKey: some LosslessStringConvertible & Sendable,
        in table: String,
        columns: [String],
        values: [any Sendable]
    ) {
        guard consumeMutationIfNeeded() else { return }
        if handle?.totalRowChanges == startingTotalRowChanges { return }
        didInsertRow(columns: columns, values: values, in: table, primaryKey: primaryKey)
    }
    
    nonisolated package func informDidUpdateRow(
        for primaryKey: some LosslessStringConvertible & Sendable,
        in table: String,
        columns: [String],
        oldValues: [any Sendable]?,
        newValues: [any Sendable]
    ) {
        guard consumeMutationIfNeeded() else { return }
        if handle?.totalRowChanges == startingTotalRowChanges { return }
        didUpdateRow(
            for: primaryKey,
            in: table,
            columns: columns,
            oldValues: oldValues,
            newValues: newValues
        )
    }
    
    nonisolated package func informDidDeleteRow(
        _ primaryKey: some LosslessStringConvertible & Sendable,
        in table: String,
        preservedColumns: [String]?,
        preservedValues: [any Sendable]?
    ) {
        guard consumeMutationIfNeeded() else { return }
        if handle?.totalRowChanges == startingTotalRowChanges { return }
        didDeleteRow(
            primaryKey,
            in: table,
            preservedColumns: preservedColumns,
            preservedValues: preservedValues
        )
    }
    
    nonisolated private func consumeMutationIfNeeded() -> Bool {
        guard let handle = self.handle else { return false }
        let total = handle.totalRowChanges
        if total != lastObservedTotalRowChanges {
            self.lastObservedTotalRowChanges = total
            return true
        } else {
            return false
        }
    }
}
