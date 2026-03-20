//
//  Attachment-TransactionAttachment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreRuntime
import DataStoreSQL
import DataStoreSupport
import SQLiteHandle
import SQLSupport
import Foundation
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

public final class TransactionObject: DatabaseTransaction {
    public typealias Store = DatabaseStore
    public typealias Handle = DatabaseStore.Handle
    #if swift(>=6.2)
    nonisolated public weak let handle: Handle?
    #else
    nonisolated(unsafe) public weak var handle: Handle?
    #endif
    nonisolated private let manager: ModelManager
    nonisolated public let timestamp: Date
    nonisolated public let storeIdentifier: String
    nonisolated public let editingState: any EditingStateProviding
    nonisolated public let transactionIdentifier: Int64
    nonisolated public let startingTotalRowChanges: Int32 = 0
    nonisolated private let _lastObservedTotalRowChanges: Atomic<Handle.Count> = .init(0)
    nonisolated private let _externalStorageTransaction: Mutex<ExternalStorageTransaction>
    
    nonisolated internal var externalStorageTransaction: ExternalStorageTransaction {
        get { _externalStorageTransaction.withLock(\.self) }
        set { _externalStorageTransaction.withLock { $0 = newValue } }
    }
    
    nonisolated private var author: String? {
        editingState.author
    }
    
    nonisolated public var lastObservedTotalRowChanges: Handle.Count {
        get { _lastObservedTotalRowChanges.load(ordering: .relaxed) }
        set { _lastObservedTotalRowChanges.store(newValue, ordering: .relaxed) }
    }
    
    nonisolated package init(
        handle: Handle,
        manager: ModelManager,
        storeIdentifier: String,
        externalStorageURL: URL,
        editingState: any EditingStateProviding
    ) {
        self.handle = handle
        self.timestamp = Date()
        self.transactionIdentifier = Int64(self.timestamp.timeIntervalSince1970 * 1_000_000)
        self.manager = manager
        self.storeIdentifier = storeIdentifier
        self.editingState = editingState
        do {
            self._externalStorageTransaction = .init(try ExternalStorageTransaction(baseURL: externalStorageURL))
        } catch {
            fatalError("Unable to initialize transaction: \(error)")
        }
    }
    
    nonisolated public func transactionWillBegin() throws {
        self.lastObservedTotalRowChanges = 0
        let result = self.manager.state.compareExchange(
            expected: .idle,
            desired: .transaction,
            ordering: .sequentiallyConsistent
        )
        guard result.exchanged else {
            throw DataStoreError.unsupportedFeature
        }
        logger.debug("Transaction will begin: \(result)")
    }
    
    nonisolated public func transactionDidCommit() {
        let result = self.manager.state.compareExchange(
            expected: .transaction,
            desired: .idle,
            ordering: .sequentiallyConsistent
        )
        try! externalStorageTransaction.commit()
        logger.debug("Transaction did commit: \(result)")
    }
    
    nonisolated public func transactionDidRollback() {
        _ = self.manager.state.compareExchange(
            expected: .transaction,
            desired: .idle,
            ordering: .sequentiallyConsistent
        )
        externalStorageTransaction.rollback()
    }
    
    nonisolated public func didInsertRow(
        columns: [String],
        values: [any Sendable],
        in table: String,
        primaryKey: some LosslessStringConvertible & Sendable
    ) {
        guard table != HistoryTable.tableName else { return }
        do {
            try record(.insert, tableName: table, primaryKey: primaryKey, context: nil)
        } catch {
            logger.error(
                "Failed to record insert history.",
                metadata: [
                    "table": "\(table)",
                    "columns": "\(columns)",
                    "values": "\(values)",
                    "error": "\(error)"
                ]
            )
        }
    }
    
    nonisolated public func didUpdateRow(
        for primaryKey: some LosslessStringConvertible & Sendable,
        in table: String,
        columns: [String],
        oldValues: [any Sendable]?,
        newValues: [any Sendable]
    ) {
        guard table != HistoryTable.tableName else { return }
        do {
            var affectedColumns = [String]()
            if let oldValues {
                for index in diff(
                    columns: columns,
                    old: oldValues,
                    new: newValues,
                    ignoring: [pk]
                ) {
                    affectedColumns.append(columns[index])
                }
            }
            guard affectedColumns.isEmpty == false else {
                logger.debug(
                    "No update to record in history.",
                    metadata: [
                        "table": "\(table)",
                        "primary key": "\(primaryKey)",
                        "columns": "\(columns)"
                    ]
                )
                return
            }
            let list = affectedColumns.joined(separator: ",")
            try record(.update, tableName: table, primaryKey: primaryKey, context: list)
        } catch {
            logger.error(
                "Failed to record update history.",
                metadata: [
                    "table": "\(table)",
                    "columns": "\(columns)",
                    "oldValues": "\(oldValues ?? [])",
                    "newValues": "\(newValues)",
                    "error": "\(error)"
                ]
            )
        }
    }
    
    nonisolated public func didDeleteRow(
        _ primaryKey: some LosslessStringConvertible & Sendable,
        in table: String,
        preservedColumns: [String]?,
        preservedValues: [any Sendable]?
    ) {
        guard table != HistoryTable.tableName else { return }
        do {
            var context: String?
            if let preservedColumns, let preservedValues {
                context = try makeDeleteContext(columns: preservedColumns, values: preservedValues)
            }
            try record(.delete, tableName: table, primaryKey: primaryKey, context: context)
        } catch {
            logger.error(
                "Failed to record delete history.",
                metadata: [
                    "table": "\(table)",
                    "preservedColumns": "\(preservedColumns ?? [])",
                    "preservedValues": "\(preservedValues ?? [])",
                    "error": "\(error)"
                ]
            )
        }
    }
    
    nonisolated private func record(
        _ event: DataStoreOperation,
        tableName: String,
        primaryKey: some LosslessStringConvertible & Sendable,
        context: (any Sendable)?
    ) throws {
        _ = try handle.unsafelyUnwrapped.fetch(
            """
            INSERT INTO "\(HistoryTable.tableName)" (
                "\(HistoryTable.event.rawValue)",
                "\(HistoryTable.timestamp.rawValue)",
                "\(HistoryTable.storeIdentifier.rawValue)",
                "\(HistoryTable.author.rawValue)",
                "\(HistoryTable.recordTarget.rawValue)",
                "\(HistoryTable.recordIdentifier.rawValue)",
                "\(HistoryTable.context.rawValue)"
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            bindings: [
                event.rawValue,
                transactionIdentifier,
                storeIdentifier,
                author,
                tableName,
                primaryKey,
                context
            ]
        )
        logger.info(
            "Recorded into transaction history.",
            metadata: [
                "event": "\(event)",
                "tableName": "\(tableName)",
                "primaryKey": "\(primaryKey)",
                "context": "\(context, default: "nil")"
            ]
        )
    }
    
    nonisolated private func makeDeleteContext(
        columns: [String],
        values: [any Sendable]
    ) throws -> String {
        try SQLValue.row(columns: columns, values: values)
    }
}
