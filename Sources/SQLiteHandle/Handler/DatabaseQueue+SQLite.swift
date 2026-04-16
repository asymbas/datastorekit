//
//  DatabaseQueue+SQLite.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import DataStoreCore
public import DataStoreSQL

extension DatabaseQueue where Store.Handle == SQLite {
    nonisolated public convenience init(
        at location: SQLite.StoreType,
        flags: SQLite.Flags,
        writers: Int,
        readers: Int,
        attachment: Store.Attachment? = nil,
        makeTransactionAttachment:
        @escaping @Sendable (any EditingStateProviding, Store.Handle) -> Store.Transaction?,
        onTransactionFailure:
        (@Sendable (borrowing DatabaseConnection<Store>) -> Void)? = nil,
        onUpdate updateCallbackHook: @escaping DataChangeNotificationCallback
    ) throws {
        try self.init(
            writers: writers,
            readers: readers,
            attachment: attachment
        ) { editingState, handle in
            makeTransactionAttachment(editingState, handle)
        } onTransactionFailure: {
            onTransactionFailure?($0)
        } makeWriterConnection: { index in
            try SQLite(
                at: location,
                flags: flags,
                role: .writer,
                onChange: index == 0 ? updateCallbackHook : nil
            )
        } makeReaderConnection: { index in
            try SQLite(
                at: location,
                flags: [.readOnly, .fullMutex],
                role: .reader
            )
        }
    }
    
    nonisolated public convenience init(
        at location: SQLite.StoreType,
        flags: SQLite.Flags,
        size: Int,
        attachment: Store.Attachment? = nil,
        makeTransactionAttachment:
        @escaping @Sendable (any EditingStateProviding, Store.Handle) -> Store.Transaction?,
        onTransactionFailure:
        (@Sendable (borrowing DatabaseConnection<Store>) -> Void)? = nil,
        onUpdate updateCallbackHook: @escaping DataChangeNotificationCallback
    ) throws {
        precondition(size >= 1, "DatabaseQueue must contain at least one connection.")
        try self.init(
            at: location,
            flags: flags,
            writers: 1,
            readers: size - 1,
            attachment: attachment,
            makeTransactionAttachment: makeTransactionAttachment,
            onTransactionFailure: onTransactionFailure,
            onUpdate: updateCallbackHook
        )
    }
}
