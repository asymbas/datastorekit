//
//  SQLStatementShortcut.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

#if swift(>=6.2) && !SwiftPlaygrounds

public struct SQLStatementShortcut: ~Copyable, Sendable {
    nonisolated package let handle: SQLite
    nonisolated package var transaction: (any DatabaseTransaction)?
    
    nonisolated package init(
        handle: SQLite,
        transaction: (any DatabaseTransaction)? = nil
    ) {
        self.handle = handle
        self.transaction = transaction
    }
}

#else

public struct SQLStatementShortcut: Sendable {
    nonisolated package let handle: SQLite
    nonisolated package var transaction: (any DatabaseTransaction)?
    
    nonisolated package init(
        handle: SQLite,
        transaction: (any DatabaseTransaction)? = nil
    ) {
        self.handle = handle
        self.transaction = transaction
    }
}

#endif
