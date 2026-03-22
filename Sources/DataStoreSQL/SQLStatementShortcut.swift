//
//  SQLStatementShortcut.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

#if swift(>=6.2) && !SwiftPlaygrounds

public struct SQLStatementShortcut<Handle>: ~Copyable, Sendable
where Handle: DatabaseHandle {
    nonisolated package let handle: Handle
    nonisolated package var transaction: (any DatabaseTransaction)?
    
    nonisolated package init(
        handle: Handle,
        transaction: (any DatabaseTransaction)? = nil
    ) {
        self.handle = handle
        self.transaction = transaction
    }
}

#else

public struct SQLStatementShortcut<Handle>: Sendable
where Handle: DatabaseHandle {
    nonisolated package let handle: Handle
    nonisolated package var transaction: (any DatabaseTransaction)?
    
    nonisolated package init(
        handle: Handle,
        transaction: (any DatabaseTransaction)? = nil
    ) {
        self.handle = handle
        self.transaction = transaction
    }
}

#endif
