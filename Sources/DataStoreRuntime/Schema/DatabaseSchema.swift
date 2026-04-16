//
//  DatabaseSchema.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

package import SQLiteStatement

package struct DatabaseSchema: Sendable {
    nonisolated package let indexes: [any IndexDefinition]
    nonisolated package let tables: [any TableDefinition]
    
    nonisolated package init(
        indexes: [any IndexDefinition],
        tables: [any TableDefinition]
    ) {
        self.indexes = indexes
        self.tables = tables
    }
}
