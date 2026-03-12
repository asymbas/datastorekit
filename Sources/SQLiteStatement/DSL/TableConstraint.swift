//
//  TableConstraint.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import DataStoreSupport
import SQLSupport

public struct TableConstraint: SQLFragment {
    nonisolated internal var order: Int
    nonisolated public var sql: String
    
    nonisolated private init(order: Int, @SQLBuilder fragments: () -> [any SQLFragment]) {
        self.order = order
        self.sql = fragments().map(\.sql).joined(separator: " ")
    }
    
    nonisolated public static func constraint(name: String, using constraint: Self) -> Self {
        .init(order: constraint.order) {
            "CONSTRAINT \(quote(name)) \(constraint.sql)"
        }
    }
    
    nonisolated public static func primaryKey(
        _ indexedColumns: [String],
        onConflict: OnConflict? = nil
    ) -> Self {
        guard !indexedColumns.isEmpty else {
            fatalError("PRIMARY KEY table constraint must provide at least one indexed column.")
        }
        return .init(order: 0) {
            "PRIMARY KEY (\(indexedColumns.map(quote).joined(separator: ", ")))"
            if let onConflict { onConflict.sql }
        }
    }
    
    nonisolated public static func unique(
        _ indexedColumns: String...,
        onConflict: OnConflict? = nil
    ) -> Self {
        Self.unique(indexedColumns, onConflict: onConflict)
    }
    
    nonisolated public static func primaryKey(
        _ indexedColumns: String...,
        onConflict: OnConflict? = nil
    ) -> Self {
        self.primaryKey(indexedColumns, onConflict: onConflict)
    }
    
    nonisolated public static func unique(
        _ indexedColumns: [String],
        onConflict: OnConflict? = nil
    ) -> Self {
        guard !indexedColumns.isEmpty else {
            fatalError("UNIQUE table constraint must provide at least one indexed column.")
        }
        return .init(order: 1) {
            "UNIQUE (\(indexedColumns.map(quote).joined(separator: ", ")))"
            if let onConflict { onConflict.sql }
        }
    }
    
    nonisolated public static func check(_ expression: [SQLExpression]) -> Self {
        guard !expression.isEmpty else {
            fatalError("CHECK table constraint must provide at least one expression.")
        }
        return .init(order: 2) {
            "CHECK (\(expression.map(\.sql).joined(separator: ", ")))"
        }
    }
    
    nonisolated public static func check(_ expression: SQLExpression...) -> Self {
        Self.check(expression)
    }
    
    nonisolated public static func foreignKey(
        _ columns: [String],
        references referencedTable: String,
        at referencedColumns: [String],
        onDelete: ReferentialAction? = nil,
        onUpdate: ReferentialAction? = nil,
        match name: String? = nil,
        deferrable: ForeignKey.Deferrable? = nil
    ) -> Self {
        guard !columns.isEmpty else {
            fatalError("FOREIGN KEY table constraint must specify at least one column.")
        }
        guard !referencedColumns.isEmpty else {
            fatalError("FOREIGN KEY table constraint must specify at least one referenced column.")
        }
        return .init(order: 3) {
            "FOREIGN KEY (\(columns.map(quote).joined(separator: ", ")))"
            ForeignKey.references(
                referencedTable,
                referencedColumns,
                onDelete: onDelete,
                onUpdate: onUpdate,
                match: name,
                deferrable: deferrable
            ).sql
        }
    }
    
    nonisolated public static func foreignKey(
        _ columns: String...,
        references referencedTable: String,
        at referencedColumns: String...,
        onDelete: ReferentialAction? = nil,
        onUpdate: ReferentialAction? = nil,
        match name: String? = nil,
        deferrable: ForeignKey.Deferrable? = nil
    ) -> Self {
        Self.foreignKey(
            columns,
            references: referencedTable,
            at: referencedColumns,
            onDelete: onDelete,
            onUpdate: onUpdate,
            match: name,
            deferrable: deferrable
        )
    }
    
    nonisolated public static func foreignKey(
        _ columns: [String],
        clause foreignKey: ForeignKey
    ) -> Self {
        guard !columns.isEmpty else {
            fatalError("FOREIGN KEY table constraint must specify at least one column.")
        }
        return .init(order: 3) {
            "FOREIGN KEY (\(columns.map(quote).joined(separator: ", ")))"
            foreignKey.sql
        }
    }
    
    nonisolated public static func foreignKey(
        _ columns: String...,
        clause foreignKey: ForeignKey
    ) -> Self {
        Self.foreignKey(columns, clause: foreignKey)
    }
}
