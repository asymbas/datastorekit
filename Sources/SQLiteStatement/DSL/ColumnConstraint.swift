//
//  ColumnConstraint.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import Foundation
import SQLSupport

package struct ColumnConstraint: SQLFragment {
    nonisolated internal var order: Int
    nonisolated package var sql: String
    
    nonisolated private init(order: Int, @SQLBuilder fragments: () -> [any SQLFragment]) {
        self.order = order
        self.sql = fragments().map(\.sql).joined(separator: " ")
    }
    
    nonisolated package static var primaryKey: Self {
        .init(order: 0) {
            "PRIMARY KEY"
        }
    }
    
    nonisolated package static func primaryKey(
        order: SortOrder? = nil,
        onConflict: OnConflict? = nil,
        autoIncrement: Bool? = nil
    ) -> Self {
        .init(order: 0) {
            "PRIMARY KEY"
            if let order { order == .forward ? "ASC" : "DESC" }
            if let onConflict { onConflict.sql }
            if autoIncrement != nil { "AUTOINCREMENT" }
        }
    }
    
    nonisolated package static var notNull: Self {
        .init(order: 1) {
            "NOT NULL"
        }
    }
    
    nonisolated package static func notNull(_ onConflict: OnConflict? = nil) -> Self {
        .init(order: 1) {
            "NOT NULL"
            if let onConflict { onConflict.sql }
        }
    }
    
    nonisolated package static var unique: Self {
        .init(order: 2) {
            "UNIQUE"
        }
    }
    
    nonisolated package static func unique(_ onConflict: OnConflict? = nil) -> Self {
        .init(order: 2) {
            "UNIQUE"
            if let onConflict { onConflict.sql }
        }
    }
    
    nonisolated package static func check(_ expression: [SQLExpression]) -> Self {
        guard !expression.isEmpty else {
            fatalError("CHECK column constraint must provide at least one expression.")
        }
        return .init(order: 3) {
            "CHECK (\(expression.map(\.sql).joined(separator: ", ")))"
        }
    }
    
    nonisolated package static func check(_ expression: SQLExpression...) -> Self {
        Self.check(expression)
    }
    
    nonisolated package static func defaultValue(_ value: Any) -> Self {
        .init(order: 4) {
            "DEFAULT \(SQLValue(any: value))"
        }
    }
    
    nonisolated package static func collate(_ collationName: String) -> Self {
        .init(order: 5) {
            "COLLATE \(collationName)"
        }
    }
    
    nonisolated package static func references(
        _ referencedTable: String,
        _ referencedColumn: String,
        onDelete: ReferentialAction? = nil,
        onUpdate: ReferentialAction? = nil,
        match name: String? = nil,
        deferrable: ForeignKey.Deferrable? = nil
    ) -> Self {
        return .init(order: 6) {
            "REFERENCES \(quote(referencedTable)) (\(referencedColumn))"
            if let name { "MATCH \(name)" }
            if let onDelete { "ON DELETE \(onDelete.rawValue)" }
            if let onUpdate { "ON UPDATE \(onUpdate.rawValue)" }
            if let deferrable { deferrable.sql }
        }
    }
    
    nonisolated package static func foreignKey(_ foreignKey: ForeignKey) -> Self {
        .init(order: 6) {
            foreignKey.sql
        }
    }
    
    nonisolated package static func constraint(
        generatedAlways: Bool = true,
        as expression: [SQLExpression],
        isStored: Bool?,
        isVirtual: Bool?
    ) -> Self {
        .init(order: 7) {
            if generatedAlways { "GENERATED ALWAYS" }
            if !expression.isEmpty {
                "(" + expression.map(\.sql).joined(separator: ", ") + ")"
            }
            if isStored != nil { "STORED" }
            if isVirtual != nil { "VIRTUAL" }
        }
    }
    
    nonisolated package static func constraint(
        generatedAlways: Bool = true,
        as expression: SQLExpression...,
        isStored: Bool?,
        isVirtual: Bool?
    ) -> Self {
        Self.constraint(
            generatedAlways: generatedAlways,
            as: expression,
            isStored: isStored,
            isVirtual: isVirtual
        )
    }
}
