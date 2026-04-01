//
//  TableDefinition.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Collections
import DataStoreSQL
import DataStoreSupport
import SQLSupport

package protocol TableDefinition: SQLFragment {
    nonisolated var schema: String? { get }
    nonisolated var name: String { get }
    nonisolated var columns: [any ColumnDefinition] { get }
    nonisolated var constraints: [TableConstraint] { get }
}

extension TableDefinition {
    /// The table name as a delimited identifier.
    nonisolated package var identifier: String {
        quote(name)
    }
    
    nonisolated package var qualifiedName: String {
        schema == nil ? identifier : "\(quote(schema!)).\(identifier)"
    }
    
    nonisolated package var sql: String {
        switch !constraints.isEmpty {
        case true:
            let constraints = OrderedSet(constraints)
                .sorted(by: { $0.order < $1.order })
                .map { $0.indent(1) }.joined(separator: ",\n")
            return """
                \(identifier) (
                \(columns.map { $0.indent(1) }.joined(separator: ",\n")),
                \(constraints)
                )
                """
        case false:
            return """
                \(identifier) (
                \(columns.map { $0.indent(1) }.joined(separator: ",\n"))
                )
                """
        }
    }
}

package final class SQLTable: TableDefinition, @unchecked Sendable {
    nonisolated package final let schema: String?
    nonisolated package final let name: String
    nonisolated package final let columns: [any ColumnDefinition]
    nonisolated package final let constraints: [TableConstraint]
    
    nonisolated package init(
        schema: String? = nil,
        name: String,
        constraints: [TableConstraint],
        @SQLColumnBuilder columns: () -> [any ColumnDefinition]
    ) {
        self.schema = schema
        self.name = name
        self.columns = columns()
        self.constraints = constraints
    }
    
    nonisolated package convenience init(
        schema: String? = nil,
        name: String,
        constraints: [TableConstraint?],
        @SQLColumnBuilder columns: () -> [any ColumnDefinition]
    ) {
        self.init(
            schema: schema,
            name: name,
            constraints: constraints.compactMap(\.self),
            columns: columns
        )
    }
}
