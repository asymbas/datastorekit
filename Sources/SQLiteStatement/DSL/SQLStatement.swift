//
//  SQLStatement.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreSupport
package import DataStoreSQL
package import Foundation
public import SQLSupport

// TODO: Dismantle DSL usage from the package.

public struct SQL: SQLFragment {
    nonisolated private let fragments: [any SQLFragment]
    nonisolated private let storage: [any Sendable]?
    nonisolated public var sql: String
    
    nonisolated public var bindings: [any Sendable] {
        storage ?? fragments.flatMap(\.bindings)
    }
    
    nonisolated private init(fragments: [any SQLFragment], bindings: [any Sendable]? = nil) {
        self.storage = bindings
        self.fragments = fragments
        self.sql = fragments.map(\.sql).joined(separator: "\n")
    }
    
    nonisolated package init(_ fragments: any SQLFragment..., bindings: [any Sendable]? = nil) {
        self.init(fragments: fragments, bindings: bindings)
    }
    
    nonisolated package init(_ fragments: [any SQLFragment], bindings: [any Sendable]? = nil) {
        self.init(fragments: fragments, bindings: bindings)
    }
    
    nonisolated package init(
        @SQLBuilder _ statement: () throws -> [any SQLFragment],
        bindings: [any Sendable]? = nil
    ) rethrows {
        self.init(fragments: try statement(), bindings: bindings)
    }
}

// `Codable` conformance is required to be used in `#Predicate`.
extension SQL: Codable {
    /// Inherited from `Decodable.init(from:)`.
    nonisolated public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        let sql = try container.decode(String.self)
        self.fragments = []
        self.storage = nil
        self.sql = sql
    }
    
    /// Inherited from `Encodable.encode(to:)`.
    nonisolated public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(sql)
    }
}

extension SQL: CustomStringConvertible {
    nonisolated public var description: String {
        "Bindings:\n\(bindings)\nSQL:\n\(sql)"
    }
}

package struct SQLForEach<Data: Sequence>: SQLFragment {
    nonisolated internal let fragments: [any SQLFragment]
    
    nonisolated package var sql: String {
        fragments.map(\.sql).joined(separator: "\n")
    }
    
    nonisolated package var bindings: [any Sendable] {
        fragments.flatMap(\.bindings)
    }
    
    nonisolated package init(
        _ data: Data,
        @SQLBuilder statement: (Data.Element) -> [any SQLFragment]
    ) {
        self.fragments = data.flatMap { statement($0) }
    }
}

package struct With: SQLFragment {
    nonisolated private let ctes: [CommonTableExpression]
    
    nonisolated package var sql: String {
        let recursive = self.ctes.contains(where: \.recursive) ? "WITH RECURSIVE\n" : "WITH\n"
        return recursive + Raw(ctes.map(\.sql).joined(separator: ",\n")).indent(1)
    }
    
    nonisolated package var bindings: [any Sendable] {
        ctes.flatMap(\.bindings)
    }
    
    nonisolated package init(_ ctes: [CommonTableExpression]) {
        self.ctes = ctes
    }
    
    nonisolated package init(_ ctes: CommonTableExpression...) {
        self.init(ctes)
    }
    
    nonisolated package init(@CommonTableExpressionBuilder _ statement: () throws -> [CommonTableExpression]) rethrows {
        self.init(try statement())
    }
}

package struct CommonTableExpression: SQLFragment {
    nonisolated package let name: String
    nonisolated package let statement: SQL
    nonisolated package let recursive: Bool
    
    nonisolated package var sql: String {
        "\(quote(name)) AS (\n\(statement.indent(1))\n)"
    }
    
    nonisolated package var bindings: [any Sendable] {
        statement.bindings
    }
    
    nonisolated package init(
        _ name: String,
        _ statement: SQL,
        recursive: Bool = false
    ) {
        self.name = name
        self.statement = statement
        self.recursive = recursive
    }
    
    nonisolated package init(
        _ name: String,
        @SQLBuilder _ statement: () throws -> [any SQLFragment],
        recursive: Bool = false
    ) rethrows {
        self.init(name, SQL(try statement()), recursive: recursive)
    }
}

@resultBuilder package enum CommonTableExpressionBuilder {
    package typealias Component = CommonTableExpression
    
    nonisolated package static func buildExpression(_ expression: SQLForEach<[Component]>) -> [Component] {
        expression.fragments.compactMap { $0 as? Component }
    }
    
    nonisolated package static func buildExpression(_ expression: [Component]) -> [CommonTableExpression] {
        expression
    }
    
    nonisolated package static func buildExpression(_ expression: String) -> [any SQLFragment] {
        [Raw(expression)]
    }
    
    nonisolated package static func buildBlock(_ component: [Component]...) -> [Component] {
        component.flatMap(\.self)
    }
}

package struct Select: SQLFragment {
    nonisolated private let columns: [String]
    nonisolated private let qualified: Bool
    
    nonisolated package var sql: String {
        if columns.isEmpty {
            return "SELECT *"
        } else {
            let joined = self.qualified
            ? columns.joined(separator: ",\n")
            : columns.map(quote).joined(separator: ",\n")
            return "SELECT \(joined)"
        }
    }
    
    nonisolated package var bindings: [any Sendable] {
        []
    }
    
    nonisolated package init(_ columns: [String], qualified: Bool = false) {
        self.columns = columns
        self.qualified = qualified
    }
    
    nonisolated package init(_ columns: String..., qualified: Bool = false) {
        self.init(columns, qualified: qualified)
    }
    
    nonisolated package init(_ literal: Int) {
        self.init(["\(literal)"], qualified: true)
    }
    
    nonisolated package init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(SQL(try statement()).sql, qualified: true)
    }
    
    nonisolated package init(@SQLBuilder _ statement: () throws -> [[any SQLFragment]]) rethrows {
        self.init(SQL(try statement().flatMap(\.self)).sql, qualified: true)
    }
}

package struct From: SQLFragment {
    nonisolated private let table: String
    nonisolated private let alias: String?
    
    nonisolated package var sql: String {
        if let alias {
            return "FROM \(quote(table)) AS \(quote(alias))"
        } else {
            return "FROM \(quote(table))"
        }
    }
    
    nonisolated package var bindings: [any Sendable] {
        []
    }
    
    nonisolated package init(_ table: String, as alias: String? = nil) {
        self.table = table
        self.alias = alias
    }
}

package enum JoinType: String, Sendable {
    case inner = "JOIN"
    case left = "LEFT JOIN"
    case right = "RIGHT JOIN"
    case full = "FULL JOIN"
}

package struct Join: SQLFragment {
    nonisolated private let type: JoinType
    nonisolated private let table: String
    nonisolated private let alias: String?
    nonisolated private let condition: String
    nonisolated package let bindings: [any Sendable]
    nonisolated package var metadata: TableReference?
    
    nonisolated package var sourceAlias: String? {
        metadata?.sourceAlias
    }
    
    nonisolated package var sourceTable: String? {
        metadata?.sourceTable
    }
    
    nonisolated package var sourceColumn: String {
        metadata?.sourceColumn ?? "INVALID"
    }
    
    nonisolated package var destinationAlias: String? {
        metadata?.destinationAlias
    }
    
    nonisolated package var destinationTable: String? {
        metadata?.destinationTable
    }
    
    nonisolated package var destinationColumn: String {
        metadata?.destinationColumn ?? "INVALID"
    }
    
    nonisolated package var lhsAlias: String? {
        sourceAlias
    }
    
    nonisolated package var lhsTable: String? {
        sourceTable
    }
    
    nonisolated package var lhsColumn: String {
        sourceColumn
    }
    
    nonisolated package var rhsAlias: String? {
        destinationAlias
    }
    
    nonisolated package var rhsTable: String? {
        destinationTable
    }
    
    nonisolated package var rhsColumn: String {
        destinationColumn
    }
    
    nonisolated package var sql: String {
        if let metadata = self.metadata {
            let lhsKey = metadata.lhsAlias ?? metadata.lhsTable
            let rhsKey = metadata.rhsAlias ?? metadata.rhsTable
            let lhsCase = metadata.lhsAlias == nil
            ? quote(lhsColumn)
            : "\(quote(lhsKey)).\(quote(lhsColumn))"
            let rhsCase = metadata.rhsAlias == nil
            ? quote(rhsColumn)
            : "\(quote(rhsKey)).\(quote(rhsColumn))"
            var sql: String
            if let rhsAlias = metadata.rhsAlias {
                sql = "\(type.rawValue) \(quote(rhsTable ?? rhsColumn)) AS \(quote(rhsAlias))"
            } else {
                sql = "\(type.rawValue) \(quote(metadata.rhsTable))"
            }
            sql += " ON \(lhsCase) = \(rhsCase)"
            return sql
        } else if let alias {
            return "\(type.rawValue) \(table) AS \(alias) ON \(condition)"
        } else {
            return "\(type.rawValue) \(table) ON \(condition)"
        }
    }
    
    nonisolated package init(
        using type: JoinType = .inner,
        _ table: String,
        as alias: String? = nil,
        on condition: String,
        bindings: [any Sendable] = []
    ) {
        self.type = type
        self.table = quote(table)
        self.alias = alias == nil ? nil : quote(alias!)
        self.condition = condition
        self.bindings = bindings
    }
    
    nonisolated package init(
        using type: JoinType = .inner,
        _ table: String,
        as alias: String? = nil,
        on left: String,
        equals right: String,
        bindings: [any Sendable] = []
    ) {
        self.init(
            using: type,
            table,
            as: alias,
            on: "\(left) = \(right)",
            bindings: bindings
        )
    }
    
    nonisolated package init(
        using type: JoinType = .inner,
        _ table: String,
        as alias: String? = nil,
        on left: (alias: String?, table: String?, column: String),
        equals right: (alias: String?, table: String?, column: String),
        bindings: [any Sendable] = []
    ) {
        let lhs = "\(quote(left.alias ?? "")).\(quote(left.column))"
        let rhs = "\(quote(right.alias ?? "")).\(quote(right.column))"
        self.init(
            using: type,
            table,
            as: alias,
            on: "\(lhs) = \(rhs)",
            bindings: bindings
        )
        if (right.table == table) || (alias != nil && right.alias == alias!) {
            self.metadata = .init(
                sourceAlias: left.alias,
                sourceTable: left.table ?? "",
                sourceColumn: left.column,
                destinationAlias: right.alias,
                destinationTable: right.table ?? "",
                destinationColumn: right.column
            )
        } else {
            self.metadata = .init(
                sourceAlias: right.alias,
                sourceTable: right.table ?? "",
                sourceColumn: right.column,
                destinationAlias: left.alias,
                destinationTable: left.table ?? "",
                destinationColumn: left.column
            )
        }
    }
    
    nonisolated package static func left(
        _ table: String,
        as alias: String? = nil,
        on left: (alias: String?, table: String?, column: String),
        equals right: (alias: String?, table: String?, column: String),
        bindings: [any Sendable] = []
    ) -> Self {
        .init(using: .left, table, as: alias, on: left, equals: right, bindings: bindings)
    }
    
    nonisolated package static func right(
        _ table: String,
        as alias: String? = nil,
        on left: (alias: String?, table: String?, column: String),
        equals right: (alias: String?, table: String?, column: String),
        bindings: [any Sendable] = []
    ) -> Self {
        .init(using: .right, table, as: alias, on: left, equals: right, bindings: bindings)
    }
}

extension Join: CustomStringConvertible {
    nonisolated package var description: String {
        "Joining from \(lhsAlias ?? "INVALID").\(lhsColumn) to \(rhsAlias ?? "INVALID").\(rhsColumn)"
    }
}

extension Join: CustomDebugStringConvertible {
    nonisolated package var debugDescription: String {
        let source = "\(lhsAlias ?? "INVALID").\(lhsTable ?? "INVALID").\(lhsColumn)"
        let destination = "\(rhsAlias ?? "INVALID").\(rhsTable ?? "INVALID").\(rhsColumn)"
        return "Source: \(source) -> Destination: \(destination)"
    }
}

package struct Where: SQLFragment {
    nonisolated private let clause: String
    nonisolated private let storage: [any Sendable]
    
    nonisolated package var sql: String {
        "WHERE \(clause)"
    }
    
    nonisolated package var bindings: [any Sendable] {
        storage
    }
    
    nonisolated package init(_ clause: String, bindings: [any Sendable]) {
        self.clause = clause
        self.storage = bindings
    }
    
    nonisolated package init(_ clause: String, bindings: any Sendable...) {
        self.init(clause, bindings: bindings)
    }
    
    nonisolated package init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        let fragments = try statement()
        self.init(
            fragments.map(\.sql).joined(separator: " "),
            bindings: fragments.map(\.bindings).flatMap(\.self)
        )
    }
    
    nonisolated package init(
        _ lhs: (table: String, column: String),
        equals rhs: (table: String, column: String),
        bindings: [any Sendable] = []
    ) {
        let lhs = "\(quote(lhs.table)).\(quote(lhs.column))"
        let rhs = "\(quote(rhs.table)).\(quote(rhs.column))"
        self.init("\(lhs) = \(rhs)", bindings: bindings)
    }
    
    nonisolated package init(_ predicates: [Self.Predicate]) {
        func buildClause(_ predicate: Self.Predicate) -> (
            clause: String,
            bindings: [any Sendable]
        ) {
            switch predicate {
            case .equals(let column, let value):
                return ("\(quote(column)) = ?", [value])
            case .notEquals(let column, let value):
                return ("\(quote(column)) != ?", [value])
            case .greaterThan(let column, let value):
                return ("\(quote(column)) > ?", [value])
            case .lessThan(let column, let value):
                return ("\(quote(column)) < ?", [value])
            case .in(let column, let values):
                let placeholders = Array(repeating: "?", count: values.count).joined(separator: ", ")
                return ("\(quote(column)) IN (\(placeholders))", values)
            case .notIn(let column, let values):
                let placeholders = Array(repeating: "?", count: values.count).joined(separator: ", ")
                return ("\(quote(column)) NOT IN (\(placeholders))", values)
            case .isNull(let column):
                return ("\(quote(column)) IS NULL", [])
            case .isNotNull(let column):
                return ("\(quote(column)) IS NOT NULL", [])
            case .custom(let clause, let bindings):
                return (clause, bindings)
            case .group(let nested, let joinWith):
                let parts = nested.map(buildClause)
                let joinedClause = parts.map(\.clause).joined(separator: " \(joinWith.rawValue) ")
                let bindings = parts.flatMap(\.bindings)
                return ("(\(joinedClause))", bindings)
            }
        }
        let parts = predicates.map(buildClause)
        let clause = parts.map(\.clause).joined(separator: " AND ")
        let bindings = parts.flatMap(\.bindings)
        self.init(clause, bindings: bindings)
    }
    
    nonisolated package init(_ predicates: Self.Predicate...) {
        self.init(predicates)
    }
    
    package enum Predicate {
        case equals(column: String, value: any Sendable)
        case notEquals(column: String, value: any Sendable)
        case greaterThan(column: String, value: any Sendable)
        case lessThan(column: String, value: any Sendable)
        case `in`(column: String, values: [any Sendable])
        case notIn(column: String, values: [any Sendable])
        case isNull(column: String)
        case isNotNull(column: String)
        case group(_ predicates: [Self], joinWith: Self.LogicalOperator = .and)
        case custom(_ clause: String, _ bindings: [any Sendable])
        
        package enum LogicalOperator: String {
            case and = "AND"
            case or = "OR"
        }
    }
}

package struct Exists: SQLFragment {
    nonisolated private let subquery: SQL
    
    nonisolated package var sql: String {
        "EXISTS (\n\(subquery.indent(1))\n)"
    }
    
    nonisolated package var bindings: [any Sendable] {
        subquery.bindings
    }
    
    nonisolated package init(_ subquery: SQL) {
        self.subquery = subquery
    }
    
    nonisolated package init(@SQLBuilder _ builder: () throws -> [any SQLFragment]) rethrows {
        self.init(try SQL(builder))
    }
}

package struct Subquery: SQLFragment {
    nonisolated private let statement: SQL
    nonisolated private let alias: String
    
    nonisolated package var sql: String {
        "(\n\(statement.indent(1))\n) AS \(quote(alias))"
    }
    
    nonisolated package var bindings: [any Sendable] {
        statement.bindings
    }
    
    nonisolated package init(_ statement: SQL, as alias: String) {
        self.statement = statement
        self.alias = alias
    }
}

package struct And: SQLFragment {
    nonisolated private let clauses: [any SQLFragment]
    
    nonisolated package var sql: String {
        "AND \(clauses.map(\.sql).joined(separator: "\n"))"
    }
    
    nonisolated package var bindings: [any Sendable] {
        clauses.flatMap(\.bindings)
    }
    
    nonisolated package init(_ clauses: any SQLFragment...) {
        self.clauses = clauses
    }
    
    nonisolated package init(_ clauses: [any SQLFragment]) {
        self.clauses = clauses
    }
    
    nonisolated package init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try statement())
    }
}

package struct Or: SQLFragment {
    nonisolated private let clauses: [any SQLFragment]
    
    nonisolated package var sql: String {
        "OR \(clauses.map(\.sql).joined(separator: "\n"))"
    }
    
    nonisolated package var bindings: [any Sendable] {
        clauses.flatMap(\.bindings)
    }
    
    nonisolated package init(_ clauses: any SQLFragment...) {
        self.clauses = clauses
    }
    
    nonisolated package init(_ clauses: [any SQLFragment]) {
        self.clauses = clauses
    }
    
    nonisolated package init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try statement())
    }
}

package struct Coalesce: SQLFragment {
    nonisolated private let expressions: [any SQLFragment]
    
    nonisolated package var sql: String {
        "COALESCE (\(expressions.map(\.sql).joined(separator: ", ")))"
    }
    
    nonisolated package var bindings: [any Sendable] {
        expressions.flatMap(\.bindings)
    }
    
    nonisolated package init(_ expressions: [any SQLFragment]) {
        self.expressions = expressions
    }
    
    nonisolated package init(_ expressions: any SQLFragment...) {
        self.expressions = expressions
    }
    
    nonisolated package init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try statement())
    }
}

package struct Parenthesis: SQLFragment {
    nonisolated private let inner: SQL
    
    nonisolated package var sql: String {
        "(\n" + inner.indent(1) + "\n)"
    }
    
    nonisolated package var bindings: [any Sendable] { inner.bindings }
    
    nonisolated package init(_ inner: SQL) {
        self.inner = inner
    }
    
    nonisolated package init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try SQL(statement))
    }
}

package enum SQLSort: Sendable {
    case ascending(String, isOptional: Bool = false)
    case descending(String, isOptional: Bool = false)
    case random
}

extension SQLSort {
    nonisolated package init(_ qualifiedName: String, isOptional: Bool, order: SortOrder) {
        switch order {
        case .forward: self = .ascending(qualifiedName, isOptional: isOptional)
        case .reverse: self = .descending(qualifiedName, isOptional: isOptional)
        }
    }
}

package struct OrderBy: SQLFragment {
    nonisolated private let sorts: [SQLSort]
    
    nonisolated package var sql: String {
        let clauses = self.sorts.map {
            switch $0 {
            case .ascending(let statement, let isOptional):
                "\(isOptional ? "\(statement) IS NULL, " : "")\(statement) ASC"
            case .descending(let statement, let isOptional):
                "\(isOptional ? "\(statement) IS NULL, " : "")\(statement) DESC"
            case .random:
                "RANDOM()"
            }
        }
        return "ORDER BY " + clauses.joined(separator: ",\n")
    }
    
    package var bindings: [any Sendable] {
        []
    }
    
    nonisolated package init(_ sorts: [SQLSort]) {
        self.sorts = sorts
    }
    
    nonisolated package init(_ sorts: SQLSort...) {
        self.init(sorts)
    }
}

package struct Limit: SQLFragment {
    nonisolated private let value: Int
    
    nonisolated package var sql: String {
        "LIMIT \(value)"
    }
    
    nonisolated package var bindings: [any Sendable] {
        []
    }
    
    nonisolated package init(_ value: Int) {
        self.value = value
    }
}

package struct Offset: SQLFragment {
    nonisolated private let value: Int
    
    nonisolated package var sql: String {
        "OFFSET \(value)"
    }
    
    nonisolated package var bindings: [any Sendable] {
        []
    }
    
    nonisolated package init(_ value: Int) {
        self.value = value
    }
}

package struct Returning: SQLFragment {
    nonisolated private let columns: [String]
    
    nonisolated package var sql: String {
        "RETURNING " + columns.map(quote).joined(separator: ", ")
    }
    
    nonisolated package var bindings: [any Sendable] {
        []
    }
    
    nonisolated package init(_ columns: [String]) {
        self.columns = columns
    }
    
    nonisolated package init(_ columns: String...) {
        self.init(columns)
    }
}

package struct SQLExpression: SQLFragment {}

package enum ReferentialAction: String, SQLFragment {
    case setNull = "SET NULL"
    case setDefault = "SET DEFAULT"
    case cascade = "CASCADE"
    case restrict = "RESTRICT"
    case noAction = "NO ACTION"
}

package struct CreateIndex: SQLFragment {
    nonisolated package let ifNotExists: Bool
    nonisolated package let index: any IndexDefinition
    
    nonisolated package var sql: String {
        var parts: [String] = ["CREATE"]
        if index.isUnique { parts.append("UNIQUE") }
        parts.append("INDEX")
        if ifNotExists { parts.append("IF NOT EXISTS") }
        parts.append(index.sql)
        return parts.joined(separator: " ")
    }
    
    nonisolated package init(ifNotExists: Bool = false, index: any IndexDefinition) {
        self.ifNotExists = ifNotExists
        self.index = index
    }
}

package struct ForeignKey: SQLFragment {
    nonisolated package let sql: String
    
    nonisolated private init(@SQLBuilder fragments: () -> [any SQLFragment]) {
        self.sql = fragments().map(\.sql).joined(separator: " ")
    }
    
    nonisolated package static func references(
        _ foreignTable: String,
        _ foreignColumns: [String],
        onDelete: ReferentialAction? = nil,
        onUpdate: ReferentialAction? = nil,
        match name: String? = nil,
        deferrable: Self.Deferrable? = nil
    ) -> Self {
        self.init {
            let foreignColumns = foreignColumns.map(quote).joined(separator: ", ")
            "REFERENCES \(quote(foreignTable)) (\(foreignColumns))"
            if let name { "MATCH \(name)" }
            if let onDelete { "ON DELETE \(onDelete.rawValue)" }
            if let onUpdate { "ON UPDATE \(onUpdate.rawValue)" }
            if let deferrable { deferrable.sql }
        }
    }
    
    package struct Deferrable: SQLFragment {
        nonisolated package static var deferrable: Self {
            .init("DEFERRABLE")
        }
        
        nonisolated package static var notDeferrable: Self {
            .init("NOT DEFERRABLE")
        }
        
        nonisolated package var initiallyDeferred: Self {
            .init(self.sql + " INITIALLY DEFERRED")
        }
        
        nonisolated package var initiallyImmediate: Self {
            .init(self.sql + " INITIALLY IMMEDIATE")
        }
        
        nonisolated package var sql: String
        
        nonisolated private init(_ sql: String) {
            self.sql = sql
        }
    }
}

package struct OnConflict: SQLFragment {
    nonisolated private var action: Action
    nonisolated package init(_ action: Action) {
        self.action = action
    }
    
    nonisolated package var sql: String {
        "ON CONFLICT" + " " + action.rawValue.uppercased()
    }
    
    package enum Action: String, Sendable {
        case rollback
        case abort
        case fail
        case ignore
        case replace
    }
}
