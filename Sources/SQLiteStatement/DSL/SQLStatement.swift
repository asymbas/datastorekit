//
//  SQLStatement.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import DataStoreSupport
import Foundation
import SQLSupport

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
    
    nonisolated public init(_ fragments: any SQLFragment..., bindings: [any Sendable]? = nil) {
        self.init(fragments: fragments, bindings: bindings)
    }
    
    nonisolated public init(_ fragments: [any SQLFragment], bindings: [any Sendable]? = nil) {
        self.init(fragments: fragments, bindings: bindings)
    }
    
    nonisolated public init(
        @SQLBuilder _ statement: () throws -> [any SQLFragment],
        bindings: [any Sendable]? = nil
    ) rethrows {
        self.init(fragments: try statement(), bindings: bindings)
    }
}

extension SQL: CustomStringConvertible {
    nonisolated public var description: String {
        "Bindings:\n\(bindings)\nSQL:\n\(sql)"
    }
}

public struct SQLForEach<Data: Sequence>: SQLFragment {
    nonisolated internal let fragments: [any SQLFragment]
    
    nonisolated public var sql: String {
        fragments.map(\.sql).joined(separator: "\n")
    }
    
    nonisolated public var bindings: [any Sendable] {
        fragments.flatMap(\.bindings)
    }
    
    nonisolated public init(
        _ data: Data,
        @SQLBuilder statement: (Data.Element) -> [any SQLFragment]
    ) {
        self.fragments = data.flatMap { statement($0) }
    }
}

public struct With: SQLFragment {
    nonisolated private let ctes: [CommonTableExpression]
    
    nonisolated public var sql: String {
        let recursive = self.ctes.contains(where: \.recursive) ? "WITH RECURSIVE\n" : "WITH\n"
        return recursive + Raw(ctes.map(\.sql).joined(separator: ",\n")).indent(1)
    }
    
    nonisolated public var bindings: [any Sendable] {
        ctes.flatMap(\.bindings)
    }
    
    nonisolated public init(_ ctes: [CommonTableExpression]) {
        self.ctes = ctes
    }
    
    nonisolated public init(_ ctes: CommonTableExpression...) {
        self.init(ctes)
    }
    
    nonisolated public init(@CommonTableExpressionBuilder _ statement: () throws -> [CommonTableExpression]) rethrows {
        self.init(try statement())
    }
}

public struct CommonTableExpression: SQLFragment {
    nonisolated public let name: String
    nonisolated public let statement: SQL
    nonisolated public let recursive: Bool
    
    nonisolated public var sql: String {
        "\(quote(name)) AS (\n\(statement.indent(1))\n)"
    }
    
    nonisolated public var bindings: [any Sendable] {
        statement.bindings
    }
    
    nonisolated public init(
        _ name: String,
        _ statement: SQL,
        recursive: Bool = false
    ) {
        self.name = name
        self.statement = statement
        self.recursive = recursive
    }
    
    nonisolated public init(
        _ name: String,
        @SQLBuilder _ statement: () throws -> [any SQLFragment],
        recursive: Bool = false
    ) rethrows {
        self.init(name, SQL(try statement()), recursive: recursive)
    }
}

@resultBuilder public enum CommonTableExpressionBuilder {
    public typealias Component = CommonTableExpression
    
    nonisolated public static func buildExpression(_ expression: SQLForEach<[Component]>) -> [Component] {
        expression.fragments.compactMap { $0 as? Component }
    }
    
    nonisolated public static func buildExpression(_ expression: [Component]) -> [CommonTableExpression] {
        expression
    }
    
    nonisolated public static func buildExpression(_ expression: String) -> [any SQLFragment] {
        [Raw(expression)]
    }
    
    nonisolated public static func buildBlock(_ component: [Component]...) -> [Component] {
        component.flatMap(\.self)
    }
}

public struct Select: SQLFragment {
    nonisolated private let columns: [String]
    nonisolated private let qualified: Bool
    
    nonisolated public var sql: String {
        if columns.isEmpty {
            return "SELECT *"
        } else {
            let joined = self.qualified
            ? columns.joined(separator: ",\n")
            : columns.map(quote).joined(separator: ",\n")
            return "SELECT \(joined)"
        }
    }
    
    nonisolated public var bindings: [any Sendable] {
        []
    }
    
    nonisolated public init(_ columns: [String], qualified: Bool = false) {
        self.columns = columns
        self.qualified = qualified
    }
    
    nonisolated public init(_ columns: String..., qualified: Bool = false) {
        self.init(columns, qualified: qualified)
    }
    
    nonisolated public init(_ literal: Int) {
        self.init(["\(literal)"], qualified: true)
    }
    
    nonisolated public init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(SQL(try statement()).sql, qualified: true)
    }
    
    nonisolated public init(@SQLBuilder _ statement: () throws -> [[any SQLFragment]]) rethrows {
        self.init(SQL(try statement().flatMap(\.self)).sql, qualified: true)
    }
}

public struct From: SQLFragment {
    nonisolated private let table: String
    nonisolated private let alias: String?
    
    nonisolated public var sql: String {
        if let alias {
            return "FROM \(quote(table)) AS \(quote(alias))"
        } else {
            return "FROM \(quote(table))"
        }
    }
    
    nonisolated public var bindings: [any Sendable] {
        []
    }
    
    nonisolated public init(_ table: String, as alias: String? = nil) {
        self.table = table
        self.alias = alias
    }
}

public enum JoinType: String, Sendable {
    case inner = "JOIN"
    case left = "LEFT JOIN"
    case right = "RIGHT JOIN"
    case full = "FULL JOIN"
}

public struct Join: SQLFragment {
    nonisolated private let type: JoinType
    nonisolated private let table: String
    nonisolated private let alias: String?
    nonisolated private let condition: String
    nonisolated public let bindings: [any Sendable]
    nonisolated public var metadata: TableReference?
    
    nonisolated public var sourceAlias: String? {
        metadata?.sourceAlias
    }
    
    nonisolated public var sourceTable: String? {
        metadata?.sourceTable
    }
    
    nonisolated public var sourceColumn: String {
        metadata?.sourceColumn ?? "INVALID"
    }
    
    nonisolated public var destinationAlias: String? {
        metadata?.destinationAlias
    }
    
    nonisolated public var destinationTable: String? {
        metadata?.destinationTable
    }
    
    nonisolated public var destinationColumn: String {
        metadata?.destinationColumn ?? "INVALID"
    }
    
    nonisolated public var lhsAlias: String? {
        sourceAlias
    }
    
    nonisolated public var lhsTable: String? {
        sourceTable
    }
    
    nonisolated public var lhsColumn: String {
        sourceColumn
    }
    
    nonisolated public var rhsAlias: String? {
        destinationAlias
    }
    
    nonisolated public var rhsTable: String? {
        destinationTable
    }
    
    nonisolated public var rhsColumn: String {
        destinationColumn
    }
    
    nonisolated public var sql: String {
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
    
    nonisolated public init(
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
    
    nonisolated public init(
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
    
    nonisolated public init(
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
    
    nonisolated public static func left(
        _ table: String,
        as alias: String? = nil,
        on left: (alias: String?, table: String?, column: String),
        equals right: (alias: String?, table: String?, column: String),
        bindings: [any Sendable] = []
    ) -> Self {
        .init(using: .left, table, as: alias, on: left, equals: right, bindings: bindings)
    }
    
    nonisolated public static func right(
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
    nonisolated public var description: String {
        "Joining from \(lhsAlias ?? "INVALID").\(lhsColumn) to \(rhsAlias ?? "INVALID").\(rhsColumn)"
    }
}

extension Join: CustomDebugStringConvertible {
    nonisolated public var debugDescription: String {
        let source = "\(lhsAlias ?? "INVALID").\(lhsTable ?? "INVALID").\(lhsColumn)"
        let destination = "\(rhsAlias ?? "INVALID").\(rhsTable ?? "INVALID").\(rhsColumn)"
        return "Source: \(source) -> Destination: \(destination)"
    }
}

public struct Where: SQLFragment {
    nonisolated private let clause: String
    nonisolated private let storage: [any Sendable]
    
    nonisolated public var sql: String {
        "WHERE \(clause)"
    }
    
    nonisolated public var bindings: [any Sendable] {
        storage
    }
    
    nonisolated public init(_ clause: String, bindings: [any Sendable]) {
        self.clause = clause
        self.storage = bindings
    }
    
    nonisolated public init(_ clause: String, bindings: any Sendable...) {
        self.init(clause, bindings: bindings)
    }
    
    nonisolated public init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        let fragments = try statement()
        self.init(
            fragments.map(\.sql).joined(separator: " "),
            bindings: fragments.map(\.bindings).flatMap(\.self)
        )
    }
    
    nonisolated public init(
        _ lhs: (table: String, column: String),
        equals rhs: (table: String, column: String),
        bindings: [any Sendable] = []
    ) {
        let lhs = "\(quote(lhs.table)).\(quote(lhs.column))"
        let rhs = "\(quote(rhs.table)).\(quote(rhs.column))"
        self.init("\(lhs) = \(rhs)", bindings: bindings)
    }
    
    nonisolated public init(_ predicates: [Self.Predicate]) {
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
    
    nonisolated public init(_ predicates: Self.Predicate...) {
        self.init(predicates)
    }
    
    public enum Predicate {
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
        
        public enum LogicalOperator: String {
            case and = "AND"
            case or = "OR"
        }
    }
}

public struct Exists: SQLFragment {
    nonisolated private let subquery: SQL
    
    nonisolated public var sql: String {
        "EXISTS (\n\(subquery.indent(1))\n)"
    }
    
    nonisolated public var bindings: [any Sendable] {
        subquery.bindings
    }
    
    nonisolated public init(_ subquery: SQL) {
        self.subquery = subquery
    }
    
    nonisolated public init(@SQLBuilder _ builder: () throws -> [any SQLFragment]) rethrows {
        self.init(try SQL(builder))
    }
}

public struct Subquery: SQLFragment {
    nonisolated private let statement: SQL
    nonisolated private let alias: String
    
    nonisolated public var sql: String {
        "(\n\(statement.indent(1))\n) AS \(quote(alias))"
    }
    
    nonisolated public var bindings: [any Sendable] {
        statement.bindings
    }
    
    nonisolated public init(_ statement: SQL, as alias: String) {
        self.statement = statement
        self.alias = alias
    }
}

public struct And: SQLFragment {
    nonisolated private let clauses: [any SQLFragment]
    
    nonisolated public var sql: String {
        "AND \(clauses.map(\.sql).joined(separator: "\n"))"
    }
    
    nonisolated public var bindings: [any Sendable] {
        clauses.flatMap(\.bindings)
    }
    
    nonisolated public init(_ clauses: any SQLFragment...) {
        self.clauses = clauses
    }
    
    nonisolated public init(_ clauses: [any SQLFragment]) {
        self.clauses = clauses
    }
    
    nonisolated public init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try statement())
    }
}

public struct Or: SQLFragment {
    nonisolated private let clauses: [any SQLFragment]
    
    nonisolated public var sql: String {
        "OR \(clauses.map(\.sql).joined(separator: "\n"))"
    }
    
    nonisolated public var bindings: [any Sendable] {
        clauses.flatMap(\.bindings)
    }
    
    nonisolated public init(_ clauses: any SQLFragment...) {
        self.clauses = clauses
    }
    
    nonisolated public init(_ clauses: [any SQLFragment]) {
        self.clauses = clauses
    }
    
    nonisolated public init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try statement())
    }
}

public struct Coalesce: SQLFragment {
    nonisolated private let expressions: [any SQLFragment]
    
    nonisolated public var sql: String {
        "COALESCE (\(expressions.map(\.sql).joined(separator: ", ")))"
    }
    
    nonisolated public var bindings: [any Sendable] {
        expressions.flatMap(\.bindings)
    }
    
    nonisolated public init(_ expressions: [any SQLFragment]) {
        self.expressions = expressions
    }
    
    nonisolated public init(_ expressions: any SQLFragment...) {
        self.expressions = expressions
    }
    
    nonisolated public init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try statement())
    }
}

public struct Parenthesis: SQLFragment {
    nonisolated private let inner: SQL
    
    nonisolated public var sql: String {
        "(\n" + inner.indent(1) + "\n)"
    }
    
    nonisolated public var bindings: [any Sendable] { inner.bindings }
    
    nonisolated public init(_ inner: SQL) {
        self.inner = inner
    }
    
    nonisolated public init(@SQLBuilder _ statement: () throws -> [any SQLFragment]) rethrows {
        self.init(try SQL(statement))
    }
}

public enum SQLSort: Sendable {
    case ascending(String, isOptional: Bool = false)
    case descending(String, isOptional: Bool = false)
    case random
}

extension SQLSort {
    nonisolated public init(_ qualifiedName: String, isOptional: Bool, order: SortOrder) {
        switch order {
        case .forward: self = .ascending(qualifiedName, isOptional: isOptional)
        case .reverse: self = .descending(qualifiedName, isOptional: isOptional)
        }
    }
}

public struct OrderBy: SQLFragment {
    nonisolated private let sorts: [SQLSort]
    
    nonisolated public var sql: String {
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
    
    public var bindings: [any Sendable] {
        []
    }
    
    nonisolated public init(_ sorts: [SQLSort]) {
        self.sorts = sorts
    }
    
    nonisolated public init(_ sorts: SQLSort...) {
        self.init(sorts)
    }
}

public struct Limit: SQLFragment {
    nonisolated private let value: Int
    
    nonisolated public var sql: String {
        "LIMIT \(value)"
    }
    
    nonisolated public var bindings: [any Sendable] {
        []
    }
    
    nonisolated public init(_ value: Int) {
        self.value = value
    }
}

public struct Offset: SQLFragment {
    nonisolated private let value: Int
    
    nonisolated public var sql: String {
        "OFFSET \(value)"
    }
    
    nonisolated public var bindings: [any Sendable] {
        []
    }
    
    nonisolated public init(_ value: Int) {
        self.value = value
    }
}

public struct Returning: SQLFragment {
    nonisolated private let columns: [String]
    
    nonisolated public var sql: String {
        "RETURNING " + columns.map(quote).joined(separator: ", ")
    }
    
    nonisolated public var bindings: [any Sendable] {
        []
    }
    
    nonisolated public init(_ columns: [String]) {
        self.columns = columns
    }
    
    nonisolated public init(_ columns: String...) {
        self.init(columns)
    }
}

public struct SQLExpression: SQLFragment {}

public enum ReferentialAction: String, SQLFragment {
    case setNull = "SET NULL"
    case setDefault = "SET DEFAULT"
    case cascade = "CASCADE"
    case restrict = "RESTRICT"
    case noAction = "NO ACTION"
}

public struct CreateIndex: SQLFragment {
    nonisolated public let ifNotExists: Bool
    nonisolated public let index: any IndexDefinition
    
    nonisolated public var sql: String {
        var parts: [String] = ["CREATE"]
        if index.isUnique { parts.append("UNIQUE") }
        parts.append("INDEX")
        if ifNotExists { parts.append("IF NOT EXISTS") }
        parts.append(index.sql)
        return parts.joined(separator: " ")
    }
    
    nonisolated public init(ifNotExists: Bool = false, index: any IndexDefinition) {
        self.ifNotExists = ifNotExists
        self.index = index
    }
}

public struct ForeignKey: SQLFragment {
    nonisolated public let sql: String
    
    nonisolated private init(@SQLBuilder fragments: () -> [any SQLFragment]) {
        self.sql = fragments().map(\.sql).joined(separator: " ")
    }
    
    nonisolated public static func references(
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
    
    public struct Deferrable: SQLFragment {
        nonisolated public static var deferrable: Self {
            .init("DEFERRABLE")
        }
        
        nonisolated public static var notDeferrable: Self {
            .init("NOT DEFERRABLE")
        }
        
        nonisolated public var initiallyDeferred: Self {
            .init(self.sql + " INITIALLY DEFERRED")
        }
        
        nonisolated public var initiallyImmediate: Self {
            .init(self.sql + " INITIALLY IMMEDIATE")
        }
        
        nonisolated public var sql: String
        
        nonisolated private init(_ sql: String) {
            self.sql = sql
        }
    }
}

public struct OnConflict: SQLFragment {
    nonisolated private var action: Action
    nonisolated public init(_ action: Action) {
        self.action = action
    }
    
    nonisolated public var sql: String {
        "ON CONFLICT" + " " + action.rawValue.uppercased()
    }
    
    public enum Action: String, Sendable {
        case rollback
        case abort
        case fail
        case ignore
        case replace
    }
}
