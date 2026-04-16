//
//  IndexDefinition.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreSQL
private import DataStoreSupport
public import Foundation
public import SQLSupport

public protocol IndexDefinition: SQLFragment {
    nonisolated var schema: String? { get }
    nonisolated var name: String { get }
    nonisolated var table: String { get }
    nonisolated var columns: [any IndexedColumnDefinition] { get }
    nonisolated var predicate: (any SQLFragment)? { get }
    nonisolated var isUnique: Bool { get }
}

extension IndexDefinition {
    /// The index name as a delimited identifier.
    nonisolated public var identifier: String {
        quote(name)
    }
    
    nonisolated public var qualifiedName: String {
        schema == nil ? identifier : "\(quote(schema!)).\(identifier)"
    }
    
    nonisolated public var sql: String {
        var result = "\(qualifiedName) ON \(quote(table)) (\n"
        result += columns.map { $0.indent(1) }.joined(separator: ",\n")
        result += "\n)"
        if let predicate = self.predicate { result += " WHERE \(predicate.sql)" }
        return result
    }
}

public final class SQLIndex: IndexDefinition, @unchecked Sendable {
    nonisolated public final let schema: String?
    nonisolated public final let name: String
    nonisolated public final let table: String
    nonisolated public final let columns: [any IndexedColumnDefinition]
    nonisolated public final let predicate: (any SQLFragment)?
    nonisolated public final let isUnique: Bool
    
    nonisolated public init(
        schema: String? = nil,
        name: String,
        table: String,
        predicate: (any SQLFragment)? = nil,
        isUnique: Bool = false,
        @SQLIndexedColumnBuilder columns: () -> [any IndexedColumnDefinition]
    ) {
        self.schema = schema
        self.name = name
        self.table = table
        self.columns = columns()
        self.predicate = predicate
        self.isUnique = isUnique
    }
}

extension SQLIndex: Equatable {
    nonisolated public static func == (lhs: SQLIndex, rhs: SQLIndex) -> Bool {
        lhs.schema == rhs.schema &&
        lhs.name == rhs.name &&
        lhs.table == rhs.table &&
        lhs.isUnique == rhs.isUnique &&
        lhs.predicate?.sql == rhs.predicate?.sql &&
        lhs.columns.elementsEqual(rhs.columns) { left, right in
            left.name == right.name &&
            left.expression.sql == right.expression.sql &&
            left.collation == right.collation &&
            left.order == right.order
        }
    }
}

extension SortOrder {
    nonisolated fileprivate var sql: String {
        switch self {
        case .forward: "ASC"
        case .reverse: "DESC"
        }
    }
}

public protocol IndexedColumnDefinition: SQLFragment {
    nonisolated var name: String? { get }
    nonisolated var expression: any SQLFragment { get }
    nonisolated var collation: String? { get }
    nonisolated var order: SortOrder? { get }
}

public struct SQLIndexedColumn: IndexedColumnDefinition {
    nonisolated public let name: String?
    nonisolated public let expression: any SQLFragment
    nonisolated public let collation: String?
    nonisolated public let order: SortOrder?
    
    nonisolated public var sql: String {
        var parts = [expression.sql]
        if let collation = self.collation {
            parts.append("COLLATE \(quote(collation))")
        }
        if let order = self.order {
            parts.append(order.sql)
        }
        return parts.joined(separator: " ")
    }
    
    nonisolated public init(
        name: String,
        collation: String? = nil,
        order: SortOrder? = nil
    ) {
        self.name = name
        self.expression = Raw(quote(name))
        self.collation = collation
        self.order = order
    }
    
    nonisolated public init(
        _ expression: any SQLFragment,
        collation: String? = nil,
        order: SortOrder? = nil
    ) {
        self.name = nil
        self.expression = expression
        self.collation = collation
        self.order = order
    }
}

extension SQLIndexedColumn: Equatable {
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.name == rhs.name &&
        lhs.expression.sql == rhs.expression.sql &&
        lhs.collation == rhs.collation &&
        lhs.order == rhs.order
    }
}
