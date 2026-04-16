//
//  ColumnDefinition.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Collections
private import DataStoreSupport
package import DataStoreSQL
package import SQLSupport

package protocol ColumnDefinition: SQLFragment {
    nonisolated var name: String { get }
    nonisolated var type: SQLType { get }
    nonisolated var constraints: [ColumnConstraint] { get }
}

extension ColumnDefinition {
    /// The column name as a delimited identifier.
    nonisolated package var identifier: String {
        quote(name)
    }
    
    nonisolated package var isOptional: Bool {
        constraints.contains(.notNull) == false
    }
    
    nonisolated package var isUnique: Bool {
        constraints.contains(.unique)
    }
    
    nonisolated package var sql: String {
        if !constraints.isEmpty {
            let constraints = OrderedSet(constraints).sorted(by: { $0.order < $1.order }).sql
            return "\(identifier) \(type) \(constraints)"
        } else {
            return "\(identifier) \(type)"
        }
    }
}

package struct SQLEmptyColumn: ColumnDefinition {
    nonisolated package let name: String
    nonisolated package let type: SQLType
    nonisolated package let constraints: [ColumnConstraint]
    
    nonisolated package init() {
        self.name = ""
        self.type = .null
        self.constraints = []
    }
}

package class SQLAttributeColumn: ColumnDefinition, @unchecked Sendable {
    nonisolated package let name: String
    nonisolated package let type: SQLType
    nonisolated package let constraints: [ColumnConstraint]
    
    nonisolated package init(
        name: String,
        valueType: Any.Type,
        constraints: [ColumnConstraint] = [],
        validate: ((Result<SQLAttributeColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.name = name
        self.constraints = constraints
        guard let type = SQLType(for: valueType) else {
            self.type = .null
            validate?(.failure(SQLError(.invalidType)))
            return
        }
        self.type = type
        validate?(.success(self))
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: ColumnConstraint?...,
        validate: ((Result<SQLAttributeColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            validate: validate
        )
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: [ColumnConstraint?],
        validate: ((Result<SQLAttributeColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            validate: validate
        )
    }
}

package final class SQLCompositeAttributeColumn: SQLAttributeColumn, @unchecked Sendable {
    nonisolated package let properties: [SQLAttributeColumn]
    
    nonisolated package init(
        name: String,
        valueType: Any.Type,
        constraints: [ColumnConstraint] = [],
        properties: [SQLAttributeColumn],
        validate: ((Result<SQLAttributeColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.properties = properties
        super.init(
            name: name,
            valueType: valueType,
            constraints: constraints,
            validate: validate
        )
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: ColumnConstraint?...,
        properties: SQLAttributeColumn?...,
        validate: ((Result<SQLAttributeColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            properties: properties.compactMap(\.self),
            validate: validate
        )
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: ColumnConstraint?...,
        validate: ((Result<SQLAttributeColumn, any Swift.Error>) -> Void)? = nil,
        @SQLColumnBuilder columns: () -> [any ColumnDefinition]
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            properties: columns().compactMap { $0 as? SQLAttributeColumn },
            validate: validate
        )
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: [ColumnConstraint?] = [],
        validate: ((Result<SQLAttributeColumn, any Swift.Error>) -> Void)? = nil,
        @SQLColumnBuilder columns: () -> [any ColumnDefinition]
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            properties: columns().compactMap { $0 as? SQLAttributeColumn },
            validate: validate
        )
    }
}

package final class SQLRelationshipColumn: ColumnDefinition, @unchecked Sendable {
    nonisolated package let name: String
    nonisolated package let type: SQLType
    nonisolated package let constraints: [ColumnConstraint]
    
    nonisolated package init(
        name: String,
        valueType: Any.Type,
        constraints: [ColumnConstraint] = [],
        validate: ((Result<SQLRelationshipColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.name = name
        self.constraints = constraints
        guard let type = SQLType(for: valueType) else {
            self.type = .null
            validate?(.failure(SQLError(.invalidType)))
            return
        }
        self.type = type
        validate?(.success(self))
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: ColumnConstraint?...,
        validate: ((Result<SQLRelationshipColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            validate: validate
        )
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: [ColumnConstraint?],
        validate: ((Result<SQLRelationshipColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            validate: validate
        )
    }
}

package final class SQLColumn: ColumnDefinition, @unchecked Sendable {
    nonisolated package let name: String
    nonisolated package let type: SQLType
    nonisolated package let constraints: [ColumnConstraint]
    nonisolated package let references: [TableReference]
    
    nonisolated package init(
        name: String,
        valueType: Any.Type,
        constraints: [ColumnConstraint] = [],
        references: [TableReference] = [],
        validate: ((Result<SQLColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.name = name
        self.constraints = constraints
        self.references = references
        guard let type = SQLType(for: valueType) else {
            self.type = .null
            validate?(.failure(SQLError(.invalidType)))
            return
        }
        self.type = type
        validate?(.success(self))
    }
    
    nonisolated package convenience init(
        name: String,
        valueType: Any.Type,
        constraints: ColumnConstraint?...,
        references: TableReference...,
        validate: ((Result<SQLColumn, any Swift.Error>) -> Void)? = nil
    ) {
        self.init(
            name: name,
            valueType: valueType,
            constraints: constraints.compactMap(\.self),
            references: references,
            validate: validate
        )
    }
}
