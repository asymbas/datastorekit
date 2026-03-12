//
//  ColumnDefinition.swift
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

public protocol ColumnDefinition: SQLFragment {
    nonisolated var name: String { get }
    nonisolated var type: SQLType { get }
    nonisolated var constraints: [ColumnConstraint] { get }
}

extension ColumnDefinition {
    /// The column name as a delimited identifier.
    nonisolated public var identifier: String {
        quote(name)
    }
    
    nonisolated public var isOptional: Bool {
        constraints.contains(.notNull) == false
    }
    
    nonisolated public var isUnique: Bool {
        constraints.contains(.unique)
    }
    
    nonisolated public var sql: String {
        if !constraints.isEmpty {
            let constraints = OrderedSet(constraints).sorted(by: { $0.order < $1.order }).sql
            return "\(identifier) \(type) \(constraints)"
        } else {
            return "\(identifier) \(type)"
        }
    }
}

public struct SQLEmptyColumn: ColumnDefinition {
    nonisolated public let name: String
    nonisolated public let type: SQLType
    nonisolated public let constraints: [ColumnConstraint]
    
    nonisolated public init() {
        self.name = ""
        self.type = .null
        self.constraints = []
    }
}

public class SQLAttributeColumn: ColumnDefinition, @unchecked Sendable {
    nonisolated public let name: String
    nonisolated public let type: SQLType
    nonisolated public let constraints: [ColumnConstraint]
    
    nonisolated public init(
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
    
    nonisolated public convenience init(
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
    
    nonisolated public convenience init(
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

public final class SQLCompositeAttributeColumn: SQLAttributeColumn, @unchecked Sendable {
    nonisolated public let properties: [SQLAttributeColumn]
    
    nonisolated public init(
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
    
    nonisolated public convenience init(
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
    
    nonisolated public convenience init(
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
    
    nonisolated public convenience init(
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

public final class SQLRelationshipColumn: ColumnDefinition, @unchecked Sendable {
    nonisolated public let name: String
    nonisolated public let type: SQLType
    nonisolated public let constraints: [ColumnConstraint]
    
    nonisolated public init(
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
    
    nonisolated public convenience init(
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
    
    nonisolated public convenience init(
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

public final class SQLColumn: ColumnDefinition, @unchecked Sendable {
    nonisolated public let name: String
    nonisolated public let type: SQLType
    nonisolated public let constraints: [ColumnConstraint]
    nonisolated public let references: [TableReference]
    
    nonisolated public init(
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
    
    nonisolated public convenience init(
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
