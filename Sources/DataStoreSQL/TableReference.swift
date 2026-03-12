//
//  TableReference.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public struct TableReference: Codable, Equatable, Hashable, Sendable {
    nonisolated public var sourceAlias: String?
    nonisolated public var sourceTable: String
    nonisolated public var sourceColumn: String
    nonisolated public var destinationAlias: String?
    nonisolated public var destinationTable: String
    nonisolated public var destinationColumn: String
    
    nonisolated public var lhsAlias: String? {
        get { sourceAlias }
        set { sourceAlias = newValue }
    }
    
    nonisolated public var lhsTable: String {
        get { sourceTable }
        set { sourceTable = newValue }
    }
    
    nonisolated public var lhsColumn: String {
        get { sourceColumn }
        set { sourceColumn = newValue }
    }
    
    nonisolated public var rhsAlias: String? {
        get { destinationAlias }
        set { destinationAlias = newValue }
    }
    
    nonisolated public var rhsTable: String {
        get { destinationTable }
        set { destinationTable = newValue }
    }
    
    nonisolated public var rhsColumn: String {
        get { destinationColumn }
        set { destinationColumn = newValue }
    }
    
    nonisolated public func isOwningReference(forKey primaryKeyColumn: String? = nil) -> Bool {
        lhsColumn != primaryKeyColumn ?? pk
    }
    
    /// Indicates whether the source is referencing the destination from the same table.
    nonisolated public var isSelfReferencing: Bool {
        lhsTable == rhsTable
    }
    
    nonisolated public init(
        sourceAlias: String? = nil,
        sourceTable: String,
        sourceColumn: String,
        destinationAlias: String? = nil,
        destinationTable: String,
        destinationColumn: String
    ) {
        self.sourceAlias = sourceAlias
        self.sourceTable = sourceTable
        self.sourceColumn = sourceColumn
        self.destinationAlias = destinationAlias
        self.destinationTable = destinationTable
        self.destinationColumn = destinationColumn
    }
    
    nonisolated public init(
        lhsAlias: String? = nil,
        lhsTable: String,
        lhsColumn: String,
        rhsAlias: String? = nil,
        rhsTable: String,
        rhsColumn: String
    ) {
        self.sourceAlias = lhsAlias
        self.sourceTable = lhsTable
        self.sourceColumn = lhsColumn
        self.destinationAlias = rhsAlias
        self.destinationTable = rhsTable
        self.destinationColumn = rhsColumn
    }
}

extension TableReference: CustomStringConvertible {
    nonisolated public var description: String {
        if let lhsAlias = self.lhsAlias, let rhsAlias = self.rhsAlias {
            "\(lhsAlias).\(lhsColumn) -> \(rhsAlias).\(rhsColumn)"
        } else {
            "\(lhsTable).\(lhsColumn) -> \(rhsTable).\(rhsColumn)"
        }
    }
}

extension TableReference: CustomDebugStringConvertible {
    nonisolated public var debugDescription: String {
        let source: String
        let destination: String
        if let sourceAlias = self.sourceAlias {
            source = "\(sourceAlias).\(sourceTable).\(sourceColumn)"
        } else {
            source = "\(sourceTable).\(sourceColumn)"
        }
        if let destinationAlias = self.destinationAlias {
            destination = "\(destinationAlias).\(destinationTable).\(destinationColumn)"
        } else {
            destination = "\(destinationTable).\(destinationColumn)"
        }
        return "Source: \(source) -> Destination: \(destination)"
    }
}

public struct IntermediaryTableReference: Equatable, Hashable, Sendable {
    nonisolated public let name: String
    nonisolated public let lhsTable: String
    nonisolated public let lhsColumn: String
    nonisolated public let lhsForeignKey: String
    nonisolated public let rhsTable: String
    nonisolated public let rhsColumn: String
    nonisolated public let rhsForeignKey: String
    
    nonisolated package init(
        name: String,
        lhsTable: String,
        lhsColumn: String,
        lhsForeignKey: String = "0_pk",
        rhsTable: String,
        rhsColumn: String,
        rhsForeignKey: String = "1_pk"
    ) {
        self.name = name
        self.lhsTable = lhsTable
        self.lhsColumn = lhsColumn
        self.lhsForeignKey = lhsForeignKey
        self.rhsTable = rhsTable
        self.rhsColumn = rhsColumn
        self.rhsForeignKey = rhsForeignKey
    }
    
    /// Deterministically sets the paired tables into a consistent ordering.
    nonisolated public init(
        lhsTable: String,
        lhsColumn: String,
        rhsTable: String,
        rhsColumn: String
    ) {
        let ordered = [(lhsTable, lhsColumn), (rhsTable, rhsColumn)].sorted { $0.0 < $1.0 }
        let lhs = ordered[0]
        let rhs = ordered[1]
        self.init(
            name: "\(lhs.0)_\(lhs.1)_\(rhs.0)_\(rhs.1)",
            lhsTable: lhs.0,
            lhsColumn: lhs.1,
            lhsForeignKey: "0_pk",
            rhsTable: rhs.0,
            rhsColumn: rhs.1,
            rhsForeignKey: "1_pk"
        )
    }
    
    nonisolated package func join(
        from lhs: (alias: String?, name: String),
        to rhs: (alias: String?, name: String)
    ) -> TableReference? {
        switch (lhs.name, rhs.name) {
        case (self.lhsTable, self.name):
            return .init(
                sourceAlias: lhs.alias ?? lhsTable,
                sourceTable: lhsTable,
                sourceColumn: pk,
                destinationAlias: rhs.alias ?? name,
                destinationTable: name,
                destinationColumn: lhsForeignKey
            )
        case (self.name, self.rhsTable):
            return .init(
                sourceAlias: lhs.alias ?? name,
                sourceTable: name,
                sourceColumn: rhsForeignKey,
                destinationAlias: rhs.alias ?? rhsTable,
                destinationTable: rhsTable,
                destinationColumn: pk
            )
        case (self.rhsTable, self.name):
            return .init(
                sourceAlias: lhs.alias ?? rhsTable,
                sourceTable: rhsTable,
                sourceColumn: pk,
                destinationAlias: rhs.alias ?? name,
                destinationTable: name,
                destinationColumn: rhsForeignKey
            )
        case (self.name, self.lhsTable):
            return .init(
                sourceAlias: lhs.alias ?? name,
                sourceTable: name,
                sourceColumn: lhsForeignKey,
                destinationAlias: rhs.alias ?? lhsTable,
                destinationTable: lhsTable,
                destinationColumn: pk
            )
        default:
            return nil
        }
    }
    
    nonisolated package static func split(from intermediaryTable: String) -> (
        lhsTable: String,
        rhsTable: String
    )? {
        let parts = intermediaryTable.split(separator: "_").map(String.init)
        guard parts.count >= 4 else {
            return nil
        }
        let lhsTable = parts[0]
        let rhsTable = parts[2]
        return (lhsTable, rhsTable)
    }
}
