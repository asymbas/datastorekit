//
//  ConstraintViolation.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public struct ConstraintViolation: Hashable, Identifiable, Sendable {
    nonisolated public let kind: Kind
    nonisolated public let table: String
    nonisolated public let header: String
    nonisolated public let rowid: Int64?
    nonisolated public let parentTable: String?
    nonisolated public let fkid: Int64?
    nonisolated public let mappings: [Mapping]
    
    nonisolated public var id: String {
        "\(kind.rawValue):\(table):\(rowid ?? -1):\(fkid ?? -1):\(header)"
    }
    
    nonisolated public init(
        kind: Kind,
        table: String,
        header: String,
        rowid: Int64? = nil,
        parentTable: String? = nil,
        fkid: Int64? = nil,
        mappings: [Mapping] = []
    ) {
        self.kind = kind
        self.table = table
        self.header = header
        self.rowid = rowid
        self.parentTable = parentTable
        self.fkid = fkid
        self.mappings = mappings
    }
    
    public enum Kind: String, Sendable, Hashable {
        case foreignKey
        case unique
        case notNull
        case primaryKey
        case check
        case constraint
    }
    
    public struct Mapping: Sendable, Hashable {
        nonisolated public let from: String
        nonisolated public let to: String
        
        nonisolated public init(from: String, to: String) {
            self.from = from
            self.to = to
        }
    }
}
