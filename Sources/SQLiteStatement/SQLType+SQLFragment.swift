//
//  SQLType+SQLFragment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import SQLite3
public import SQLSupport

extension SQLType {
    nonisolated public init(sqlite rawValue: Int32) {
        switch rawValue {
        case SQLITE_NULL: self = .null
        case SQLITE_INTEGER: self = .integer
        case SQLITE_FLOAT: self = .real
        case SQLITE_TEXT: self = .text
        case SQLITE_BLOB: self = .blob
        default: preconditionFailure("The raw value is not a valid SQLite type: \(rawValue)")
        }
    }
}

extension SQLType: SQLFragment {
    nonisolated public var sql: String {
        rawValue
    }
}
