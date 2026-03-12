//
//  SQLType+SQLFragment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import SQLite3
import SQLSupport

extension SQLType {
    nonisolated public init(sqlite rawValue: Int32) {
        switch rawValue {
        case SQLITE_NULL: self = .null
        case SQLITE_INTEGER: self = .integer
        case SQLITE_FLOAT: self = .real
        case SQLITE_TEXT: self = .text
        case SQLITE_BLOB: self = .blob
        default: fatalError()
        }
    }
}

extension SQLType: SQLFragment {
    nonisolated public var sql: String {
        rawValue
    }
}
