//
//  InternalTable.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

package enum InternalTable: String {
    nonisolated package static let tableName: String = "_Internal"
    case key
    case value
    
    nonisolated package static var createTable: String {
        """
        CREATE TABLE IF NOT EXISTS \(Self.tableName) (
            \(Self.key.rawValue) TEXT PRIMARY KEY NOT NULL,
            \(Self.value.rawValue) TEXT
        )
        """
    }
}
