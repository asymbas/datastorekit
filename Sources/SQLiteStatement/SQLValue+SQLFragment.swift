//
//  SQLValue+SQLFragment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import SQLSupport

extension SQLValue: SQLFragment {
    nonisolated public var sql: String {
        if self.valueType is Bool.Type {
            "\(Bool(base as! Int64 != 0).description.uppercased())"
        } else {
            description
        }
    }
}
