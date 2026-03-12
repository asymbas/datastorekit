//
//  SQLError+SQLite.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL

extension SQLError {
    nonisolated package init(
        _ errorCode: SQLite.Error,
        _ extendedErrorCode: SQLite.Error? = nil,
        message: String? = nil,
        sql: String? = nil,
        bindings: [any Sendable]? = []
    ) {
        var output = errorCode.description
        if let extendedErrorCode {
            output += " " + extendedErrorCode.description
        }
        if let message {
            output += " " + message
        }
        if let sql {
            if !output.isEmpty { output += "\n" }
            output += "SQL: \(sql)"
        }
        if let bindings = bindings?
            .map({ binding -> String in "\(binding)" })
            .joined(separator: ", "),
           !bindings.isEmpty {
            if !output.isEmpty { output += "\n" }
            output += "Bindings: [\(bindings)]"
        }
        self.init(message: output)
    }
}
