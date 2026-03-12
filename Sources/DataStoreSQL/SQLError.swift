//
//  SQLError.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import Foundation

public struct SQLError: CustomStringConvertible, Error, LocalizedError {
    nonisolated public var code: Code?
    nonisolated public var message: String
    
    nonisolated package init(message: String) {
        self.message = message
    }
    
    nonisolated package init(
        _ message: String? = nil,
        sql: String? = nil,
        bindings: [any Sendable]? = []
    ) {
        var output = message ?? ""
        if let sql = sql {
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
        self.message = output
    }
    
    nonisolated package init(
        _ code: Code? = nil,
        message: String? = nil,
        sql: String? = nil,
        bindings: [any Sendable]? = []
    ) {
        var output = ""
        if let code {
            output = code.localizedDescription + "_" + (message ?? "")
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
        self.code = code
        self.message = output
    }
    
    nonisolated public var description: String {
        "\(message) - \(localizedDescription)"
    }
    
    public enum Code: Equatable, LocalizedError {
        case columnNotFound(String)
        case invalidType
        case rowNotFound
        case unknown
    }
}
