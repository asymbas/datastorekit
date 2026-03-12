//
//  SQLFragment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public protocol SQLFragment: Equatable, Hashable, Sendable {
    var sql: String { get }
    nonisolated var bindings: [any Sendable] { get }
}

extension SQLFragment {
    nonisolated public var sql: String {
        "\(self.self)"
    }
    
    nonisolated public var bindings: [any Sendable] {
        []
    }
    
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.sql == rhs.sql
    }
    
    nonisolated public func hash(into hasher: inout Hasher) {
        hasher.combine(sql)
    }
    
    nonisolated public func indent(_ level: Int) -> String {
        #if DEBUG
        let pad = String(repeating: " ", count: 4 * level)
        return self.sql
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map { $0.isEmpty ? "" : pad + $0 }
            .joined(separator: "\n")
        #else
        return self.sql
        #endif
    }
}

extension Collection where Element: SQLFragment {
    nonisolated public var sql: String {
        sql(separator: " ")
    }
    
    /// Join fragments into a single SQL string.
    nonisolated public func sql(separator: String = " ") -> String {
        self.map(\.sql).joined(separator: separator)
    }
}

extension Collection where Element: SQLFragment {
    /// Flatten all bindings in order.
    nonisolated public var allBindings: [any Sendable] {
        self.flatMap(\.bindings)
    }
}
