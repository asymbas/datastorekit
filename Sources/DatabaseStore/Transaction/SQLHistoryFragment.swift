//
//  SQLHistoryFragment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

nonisolated internal struct SQLHistoryFragment {
    internal var clause: String
    internal var bindings: [any Sendable]
    internal var column: SQLHistoryColumn?
    internal var literal: (any Sendable)?
    internal var isLiteral: Bool
    internal var isNull: Bool
    internal var isScope: Bool
    internal var interval: TimestampInterval
    internal var isComplete: Bool
    
    internal init(
        clause: String,
        bindings: [any Sendable] = [],
        column: SQLHistoryColumn? = nil,
        literal: (any Sendable)? = nil,
        isLiteral: Bool = false,
        isNull: Bool = false,
        isScope: Bool = false,
        interval: TimestampInterval = .unbounded,
        isComplete: Bool = true
    ) {
        self.clause = clause
        self.bindings = bindings
        self.column = column
        self.literal = literal
        self.isLiteral = isLiteral
        self.isNull = isNull
        self.isScope = isScope
        self.interval = interval
        self.isComplete = isComplete
    }
    
    internal static var scope: Self {
        .init(clause: "", isScope: true)
    }
    
    internal static var untranslatable: Self {
        .init(clause: "1", isComplete: false)
    }
    
    internal static func column(_ column: SQLHistoryColumn) -> Self {
        .init(clause: column.columnName, column: column)
    }
    
    internal static func literal(_ value: (any Sendable)?, isNull: Bool) -> Self {
        .init(clause: "?", literal: value, isLiteral: true, isNull: isNull)
    }
    
    internal static func boolean(_ value: Bool) -> Self {
        .init(clause: value ? "1" : "0")
    }
    
    internal static func predicate(
        clause: String,
        bindings: [any Sendable],
        interval: TimestampInterval,
        isComplete: Bool
    ) -> Self {
        .init(clause: clause, bindings: bindings, interval: interval, isComplete: isComplete)
    }
}
