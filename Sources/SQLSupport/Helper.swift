//
//  Helper.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

nonisolated public func diff(
    columns: [String],
    old oldValues: [any Sendable],
    new newValues: [any Sendable],
    ignoring ignoreColumns: Set<String> = []
) -> [Int] {
    var changedIndices: [Int] = []
    for (index, name) in columns.enumerated() {
        if ignoreColumns.contains(name) { continue }
        let oldValue = SQLValue(any: oldValues[index] as Any)
        let newValue = SQLValue(any: newValues[index] as Any)
        if oldValue != newValue { changedIndices.append(index) }
    }
    return changedIndices
}
