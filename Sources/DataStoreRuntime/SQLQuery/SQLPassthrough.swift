//
//  SQLPassthrough.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import SQLiteHandle
import SQLiteStatement

public protocol SQLQueryPassthrough {}

extension SQLQueryPassthrough {
    nonisolated public var sql: SQL { .init(Raw("")) }
}

public protocol SQLPredicateExpressionSortOption {}

extension SQLPredicateExpressionSortOption {
    nonisolated public var sortByRandom: Bool { false }
}
