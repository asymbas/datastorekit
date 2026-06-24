//
//  SQLHistoryExpression.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

internal protocol SQLHistoryExpression {
    typealias Context = SQLHistoryTranslator
    typealias Fragment = SQLHistoryFragment
    nonisolated func evaluate<T>(_ context: inout Context<T>) -> Fragment
}

extension SQLHistoryExpression {
    nonisolated internal func query<T>(_ context: inout Context<T>) -> Fragment {
        evaluate(&context)
    }
}
