//
//  SQLHistoryTranslator.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

#if swift(>=6.2)
internal import SwiftData
#else
@preconcurrency internal import SwiftData
#endif

// TODO: Use `SQLPredicateTranslator` as a template.
// TODO: Accessing date-related key paths should prepare to `ATTACH`/`DETACH` archived transactions.
internal struct SQLHistoryTranslator<T: HistoryTransaction>: ~Copyable, Sendable {
    nonisolated internal func translate(_ descriptor: HistoryDescriptor<T>) throws -> String {
        return ""
    }
}
