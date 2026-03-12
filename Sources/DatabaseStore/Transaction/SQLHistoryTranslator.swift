//
//  SQLHistoryTranslator.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Collections
import DataStoreCore
import DataStoreRuntime
import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SQLiteHandle
import SwiftUI
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

public struct SQLHistoryTranslator<T: HistoryTransaction>: ~Copyable, Sendable {
    nonisolated public func translate(_ descriptor: HistoryDescriptor<T>) throws -> String {
        return ""
    }
}
