//
//  DataStoreMigrationHandler.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import DataStoreSQL
public import SwiftData

nonisolated public struct DataStoreMigrationContext: Sendable {
    public let oldSchema: Schema
    public let newSchema: Schema
    public let pendingChanges: [String]
}

public typealias DataStoreMigrationHandler =
@Sendable (DataStoreMigrationContext, borrowing DatabaseConnection<DatabaseStore>) throws -> Void
