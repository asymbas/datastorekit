//
//  ModelContainer+Migration.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import SwiftData

extension ModelContainer {
    @available(*, unavailable, message: "")
    nonisolated private convenience init(
        for schema: consuming Schema,
        migrationPlan: (any SchemaMigrationPlan.Type)?,
        configurations: [DatabaseConfiguration]
    ) throws {
        if migrationPlan != nil {
            for configuration in configurations {
                _ = try DatabaseStore(configuration, migrationPlan: migrationPlan)
            }
        }
        try self.init(for: schema, configurations: configurations)
    }
}

extension ModelContainer {
    @available(*, unavailable, message: "")
    nonisolated private convenience init(
        for schema: consuming Schema,
        migrationPlan: (any SchemaMigrationPlan.Type)?,
        configurations: DatabaseConfiguration...
    ) throws {
        try self.init(for: schema, migrationPlan: migrationPlan, configurations: configurations)
    }
}
