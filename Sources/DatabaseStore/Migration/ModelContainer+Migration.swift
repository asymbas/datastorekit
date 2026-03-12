//
//  ModelContainer+Migration.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Logging
import SwiftData

/*
 Observation: `ModelContainer` never provides a migration plan, likely a temporary store is used. `ModelContainer` has a migration plan property, this could mean it hasn't been implemented (also there's a typo in their property documentation).
 */

extension ModelContainer {
    nonisolated public convenience init(
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
    nonisolated public convenience init(
        for schema: consuming Schema,
        migrationPlan: (any SchemaMigrationPlan.Type)?,
        configurations: DatabaseConfiguration...
    ) throws {
        try self.init(for: schema, migrationPlan: migrationPlan, configurations: configurations)
    }
}
