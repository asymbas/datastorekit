//
//  Schema+TypeRegistry.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreSupport
import SwiftData

extension Schema {
    /// Returns the model type associated with the entity's name.
    nonisolated public static func type(for entityName: String)
    -> (any (PersistentModel & SendableMetatype).Type)? {
        TypeRegistry.getType(forName: entityName) as? any (PersistentModel & SendableMetatype).Type
    }
}

extension Schema.Entity {
    /// The model type associated to this entity.
    nonisolated public var type: (any (PersistentModel & SendableMetatype).Type)? {
        Schema.type(for: self.name) ?? reflectEntity(self)
    }
}
