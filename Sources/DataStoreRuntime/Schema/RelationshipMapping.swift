//
//  RelationshipMapping.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation
import SQLiteHandle
import SwiftData

extension Schema {
    nonisolated public func isManyToManyRelationship(
        _ relationship: Relationship,
        _ inverseRelationship: Relationship? = nil
    ) -> Bool {
        if let inverseRelationship {
            assert(relationship.keypath == inverseRelationship.inverseKeyPath)
            assert(inverseRelationship.keypath == relationship.inverseKeyPath)
            return !relationship.isToOneRelationship && !inverseRelationship.isToOneRelationship
        }
        guard let targetEntity = self.entitiesByName[relationship.destination] else {
            fatalError("\(SchemaError.relationshipTargetEntityNotRegistered)")
        }
        guard let inverseName = relationship.inverseName,
              let inverseRelationship = targetEntity.relationshipsByName[inverseName] else {
            return false
        }
        return !relationship.isToOneRelationship && !inverseRelationship.isToOneRelationship
    }
}

extension Schema.Relationship {
    /// Indicates whether this relationship has an inverse.
    nonisolated public var isUnidirectional: Bool {
        (inverseName == nil) && (inverseKeyPath == nil)
    }
}
