//
//  DeleteRuleMapping.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

package import SQLiteStatement
package import SwiftData

extension Schema.Relationship.DeleteRule {
    nonisolated package var referentialAction: ReferentialAction {
        switch self {
        case .nullify: .setNull
        case .cascade: .cascade
        case .deny: .restrict
        case .noAction: .noAction
        @unknown default:
            fatalError(DataStoreError.unsupportedFeature.localizedDescription)
        }
    }
}

nonisolated internal func joinTableDeleteAction(from deleteRule: Schema.Relationship.DeleteRule)
-> ReferentialAction {
    switch deleteRule {
    case .deny: .restrict
    case .noAction: .noAction
    case .nullify, .cascade: .cascade
    @unknown default:
        fatalError(DataStoreError.unsupportedFeature.localizedDescription)
    }
}

nonisolated internal func referenceDeleteAction(
    for relationship: Schema.Relationship,
    inverse inverseRelationship: Schema.Relationship?
) -> ReferentialAction? {
    switch inverseRelationship?.deleteRule ?? relationship.deleteRule {
    case .cascade:
        if relationship.isToOneRelationship
            && inverseRelationship?.isToOneRelationship ?? false
            && relationship.isOptional {
            return .setNull
        } else {
            return .cascade
        }
    case .nullify:
        return relationship.isOptional ? .setNull : .noAction
    case .deny:
        return .restrict
    case .noAction:
        return .noAction
    @unknown default:
        fatalError(DataStoreError.unsupportedFeature.localizedDescription)
    }
}
