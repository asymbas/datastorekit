//
//  RelationshipDiff.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import SwiftData

/// Use for comparing against another to-many relationship collection.
///
/// - Parameters:
///   - oldPersistentIdentifiers: The identifiers previously referenced (the "before" state).
///   - newPersistentIdentifiers: The identifiers currently referenced (the "after" state).
/// - Returns:
///   A tuple containing:
///   - `inserted`: Identifiers present in the new set, but not in the old set.
///   - `deleted`: Identifiers present in the old set, but not in the new set.
///   - `unchanged`: Identifiers present in both the old and new sets.
nonisolated package func diffReferencedRelationships(
    old oldPersistentIdentifiers: [PersistentIdentifier],
    new newPersistentIdentifiers: [PersistentIdentifier]
) -> (
    inserted: Set<PersistentIdentifier>,
    deleted: Set<PersistentIdentifier>,
    unchanged: Set<PersistentIdentifier>
) {
    let oldSet = Set(oldPersistentIdentifiers)
    let newSet = Set(newPersistentIdentifiers)
    return (
        inserted: newSet.subtracting(oldSet),
        deleted: oldSet.subtracting(newSet),
        unchanged: oldSet.intersection(newSet)
    )
}
