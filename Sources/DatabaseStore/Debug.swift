//
//  Debug.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import Logging
import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

nonisolated package func logMemoryLayout<T>(_ value: T) {
    #if DEBUG
    logger.trace(
    """
    \(T.self).self
    type layout ->
        size: \(MemoryLayout<T>.size),
        stride: \(MemoryLayout<T>.stride),
        alignment: \(MemoryLayout<T>.alignment)
    instance layout ->
        size: \(MemoryLayout.size(ofValue: value)),
        stride: \(MemoryLayout.stride(ofValue: value)),
        alignment: \(MemoryLayout.alignment(ofValue: value))
    """
    )
    #endif
}

nonisolated internal func assertSaveChangesResult(
    request: some SaveChangesRequest<DatabaseSnapshot>,
    remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier]
) {
    assert(
        remappedIdentifiers.keys.allSatisfy { temporaryIdentifier in
            temporaryIdentifier.storeIdentifier == nil &&
            request.inserted.contains { snapshot in
                snapshot.persistentIdentifier == temporaryIdentifier
            }
        },
        """
        One or more inserted snapshots were not remapped properly.
        Expected all inserted identifiers to appear in remappedIdentifiers.
        Inserted count: \(request.inserted.count)
        Remapped count: \(remappedIdentifiers.count)
        Missing identifiers:
        \(
            {
                let insertedSet = Set(request.inserted.map(\.persistentIdentifier))
                let remappedKeys = Set(remappedIdentifiers.keys)
                let missing = insertedSet.subtracting(remappedKeys)
                return Array(missing)
            }()
        )
        Extra remapped identifiers (not in inserted):
        \(
            {
                let insertedSet = Set(request.inserted.map(\.persistentIdentifier))
                let remappedKeys = Set(remappedIdentifiers.keys)
                let extra = remappedKeys.subtracting(insertedSet)
                return Array(extra)
            }()
        )
        """
    )
}

/// Ensures all `PersistentIdentifier` values in the snapshot are valid and is not temporary.
///
/// - Parameters:
///   - persistentIdentifier:
///     The permanent identifier provided to the snapshot.
///   - snapshot:
///     The requested snapshot.
///   - includeToManyRelationships:
///     By default, this is set to `false`, because only foreign keys should only guarantee validity.
nonisolated package func ensureRemappedIdentifiers(
    for persistentIdentifier: PersistentIdentifier,
    snapshot: DatabaseSnapshot,
    includeToManyRelationships: Bool = false
) {
    #if RELEASE
    return
    #endif
    precondition(
        persistentIdentifier.entityName == snapshot.entityName,
        "Entity mismatch: \(persistentIdentifier.entityName) != \(snapshot.entityName)"
    )
    precondition(
        persistentIdentifier.id == snapshot.id,
        "ID mismatch: \(persistentIdentifier.id) != \(snapshot.id)"
    )
    if persistentIdentifier.storeIdentifier == nil, snapshot.storeIdentifier == nil {
        logger.critical("Snapshot has nil store identifier: \(persistentIdentifier)")
    }
    let description = snapshot.primaryKey
    for property in snapshot.properties where property.metadata is Schema.Relationship {
        let description = "\(description)\n\(snapshot.entityName).\(property.name)"
        switch snapshot.values[property.index] {
        case let identifier as PersistentIdentifier:
            if identifier.storeIdentifier == nil {
                output(isToOneRelationship: true, identifier: identifier)
            }
        case let identifiers as [PersistentIdentifier] where includeToManyRelationships:
            for identifier in identifiers where identifier.storeIdentifier == nil {
                output(isToOneRelationship: false, identifier: identifier)
            }
        default:
            continue
        }
        func output(isToOneRelationship: Bool, identifier: PersistentIdentifier) {
            let component = "\(isToOneRelationship ? "To-one" : "To-many") relationship"
            logger.critical(
                "\(description)\n\(component) has nil store identifier: \(identifier)",
                metadata: [
                    "entity name": .string(snapshot.entityName),
                    "primary key": .string(snapshot.primaryKey)
                ]
            )
        }
    }
}

nonisolated package func debugSnapshotDictionary(_ snapshots: [PersistentIdentifier: DatabaseSnapshot]) {
    print("\nAll snapshots:\n")
    for (persistentIdentifier, snapshot) in snapshots {
        print("Snapshot: \(persistentIdentifier)")
        for property in snapshot.properties {
            print(" * \(snapshot.entityName).\(property.name) = \(snapshot.values[property.index])")
        }
        print()
    }
}

nonisolated package func logPersistentIdentifiers(
    request: some SaveChangesRequest<DatabaseSnapshot>,
    listAll: Bool = false
) {
    func summarize(_ snapshots: [DatabaseSnapshot], label: String) {
        let persistentIdentifiers = snapshots.map(\.persistentIdentifier)
        let grouped = Dictionary(grouping: persistentIdentifiers, by: { $0.entityName })
        let total = persistentIdentifiers.count
        let entitySummaries = grouped
            .sorted { $0.key < $1.key }
            .map { "\($0.key): \($0.value.count)" }
            .joined(separator: ", ")
        logger.info("Request.\(label) — total: \(total), entities: \(grouped.count) [\(entitySummaries)]")
        guard listAll else { return }
        for (entity, persistentIdentifiers) in grouped.sorted(by: {
            ($0.value.count, $0.key) > ($1.value.count, $1.key)
        }) {
            let line = persistentIdentifiers
                .sorted { "\($0)" > "\($1)" }
                .map { "\($0)" }
                .joined(separator: ", ")
            logger.info("[\(entity)] \(persistentIdentifiers.count) identifiers: \(line)")
        }
    }
    if !request.inserted.isEmpty {
        summarize(request.inserted, label: "inserted")
    }
    if !request.updated.isEmpty {
        summarize(request.updated, label: "updated")
    }
    if !request.deleted.isEmpty {
        summarize(request.deleted, label: "deleted")
    }
}

nonisolated internal func logSaveChangesResult(
    remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier],
    snapshotsToReregister: [PersistentIdentifier: DatabaseSnapshot]
) {
    print("\nSave complete:\n")
    for (key, value) in remappedIdentifiers.sorted(by: {
        $0.key == $1.key ? "\($0.value)" < "\($1.value)" : $0.key < $1.key
    }) {
        print("remappedIdentifiers[\(key)] = \(value)")
    }
    for (key, value) in snapshotsToReregister.sorted(by: {
        $0.key == $1.key ? "\($0.value)" < "\($1.value)" : $0.key < $1.key
    }) {
        print("snapshotsToReregister[\(key)] = \(value.persistentIdentifier) \(value.contentDescriptions)")
    }
}
