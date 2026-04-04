//
//  InheritanceResolver.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreRuntime
import DataStoreSQL
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

package final class InheritanceResolver: Sendable {
    nonisolated private let storage: Mutex<[String: String]> = .init([:])
    
    nonisolated package func entityName(for primaryKey: String) -> String? {
        storage.withLock { $0[primaryKey] }
    }
    
    nonisolated package func set(resolvedEntityName: String, primaryKey: String) {
        storage.withLock { $0[primaryKey] = resolvedEntityName }
    }
    
    nonisolated package func remove(primaryKey: String) {
        storage.withLock { $0[primaryKey] = nil }
    }
    
    nonisolated package func warmUp(
        subentities: [Schema.Entity],
        primaryKeys: [String],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        guard !primaryKeys.isEmpty, !subentities.isEmpty else { return }
        let inList = Array(repeating: "?", count: primaryKeys.count).joined(separator: ",")
        let bindings: [any Sendable] = primaryKeys.map { $0 }
        for subentity in subentities {
//            guard let superentityName = subentity.superentity?.name else { continue }
            let rows = try connection.fetch(
                """
                SELECT "\(pk)" FROM "\(subentity.name)"
                WHERE "\(pk)" IN (\(inList))
                """,
                bindings: bindings
            )
            storage.withLock { storage in
                for row in rows {
                    if let resolvedPrimaryKey = row[0] as? String {
                        storage[resolvedPrimaryKey] = subentity.name
                    }
                }
            }
        }
    }
    
    nonisolated package func warmUp(
        destination: String,
        subentities: [Schema.Entity],
        primaryKeys: [String],
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        guard !primaryKeys.isEmpty, !subentities.isEmpty else { return }
        let inList = Array(repeating: "?", count: primaryKeys.count).joined(separator: ",")
        let bindings: [any Sendable] = primaryKeys.map { $0 }
        for subentity in subentities {
            let rows = try connection.fetch(
                """
                SELECT "\(pk)" FROM "\(subentity.name)"
                WHERE "\(pk)" IN (\(inList))
                """,
                bindings: bindings
            )
            var resolved = [(PersistentIdentifier, String)]()
            resolved.reserveCapacity(rows.count)
            for row in rows {
                if let resolvedPrimaryKey = row[0] as? String {
                    let persistentIdentifier = try PersistentIdentifier.identifier(
                        for: connection.storeIdentifier,
                        entityName: destination,
                        primaryKey: resolvedPrimaryKey
                    )
                    resolved.append((persistentIdentifier, subentity.name))
                }
            }
            storage.withLock { storage in
                for (persistentIdentifier, entityName) in resolved {
//                    storage[persistentIdentifier] = entityName
                }
            }
        }
    }
}
