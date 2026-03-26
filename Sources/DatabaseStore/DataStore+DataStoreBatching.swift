//
//  DataStore+DataStoreBatching.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreRuntime
import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension DatabaseStore: DataStoreBatching {
    /// Inherited from `DataStoreBatching.delete(_:)`.
    nonisolated public final func delete<T>(_ request: DataStoreBatchDeleteRequest<T>)
    throws where T: PersistentModel & SendableMetatype {
        var translator = SQLPredicateTranslator<T>(configuration: configuration)
        let translation = try translator.translate(FetchDescriptor<T>(predicate: request.predicate))
        let entityName = Schema.entityName(for: T.self)
        var relatedIdentifiers = Set<PersistentIdentifier>()
        var deletedIdentifiers = Set<PersistentIdentifier>()
        #if swift(>=6.2) && !SwiftPlaygrounds
        let connection = try queue.connection(.writer, for: request.editingState)
        #else
        var connection = try queue.connection(.writer, for: request.editingState)
        #endif
        try connection.withTransaction(nil) {
            var relatedSnapshots = [PersistentIdentifier: Snapshot]()
            for row in try connection.fetch(translation.statement) {
                logger.debug("Iterating batched \(entityName) delete on row: \((row.first as? String) ?? "N/A")")
                var snapshot = try Snapshot(
                    store: self,
                    properties: translation.properties[...],
                    values: row[...],
                    relatedSnapshots: &relatedSnapshots
                )
                let export = snapshot.delete
                let (unlinked, cascaded) = try snapshot.reconcileExternalReferencesBeforeDelete(
                    indices: export.toOneDependencies + export.toManyDependencies,
                    connection: connection
                )
                relatedIdentifiers.formUnion(unlinked)
                relatedIdentifiers.formUnion(cascaded)
                try connection.delete(snapshot)
                relatedIdentifiers.insert(snapshot.persistentIdentifier)
                deletedIdentifiers.insert(snapshot.persistentIdentifier)
                logger.debug("Successfully deleted from batch: \(snapshot.persistentIdentifier)")
            }
        }
        let allIdentifiers = deletedIdentifiers.union(relatedIdentifiers)
        queue.release(connection)
        attachment?.storeDidSave(inserted: [], updated: [], deleted: .init(allIdentifiers))
        manager.registry(for: request.editingState)?.synchronize(
            snapshots: [:],
            invalidateIdentifiers: allIdentifiers
        )
        if !allIdentifiers.isEmpty {
            Task { @DatabaseActor in
                self.history?.scheduleSynchronizationIfNeeded()
            }
        }
        logger.info(
            "Batch deleted \(allIdentifiers.count) snapshots.",
            metadata: [
                "editing_state": "\(request.editingState.id)",
                "author": "\(request.editingState.author ?? "nil")",
                "deleted_snapshots": "\(deletedIdentifiers.count)",
                "related_snapshots": "\(relatedIdentifiers.count)"
            ]
        )
        DispatchQueue.main.async {
            NotificationCenter.default.post(
                name: .dataStoreDidSave,
                object: nil,
                userInfo: ["operation": [DataStoreOperation.delete: allIdentifiers]]
            )
        }
    }
}
