//
//  DataStore.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Collections
import DataStoreCore
import DataStoreRuntime
import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SQLiteHandle
import SQLiteStatement
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension DatabaseStore: DatabaseProtocol {
    public typealias Handle = SQLite
    public typealias Attachment = ModelManager
    public typealias Context = SnapshotRegistry
    public typealias Transaction = TransactionObject
}

public final class DatabaseStore: DataStore, Sendable {
    /// Inherited from `DataStore.Configuration`.
    public typealias Configuration = DatabaseConfiguration
    /// Inherited from `DataStore.Snapshot`.
    public typealias Snapshot = DatabaseSnapshot
    /// Inherited from `DataStore.identifier`.
    nonisolated public final let identifier: String
    /// Inherited from `DataStore.configuration`.
    nonisolated public final let configuration: Configuration
    /// Inherited from `DataStore.schema`.
    nonisolated public final let schema: Schema
    /// The connection pools for reader and writers.
    nonisolated public final let queue: DatabaseQueue<DatabaseStore>
    /// Manages and caches snapshots belonging to each `ModelContext`.
    nonisolated public final let manager: ModelManager
    
    /// The attachment assigned when this object was created.
    nonisolated package final var attachment: (any DataStoreDelegate)? {
        configuration.attachment
    }
    
    @DatabaseActor internal final var transaction: HistoryState?
    
    /// Inherited from `DataStore.init(_:migrationPlan:)`.
    ///
    /// Initialized for `ModelContainer` when used with the associated `DatabaseConfiguration` type.
    ///
    /// - Creating a `Schema` instance from the versions given by the stage will provide both old and new entities.
    ///
    /// - Note:
    ///   The `ModelContainer` only has an initializer for `ModelConfiguration` and `SchemaMigrationPlan`.
    ///   SwiftData does not receive a migration plan elsewhere.
    nonisolated public init(
        _ configuration: Configuration,
        migrationPlan: (any SchemaMigrationPlan.Type)?
    ) throws {
        var schema = configuration.schema ?? Schema()
        self.schema = schema
        logger.trace("Using schema: \(schema.entities.map(\.name))")
        if let migrationPlan = configuration.migrationPlan {
            do {
                for (index, stage) in migrationPlan.stages.enumerated() {
                    switch stage {
                    case .custom(let fromVersion, let toVersion, let willMigrate, let didMigrate):
                        let fromSchema = Schema(versionedSchema: fromVersion)
                        let toSchema = Schema(versionedSchema: toVersion)
                        logger.notice(
                            "Performing custom migration #\(index): \(fromVersion)",
                            metadata: [
                                "from_names": "\(fromSchema.entities.map(\.name))",
                                "from_hash_values": "\(fromSchema.hashValue)",
                                "schema_hash_values": "\(schema.hashValue)"
                            ]
                        )
                        logger.notice(
                            "Performing custom migration #\(index): \(toVersion)",
                            metadata: [
                                "to_names": "\(toSchema.entities.map(\.name))",
                                "to_hash_values": "\(toSchema.hashValue)",
                                "schema_hash_values": "\(schema.hashValue)"
                            ]
                        )
                        if let willMigrate {
                            let fromModelContainer = try ModelContainer(
                                for: fromSchema,
                                configurations: [DatabaseConfiguration(
                                    name: configuration.name,
                                    url: configuration.url
                                )]
                            )
                            let fromModelContext = ModelContext(fromModelContainer)
                            try willMigrate(fromModelContext)
                        }
                        if let didMigrate {
                            let toModelContainer = try ModelContainer(
                                for: toSchema,
                                configurations: [DatabaseConfiguration(
                                    name: configuration.name,
                                    url: configuration.url
                                )]
                            )
                            let toModelContext = ModelContext(toModelContainer)
                            try didMigrate(toModelContext)
                        }
                        schema = toSchema
                    case .lightweight(let fromVersion, let toVersion):
                        logger.debug(
                            "Performing lightweight migration #\(index).",
                            metadata: ["from": "\(fromVersion)", "to": "\(toVersion)"]
                        )
                        let fromSchema = Schema(versionedSchema: fromVersion)
                        schema = fromSchema
                    @unknown default:
                        fatalError("Encountered an unexpected case migration stage: \(stage)")
                    }
                }
            } catch {
                #if DEBUG
                logger.critical("Migration error: \(error)")
                #else
                fatalError("Migration error: \(error)")
                #endif
            }
        }
        self.identifier = configuration.name
        do {
            let manager = ModelManager(configuration: configuration)
            self.queue = try configuration.makeDatabaseQueue(manager)
            self.manager = manager
            self.configuration = configuration
            self.transaction = configuration.options.contains(.disablePersistentHistoryTracking)
            ? nil : HistoryState(store: self, cloudKit: configuration.cloudKit)
        }
        defer {
            self.configuration.bind(container: initialize())
        }
        try queue.withConnection(.writer) { connection in
            let applicationID = try connection.withPreparedStatement("PRAGMA application_id;") {
                var iterator = $0.rows.makeIterator()
                return iterator.next()?[0, as: Int32.self] ?? 0
            }
            let userVersion = try connection.withPreparedStatement("PRAGMA user_version;") {
                var iterator = $0.rows.makeIterator()
                return iterator.next()?[0, as: Int32.self] ?? 0
            }
            if applicationID == 0 {
                guard userVersion == 0 else {
                    throw Self.Error.storeFileTypeIsNotCompatible
                }
                try connection.execute("PRAGMA application_id = \(Self.applicationID);")
                try connection.execute("PRAGMA user_version = \(Self.userVersion);")
            } else if Self.applicationID != applicationID  {
                throw Self.Error.storeFileTypeIsNotCompatible
            } else if Self.userVersion < userVersion {
                throw Self.Error.storeFileVersionIsNotCompatible
            }
            try connection.execute("PRAGMA journal_mode = WAL;")
            try connection.execute("PRAGMA foreign_keys = ON;")
            try connection.execute("PRAGMA defer_foreign_keys = ON;")
            try connection.execute(InternalTable.createTable)
            try connection.execute(HistoryTable.createTable)
        }
        try DataStoreMigration(store: self) { context, connection in }
        logger.debug("DataStore init: \(self.configuration.url?.path, default: "nil")")
    }
    
    deinit {
        do {
            try queue.close()
            deinitialize()
            logger.debug("DataStore deinit: \(identifier)")
        } catch {
            fatalError("Unable to deinit DataStore: \(error)")
        }
    }
    
    /// Inherited from `DataStore.fetch(_:)`.
    ///
    /// Fetches the backing data of requested models from the data store or in-memory cache.
    ///
    /// - Parameter request:
    ///   The fetch request containing what models to query for and the editing state.
    /// - Returns:
    ///   The fetch result primarily containing the typed `Model` snapshots.
    ///   Related snapshots include relationships that were included with each result set.
    nonisolated public final func fetch<Model, Request, Result>(_ request: Request) throws -> Result
    where Request: FetchRequest<Model>, Result: FetchResult<Model, Snapshot> {
        logger.trace(.init(stringLiteral: threadDescription))
        let shouldCheckCancellation = {
            // Always allow faulting.
            Request.self is DataStoreFetchRequest<Model>.Type == false
        }()
        let entityName = Schema.entityName(for: Model.self)
        var translator = SQLPredicateTranslator<Model>(configuration: configuration)
        do {
            var preloadedResult: PreloadFetchResult<Model, Snapshot>?
            if let result = self.manager.preload(for: request.editingState, as: Result.self) {
                guard !result.isUnchecked else {
                    return finalize(type: "preloaded", result: result.convert(into: Result.self))
                }
                preloadedResult = consume result
            }
            let translation = try translator.translate(request.descriptor)
            if shouldCheckCancellation { try Task.checkCancellation() }
            request: if let result = preloadedResult.take() {
                guard result.key == translation.key else {
                    break request
                }
                return finalize(type: "preloaded", result: result.convert(into: Result.self))
            }
            let registry = self.manager.registry(for: request.editingState)
            request: if !configuration.options.contains(.disablePredicateCaching),
                      let hash = translation.key,
                      let result = try? registry?.cachedFetchResult(forKey: hash, on: entityName) {
                guard !result.fetchedSnapshots.isEmpty else {
                    break request
                }
                return finalize(type: "cached", result: .init(
                    descriptor: request.descriptor,
                    fetchedSnapshots: result.fetchedSnapshots,
                    relatedSnapshots: result.relatedSnapshots
                ))
            }
            let rows = try queue.reader { try $0.fetch(translation.statement) }
            if shouldCheckCancellation { try Task.checkCancellation() }
            let total = rows.count
            let fetchedSnapshots: [Snapshot]
            var relatedSnapshots: [PersistentIdentifier: Snapshot] = try prefetch(
                entityName: entityName,
                properties: translation.properties,
                values: rows,
                registry: registry
            )
            switch !configuration.options.contains(.synchronouslyCreateSnapshots) {
            case true:
                let fetchedSnapshotsMutex = Mutex<[Snapshot?]>(.init(repeating: nil, count: total))
                let relatedSnapshotsMutex = Mutex<[PersistentIdentifier: Snapshot]>(relatedSnapshots)
                let lastErrorMutex: Mutex<(Swift.Error)?> = .init(nil)
                DispatchQueue.concurrentPerform(iterations: total) { index in
                    do {
                        var sink = [PersistentIdentifier: Snapshot]()
                        let snapshot = try Snapshot(
                            store: self,
                            registry: registry,
                            properties: translation.properties[...],
                            values: rows[index][...],
                            relatedSnapshots: &sink
                        )
                        fetchedSnapshotsMutex.withLock { $0[index] = snapshot }
                        if !sink.isEmpty {
                            relatedSnapshotsMutex.withLock {
                                $0.merge(sink, uniquingKeysWith: { existing, incoming in existing })
                            }
                        }
                    } catch {
                        logger.error("An error occurred creating snapshot at index \(index): \(error)")
                        lastErrorMutex.withLock{ $0 = error }
                    }
                }
                if let lastError = lastErrorMutex.withLock(\.self) {
                    throw lastError
                }
                fetchedSnapshots = fetchedSnapshotsMutex.withLock { $0.compactMap(\.self) }
                relatedSnapshots = relatedSnapshotsMutex.withLock { $0 }
            case false:
                fetchedSnapshots = try rows.map { row -> Snapshot in
                    try Snapshot(
                        store: self,
                        registry: registry,
                        properties: translation.properties[...],
                        values: row[...],
                        relatedSnapshots: &relatedSnapshots
                    )
                }
            }
            let result: Result
            switch request {
            case let request as PreloadFetchRequest<Model>:
                result = PreloadFetchResult<Model, Snapshot>(
                    request: request,
                    forKey: translation.key,
                    fetchedSnapshots: fetchedSnapshots,
                    relatedSnapshots: relatedSnapshots
                ) as! Result
            default:
                result = Result(
                    descriptor: request.descriptor,
                    fetchedSnapshots: fetchedSnapshots,
                    relatedSnapshots: relatedSnapshots
                )
            }
            if !configuration.options.contains(.disablePredicateCaching),
               request.descriptor.propertiesToFetch.isEmpty,
               let key = translation.key {
                registry?.scheduleCacheFetchResult(
                    forKey: key,
                    fetchedSnapshots: result.fetchedSnapshots,
                    relatedSnapshots: result.relatedSnapshots
                )
            }
            #if DEBUG
            if configuration.options.contains(.useVerboseLogging) {
                for snapshot in result.fetchedSnapshots {
                    manager.graph.debugOutgoingOrdering(owner: snapshot.persistentIdentifier)
                    manager.graph.debugIncomingOrdering(target: snapshot.persistentIdentifier)
                }
            }
            #endif
            return finalize(translation.requestedIdentifiers == nil, type: nil, result: result)
        } catch DataStoreError.preferInMemoryFilter {
            throw DataStoreError.unsupportedFeature
        } catch DataStoreError.preferInMemorySort {
            throw DataStoreError.unsupportedFeature
        } catch {
            logger.error("Failed to fetch snapshots: \(error)")
            if let attachment = self.attachment as? DataStoreObservable {
                attachment.insertPredicateTreeNode(
                    translator.id,
                    title: "Error",
                    content: "\(error)"
                )
            }
            throw error
        }
        func finalize(_ shouldLog: Bool = true, type: String?, result: Result) -> Result {
            if shouldLog || DataStoreDebugging.mode == .trace {
                let count = (result.fetchedSnapshots.count, result.relatedSnapshots.count)
                let description = type == nil ? entityName : "\(type!) \(entityName)"
                logger.info(
                    "Fetched \(count.0) \(description) snapshots (\(count.1) related).",
                    metadata: ["fetched_snapshots": "\(count.0)", "related_snapshots": "\(count.1)"]
                )
                if let attachment = self.attachment as? DataStoreObservable {
                    attachment.insertPredicateTreeNode(
                        translator.id,
                        title: "Success",
                        content: "\(count)"
                    )
                }
            }
            return result
        }
    }
    
    /// Inherited from `DataStore.fetchIdentifiers(_:)`.
    ///
    /// - Parameter request: The fetch request.
    nonisolated public final func fetchIdentifiers<T>(_ request: DataStoreFetchRequest<T>)
    throws -> [PersistentIdentifier] where T: PersistentModel & SendableMetatype {
        var descriptor = request.descriptor
        descriptor.propertiesToFetch = [\.persistentModelID]
        var translator = SQLPredicateTranslator<T>(configuration: configuration)
        let translation = try translator.translate(descriptor)
        let entityName = Schema.entityName(for: T.self)
        if !configuration.options.contains(.disablePredicateCaching),
           let key = translation.key,
           let registry = self.manager.registry(for: request.editingState),
           let result = try registry.cachedResult(forKey: key) as? DataStoreFetchResultMap {
            logger.info("Fetched \(result.fetchedIdentifiers.count) cached \(entityName) identifiers.")
            return result.fetchedIdentifiers
        }
        let persistentIdentifiers = try queue.withConnection(nil) { connection in
            try connection.fetch(translation.statement).compactMap { row -> PersistentIdentifier? in
                try (row.first as? String).flatMap { primaryKey -> PersistentIdentifier? in
                    try PersistentIdentifier.identifier(
                        for: self.identifier,
                        entityName: entityName,
                        primaryKey: primaryKey
                    )
                }
            }
        }
        logger.info("Fetched \(persistentIdentifiers.count) \(entityName) identifiers.")
        return persistentIdentifiers
    }
    
    /// Inherited from `DataStore.fetchCount(_:)`.
    ///
    /// - SwiftData may call `fetchIdentifiers(_:)` instead when the `ModelContext` has a save pending.
    ///
    /// - Parameter request: The fetch request.
    nonisolated public final func fetchCount<T>(_ request: DataStoreFetchRequest<T>)
    throws -> Int where T: PersistentModel & SendableMetatype {
        var translator = SQLPredicateTranslator<T>(configuration: configuration)
        let statement = try translator.translate(request.descriptor, select: "COUNT(*)").statement
        let count = try queue.withConnection(nil) { try $0.execute.count(statement) }
        logger.info("Fetched count of \(count) \(Schema.entityName(for: T.self)) rows.")
        return count
    }
    
    nonisolated public final func fetch<T>(_ descriptor: FetchDescriptor<T>)
    throws -> DatabaseFetchResult<T, Snapshot> where T: PersistentModel & SendableMetatype {
        try self.fetch(DatabaseFetchRequest(
            descriptor: descriptor,
            editingState: .init(id: .init(), author: nil)
        ))
    }
    
    nonisolated public final func fetch<T>(
        for persistentIdentifier: PersistentIdentifier,
        as type: T.Type = T.self,
        editingState: DatabaseEditingState
    ) throws -> Snapshot? where T: PersistentModel & SendableMetatype {
        var descriptor = FetchDescriptor<T>(predicate: #Predicate<T> {
            $0.persistentModelID == persistentIdentifier
        })
        descriptor.fetchLimit = 1
        let request = DatabaseFetchRequest<T>(descriptor: descriptor, editingState: editingState)
        let result: DatabaseFetchResult<T, Snapshot> = try self.fetch(request)
        return result.fetchedSnapshots.first
    }
    
    nonisolated public final func fetch<T>(
        for persistentIdentifier: PersistentIdentifier,
        as type: T.Type,
        relatedSnapshots: inout [PersistentIdentifier: Snapshot]?
    ) throws -> Snapshot where T: PersistentModel & SendableMetatype {
        let bindings = Set([persistentIdentifier])
        var descriptor = FetchDescriptor<T>(predicate: #Predicate<T> {
            bindings.contains($0.persistentModelID)
        })
        descriptor.fetchLimit = 1
        var translator = SQLPredicateTranslator<T>(configuration: configuration)
        let translation = try translator.translate(descriptor)
        guard let row = try self.queue.reader({ connection in
            try connection.fetch(translation.statement).first
        }) else {
            let primaryKey = persistentIdentifier.primaryKey()
            throw SQLError(.rowNotFound, message: "Found no row to load: \(primaryKey)")
        }
        var relatedSnapshots = relatedSnapshots ?? [:]
        return try Snapshot(
            store: consume self,
            properties: translation.properties[...],
            values: row[...],
            relatedSnapshots: &relatedSnapshots
        )
    }
    
    nonisolated public final func fetch<PrimaryKey>(
        for primaryKey: PrimaryKey,
        entity: Schema.Entity,
        relatedSnapshots: inout [PersistentIdentifier: Snapshot]?
    ) throws -> Snapshot where PrimaryKey: LosslessStringConvertible & Sendable {
        guard let row = try self.queue.reader({ connection in
            try connection.query {
                "SELECT * FROM \(quote(entity.name))"
                Where("\(pk) = ?", bindings: primaryKey)
                Limit(1)
            }
        }).first else {
            throw SQLError(.rowNotFound, message: "Row is missing: \(primaryKey)")
        }
        var relatedSnapshots = relatedSnapshots ?? [:]
        return try Snapshot(
            store: self,
            entity: consume entity,
            row: consume row,
            relatedSnapshots: &relatedSnapshots
        )
    }
    
    /// Performs a prefetch on relationships that references these rows in batches.
    ///
    /// - Parameters:
    ///   - entityName: The entity name for the main query.
    ///   - properties: The selected properties.
    ///   - values: The result values that were selected.
    ///   - registry: The associated registry.
    /// - Returns: Related snapshots.
    nonisolated package final func prefetch(
        entityName: String,
        properties: [PropertyMetadata],
        values: [[any Sendable]],
        registry: SnapshotRegistry? = nil
    ) throws -> [PersistentIdentifier: Snapshot] {
        let ownerPrimaryKeys = values.compactMap { $0[0] as? String }
        let ownerPersistentIdentifiers = try ownerPrimaryKeys.compactMap { primaryKey in
            try PersistentIdentifier.identifier(
                for: self.identifier,
                entityName: entityName,
                primaryKey: primaryKey
            )
        }
        let ownerIndexByPrimaryKey = Dictionary(uniqueKeysWithValues: ownerPrimaryKeys
            .enumerated()
            .map { ($0.element, $0.offset) }
        )
        let allowImplicitCachedRelatedSnapshots =
        !self.configuration.options.contains(.disableImplicitPrefetchingUsingCaches)
        let references = self.manager.graph
        var prefetchedRelatedSnapshots = [PersistentIdentifier: Snapshot]()
        for property in properties where property.metadata is Schema.Relationship {
            let relationship = property.metadata as! Schema.Relationship
            let targetsByOwner = try queue.reader { connection in
                try fetchExternalReferenceKeysBatched(
                    ownerPrimaryKeys: ownerPrimaryKeys,
                    ownerPersistentIdentifiers: ownerPersistentIdentifiers,
                    ownerIndexByPrimaryKey: ownerIndexByPrimaryKey,
                    in: property,
                    graph: references,
                    connection: connection,
                    chunkSize: 400
                )
            }
            let shouldIncludeCachedRelatedSnapshots =
            property.flags.contains(.prefetch) || allowImplicitCachedRelatedSnapshots
            if shouldIncludeCachedRelatedSnapshots,
               let registry,
               !targetsByOwner.isEmpty {
                var referencedIdentifiers = Set<PersistentIdentifier>()
                referencedIdentifiers.reserveCapacity(targetsByOwner.count)
                for (_, relatedIdentifiers) in targetsByOwner {
                    referencedIdentifiers.formUnion(relatedIdentifiers)
                }
                if !referencedIdentifiers.isEmpty {
                    let cached = registry.snapshots(for: Array(referencedIdentifiers))
                    if !cached.isEmpty {
                        prefetchedRelatedSnapshots.merge(
                            cached,
                            uniquingKeysWith: { existing, _ in existing }
                        )
                    }
                }
            }
            if property.flags.contains(.prefetch) {
                guard !relationship.isToOneRelationship else {
                    continue
                }
                var ownerPrimaryKeysToPrefetch = [String]()
                ownerPrimaryKeysToPrefetch.reserveCapacity(ownerPrimaryKeys.count)
                for (index, ownerPersistentIdentifier) in ownerPersistentIdentifiers.enumerated() {
                    guard let relatedIdentifiers = targetsByOwner[ownerPersistentIdentifier],
                          !relatedIdentifiers.isEmpty else {
                        continue
                    }
                    if relatedIdentifiers.contains(where: { prefetchedRelatedSnapshots[$0] == nil }) {
                        ownerPrimaryKeysToPrefetch.append(ownerPrimaryKeys[index])
                    }
                }
                if ownerPrimaryKeysToPrefetch.isEmpty {
                    continue
                }
                let rowsByOwnerPrimaryKey = try queue.reader { connection in
                    try fetchExternalRowsBatched(
                        for: ownerPrimaryKeysToPrefetch,
                        in: property,
                        connection: connection
                    )
                }
                var destinationType = unwrapArrayMetatype(relationship.valueType)
                if !relationship.isOptional { destinationType = unwrapOptionalMetatype(destinationType) }
                guard let destinationType = destinationType as? any PersistentModel.Type else {
                    continue
                }
                let discriminator = PropertyMetadata.discriminator(for: destinationType)
                let destinationProperties = [discriminator] + destinationType.databaseSchemaMetadata
                for (ownerPrimaryKey, destinationRows) in rowsByOwnerPrimaryKey {
                    guard ownerIndexByPrimaryKey[ownerPrimaryKey] != nil else {
                        continue
                    }
                    let relatedIdentifiers = try destinationRows.compactMap { row in
                        try (row.first as? String).flatMap { destinationPrimaryKey in
                            try PersistentIdentifier.identifier(
                                for: self.identifier,
                                entityName: relationship.destination,
                                primaryKey: destinationPrimaryKey
                            )
                        }
                    }
                    for (index, row) in destinationRows.enumerated() {
                        let relatedIdentifier = relatedIdentifiers[index]
                        if prefetchedRelatedSnapshots[relatedIdentifier] != nil {
                            continue
                        }
                        var sink = [PersistentIdentifier: Snapshot]()
                        let snapshot = try Snapshot(
                            store: self,
                            registry: registry,
                            properties: destinationProperties[...],
                            values: row[...],
                            relatedSnapshots: &sink
                        )
                        prefetchedRelatedSnapshots[relatedIdentifier] = snapshot
                        prefetchedRelatedSnapshots.merge(sink, uniquingKeysWith: { $1 })
                    }
                }
            }
        }
        return prefetchedRelatedSnapshots
    }
    
    /// Fetches the backing data asynchronously as a preload warm-up for the `EditingState` expected to request for it.
    /// - Parameter request: A specific preloading fetch request.
    @concurrent @discardableResult
    package final func preload<Model>(_ request: PreloadFetchRequest<Model>)
    async throws -> any Hashable & Sendable {
        let result: PreloadFetchResult = try fetch(request)
        try Task.checkCancellation()
        // Is unchecked?
        await manager.preload(result, for: request.editingState)
        return result.key // modifier
    }
    
    // TODO: Inheritance has not been applied to update and delete operations.
    
    /// Inherited from `DataStore.save(_:)`.
    ///
    /// - Parameter request:
    ///   - `DataStoreSaveChangesRequest` is provided by the `ModelContext`.
    ///   - `DatabaseStoreSaveChangesRequest` is the default user-provided type.
    /// - Returns:
    ///   - `remappedIdentifiers`: The temporary and permanent identifiers of inserted models.
    ///   - `snapshotsToReregister`: The requested snapshots where their identity has changed.
    nonisolated public final func save<Request, Result>(_ request: Request) throws -> Result
    where Request: SaveChangesRequest<Snapshot>, Result: SaveChangesResult<Snapshot> {
        attachment?.storeWillSave()
        let count = (request.inserted.count, request.updated.count, request.deleted.count)
        let metadata: Logger.Metadata = [
            "editing_state": "\(request.editingState.id)",
            "author": "\(request.editingState.author ?? "nil")"
        ]
        logger.log(
            level: configuration.options.contains(.logSaveRequest) ? .notice : .info,
            "DataStore save request: \(count.0) inserts, \(count.1) updates, \(count.2) deletes.",
            metadata: metadata
        )
        var remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] = [:]
        var snapshotsToReregister: [PersistentIdentifier: Snapshot] = [:]
        var snapshots: [PersistentIdentifier: Snapshot] = [:]
        var dependencies: [PersistentIdentifier: [Int]] = [:]
        var invalidatedIdentifiers: Set<PersistentIdentifier> = []
        var operation: [DataStoreOperation: [PersistentIdentifier]] = [:]
        var upsertedUpdatedIdentifiers: Set<PersistentIdentifier> = .init()
        #if swift(>=6.2) && !SwiftPlaygrounds
        let connection = try queue.connection(.writer, for: request.editingState)
        #else
        var connection = try queue.connection(.writer, for: request.editingState)
        #endif
        try connection.withTransaction(nil) {
            var inheritedIdentifiers = Set<UUID>()
            var inserted = Deque(request.inserted.map { Payload(snapshot: $0) })
            let maxInsertAttempts = 10
            while let insert = inserted.popFirst() {
                var snapshot = insert.snapshot
                try connection.checkCancellation()
                do {
                    let temporaryIdentifier = snapshot.persistentIdentifier
                    let permanentIdentifier = try remappedIdentifiers[temporaryIdentifier]
                    ?? PersistentIdentifier.identifier(
                        for: self.identifier,
                        entityName: temporaryIdentifier.entityName,
                        primaryKey: UUID().uuidString
                    )
                    #if DEBUG
                    logger.trace({
                        let attempts = "\(insert.attempts + 1) of \(maxInsertAttempts)"
                        let description = "\(temporaryIdentifier) -> \(permanentIdentifier)"
                        return "Inserting snapshot (attempt: \(attempts)): \(description)"
                    }())
                    #endif
                    snapshot = snapshot.copy(
                        persistentIdentifier: permanentIdentifier,
                        remappedIdentifiers: remappedIdentifiers
                    )
                    var export = snapshot.export
                    if !export.toOneDependencies.isEmpty {
                        logger.debug("Insert has to-one dependencies: \(export.toOneDependencies)")
                        for index in export.toOneDependencies {
                            let pair = snapshot[index]
                            guard let relatedIdentifier = pair.value as? PersistentIdentifier else {
                                continue
                            }
                            if pair.property.metadata.isOptional {
                                continue
                            }
                            if relatedIdentifier.storeIdentifier == nil,
                               let destination = self.schema.entitiesByName[relatedIdentifier.entityName],
                               let relationship = pair.property.metadata as? Schema.Relationship,
                               let inverseName = relationship.inverseName,
                               let inverseRelationship = destination.relationshipsByName[inverseName] {
                                if !inverseRelationship.isOptional {
                                    let resolvedRelatedIdentifier =
                                    try remappedIdentifiers[relatedIdentifier]
                                    ?? PersistentIdentifier.identifier(
                                        for: self.identifier,
                                        entityName: relatedIdentifier.entityName,
                                        primaryKey: UUID().uuidString
                                    )
                                    remappedIdentifiers[relatedIdentifier] =
                                    resolvedRelatedIdentifier
                                    snapshot = snapshot.copy(
                                        persistentIdentifier: snapshot.persistentIdentifier,
                                        remappedIdentifiers: remappedIdentifiers
                                    )
                                    export = snapshot.export
                                    logger.debug(
                                        "Breaking dependency cycle for to-one relationship.",
                                        metadata: [
                                            "destination_entity": "\(destination.name)",
                                            "inverse_relationship": "\(inverseRelationship.name)",
                                            "old_identifier": "\(relatedIdentifier)",
                                            "new_identifier": "\(resolvedRelatedIdentifier)"
                                        ]
                                    )
                                    if snapshots[resolvedRelatedIdentifier] == nil {
                                        throw Snapshot.Error.referencesInvalidPersistentIdentifier
                                    }
                                }
                            }
                        }
                    }
                    if !export.toOneDependencies.isEmpty {
                        // All to-one relationships have a foreign key column.
                        // Do not continue until all related identifiers has been remapped.
                        throw Snapshot.Error.referencesInvalidPersistentIdentifier
                    }
                    remappedIdentifiers[temporaryIdentifier] = permanentIdentifier
                    if !export.inheritedDependencies.isEmpty,
                       inheritedIdentifiers.insert(insert.id).inserted,
                       let entity = self.schema.entitiesByName[snapshot.entityName],
                       let superentity = entity.superentity {
                        var superentitySnapshots = [Snapshot]()
                        try snapshot.recursiveExportChain(
                            on: superentity,
                            indices: export.inheritedDependencies,
                            inheritedTraversalSnapshots: &superentitySnapshots
                        )
                        inserted.prepend(contentsOf: superentitySnapshots.reduce(into: [Payload]()) {
                            remappedIdentifiers[$1.persistentIdentifier] = $1.persistentIdentifier
                            $0.append(Payload(id: insert.id, snapshot: $1))
                        })
                        throw Snapshot.Error.referencesInvalidPersistentIdentifier
                    }
                    switch configuration.constraints[permanentIdentifier.entityName] {
                    case let uniquenessConstraints? where !uniquenessConstraints.isEmpty:
                        let candidate = try connection.fetchByUniqueness(snapshot, uniquenessConstraints: uniquenessConstraints) { snapshot in
                            remappedIdentifiers[temporaryIdentifier] = snapshot.persistentIdentifier
                            try connection.insert(snapshot)
                            logger.info("Inserted snapshot (no uniqueness match): \(snapshot.persistentIdentifier)")
                            operation[.insert, default: []].append(snapshot.persistentIdentifier)
                            return snapshot
                        } onExisting: { existing, candidate in
                            remappedIdentifiers[temporaryIdentifier] = existing.persistentIdentifier
                            let candidate = candidate.copy(
                                persistentIdentifier: existing.persistentIdentifier,
                                remappedIdentifiers: remappedIdentifiers
                            )
                            try connection.update(from: existing, to: candidate)
                            logger.info("Upserted snapshot on matching key: \(candidate.persistentIdentifier)")
                            upsertedUpdatedIdentifiers.insert(candidate.persistentIdentifier)
                            operation[.update, default: []].append(candidate.persistentIdentifier)
                            return candidate
                        } onConflict: { existing, candidate in
                            remappedIdentifiers[temporaryIdentifier] = existing.persistentIdentifier
                            let candidate = candidate.copy(
                                persistentIdentifier: existing.persistentIdentifier,
                                remappedIdentifiers: remappedIdentifiers
                            )
                            try connection.update(from: existing, to: candidate)
                            logger.info("Upserted snapshot on conflict: \(candidate.persistentIdentifier)")
                            upsertedUpdatedIdentifiers.insert(candidate.persistentIdentifier)
                            operation[.update, default: []].append(candidate.persistentIdentifier)
                            return candidate
                        }
                        guard let candidate else {
                            fatalError()
                        }
                        snapshot = candidate
                        export = snapshot.export
                    default:
                        try connection.insert(snapshot)
                        operation[.insert, default: []].append(snapshot.persistentIdentifier)
                        logger.info("Inserted snapshot: \(snapshot.persistentIdentifier)")
                    }
                    if !export.toManyDependencies.isEmpty {
                        dependencies[snapshot.persistentIdentifier, default: []]
                            .append(contentsOf: export.toManyDependencies)
                    }
                    snapshots[snapshot.persistentIdentifier] = /*consume*/ snapshot
                } catch Snapshot.Error.referencesInvalidPersistentIdentifier {
                    var deferred = consume insert
                    deferred.attempts += 1
                    if deferred.attempts < maxInsertAttempts {
                        inserted.append(deferred)
                    } else {
                        throw Self.Error.exceededMaximumInsertAttempts
                    }
                } catch {
                    logger.debug(
                        "An insert error occurred: \(error)",
                        metadata: ["snapshot": "\(snapshot.debugDescription)"]
                    )
                    throw error
                }
            }
            for (temporaryIdentifier, _) in remappedIdentifiers {
                if temporaryIdentifier.storeIdentifier != nil {
                    remappedIdentifiers[temporaryIdentifier] = nil
                    logger.trace("Cleaning up inherited identifier: \(temporaryIdentifier)")
                }
            }
            for (persistentIdentifier, indices) in dependencies {
                try connection.checkCancellation()
                guard var snapshot = snapshots[persistentIdentifier] else {
                    fatalError("Inserted snapshot not found: \(persistentIdentifier)")
                }
                defer { snapshots[persistentIdentifier] = snapshot }
                logger.debug("Inserted dependencies: \(snapshot) - \(indices)")
                snapshot = snapshot.copy(
                    persistentIdentifier: snapshot.persistentIdentifier,
                    remappedIdentifiers: remappedIdentifiers
                )
                let isUpsertUpdate = upsertedUpdatedIdentifiers.contains(persistentIdentifier)
                let results = try snapshot.reconcileExternalReferences(
                    comparingTo: nil,
                    indices: indices,
                    shouldAddOnly: isUpsertUpdate,
                    graph: connection.context?.graph,
                    connection: connection
                )
                invalidatedIdentifiers.formUnion(results.unlinked)
                logger.debug(
                    "The backing datas referencing this snapshot has been updated.",
                    metadata: [
                        "event": "insert",
                        "entity": "\(persistentIdentifier.entityName)",
                        "primary_key": "\(snapshot.primaryKey)",
                        "snapshot": "\(snapshot.contentDescriptions(where: { $0.isRelationship }))",
                        "linked": "\(results.linked)",
                        "unlinked": "\(results.unlinked)"
                    ]
                )
            }
            dependencies.removeAll(keepingCapacity: true)
            var updated = Deque(request.updated.map { Payload(snapshot: $0) })
            while let update = updated.popFirst() {
                var snapshot = update.snapshot
                try connection.checkCancellation()
                if !request.inserted.isEmpty {
                    snapshot = snapshot.copy(
                        persistentIdentifier: snapshot.persistentIdentifier,
                        remappedIdentifiers: remappedIdentifiers
                    )
                }
                let cachedSnapshot = connection.context?.snapshot(for: snapshot.persistentIdentifier)
                let export = snapshot.export
                try connection.update(from: cachedSnapshot, to: snapshot)
                if !export.toManyDependencies.isEmpty {
                    dependencies[snapshot.persistentIdentifier, default: []]
                        .append(contentsOf: export.toManyDependencies)
                }
                operation[.update, default: []].append(snapshot.persistentIdentifier)
                snapshots[snapshot.persistentIdentifier] = snapshot
                logger.info("Updated snapshot: \(snapshot.persistentIdentifier)")
            }
            for (persistentIdentifier, indices) in dependencies {
                try connection.checkCancellation()
                guard var snapshot = snapshots[persistentIdentifier] else {
                    fatalError("Updated snapshot not found: \(persistentIdentifier)")
                }
                defer { snapshots[persistentIdentifier] = snapshot }
                snapshot = snapshot.copy(
                    persistentIdentifier: snapshot.persistentIdentifier,
                    remappedIdentifiers: remappedIdentifiers
                )
                let cachedSnapshot = connection.context?.snapshot(for: snapshot.persistentIdentifier)
                let results = try snapshot.reconcileExternalReferences(
                    comparingTo: cachedSnapshot?.copy(
                        persistentIdentifier: cachedSnapshot!.persistentIdentifier,
                        remappedIdentifiers: remappedIdentifiers
                    ),
                    indices: indices,
                    shouldAddOnly: false,
                    graph: connection.context?.graph,
                    connection: connection
                )
                invalidatedIdentifiers.formUnion(results.unlinked)
                logger.debug(
                    "The backing datas referencing this snapshot has been updated.",
                    metadata: [
                        "event": "update",
                        "entity": "\(persistentIdentifier.entityName)",
                        "primary_key": "\(snapshot.primaryKey)",
                        "snapshot": "\(snapshot.contentDescriptions(where: { $0.isRelationship }))",
                        "linked": "\(results.linked)",
                        "unlinked": "\(results.unlinked)"
                    ]
                )
            }
            dependencies.removeAll(keepingCapacity: true)
            var deleted = Deque(request.deleted.map { Payload(snapshot: $0) })
            while let delete = deleted.popFirst() {
                var snapshot = delete.snapshot
                do {
                    let export = snapshot.delete
                    if false {
                        let results = try snapshot.reconcileExternalReferencesBeforeDelete(
                            indices: export.toOneDependencies + export.toManyDependencies,
                            connection: connection
                        )
                        invalidatedIdentifiers.formUnion(results.unlinked)
                        invalidatedIdentifiers.formUnion(results.cascaded)
                        for _ in results.unlinked {}
                        for _ in results.cascaded {}
                        logger.debug(
                            "The backing datas referencing this snapshot has been updated.",
                            metadata: [
                                "event": "delete",
                                "entity": "\(snapshot.entityName)",
                                "primary_key": "\(snapshot.primaryKey)",
                                "snapshot": "\(snapshot.contentDescriptions(where: { $0.isRelationship }))",
                                "unlinked": "\(results.unlinked)",
                                "cascaded": "\(results.cascaded)"
                            ]
                        )
                    }
                    try connection.delete(snapshot)
                    operation[.delete, default: []].append(snapshot.persistentIdentifier)
                    invalidatedIdentifiers.insert(snapshot.persistentIdentifier)
                    snapshots[snapshot.persistentIdentifier] = nil
                    snapshotsToReregister[snapshot.persistentIdentifier] = snapshot
                    logger.notice(
                        "Deleted snapshot: \(snapshot.persistentIdentifier)",
                        metadata: ["preserved_values": "\(export.values)"]
                    )
                } catch {
                    if invalidatedIdentifiers.insert(snapshot.persistentIdentifier).inserted {
                        deleted.append(delete)
                        logger.debug("Deferring deleted snapshot: \(snapshot.persistentIdentifier)")
                    } else {
                        throw error
                    }
                }
            }
            dependencies.removeAll(keepingCapacity: true)
            let canonicalRemappings = self.manager.graph.canonicalRemaps(remappedIdentifiers)
            self.manager.graph.remap(using: canonicalRemappings)
            if let attachment = self.attachment {
                attachment.storeDidSave(
                    inserted: operation[.insert] ?? [],
                    updated: operation[.update] ?? [],
                    deleted: operation[.delete] ?? []
                )
            }
            #if DEBUG
            if configuration.options.contains(.useDetailedLogging) {
                logPersistentIdentifiers(request: request)
                logSaveChangesResult(
                    remappedIdentifiers: remappedIdentifiers,
                    snapshotsToReregister: snapshotsToReregister
                )
                assertSaveChangesResult(
                    request: request,
                    remappedIdentifiers: remappedIdentifiers
                )
            }
            #endif
        }
        connection.context?.synchronize(
            snapshots: snapshots,
            invalidateIdentifiers: invalidatedIdentifiers
        )
        logger.info(
            "Saved \(remappedIdentifiers.count) new snapshots.",
            metadata: [
                "editing_state": "\(request.editingState.id)",
                "author": "\(request.editingState.author ?? "nil")",
                "snapshots_to_reregister": "\(snapshotsToReregister.count)"
            ]
        )
        let operationCopy = operation
        DispatchQueue.main.async {
            NotificationCenter.default.post(
                name: .dataStoreDidSave,
                object: nil,
                userInfo: ["operation": operationCopy]
            )
        }
        return Result(
            for: self.identifier,
            remappedIdentifiers: remappedIdentifiers,
            snapshotsToReregister: snapshotsToReregister
        )
    }
    
    /// Inherited from `DataStore.erase()`.
    nonisolated public final func erase() throws {
        try self.queue.close()
        let externalStorageURL = self.configuration.externalStorageURL
        if FileManager.default.fileExists(atPath: externalStorageURL.path) {
            try FileManager.default.removeItem(at: externalStorageURL)
            logger.info("The external storage directory has been deleted: \(externalStorageURL.path)")
        } else {
            logger.warning("The external storage directory cannot be found: \(externalStorageURL.path)")
        }
        guard let storeURL = self.configuration.url else {
            return
        }
        try Handle.remove(storeURL: storeURL)
    }
    
    private struct Payload {
        nonisolated internal var id: UUID = .init()
        nonisolated internal var attempts: Int = 0
        nonisolated internal var snapshot: Snapshot
    }
}

extension DatabaseStore {
    /// Inherited from `DataStore.initializeState(for:)`.
    ///
    /// Executes when a `ModelContext` is initialized or before a model was reregistered.
    /// - Parameter editingState: A type belonging to a `ModelContext` that reveals an identifier and author.
    nonisolated public final func initializeState(for editingState: EditingState) {
        logger.debug(
            "Initializing editing state: \(editingState.id) - \(editingState.author ?? "nil")",
            metadata: ["thread": .string(threadDescription)]
        )
        manager.initializeState(for: editingState)
        DataStoreContainer.initializeState(for: editingState, store: self)
    }
    
    /// Inherited from `DataStore.invalidateState(for:)`.
    ///
    /// Executes when a `ModelContext` is deinitialized or after a model was reregistered.
    /// - Parameter editingState: A type belonging to a `ModelContext` that reveals an identifier and author.
    nonisolated public final func invalidateState(for editingState: EditingState) {
        logger.debug(
            "Invalidating editing state: \(editingState.id) - \(editingState.author ?? "nil")",
            metadata: ["thread": .string(threadDescription)]
        )
        manager.invalidateState(for: editingState)
        DataStoreContainer.invalidateState(for: editingState)
    }
    
    /// Inherited from `DataStore.cachedSnapshots(for:editingState:)`.
    nonisolated public final func cachedSnapshots(
        for persistentIdentifiers: [PersistentIdentifier],
        editingState: EditingState
    ) throws -> [PersistentIdentifier: Snapshot] {
        #if DEBUG
        if configuration.options.contains(._internal) {
            fatalError("\(#function) - called")
        }
        #endif
        if let registry = self.manager.registry(for: editingState) {
            return registry.snapshots(for: persistentIdentifiers)
        } else {
            return [:]
        }
    }
}

extension DatabaseStore {
    /// Returns the value associated with the specified key from the data store's internal table.
    ///
    /// - Parameters:
    ///   - key: The key to retrieve from the database.
    ///   - type: The value type to be decoded back into.
    nonisolated public final func getValue<T>(
        forKey key: String,
        as type: T.Type
    ) throws -> T? where T: DataStoreSnapshotValue {
        let connection = try self.queue.connection(nil)
        return try getValue(forKey: key, as: type, connection: connection)
    }
    
    nonisolated public final func getValue<T>(
        forKey key: String,
        as type: T.Type,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws -> T? where T: DataStoreSnapshotValue {
        guard let result = try connection.fetch(
            """
            SELECT \(InternalTable.value.rawValue)
            FROM \(InternalTable.tableName)
            WHERE \(InternalTable.key.rawValue) = ?
            LIMIT 1
            """,
            bindings: [key]
        ).first as? [String] else {
            return nil
        }
        return try JSONDecoder().decode(T.self, from: Data(result[0].utf8))
    }
    
    /// Sets the value of the specified key to the data store's internal table.
    ///
    /// - Parameters:
    ///   - value: The value to store in the database.
    ///   - key: The key to store the value into.
    nonisolated public final func setValue<T>(
        _ value: T,
        forKey key: String
    ) throws where T: DataStoreSnapshotValue {
        let connection = try self.queue.connection(.writer)
        try setValue(value, forKey: key, connection: connection)
    }
    
    nonisolated public final func setValue<T>(
        _ value: T,
        forKey key: String,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws where T: DataStoreSnapshotValue {
        try connection.execute.insert(
            into: InternalTable.tableName,
            orReplace: true,
            values: [
                InternalTable.key.rawValue: key,
                InternalTable.value.rawValue: try String(
                    decoding: JSONEncoder().encode(value),
                    as: UTF8.self
                )
            ]
        )
    }
    
    /// Removes the value for the specified key from the data store's internal table.
    ///
    /// - Parameter key: The key with the value to remove.
    nonisolated public final func removeValue(forKey key: String) throws {
        try self.queue.connection(.writer).execute.delete(
            from: InternalTable.tableName,
            where: "\(InternalTable.key.rawValue) = ?",
            bindings: [key]
        )
    }
}
