//
//  DataStoreConfiguration.swift
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
import Synchronization
import SQLiteHandle
import SQLiteStatement

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

/// A type that describes the configuration of an app's schema or specific group of models.
public struct DatabaseConfiguration: DataStoreConfiguration, Sendable {
    /// Inherited from `DataStoreConfiguration.Store`.
    public typealias Store = DatabaseStore
    nonisolated private var storage: Storage
    
    nonisolated private mutating func ensureUniqueStorage() {
        if storage.container.load() != nil {
            logger.warning("DatabaseConfiguration mutated after DatabaseStore initialization has no effect.")
        }
        if !isKnownUniquelyReferenced(&storage) {
            self.storage = storage.copy()
        }
    }
    
    nonisolated internal var makeDatabaseQueue:
    @Sendable (Store.Attachment?) throws -> DatabaseQueue<Store> {
        storage.makeDatabaseQueue
    }
    
    /// The `DataStore` instantiated by the `ModelContainer`.
    nonisolated public var store: Store? {
        try? storage.container.load()?.load() as? Store
    }
    
    /// The attachment assigned when this object was created.
    nonisolated public var attachment: (any DataStoreDelegate)? {
        storage.attachment
    }
    
    /// Inherited from `DataStoreConfiguration.name`.
    nonisolated public var name: String {
        storage.name.withLock(\.self)
    }
    
    /// Inherited from `DataStoreConfiguration.schema`.
    nonisolated public var schema: Schema? {
        get { storage.schema.load() }
        set {
            ensureUniqueStorage()
            if let newValue { _ = self.storage.schema.storeIfNil(newValue) }
        }
    }
    
    /// The migration plan that describes how the schema migrates between specific versions.
    nonisolated public var migrationPlan: (any SchemaMigrationPlan.Type)? {
        get { storage.migrationPlan.withLock(\.self) }
        set { ensureUniqueStorage(); storage.migrationPlan.withLock { $0 = newValue } }
    }
    
    /// The uniqueness constraints applied to the SQL schema.
    nonisolated internal var constraints: [String: [[String]]] {
        storage.constraints
    }
    
    /// The underlying SQLite store type representing where the database resides.
    nonisolated internal var location: SQLite.StoreType {
        get { storage.location.withLock(\.self) }
        set { ensureUniqueStorage(); storage.location.withLock { $0 = newValue } }
    }
    
    /// The location of the data store.
    nonisolated public var url: URL? {
        switch location {
        case .file(let path): URL(filePath: path)
        default: nil
        }
    }
    
    /// The auxiliary location of attributes that are configured to use external storage.
    nonisolated public var externalStorageURL: URL {
        storage.externalStorageURL
    }
    
    /// Indicates whether the data store is writable.
    nonisolated internal var allowsSave: Bool {
        get { storage.allowsSave.withLock(\.self) }
        set { ensureUniqueStorage(); storage.allowsSave.withLock { $0 = newValue } }
    }
    
    /// The options that configure additional flags related to the data store runtime.
    nonisolated public var options: DataStoreOptions {
        get { storage.options.withLock(\.self) }
        set { ensureUniqueStorage(); storage.options.withLock { $0 = newValue } }
    }
    
    nonisolated public var configurations: [Key: any OptionSet & Sendable] {
        get { storage.configurations.withLock(\.self) }
        set { ensureUniqueStorage(); storage.configurations.withLock { $0 = newValue } }
    }
    
    /// The cache policy used by the data store for managing in-memory object lifetimes.
    nonisolated internal var cachePolicy: CachePolicy {
        get { storage.cachePolicy.withLock(\.self) }
        set { ensureUniqueStorage(); storage.cachePolicy.withLock { $0 = newValue } }
    }
    
    nonisolated public var synchronizers: [any DataStoreSynchronizerConfiguration] {
        get { storage.synchronizers.withLock(\.self) }
        set { ensureUniqueStorage(); storage.synchronizers.withLock { $0 = newValue } }
    }
    
    /// The CloudKit configuration to sync the database with.
    nonisolated internal var cloudKit: CloudKitDatabase? {
        get {
            storage.synchronizers.withLock { synchronizers in
                synchronizers.first { $0 is CloudKitDatabase } as? CloudKitDatabase
            }
        }
        set {
            ensureUniqueStorage()
            storage.synchronizers.withLock { synchronizers in
                synchronizers.removeAll { $0 is CloudKitDatabase }
                if let newValue {
                    synchronizers.append(newValue)
                }
            }
        }
    }
    
    public enum Key: Equatable, Hashable, Sendable {
        case predicate
    }
    
    nonisolated private init(
        name: String,
        types: [any (PersistentModel & SendableMetatype).Type] = [],
        schema: Schema? = nil,
        migrationPlan: (any SchemaMigrationPlan.Type)? = nil,
        options: consuming DataStoreOptions = [],
        flags: SQLite.Flags,
        location: SQLite.StoreType,
        externalStorageURL: URL? = nil,
        allowsSave: Bool,
        size: Int,
        mode: DataStoreDebugging = .default,
        cachePolicy: CachePolicy = .default,
        attachment: (any DataStoreDelegate)? = nil,
        synchronizers: [any DataStoreSynchronizerConfiguration] = []
    ) {
        let location: SQLite.StoreType = {
            guard case .file(let path) = location else {
                return location
            }
            let url = URL(filePath: path)
            guard url.hasDirectoryPath else {
                return location
            }
            let storeURL = url.appending(component: name + ".store", directoryHint: .notDirectory)
            return .file(path: storeURL.path)
        }()
        let externalStorageURL: URL = {
            let externalStorageName = ".Storage"
            func validateDirectoryIfExists(_ url: URL) -> URL {
                var isDirectory: ObjCBool = false
                if FileManager.default.fileExists(atPath: url.path, isDirectory: &isDirectory) {
                    precondition(
                        isDirectory.boolValue,
                        "External storage URL exists, but is not a directory: \(url)"
                    )
                }
                return url
            }
            if let externalStorageURL {
                return validateDirectoryIfExists(externalStorageURL)
            }
            guard case .file(let path) = location else {
                return URL.temporaryDirectory
                    .appending(path: UUID().uuidString, directoryHint: .isDirectory)
                    .appending(path: externalStorageName, directoryHint: .isDirectory)
            }
            let url = URL(filePath: path)
            return validateDirectoryIfExists(
                url
                    .deletingLastPathComponent()
                    .appending(path: externalStorageName, directoryHint: .isDirectory)
            )
        }()
        if mode == .trace || options.contains(._internal) { DataStoreDebugging.mode = .trace }
        let schema = schema ?? (!types.isEmpty ? Schema(types) : nil)
        #if true
        if let schema {
            TypeRegistry.bootstrap(schema: schema, types: types)
        }
        #else
        for entity in schema?.entities ?? [] {
            switch types.first(where: { String(describing: $0) == entity.name }) ?? entity.type {
            case let type as any (PersistentModel & AnyObject).Type:
                TypeRegistry.register(type, typeName: entity.name, metadata: entity)
            default:
                preconditionFailure("Entity has an unknown type: \(entity.name)")
            }
        }
        #endif
        let constraints = (schema?.entities ?? []).reduce(into: [String: [[String]]]()) { result, entity in
            result[entity.name] = entity.uniquenessConstraints.reduce(into: [[String]]()) { result, group in
                result.append(group.reduce(into: [String]()) { result, propertyName in
                    switch entity.relationshipsByName[propertyName] {
                    case let relationship? where relationship.isToOneRelationship:
                        result.append("\(relationship.name)_pk")
                    default:
                        result.append(propertyName)
                    }
                })
            }
        }
        if getEnvironmentValue(for: "DATASTORE_TRACE") == "TRUE" {
            options.insert(.useVerboseLogging)
            options.insert(.useDetailedLogging)
        }
        let options = options
        let makeDatabaseQueue:
        @Sendable (Store.Attachment?) throws -> DatabaseQueue<Store> = { attachment in
            try DatabaseQueue(
                at: location,
                flags: flags,
                size: size,
                attachment: attachment,
                makeTransactionAttachment: { editingState, handle in
                    guard options.contains(.disablePersistentHistoryTracking) == false else {
                        return Optional<Store.Transaction>.none
                    }
                    guard let attachment, let identifier = attachment.store?.identifier else {
                        logger.warning("No store for transaction attachment.")
                        return nil
                    }
                    defer {
                        Task { @DatabaseActor in
                            attachment.store?.history?.run(force: false)
                        }
                    }
                    return TransactionObject(
                        handle: handle,
                        manager: attachment,
                        storeIdentifier: identifier,
                        externalStorageURL: externalStorageURL,
                        editingState: editingState
                    )
                },
                onTransactionFailure: { connection in
                    let error = connection.error
                    let violations = connection.checkConstraintDiagnostics(error)
                    violations.forEach { violation in
                        if let rowid = violation.rowid {
                            let result = (try? connection.query(
                                """
                                SELECT rowid, * 
                                FROM \(violation.table) 
                                WHERE rowid = \(rowid.description)
                                """
                            )) ?? []
                            logger.critical("Affected rows: \(violation.table) Row ID - \(rowid)\n\(result)")
                        }
                    }
                    if let observable = attachment as? any DataStoreObservable {
                        observable.onTransactionFailure(violations)
                    } else {
                        logger.debug(
                            "No observable model found.",
                            metadata: [
                                "name": .string(name),
                                "url": "\(location.description)",
                                "error": "\(error)",
                                "violations": "\(violations)"
                            ]
                        )
                    }
                },
                onUpdate: { operation, database, table, rowID in
                    guard table != InternalTable.tableName && table != HistoryTable.tableName else {
                        return
                    }
                    logger.trace("Callback \(operation) \(database) \(table) \(rowID)")
                }
            )
        }
        self.storage = .init(
            name: name,
            schema: schema,
            migrationPlan: migrationPlan,
            constraints: constraints,
            configurations: [:],
            options: options,
            location: location,
            externalStorageURL: externalStorageURL,
            allowsSave: allowsSave,
            cachePolicy: cachePolicy,
            attachment: attachment,
            synchronizers: synchronizers,
            container: nil,
            makeDatabaseQueue: makeDatabaseQueue
        )
    }
    
    /// Creates a persistent storage data store configuration.
    ///
    /// - Parameters:
    ///   - name:
    ///     The name of the data store configuration, which also becomes the store's identifier.
    ///     If `nil`, the filename is used.
    ///   - types:
    ///     Model types to manually register in the data store.
    ///   - schema:
    ///     A schema that maps model classes to the associated data in persistent storage.
    ///   - migrationPlan:
    ///     A plan that describes how the schema migrates between specific versions.
    ///   - url:
    ///     The on-disk location of the schema's persistent storage.
    ///     If `nil`, it defaults to `Data.store` in the user's Application Support directory.
    ///   - externalStorageURL:
    ///     The on-disk location that contains the schema's external storage.
    ///     If `nil`, it defaults to a hidden directory alongside the store.
    ///   - allowsSave:
    ///     A Boolean value that determines whether the database is opened with read/write or read-only capabilities.
    ///   - size:
    ///     The number of database connections to add to the database queue. Only one connection is a writer.
    ///   - options:
    ///     Additional flags related to the data store runtime.
    ///   - attachment:
    ///     An attachment object that conforms to `DataStoreDelegate`.
    ///   - cloudKit:
    ///     A configuration for setting up CloudKit synchronization.
    nonisolated public init(
        name: String? = nil,
        types: [any (PersistentModel & SendableMetatype).Type] = [],
        schema: Schema? = nil,
        migrationPlan: (any SchemaMigrationPlan.Type)? = nil,
        url: URL? = nil,
        externalStorageURL: URL? = nil,
        allowsSave: Bool = true,
        size: Int = 4,
        options: DataStoreOptions = [],
        attachment: (any DataStoreDelegate)? = nil,
        cloudKit: CloudKitDatabase? = nil,
        synchronizers: [any DataStoreSynchronizerConfiguration] = [] // TODO: Update documentation.
    ) {
        let resolvedName: String = {
            if let name, !name.isEmpty { return name }
            if let url {
                if url.hasDirectoryPath {
                    let component = url.lastPathComponent
                    if !component.isEmpty { return component }
                    let trimmedComponent = url.deletingLastPathComponent()
                    return trimmedComponent.lastPathComponent.isEmpty
                    ? "Data"
                    : trimmedComponent.lastPathComponent
                } else {
                    return url.deletingPathExtension().lastPathComponent
                }
            }
            return "Data"
        }()
        let isTransient = options.contains(.temporary)
        let resolvedURL: URL = {
            if isTransient {
                return Self.transientStoreURL(for: url, name: resolvedName)
            }
            guard let url else {
                return defaultStoreURL(in: .applicationSupportDirectory)
            }
            if url.hasDirectoryPath {
                return defaultStoreURL(in: url)
            }
            if name != nil {
                return defaultStoreURL(in: url.deletingLastPathComponent())
            }
            return url
        }()
        func defaultStoreURL(in directoryURL: URL) -> URL {
            directoryURL.appending(
                component: resolvedName + ".store",
                directoryHint: .notDirectory
            )
        }
        precondition(resolvedURL.isFileURL, "Store URL must be a file URL: \(resolvedURL)")
        var resolvedSynchronizers = synchronizers
        if let cloudKit {
            resolvedSynchronizers.removeAll { $0 is CloudKitDatabase }
            resolvedSynchronizers.append(cloudKit)
        }
        self.init(
            name: resolvedName,
            types: types,
            schema: schema,
            migrationPlan: migrationPlan,
            options: options,
            flags: allowsSave
            ? [.readWrite, .create, .fullMutex]
            : [.readOnly, .fullMutex],
            location: .file(path: resolvedURL.path),
            externalStorageURL: externalStorageURL,
            allowsSave: allowsSave,
            size: size,
            attachment: attachment,
            synchronizers: resolvedSynchronizers
        )
    }
    
    /// Creates a transient data store configuration.
    @available(*, deprecated, message: "Use DatabaseConfiguration.transient(types:schema:options:size:attachment:) instead.")
    nonisolated public init(
        transient: Void,
        types: [any (PersistentModel & SendableMetatype).Type] = [],
        schema: Schema? = nil,
        options: DataStoreOptions = [],
        size: Int = 1,
        attachment: (any DataStoreDelegate)? = nil
    ) {
        self.init(
            name: UUID().uuidString,
            types: types,
            schema: schema,
            options: options,
            flags: [.memory, .readWrite, .create, .fullMutex],
            location: .inMemory,
            externalStorageURL: nil,
            allowsSave: true,
            size: size,
            attachment: attachment
        )
    }
    
    /// Creates an in-memory data store configuration.
    nonisolated public init(isStoredInMemoryOnly: Bool = true) {
        self.init(
            name: UUID().uuidString,
            flags: [.memory, .readWrite],
            location: .inMemory,
            allowsSave: true,
            size: 1 // Cannot be more than 1, because each handle is its own database.
        )
    }
    
    nonisolated public static func transient(
        types: [any (PersistentModel & SendableMetatype).Type] = [],
        schema: Schema? = nil,
        options: DataStoreOptions = [],
        size: Int = 1,
        attachment: (any DataStoreDelegate)? = nil
    ) -> Self {
        self.init(
            name: UUID().uuidString,
            types: types,
            schema: schema,
            options: options,
            flags: [.memory, .readWrite, .create, .fullMutex],
            location: .inMemory,
            externalStorageURL: nil,
            allowsSave: true,
            size: size,
            attachment: attachment
        )
    }
    
    /// `PersistentModel` types that conform to `PredicateCodableKeyPathProviding`.
    nonisolated public var predicateCodableKeyPathProvidingTypes:
    [any (PersistentModel & PredicateCodableKeyPathProviding).Type] {
        TypeRegistry.entries.map(\.type).compactMap {
            $0 as? any (PersistentModel & PredicateCodableKeyPathProviding).Type
        }
    }
    
    /// `PersistentModel` types mapped by their entity names.
    nonisolated public var typesByEntityName: [String: any (PersistentModel & SendableMetatype).Type] {
        Dictionary(uniqueKeysWithValues: TypeRegistry.entries.map(\.type).compactMap {
            $0 as? any (PersistentModel & SendableMetatype).Type
        }.map { type in
            (Schema.entityName(for: type), type)
        })
    }
    
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.name == rhs.name && lhs.url == rhs.url
    }
    
    nonisolated public func hash(into hasher: inout Hasher) {
        hasher.combine(name)
        hasher.combine(url)
    }
    
    /// Inherited from `DataStoreConfiguration.validate()`.
    ///
    /// - SwiftData calls `validate()` after initialization and before `ModelContainer` is initialized.
    nonisolated public func validate() throws {
        #if false
        try schema.save(to: .temporaryDirectory)
        #endif
        guard let storeURL = self.url else {
            return
        }
        let component = storeURL.lastPathComponent
        let allowedCharacters = CharacterSet.urlPathAllowed.union(.whitespaces)
        guard component.rangeOfCharacter(from: allowedCharacters.inverted) == nil else {
            throw SwiftDataError.configurationFileNameContainsInvalidCharacters
        }
        guard component.count <= 255 else {
            throw SwiftDataError.configurationFileNameTooLong
        }
        guard storeURL.isFileURL else {
            throw URLError(.unsupportedURL)
        }
        let directoryURL = storeURL.deletingLastPathComponent()
        if !FileManager.default.fileExists(atPath: directoryURL.path) {
            guard allowsSave else {
                throw CocoaError(.fileReadNoSuchFile)
            }
            try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        } else {
            guard FileManager.default.isReadableFile(atPath: directoryURL.path) else {
                throw CocoaError(.fileReadNoPermission)
            }
            if allowsSave {
                guard FileManager.default.isWritableFile(atPath: directoryURL.path) else {
                    throw CocoaError(.fileWriteNoPermission)
                }
            }
        }
        if !allowsSave && !FileManager.default.fileExists(atPath: storeURL.path) {
            throw CocoaError(.fileReadNoSuchFile)
        }
        if allowsSave {
            if !FileManager.default.fileExists(atPath: externalStorageURL.path) {
                try FileManager.default.createDirectory(
                    at: externalStorageURL,
                    withIntermediateDirectories: true
                )
            } else {
                guard FileManager.default.isReadableFile(atPath: externalStorageURL.path) else {
                    throw CocoaError(.fileReadNoPermission)
                }
                guard FileManager.default.isWritableFile(atPath: externalStorageURL.path) else {
                    throw CocoaError(.fileWriteNoPermission)
                }
            }
        } else if FileManager.default.fileExists(atPath: externalStorageURL.path) {
            guard FileManager.default.isReadableFile(atPath: externalStorageURL.path) else {
                throw CocoaError(.fileReadNoPermission)
            }
        }
        if options.contains(.eraseDatabaseOnSetup) {
            guard allowsSave else {
                throw CocoaError(.fileWriteNoPermission)
            }
            if FileManager.default.fileExists(atPath: storeURL.path) {
                try Store.Handle.remove(storeURL: storeURL)
                logger.notice(
                    "Database deleted on initialization.",
                    metadata: ["url": .stringConvertible(storeURL)]
                )
            }
            if FileManager.default.fileExists(atPath: externalStorageURL.path) {
                try FileManager.default.removeItem(at: externalStorageURL)
                logger.notice(
                    "Database deleted external storage on initialization.",
                    metadata: ["url": .stringConvertible(storeURL)]
                )
            }
        }
        if FileManager.default.fileExists(atPath: storeURL.path) {
            do {
                let connection = try Store.Handle(at: .file(path: storeURL.path), flags: .readOnly, role: .reader)
                let applicationID = try execute(sql: "PRAGMA application_id;")
                let userVersion = try execute(sql: "PRAGMA user_version;")
                logger.trace(
                    "Validating store file.",
                    metadata: ["applicationID": "\(applicationID)", "userVersion": "\(userVersion)"]
                )
                func execute(sql: String) throws -> Int32 {
                    let statement = try PreparedStatement(sql: sql, handle: connection)
                    var iterator = statement.rows.makeIterator()
                    let result = iterator.next()?[0, as: Int32.self] ?? 0
                    try statement.finalize()
                    return result
                }
                if applicationID != 0 && applicationID != Store.applicationID {
                    throw Store.Error.invalidStoreConfiguration
                }
                try connection.close()
            } catch {
                if options.contains(.ignoreStoreValidationErrors) {
                    logger.notice("Error while reading store file: \(error)")
                } else {
                    throw error
                }
            }
        }
    }
    
    nonisolated internal func bind(container: DataStoreContainer) {
        _ = storage.container.storeIfNil(container)
    }
    
    nonisolated public mutating func appendSynchronizer(
        _ synchronizer: some DataStoreSynchronizerConfiguration
    ) {
        ensureUniqueStorage()
        storage.synchronizers.withLock { $0.append(synchronizer) }
    }
    
    nonisolated private static func transientBaseURL() -> URL {
        URL.temporaryDirectory
            .appending(path: "DataStoreKit", directoryHint: .isDirectory)
    }
    
    nonisolated private static func transientStoreURL(
        for url: URL?,
        name: String
    ) -> URL {
        let baseURL = transientBaseURL()
        guard let url else {
            return baseURL.appending(
                component: name + ".store",
                directoryHint: .notDirectory
            )
        }
        if url.hasDirectoryPath {
            let directoryName = url.lastPathComponent.isEmpty
            ? name
            : url.lastPathComponent
            return baseURL
                .appending(path: directoryName, directoryHint: .isDirectory)
                .appending(component: name + ".store", directoryHint: .notDirectory)
        }
        return baseURL.appending(
            component: url.lastPathComponent,
            directoryHint: .notDirectory
        )
    }
    
    fileprivate final class Storage: Sendable {
        nonisolated fileprivate final let name: Mutex<String>
        nonisolated fileprivate final let schema: AtomicLazyReference<Schema>
        nonisolated fileprivate final let migrationPlan: Mutex<(any SchemaMigrationPlan.Type)?>
        nonisolated fileprivate final let constraints: [String: [[String]]]
        nonisolated fileprivate final let options: Mutex<DataStoreOptions>
        nonisolated fileprivate final let configurations: Mutex<[Key: any OptionSet & Sendable]>
        nonisolated fileprivate final let location: Mutex<SQLite.StoreType>
        nonisolated fileprivate final let externalStorageURL: URL
        nonisolated fileprivate final let allowsSave: Mutex<Bool>
        nonisolated fileprivate final let cachePolicy: Mutex<CachePolicy>
        nonisolated fileprivate final let attachment: (any DataStoreDelegate)?
        nonisolated fileprivate final let synchronizers: Mutex<[any DataStoreSynchronizerConfiguration]>
        nonisolated fileprivate final let container: AtomicLazyReference<DataStoreContainer>
        nonisolated fileprivate final let makeDatabaseQueue: @Sendable
        (Store.Attachment?) throws -> DatabaseQueue<Store>
        
        nonisolated fileprivate init(
            name: String,
            schema: Schema?,
            migrationPlan: (any SchemaMigrationPlan.Type)?,
            constraints: [String: [[String]]],
            configurations: [Key: any OptionSet & Sendable],
            options: DataStoreOptions,
            location: SQLite.StoreType,
            externalStorageURL: URL,
            allowsSave: Bool,
            cachePolicy: CachePolicy,
            attachment: (any DataStoreDelegate)?,
            synchronizers: [any DataStoreSynchronizerConfiguration],
            container: DataStoreContainer?,
            makeDatabaseQueue: @escaping @Sendable
            (Store.Attachment?) throws -> DatabaseQueue<Store>
        ) {
            self.name = .init(name)
            self.schema = .init()
            self.migrationPlan = .init(migrationPlan)
            self.constraints = constraints
            self.configurations = .init(configurations)
            self.options = .init(options)
            self.location = .init(location)
            self.externalStorageURL = externalStorageURL
            self.allowsSave = .init(allowsSave)
            self.attachment = attachment
            self.cachePolicy = .init(cachePolicy)
            self.synchronizers = .init(synchronizers)
            self.container = .init()
            self.makeDatabaseQueue = makeDatabaseQueue
            if let container { _ = self.container.storeIfNil(container) }
            if let schema { _ = self.schema.storeIfNil(schema) }
        }
        
        nonisolated fileprivate func copy() -> Self {
            .init(
                name: name.withLock(\.self),
                schema: schema.load(),
                migrationPlan: migrationPlan.withLock(\.self),
                constraints: constraints,
                configurations: configurations.withLock(\.self),
                options: options.withLock(\.self),
                location: location.withLock(\.self),
                externalStorageURL: externalStorageURL,
                allowsSave: allowsSave.withLock(\.self),
                cachePolicy: cachePolicy.withLock(\.self),
                attachment: attachment,
                synchronizers: synchronizers.withLock(\.self),
                container: container.load(),
                makeDatabaseQueue: makeDatabaseQueue
            )
        }
    }
}
