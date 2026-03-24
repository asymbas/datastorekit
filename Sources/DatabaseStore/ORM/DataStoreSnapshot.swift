//
//  DataStoreSnapshot.swift
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
import SQLiteHandle
import SQLiteStatement
import SQLSupport
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension DatabaseSnapshot: DatabaseContext {
    public typealias Store = DatabaseStore
}

/// A data transfer object that represents a `PersistentModel` in `DatabaseStore`.
public struct DatabaseSnapshot: DataStoreSnapshot {
    /// Cache for `RandomAccessCollection`.
    nonisolated private var cache: Cache = .init()
    /// Any state or conditional flags.
    nonisolated internal var flags: Flags = .init()
    /// The reference-backed storage for `DatabaseModel`.
    nonisolated internal weak var backingData: DatabaseBackingData?
    /// The creation date of the snapshot.
    nonisolated package var timestamp: DispatchTime = .now()
    /// The properties that will describe how to map its value.
    nonisolated public var properties: ContiguousArray<PropertyMetadata>
    /// The values associated with each property.
    nonisolated public var values: ContiguousArray<any DataStoreSnapshotValue>
    /// The internal stable identity derived from its `PersistentIdentifier`.
    nonisolated public let primaryKey: String
    /// Inherited from `DataStoreSnapshot.persistentIdentifier`.
    nonisolated public let persistentIdentifier: PersistentIdentifier
    
    /// The entity name for the associated model.
    nonisolated public var entityName: String {
        persistentIdentifier.entityName
    }
    
    /// The identifier of the store that contains the associated model.
    nonisolated public var storeIdentifier: String? {
        persistentIdentifier.storeIdentifier
    }
    
    /// The value that uniquely identifies the associated model within the containing store.
    nonisolated public var id: PersistentIdentifier.ID {
        persistentIdentifier.id
    }
    
    /// Indicates whether the snapshot is now persisting in the store.
    nonisolated public var isTemporary: Bool {
        persistentIdentifier.storeIdentifier == nil
    }
    
    /// The `PersistentModel` type associated to this entity.
    nonisolated public var type: any (PersistentModel & SendableMetatype).Type {
        guard let type = Schema.type(for: entityName) else {
            fatalError("\(SchemaError.entityNotRegistered)")
        }
        return type
    }
    
    internal struct Flags: AtomicRepresentable, OptionSet, Sendable {
        typealias RawValue = UInt16
        nonisolated internal let rawValue: RawValue
        nonisolated static let isModified: Self = .init(rawValue: 1 << 0)
        nonisolated static let isStale: Self = .init(rawValue: 1 << 1)
    }
    
    /// Creates an empty snapshot instance from the data store side.
    nonisolated package init<PrimaryKey: LosslessStringConvertible>(
        primaryKey: PrimaryKey,
        storeIdentifier: String,
        type: (any (PersistentModel & SendableMetatype).Type)?,
        entityName: String,
        properties: ContiguousArray<PropertyMetadata> = [],
        values: ContiguousArray<any DataStoreSnapshotValue> = []
    ) throws {
        let persistentIdentifier = try PersistentIdentifier.identifier(
            for: storeIdentifier,
            entityName: entityName,
            primaryKey: primaryKey.description
        )
        self.init(
            persistentIdentifier: persistentIdentifier,
            primaryKey: primaryKey,
            type: type,
            properties: properties,
            values: values
        )
    }
    
    /// Creates an empty snapshot instance for SwiftData.
    nonisolated package init(
        persistentIdentifier: PersistentIdentifier,
        type: (any (PersistentModel & SendableMetatype).Type)?,
        properties: ContiguousArray<PropertyMetadata> = [],
        values: ContiguousArray<any DataStoreSnapshotValue> = []
    ) {
        let primaryKey = persistentIdentifier.primaryKey()
        self.init(
            persistentIdentifier: persistentIdentifier,
            primaryKey: primaryKey,
            type: type,
            properties: properties,
            values: values
        )
    }
    
    nonisolated private init<PrimaryKey: LosslessStringConvertible>(
        persistentIdentifier: PersistentIdentifier,
        primaryKey: PrimaryKey,
        type: (any (PersistentModel & SendableMetatype).Type)?,
        properties: ContiguousArray<PropertyMetadata>,
        values: ContiguousArray<any DataStoreSnapshotValue>
    ) {
        let entityName = persistentIdentifier.entityName
        guard let type = type ?? Schema.type(for: entityName) else {
            preconditionFailure("\(SchemaError.entityNotRegistered)")
        }
        self.persistentIdentifier = persistentIdentifier
        self.primaryKey = primaryKey.description
        self.properties = !properties.isEmpty ? properties : .init(type.databaseSchemaMetadata)
        self.values = !values.isEmpty ? values : .init(repeating: SQLNull(), count: self.properties.count)
        assert(
            self.properties.count == self.values.count,
            "Property and value counts do not match: \(self.properties.count) != \(self.values.count)"
        )
        ensureIndexes()
    }
    
    nonisolated package init(backingData: DatabaseBackingData) throws {
        try self.init(
            primaryKey: backingData.primaryKey.description,
            storeIdentifier: backingData.storeIdentifier,
            type: nil,
            entityName: backingData.tableName
        )
        self.values = backingData.values.withLock(\.self)
        self.backingData = backingData
    }
    
    /// Prepares the snapshot to be processed by the data store.
    ///
    /// - Note:
    ///   - `Optional<DataStoreSnapshotValue>.none` or `nil` values are converted to `NSNull`.
    ///   - `toOneDependencies`:
    ///     - It will always map to a foreign key column in the row, unless it's a unidirectional relationship.
    ///     - There shouldn't be dependencies, unless the related identifier is stale and should be reprocessed.
    ///   - `toManyDependencies`:
    ///     - A table cannot reference more than one foreign key, which means the property and value will not be exported.
    ///     - Multiple foreign keys are managed later by the tables that own them.
    ///
    /// All relationships, other than required to-one relationships, will be deferred to a later operation.
    /// This is due to an incomplete remapping of `PersistentIdentifier` while processing inserts.
    package lazy var export: DatabaseBackingData.Export = {
        var inheritedDependencies = [Int]()
        var toOneDependencies = [Int]()
        var toManyDependencies = [Int]()
        var externalStorageData = [ExternalStoragePath]()
        let count = self.properties.count
        var columns = [String]()
        columns.reserveCapacity(count + 1)
        let values = Array<any Sendable>(unsafeUninitializedCapacity: count + 1) { buffer, initializedCount in
            guard let baseAddress = buffer.baseAddress else {
                fatalError()
            }
            columns.append(pk)
            baseAddress.initialize(to: primaryKey)
            var offset = 1
            defer { initializedCount = offset }
            for index in 0..<count {
                let property = self.properties[index]
                let value = self.values[index]
                let description = "\(primaryKey) - \(entityName).\(property.name)"
                switch property.metadata {
                case let attribute as Schema.Attribute:
                    guard property.reference == nil else {
                        logger.debug("Inherited property belongs to superentity: \(description)")
                        inheritedDependencies.append(property.index)
                        continue
                    }
                    switch value {
                    case let value as Encodable where attribute.options.contains(.externalStorage):
                        let relativePath = "\(entityName)/\(attribute.name)/\(primaryKey)"
                        let data = value is SQLNull
                        ? nil
                        : value is Data ? (value as! Data)
                        : try? JSONEncoder().encode(value)
                        (baseAddress + offset).initialize(to: SQLNull() as any Sendable)
                        externalStorageData.append(.init(
                            relativePath: relativePath,
                            component: relativePath,
                            data: data,
                            storeType: .redirect
                        ))
                        let valueType = Swift.type(of: value)
                        logger.trace(
                            "Exporting external storage: \(description) = \(value)",
                            metadata: ["type": "\(valueType)", "relative_path": "\(relativePath)"]
                        )
                    case is SQLNull:
                        (baseAddress + offset).initialize(to: NSNull() as any Sendable)
                    case let value:
                        (baseAddress + offset).initialize(to: value)
                    }
                    columns.append(property.name)
                    offset += 1
                case let relationship as Schema.Relationship where relationship.isToOneRelationship:
                    guard let relatedIdentifier = value as? PersistentIdentifier else {
                        if relationship.isOptional {
                            columns.append(property.name + "_pk")
                            (baseAddress + offset).initialize(to: NSNull() as any Sendable)
                            offset += 1
                            continue
                        }
                        preconditionFailure("Expected an identifier for to-one relationship: \(description)")
                    }
                    guard relatedIdentifier.storeIdentifier != nil else {
                        #if DEBUG
                        logger.trace("References uncommitted identifier: \(description) = \(relatedIdentifier)")
                        #endif
                        toOneDependencies.append(property.index)
                        continue
                    }
                    let foreignKey = relatedIdentifier.primaryKey()
                    (baseAddress + offset).initialize(to: foreignKey as any Sendable)
                    columns.append(property.name + "_pk")
                    offset += 1
                case let relationship as Schema.Relationship where !relationship.isToOneRelationship:
                    guard let relatedIdentifiers = value as? [PersistentIdentifier] else {
                        if relationship.isOptional {
                            toManyDependencies.append(property.index)
                            continue
                        }
                        preconditionFailure("Expected identifiers for to-many relationship: \(description) = \(value)")
                    }
                    if relatedIdentifiers.isEmpty { continue }
                    #if DEBUG
                    for relatedIdentifier in relatedIdentifiers where relatedIdentifier.storeIdentifier == nil {
                        logger.trace("References uncommitted identifier: \(description) = \(relatedIdentifier)")
                    }
                    #endif
                    toManyDependencies.append(property.index)
                    logger.trace("To-many relationship will be deferred: \(description)")
                default:
                    preconditionFailure()
                }
            }
        }
        return .init(
            columns: columns,
            values: values,
            inheritedDependencies: inheritedDependencies,
            toOneDependencies: toOneDependencies,
            toManyDependencies: toManyDependencies,
            externalStorageData: externalStorageData
        )
    }()
    
    /// Prepares the snapshot to be deleted by the data store.
    package lazy var delete: DatabaseBackingData.Export = {
        var preservedValues = [String: any Sendable]()
        for property in properties where property.metadata is Schema.Attribute {
            let attribute = property.metadata as! Schema.Attribute
            if attribute.options.contains(.preserveValueOnDeletion) {
                guard !attribute.options.contains(.externalStorage) else {
                    fatalError(DataStoreError.unsupportedFeature.localizedDescription)
                }
                // Export has prepended its primary key column at this point.
                let value = export.values[property.index + 1]
                preservedValues[attribute.name] = value
                logger.trace("Preserving deleted value: \(property) = \(value)")
            }
        }
        return .init(
            columns: Array(preservedValues.keys),
            values: Array(preservedValues.values),
            inheritedDependencies: export.inheritedDependencies,
            toOneDependencies: export.toOneDependencies,
            toManyDependencies: export.toManyDependencies,
            externalStorageData: export.externalStorageData
        )
    }()
    
    nonisolated public consuming func validate() throws(Error) -> Self {
        let schemaMetadata = self.type.databaseSchemaMetadata
        for (snapshotProperty, schemaProperty) in zip(self.properties, schemaMetadata) {
            if snapshotProperty != schemaProperty {
                throw Self.Error.propertyDoesNotMatchSchema(snapshotProperty, schemaProperty)
            }
        }
        return consume self
    }
    
    nonisolated public func uniquePropertyValuePair() -> [(PropertyMetadata, any DataStoreSnapshotValue)] {
        var array = [(PropertyMetadata, any DataStoreSnapshotValue)]()
        array.reserveCapacity(array.count)
        for property in self.properties where property.metadata.isUnique {
            array.append((property, values[property.index]))
        }
        return array
    }
    
    public enum Error: Swift.Error {
        /// An empty result set was used to create a snapshot.
        case insufficientProperties
        /// The snapshot is referencing a temporary or stale `PersistentIdentifier`.
        case referencesInvalidPersistentIdentifier
        case identifierNotAssociatedToStore
        case mismatchingEntities
        case mismatchingPrimaryKeys
        case mismatchingStoreIdentifiers
        case propertyDoesNotMatchSchema(PropertyMetadata, PropertyMetadata)
        case fieldCountNotEqual(Int, Int)
    }
}

extension DatabaseSnapshot {
    nonisolated public init<T: PersistentModel>(_ model: T) {
        self.init(persistentIdentifier: model.persistentModelID, type: T.self)
        extractBackingData(model.persistentBackingData, from: self.properties)
    }
    
    nonisolated public init<T: PersistentModel>(_ model: T, excluding keyPaths: [PartialKeyPath<T>]) {
        self.init(persistentIdentifier: model.persistentModelID, type: T.self)
        let excluded = Set(keyPaths.map { $0 as AnyKeyPath })
        extractBackingData(
            model.persistentBackingData,
            from: ContiguousArray(T.databaseSchemaMetadata.filter { property in
                !excluded.contains(property.keyPath)
            })
        )
    }
    
    nonisolated public init<T: PersistentModel>(_ model: T, excluding keyPaths: PartialKeyPath<T>...) {
        self.init(model, excluding: keyPaths)
    }
    
    nonisolated public init<T: PersistentModel>(_ model: T, only keyPaths: [PartialKeyPath<T>]) {
        self.init(persistentIdentifier: model.persistentModelID, type: T.self)
        let included = Set(keyPaths.map { $0 as AnyKeyPath })
        extractBackingData(
            model.persistentBackingData,
            from: ContiguousArray(T.databaseSchemaMetadata.filter { property in
                included.contains(property.keyPath)
            })
        )
    }
    
    nonisolated public init<T: PersistentModel>(_ model: T, only keyPaths: PartialKeyPath<T>...) {
        self.init(model, only: keyPaths)
    }
}

extension DatabaseSnapshot {
    /// Inherited from `DataStoreSnapshot.init(from:relatedBackingDatas:)`.
    ///
    /// Creates a snaphot of the model's `BackingData<Model>` that was instantiated without a store.
    /// SwiftData calls this initializer whenever it sends a requests to the `DataStore`.
    ///
    /// - Note:
    ///   - New model instances are not associated with a `Schema`, `DataStore`, and `ModelContext`.
    ///   - `FetchDescriptor.includePendingChanges` set to `false` affects persistence/saves.
    ///     - This results in `BackingData` providing stale snapshots on models associated to this `ModelContext`.
    ///   - Variations of `BackingData`:
    ///     - `SwiftData._KKMDBackingData<Model>`:
    ///       - The most common backing data and is used in `PersistentModel.createBackingData()`.
    ///     - `SwiftData._StitchedBackingData<Model>`:
    ///       - Provided when creating snapshots from inherited models.
    ///       - Unable to cast the key path or backing data to extract values from it.
    ///     - `SwiftData._FullFutureBackingData<Model>`:
    ///       - Provided when creating snapshots manually and its relationship was accessed before it was fetched.
    ///       - Causes a crash accessing values from the backing data.
    ///     - `SwiftData._InvalidFutureBackingData<Model>`:
    ///       - Causes a crash mentioning "unexpected backingdata type for inverse maintenance".
    /// - Important:
    ///   - Unwrap the metatype before getting the value, otherwise some optional values may fail to cast.
    /// - Parameters:
    ///   - backingData: The model data managed by SwiftData that contains its attributes and relationships.
    ///   - relatedBackingDatas: [Unknown]
    nonisolated public init(
        from backingData: any BackingData,
        relatedBackingDatas: inout [PersistentIdentifier: any BackingData]
    ) {
        guard let persistentIdentifier = backingData.persistentModelID else {
            fatalError("The provided BackingData does not contain a PersistentIdentifier.")
        }
        relatedBackingDatas[persistentIdentifier] = backingData
        self.init(persistentIdentifier: persistentIdentifier, type: Self.getMetatype(backingData))
        extractBackingData(backingData, from: self.properties)
    }
    
    nonisolated private mutating func extractBackingData(
        _ backingData: any BackingData,
        from properties: ContiguousArray<PropertyMetadata>
    ) {
        for property in properties {
            let description = "\(entityName).\(property.name) as \(property.valueType).self"
            switch property.metadata {
            case let attribute as Schema.Attribute:
                if attribute.isTransformable, property.isInherited {
                    logger.info("Received BackingData inherited transformed attribute: \(description)")
                    continue
                }
                guard let valueType = unwrapOptionalMetatype(attribute.valueType) as? any DataStoreSnapshotValue.Type else {
                    preconditionFailure("Attribute must conform to DataStoreSnapshotValue: \(description)")
                }
                switch getValue(backingData, as: valueType, keyPath: property.keyPath) {
                case let value? where property.reference != nil:
                    self.values[property.index] = value
                    logger.trace("Received BackingData inherited attribute: \(description) = \(value)")
                case _ where attribute.options.contains(.ephemeral):
                    if let value = getValue(attribute.defaultValue, as: valueType) {
                        self.values[property.index] = value
                        logger.trace("Received BackingData ephemeral attribute: \(description) = \(value)")
                    } else if attribute.isOptional {
                        self.values[property.index] = SQLNull()
                        logger.trace("Received BackingData ephemeral attribute: \(description) = NULL")
                    } else {
                        preconditionFailure("Ephemeral attributes must have a default value: \(description)")
                    }
                case let value?:
                    self.values[property.index] = value
                    logger.trace("Received BackingData attribute: \(description) = \(value)")
                case nil where attribute.isOptional:
                    self.values[property.index] = SQLNull()
                    logger.trace("Received BackingData attribute: \(description) = NULL")
                default:
                    fatalError("All attributes must carry over from BackingData: \(description)")
                }
            case let relationship as Schema.Relationship:
                guard let keyPath = relationship.keypath else {
                    preconditionFailure("Relationship must have a key path: \(description)")
                }
                switch unwrapOptionalMetatype(relationship.valueType) {
                case let valueType as any PersistentModel.Type:
                    switch getValue(backingData, as: valueType, keyPath: keyPath) {
                    case let model?:
                        self.values[property.index] = model.persistentModelID
                        logger.trace("Received BackingData relationship: \(description) = \(model.persistentModelID)")
                    case nil where relationship.isOptional:
                        self.values[property.index] = SQLNull()
                        logger.trace("Received BackingData relationship: \(description) = NULL")
                    default:
                        fatalError("Required to-one relationship is missing in BackingData: \(description)")
                    }
                case let valueType as any RelationshipCollection.Type:
                    switch getValue(backingData, as: valueType, keyPath: keyPath) {
                    case let models?:
                        let relatedIdentifiers = models.map(\.persistentModelID)
                        self.values[property.index] = relatedIdentifiers
                        logger.trace("Received BackingData relationship: \(description) = \(relatedIdentifiers)")
                    case nil where relationship.isOptional:
                        self.values[property.index] = SQLNull()
                        logger.trace("Received BackingData relationship: \(description) = NULL")
                    default:
                        self.values[property.index] = [PersistentIdentifier]()
                        logger.trace("Received BackingData relationship: \(description) = []")
                    }
                default:
                    fatalError("All relationships must carry over from BackingData: \(description)")
                }
            default:
                fatalError("Unhandled property type: \(property)")
            }
        }
    }
    
    nonisolated private static func getMetatype<B, M>(_ backingData: B) -> B.Model.Type
    where B: BackingData, B.Model == M {
        B.Model.self
    }
    
    nonisolated private func getValue<V>(_ defaultValue: Any?, as valueType: V.Type) -> V?
    where V: Decodable {
        switch defaultValue {
        case let value? as V?: value
        default: nil
        }
    }
    
    /// Extracts the required or optional attribute value from the backing data.
    ///
    /// - Note:
    ///   - SwiftData does not provide a key path in `Schema.Attribute` as it does with `Schema.Relationship`.
    ///   - Use `CodingKeys` and `PredicateCodableKeyPathProviding` to get values.
    @available(*, deprecated, message: "Use Mirror to retrieve the key path.")
    nonisolated private func getValue<B, M, R, V>(
        _ propertyName: String,
        from backingData: B,
        of rootType: R.Type,
        as valueType: V.Type
    ) -> V? where B: BackingData, B.Model == M, R: PredicateCodableKeyPathProviding, V: Decodable {
        switch R.predicateCodableKeyPaths[propertyName] as? KeyPath<B.Model, V> {
        case let keyPath?: backingData.getValue(forKey: keyPath)
        case nil: nil
        }
    }
    
    /// Extracts the required or optional attribute value from the backing data.
    nonisolated private func getValue<B, M, V>(
        _ backingData: B,
        as valueType: V.Type,
        keyPath: AnyKeyPath
    ) -> V? where B: BackingData, B.Model == M, V: Decodable {
        #if swift(>=6.2)
        if #available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *),
           (B.Model.Root.self is B.Model.Type) == false {
            return getInheritedValue(backingData, as: valueType, keyPath: keyPath)
        }
        #endif
        switch keyPath {
        case let keyPath as KeyPath<B.Model, V>: return backingData.getValue(forKey: keyPath)
        case let keyPath as KeyPath<B.Model, V?>: return backingData.getValue(forKey: keyPath)
        default: return nil
        }
    }
    
    #if swift(>=6.2)
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    nonisolated private func getInheritedValue<B, V>(
        _ backingData: B,
        as valueType: V.Type,
        keyPath: AnyKeyPath
    ) -> V? where B: BackingData, V: Decodable {
        resolveInheritedValue(backingData, from: B.Model.self, as: valueType, keyPath: keyPath)
    }
    
    /// Recursively ascends the inheritance hierarchy until reaching root.
    ///
    /// - Note:
    ///   - `PersistentModel.Root` is the top-level superclass type, use `class_getSuperclass(_:)` instead.
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    nonisolated private func resolveInheritedValue<B, M, V>(
        _ backingData: B,
        from modelType: M.Type,
        as valueType: V.Type,
        keyPath: AnyKeyPath
    ) -> V? where B: BackingData, M: PersistentModel, V: Decodable {
        if let keyPath = keyPath as? KeyPath<B.Model, V> {
            return backingData.getValue(forKey: keyPath)
        }
        if let keyPath = keyPath as? KeyPath<B.Model, V?> {
            return backingData.getValue(forKey: keyPath)
        }
        if let inheritedKeyPath = keyPath as? KeyPath<M, V>,
           let lifted: KeyPath<B.Model, V> = liftKeyPath(inheritedKeyPath, to: B.Model.self) {
            return backingData.getValue(forKey: lifted)
        }
        if let inheritedKeyPath = keyPath as? KeyPath<M, V?>,
           let lifted: KeyPath<B.Model, V?> = liftKeyPath(inheritedKeyPath, to: B.Model.self) {
            return backingData.getValue(forKey: lifted)
        }
        guard let superclass = class_getSuperclass(modelType) as? any PersistentModel.Type else {
            logger.debug("Reached top of inheritance chain: \(modelType).self -> \(keyPath)")
            return nil
        }
        return resolveInheritedValue(backingData, from: superclass, as: valueType, keyPath: keyPath)
    }
    
    #endif
    
    /// Extracts the required or optional to-one relationship value from the backing data.
    nonisolated private func getValue<B, M, R>(
        _ backingData: B,
        as type: R.Type,
        keyPath: AnyKeyPath
    ) -> R? where
    B: BackingData,
    B.Model == M,
    R: PersistentModel {
        switch keyPath {
        case let keyPath as KeyPath<B.Model, R>: backingData.getValue(forKey: keyPath)
        case let keyPath as KeyPath<B.Model, R?>: backingData.getValue(forKey: keyPath)
        default: nil
        }
    }
    
    /// Extracts the required or optional to-many relationship value from the backing data.
    nonisolated private func getValue<B, M, R>(
        _ backingData: B,
        as type: R.Type,
        keyPath: AnyKeyPath
    ) -> [R.PersistentElement]? where
    B: BackingData,
    B.Model == M,
    R: RelationshipCollection,
    R.PersistentElement: PersistentModel {
        switch keyPath {
        case let keyPath as KeyPath<B.Model, [R.PersistentElement]>: backingData.getValue(forKey: keyPath)
        case let keyPath as KeyPath<B.Model, [R.PersistentElement]?>: backingData.getValue(forKey: keyPath)
        default: nil
        }
    }
    
    /// Extracts the required to-many relationship from the backing data.
    @available(*, deprecated, message: "")
    nonisolated private func _getValue<B, M, R>(
        _ backingData: B,
        as type: R.Type,
        keyPath: AnyKeyPath
    ) -> R? where
    B: BackingData,
    B.Model == M,
    R: RelationshipCollection,
    R.PersistentElement: PersistentModel {
        switch keyPath as? KeyPath<B.Model, R> {
        case let keyPath?: backingData.getValue(forKey: keyPath)
        case nil: nil
        }
    }
}

extension DatabaseSnapshot {
    /// Inherited from `DataStoreSnapshot.copy(persistentIdentifier:remappedIdentifiers:)`.
    ///
    /// - Parameters:
    ///   - persistentIdentifier: The new permanent identifier to replace the temporary one.
    ///   - remappedIdentifiers: A dictionary for updating stale references in relationship properties.
    /// - Returns: A copy of the snapshot with updated persistent identifiers.
    nonisolated public func copy(
        persistentIdentifier: PersistentIdentifier,
        remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier]? = nil
    ) -> Self {
        precondition(
            self.persistentIdentifier.entityName == persistentIdentifier.entityName,
            """
            The entity must be stable during remapping:
            \(self.persistentIdentifier) -> \(persistentIdentifier)
            """
        )
        #if DEBUG
        let useDetailedLogging = DataStoreDebugging.mode == .trace
        var count = 0
        var mappings = [MappingLog]()
        struct MappingLog {
            let propertyName: String
            let (oldIdentifier, newIdentifier): (PersistentIdentifier, PersistentIdentifier)
        }
        if useDetailedLogging, self.persistentIdentifier != persistentIdentifier {
            logger.debug("Snapshot remapped: \(self.persistentIdentifier) -> \(persistentIdentifier)")
        }
        #endif
        let values = ContiguousArray(zip(properties, values).map { key, value in
            guard key.metadata is Schema.Relationship else {
                return value
            }
            switch value {
            case let oldIdentifiers as [PersistentIdentifier]:
                return oldIdentifiers.map(append(_:))
            case let oldIdentifier as PersistentIdentifier:
                return append(oldIdentifier)
            default:
                return SQLNull()
            }
            func append(_ oldIdentifier: PersistentIdentifier) -> PersistentIdentifier {
                if let newIdentifier = remappedIdentifiers?[oldIdentifier] {
                    #if DEBUG
                    count += 1
                    if useDetailedLogging {
                        mappings.append(.init(
                            propertyName: key.name,
                            oldIdentifier: oldIdentifier,
                            newIdentifier: newIdentifier
                        ))
                    }
                    #endif
                    return newIdentifier
                } else {
                    return oldIdentifier
                }
            }
        })
        #if DEBUG
        if useDetailedLogging {
            logger.debug("\nRemapped \(count) identifiers: \(persistentIdentifier)")
            for mapping in mappings {
                logger.debug(
                    """
                    * [\(entityName).\(mapping.propertyName)]
                    * \(mapping.oldIdentifier) -> \(mapping.newIdentifier)\n
                    """
                )
            }
        }
        #endif
        return .init(
            persistentIdentifier: persistentIdentifier,
            type: type,
            properties: properties,
            values: values
        )
    }
    
    nonisolated public func update(
        from other: Self,
        onChange: (
            PropertyMetadata,
            any DataStoreSnapshotValue,
            any DataStoreSnapshotValue
        ) throws -> (any DataStoreSnapshotValue)?
    ) throws -> Self {
        let lhsCount = self.values.count
        let rhsCount = other.values.count
        guard lhsCount == rhsCount else {
            throw Self.Error.fieldCountNotEqual(lhsCount, rhsCount)
        }
        var copy = self
        for property in self.properties {
            let lhs = copy.values[property.index]
            let rhs = other.values[property.index]
            guard SQLValue(any: lhs) != SQLValue(any: rhs) else { continue }
            if let collected = try onChange(property, lhs, rhs) {
                copy.values[property.index] = collected
            }
        }
        return copy
    }
    
    nonisolated public func diff<Collected>(
        from other: Self,
        onChange: (PropertyMetadata, any Sendable, any Sendable) throws -> Collected?
    ) rethrows -> [Collected] {
        var result = [Collected]()
        for property in self.properties {
            let lhs = self.values[property.index]
            let rhs = other.values[property.index]
            guard SQLValue(any: lhs) != SQLValue(any: rhs) else { continue }
            if let collected = try onChange(property, lhs, rhs) {
                result.append(collected)
            }
        }
        return result
    }
    
    nonisolated package mutating func recursiveExportChain(
        on entity: Schema.Entity,
        indices: [Int],
        inheritedTraversalSnapshots: inout [Self]
    ) throws {
        guard let superType = Schema.type(for: entity.name) else {
            fatalError(SchemaError.entityNotRegistered.localizedDescription)
        }
        let schemaMetadata = superType.databaseSchemaMetadata
        var superProperties = ContiguousArray<PropertyMetadata>()
        var superValues = ContiguousArray<any DataStoreSnapshotValue>()
        for index in indices {
            let property = self.properties[index]
            if entity.inheritedPropertiesByName[property.name] == nil {
                if let superProperty = schemaMetadata.first(where: { $0.name == property.name }) {
                    superProperties.append(superProperty)
                    superValues.append(self.values[index])
                }
            }
        }
        if !superProperties.isEmpty {
            let newSnapshot = Self(
                persistentIdentifier: try PersistentIdentifier.identifier(
                    for: persistentIdentifier.storeIdentifier.unsafelyUnwrapped,
                    entityName: entity.name,
                    primaryKey: persistentIdentifier.primaryKey()
                ),
                type: superType,
                values: superValues
            )
            inheritedTraversalSnapshots.append(newSnapshot)
            logger.debug(
                """
                Created superentity snapshot: \(entity.name):
                \(newSnapshot.persistentIdentifier)
                \(newSnapshot.properties) \(newSnapshot.values)
                """
            )
        }
        if let superentity = entity.superentity {
            try recursiveExportChain(
                on: superentity,
                indices: indices,
                inheritedTraversalSnapshots: &inheritedTraversalSnapshots
            )
        }
    }
    
    /// Diffs non-owning relationships against any preexisting data.
    nonisolated package mutating func reconcileExternalReferences(
        comparingTo oldSnapshot: Self?,
        indices: [Int],
        shouldAddOnly: Bool,
        graph: ReferenceGraph? = nil,
        connection: borrowing DatabaseConnection<Store>
    ) throws -> (linked: Set<PersistentIdentifier>, unlinked: Set<PersistentIdentifier>) {
        var linkedIdentifiers = Set<PersistentIdentifier>()
        var unlinkedIdentifiers = Set<PersistentIdentifier>()
        for index in indices {
            let property = self.properties[index]
            let oldValue: any DataStoreSnapshotValue
            if let oldSnapshot {
                oldValue = oldSnapshot.values[property.index]
                logger.debug("Diffing against old snapshot value: \(property.name) = \(oldValue)")
            } else {
                guard let relationship = property.metadata as? Schema.Relationship else {
                    preconditionFailure("Property should have been a relationship: \(property)")
                }
                if let graph, let cachedTargets = graph.cachedReferencesIfPresent(
                    for: persistentIdentifier,
                    at: property.name
                   ) {
                    if relationship.isToOneRelationship {
                        oldValue = cachedTargets.first ?? SQLNull()
                    } else {
                        oldValue = cachedTargets
                    }
                } else {
                    oldValue = try fetchReference(in: property, connection: connection)
                }
            }
            let identifiers = try processExternalReference(
                comparingTo: oldValue,
                in: property,
                shouldAddOnly: shouldAddOnly,
                connection: connection
            )
            let newValue = self.values[index]
            if let graph, let newTargets = ReferenceGraph.normalizeTargets(newValue) {
                graph.set(owner: persistentIdentifier, property: property.name, targets: newTargets)
                let persistentIdentifier = self.persistentIdentifier
                DataStoreDebugging.execute(body: {
                    logger.debug("Updating references: \(newTargets)")
                    if let references = graph.cachedReferencesIfPresent(
                        for: persistentIdentifier,
                        at: property.name
                    ) {
                        logger.debug("Found references: \(references)")
                    }
                }())
            }
            linkedIdentifiers.formUnion(identifiers.linked)
            unlinkedIdentifiers.formUnion(identifiers.unlinked)
        }
        return (linkedIdentifiers, unlinkedIdentifiers)
    }
    
    nonisolated internal mutating func processExternalReference(
        comparingTo oldValue: any DataStoreSnapshotValue,
        in property: consuming PropertyMetadata,
        shouldAddOnly: Bool,
        connection: borrowing DatabaseConnection<Store>
    ) throws -> (linked: Set<PersistentIdentifier>, unlinked: Set<PersistentIdentifier>) {
        let description = "\(persistentIdentifier) - \(entityName).\(property.name)"
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(description)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(description)")
        }
        var linkedIdentifiers = Set<PersistentIdentifier>()
        var unlinkedIdentifiers = Set<PersistentIdentifier>()
        // The relationship min and max values are optional, but defaults to `0` for unset values.
        func checkRelationshipCountConstraint(_ newIdentifiers: [PersistentIdentifier]) throws {
            let count = newIdentifiers.count
            if let minimum = relationship.minimumModelCount, minimum > 0, count < minimum {
                throw ConstraintError(.cardinalityViolation(.minimumModelCountRequired))
            }
            if let maximum = relationship.maximumModelCount, maximum > 0, count > maximum {
                throw ConstraintError(.cardinalityViolation(.maximumModelCountExceeded))
            }
        }
        func linkManyToManyReferences(inserted identifiers: [PersistentIdentifier]) throws {
            for identifier in identifiers {
                if identifier.storeIdentifier == nil {
                    logger.warning("Inserted many-to-many relationship has nil store identifier: \(description) = \(identifier)")
                }
                logger.debug("Inserting foreign key pair into join table: \(description)")
                try linkManyToManyReference(identifier, in: property, connection: connection)
            }
        }
        func unlinkManyToManyReferences(deleted identifiers: [PersistentIdentifier]) throws {
            for identifier in identifiers {
                if identifier.storeIdentifier == nil {
                    logger.warning("Deleted many-to-many relationship has nil store identifier: \(description) = \(identifier)")
                }
                logger.debug("Deleting foreign key pair from join table: \(description)")
                try unlinkManyToManyReference(identifier, in: property, connection: connection)
            }
        }
        func linkToManyReferences(inserted identifiers: [PersistentIdentifier]) throws {
            for identifier in identifiers {
                if identifier.storeIdentifier == nil {
                    logger.warning("Inserted to-many relationship has nil store identifier: \(description) = \(identifier)")
                }
                logger.debug("Updating relationship: \(description)")
                try linkToManyReference(identifier, in: property, connection: connection)
            }
        }
        func unlinkToManyReferences(deleted identifiers: [PersistentIdentifier]) throws {
            for identifier in identifiers {
                if identifier.storeIdentifier == nil {
                    logger.warning("Deleted to-many relationship has nil store identifier: \(description) = \(identifier)")
                }
                logger.debug("Deleting relationship: \(description)")
                try unlinkToManyReference(identifier, in: property, connection: connection)
            }
        }
        switch (oldValue, values[property.index]) {
        case let (oldIdentifiers as [PersistentIdentifier], newIdentifiers as [PersistentIdentifier]):
            try checkRelationshipCountConstraint(newIdentifiers)
            guard oldIdentifiers != newIdentifiers else { break }
            if shouldAddOnly {
                let inserted = Array(Set(newIdentifiers).subtracting(Set(oldIdentifiers)))
                if reference.count == 2 {
                    try linkManyToManyReferences(inserted: inserted)
                } else if !relationship.isToOneRelationship {
                    try linkToManyReferences(inserted: inserted)
                }
                linkedIdentifiers.formUnion(inserted)
                var seen = Set<PersistentIdentifier>()
                var combined = [PersistentIdentifier]()
                combined.reserveCapacity(oldIdentifiers.count + newIdentifiers.count)
                for oldIdentifier in oldIdentifiers where seen.insert(oldIdentifier).inserted {
                    combined.append(oldIdentifier)
                }
                for oldIdentifier in newIdentifiers where seen.insert(oldIdentifier).inserted {
                    combined.append(oldIdentifier)
                }
                self.values[property.index] = consume combined
                break
            }
            let diff = diffReferencedRelationships(old: oldIdentifiers, new: newIdentifiers)
            if reference.count == 2 {
                try linkManyToManyReferences(inserted: Array(diff.inserted))
                linkedIdentifiers.formUnion(diff.inserted)
                try unlinkManyToManyReferences(deleted: Array(diff.deleted))
                unlinkedIdentifiers.formUnion(diff.deleted)
                let count = (inserted: diff.inserted.count, deleted: diff.deleted.count)
                if count.inserted != 0 || count.deleted != 0 {
                    logger.debug("Diffed: \(description) \(reference[0].rhsTable) +\(count.0) -\(count.1)")
                }
            } else if !relationship.isToOneRelationship {
                try linkToManyReferences(inserted: Array(diff.inserted))
                linkedIdentifiers.formUnion(diff.inserted)
                try unlinkToManyReferences(deleted: Array(diff.deleted))
                unlinkedIdentifiers.formUnion(diff.deleted)
                let count = (inserted: diff.inserted.count, deleted: diff.deleted.count)
                if count.inserted != 0 || count.deleted != 0 {
                    logger.debug("Diffed: \(description) +\(count.0) -\(count.1)")
                }
            }
            self.values[property.index] = consume newIdentifiers
        case let (oldIdentifiers as [PersistentIdentifier], is SQLNull):
            guard relationship.isOptional else {
                fatalError("Relationship is not optional and cannot be set to NULL: \(description)")
            }
            if shouldAddOnly {
                self.values[property.index] = oldIdentifiers
                break
            }
            if reference.count == 2 {
                try unlinkManyToManyReferences(deleted: oldIdentifiers)
            } else if !relationship.isToOneRelationship {
                try unlinkToManyReferences(deleted: oldIdentifiers)
            }
            self.values[property.index] = SQLNull()
            unlinkedIdentifiers.formUnion(oldIdentifiers)
            logger.debug("Removed to-many relationships: \(description) -\(oldIdentifiers.count)")
            
        case let (is SQLNull, newIdentifiers as [PersistentIdentifier]):
            try checkRelationshipCountConstraint(newIdentifiers)
            if reference.count == 2 {
                try linkManyToManyReferences(inserted: newIdentifiers)
            } else if !relationship.isToOneRelationship {
                try linkToManyReferences(inserted: newIdentifiers)
            }
            logger.debug("Added to-many relationships: \(description) +\(newIdentifiers.count)")
            linkedIdentifiers.formUnion(newIdentifiers)
            self.values[property.index] = consume newIdentifiers
        case let (oldIdentifier as PersistentIdentifier, newIdentifier as PersistentIdentifier):
            guard oldIdentifier != newIdentifier else { break }
            assert(reference.count == 1, "Expected one reference, got \(reference.count): \(description)")
            logger.debug("Updating to-one relationship: \(description) = \(oldIdentifier) -> \(newIdentifier)")
            try linkToOneReference(newIdentifier, in: property, connection: connection)
            logger.debug("Clearing stale to-one relationship: \(description)")
            try unlinkToOneReference(oldIdentifier, in: property, connection: connection)
            unlinkedIdentifiers.insert(oldIdentifier)
            linkedIdentifiers.insert(newIdentifier)
            self.values[property.index] = consume newIdentifier
        case (is SQLNull, let newIdentifier as PersistentIdentifier):
            assert(reference.count == 1, "Expected one reference, got \(reference.count): \(description)")
            logger.debug("Linking to-one relationship: \(description) = NULL -> \(newIdentifier)")
            try linkToOneReference(newIdentifier, in: property, connection: connection)
            linkedIdentifiers.insert(newIdentifier)
            self.values[property.index] = consume newIdentifier
        case (let oldIdentifier as PersistentIdentifier, is SQLNull):
            guard relationship.isOptional else {
                fatalError("Relationship is not optional and cannot be set to NULL: \(description)")
            }
            assert(reference.count == 1, "Expected one reference, got \(reference.count): \(description)")
            logger.debug("Unlinking to-one relationship: \(description) = \(oldIdentifier) -> NULL")
            try unlinkToOneReference(oldIdentifier, in: property, connection: connection)
            unlinkedIdentifiers.insert(oldIdentifier)
            self.values[property.index] = SQLNull()
        case (is SQLNull, is SQLNull):
            guard relationship.isOptional else {
                fatalError("Relationship is not optional and cannot be set to NULL: \(description)")
            }
            logger.trace("External references are NULL and unchanged: \(description)")
        default:
            let oldValue = "\(oldValue) as \(Swift.type(of: oldValue)).self"
            let newValue = "\(values[property.index]) as \(Swift.type(of: values[property.index])).self"
            logger.notice("Unhandled case for external references: \(description) = \(oldValue) -> \(newValue)")
        }
        return (linkedIdentifiers, unlinkedIdentifiers)
    }
    
    /// Deletes external rows that references this one and enforces delete rules.
    ///
    /// - Note:
    ///   - `M:M`: Removes join rows, maybe cascade, maybe deny.
    ///   - `1:M`: Children rows (foreign key side) pointing to this parent row,
    ///   - `1:1`: A single other row (foreign key side) pointing to this row.
    /// - Parameters:
    ///   - indices: The index of the relationship property to reconcile.
    ///   - connection: The database connection to execute any row deletions.
    /// - Returns:
    ///   The references that were unlinked or cascaded (deleted).
    nonisolated package mutating func reconcileExternalReferencesBeforeDelete(
        indices: [Int],
        connection: borrowing DatabaseConnection<Store>
    ) throws -> (unlinked: Set<PersistentIdentifier>, cascaded: Set<PersistentIdentifier>) {
        var unlinkedIdentifiers = Set<PersistentIdentifier>()
        var cascadedIdentifiers = Set<PersistentIdentifier>()
        for index in indices {
            let property = self.properties[index]
            let description = "\(primaryKey) - \(entityName).\(property.name)"
            guard let relationship = property.metadata as? Schema.Relationship else {
                preconditionFailure("The property must be a relationship: \(description)")
            }
            guard let reference = property.reference else {
                preconditionFailure("The relationship must have a reference: \(description)")
            }
            let value = try fetchReference(in: property, connection: connection)
            if property.isManyToManyRelationship {
                guard let identifiers = value as? [PersistentIdentifier] else {
                    fatalError("Expected identifiers for many-to-many relationship: \(description)")
                }
                switch relationship.deleteRule {
                case .deny:
                    guard identifiers.isEmpty else {
                        throw ConstraintError(
                            for: persistentIdentifier,
                            references: identifiers,
                            deleteRule: .deny
                        )
                    }
                case .cascade:
                    cascadedIdentifiers.formUnion(identifiers)
                case .nullify, .noAction:
                    break
                @unknown default:
                    fatalError(DataStoreError.unsupportedFeature.localizedDescription)
                }
                _ = try connection.query(
                    """
                    DELETE FROM "\(reference[0].rhsTable)"
                    WHERE "\(reference[0].rhsColumn)" = ?
                    """,
                    bindings: primaryKey
                )
                unlinkedIdentifiers.formUnion(identifiers)
                if case is [PersistentIdentifier] = values[property.index] {
                    self.values[property.index] = [PersistentIdentifier]()
                }
            } else if !relationship.isToOneRelationship {
                guard let childIdentifiers = value as? [PersistentIdentifier] else {
                    if relationship.isOptional {
                        logger.debug("Optional one-to-many relationship had a nil value: \(description)")
                        continue
                    } else {
                        preconditionFailure("No value was provided for relationship: \(description)")
                    }
                }
                switch relationship.deleteRule {
                case .deny:
                    guard childIdentifiers.isEmpty else {
                        throw ConstraintError(
                            for: persistentIdentifier,
                            references: childIdentifiers,
                            deleteRule: .deny
                        )
                    }
                case .cascade:
                    cascadedIdentifiers.formUnion(childIdentifiers)
                case .nullify:
                    for childIdentifier in childIdentifiers {
                        try unlinkToManyReference(childIdentifier, in: property, connection: connection)
                    }
                    unlinkedIdentifiers.formUnion(childIdentifiers)
                case .noAction:
                    break
                @unknown default:
                    fatalError(DataStoreError.unsupportedFeature.localizedDescription)
                }
                if case is [PersistentIdentifier] = values[property.index] {
                    self.values[property.index] = [PersistentIdentifier]()
                }
            } else {
                if reference[0].isOwningReference() {
                    self.values[property.index] = SQLNull()
                    logger.debug("Relationship owns relations")
                    continue
                }
                guard !reference[0].isOwningReference(),
                      let relatedIdentifier = value as? PersistentIdentifier else {
                    if relationship.isOptional {
                        precondition(values[index] is SQLNull, "Snapshot should already have SQLNull in place.")
                        logger.debug("Optional one-to-one relationship had a nil value: \(description)")
                        continue
                    } else {
                        fatalError("The value is missing for a required one-to-one relationship: \(description)")
                    }
                }
                switch relationship.deleteRule {
                case .deny:
                    throw ConstraintError(
                        for: persistentIdentifier,
                        references: [relatedIdentifier],
                        deleteRule: .deny
                    )
                case .cascade:
                    cascadedIdentifiers.insert(relatedIdentifier)
                case .nullify:
                    try unlinkToOneReference(relatedIdentifier, in: property, connection: connection)
                    unlinkedIdentifiers.insert(relatedIdentifier)
                case .noAction:
                    break
                @unknown default:
                    fatalError(DataStoreError.unsupportedFeature.localizedDescription)
                }
                self.values[property.index] = SQLNull()
            }
        }
        return (unlinkedIdentifiers, cascadedIdentifiers)
    }
    
    nonisolated internal func linkManyToManyReference(
        _ relatedIdentifier: PersistentIdentifier,
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        precondition(
            relationship.destination == relatedIdentifier.entityName,
            "Destination mismatch: \(relationship.destination) != \(relatedIdentifier.entityName)"
        )
        precondition(
            relationship.destination == reference[1].destinationTable,
            "Destination mismatch: \(relationship.destination) != \(reference[1].destinationTable)"
        )
        let foreignKey = relatedIdentifier.primaryKey()
        _ = try connection.query(
            """
            INSERT OR IGNORE INTO "\(reference[0].destinationTable)" (
                "\(reference[0].rhsColumn)",
                "\(reference[1].lhsColumn)"
            ) VALUES (?, ?)
            """,
            bindings: primaryKey, foreignKey
        )
        #if DEBUG
        let rows = try fetchManyToManyReference(
            self.primaryKey,
            relatedIdentifier.primaryKey(),
            for: property,
            into: [String: any Sendable](),
            connection: connection
        ) { collection, row in
            for column in row.columns { collection[column.name] = column.value }
        }
        logger.trace("Validation result on many-to-many link: \(rows)")
        #endif
    }
    
    nonisolated internal func unlinkManyToManyReference(
        _ relatedIdentifier: PersistentIdentifier,
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        precondition(
            relationship.destination == relatedIdentifier.entityName,
            "Destination mismatch: \(relationship.destination) != \(relatedIdentifier.entityName)"
        )
        precondition(
            relationship.destination == reference[1].destinationTable,
            "Destination mismatch: \(relationship.destination) != \(reference[1].destinationTable)"
        )
        let foreignKey = relatedIdentifier.primaryKey()
        _ = try connection.query(
            """
            DELETE FROM "\(reference[0].destinationTable)"
            WHERE "\(reference[0].rhsColumn)" = ? AND "\(reference[1].lhsColumn)" = ?
            """,
            bindings: primaryKey, foreignKey
        )
        #if DEBUG
        let rows = try fetchManyToManyReference(
            self.primaryKey,
            relatedIdentifier.primaryKey(),
            for: property,
            into: [String: any Sendable](),
            connection: connection
        ) { collection, row in
            for column in row.columns { collection[column.name] = column.value }
        }
        logger.trace("Validation result on many-to-many unlink: \(rows)")
        #endif
    }
    
    /// Links the non-owning one-to-many or to-many relationship by the new referenced identifier.
    nonisolated internal func linkToManyReference(
        _ relatedIdentifier: PersistentIdentifier,
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        let primaryKey = relatedIdentifier.primaryKey()
        let foreignKey = self.primaryKey
        precondition(
            relationship.destination == relatedIdentifier.entityName,
            "Destination mismatch: \(relationship.destination) != \(relatedIdentifier.entityName)"
        )
        precondition(
            relationship.destination == reference[0].destinationTable,
            "Destination mismatch: \(relationship.destination) != \(reference[0].destinationTable)"
        )
        _ = try connection.query(
            """
            UPDATE "\(relationship.destination)"
            SET "\(reference[0].destinationColumn)" = ?
            WHERE "\(pk)" = ?
            """,
            bindings: foreignKey, primaryKey
        )
        #if DEBUG
        let rows = try fetchToManyReference(
            relatedIdentifier.primaryKey(),
            for: property,
            into: [String: any Sendable](),
            connection: connection
        ) { collection, row in
            for column in row.columns { collection[column.name] = column.value }
        }
        logger.trace("Validation result on to-many link: \(rows)")
        #endif
    }
    
    /// Unlinks the non-owning one-to-many or to-many relationship by the old referenced identifier.
    nonisolated internal func unlinkToManyReference(
        _ relatedIdentifier: PersistentIdentifier,
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        precondition(
            relationship.destination == relatedIdentifier.entityName,
            "Destination mismatch: \(relationship.destination) != \(relatedIdentifier.entityName)"
        )
        precondition(
            relationship.destination == reference[0].destinationTable,
            "Destination mismatch: \(relationship.destination) != \(reference[0].destinationTable)"
        )
        let primaryKey = relatedIdentifier.primaryKey()
        switch relationship.deleteRule {
        case .cascade:
            _ = try connection.query(
                """
                DELETE FROM "\(relationship.destination)"
                WHERE "\(pk)" = ?
                """,
                bindings: primaryKey
            )
        case .nullify:
            _ = try connection.query(
                """
                UPDATE "\(relationship.destination)"
                SET "\(reference[0].destinationColumn)" = NULL
                WHERE "\(pk)" = ?
                """,
                bindings: primaryKey
            )
        case .deny:
            logger.warning("Unable to delete relationship: \(description)")
        case .noAction:
            break
        @unknown default:
            fatalError(DataStoreError.unsupportedFeature.localizedDescription)
        }
        #if DEBUG
        let rows = try fetchToManyReference(
            relatedIdentifier.primaryKey(),
            for: property,
            into: [String: any Sendable](),
            connection: connection
        ) { collection, row in
            for column in row.columns { collection[column.name] = column.value }
        }
        logger.trace("Validation result on to-many unlink: \(rows)")
        #endif
    }
    
    /// Links the owning many-to-one or one-to-one relationship by the new referenced identifier.
    nonisolated internal func linkToOneReference(
        _ relatedIdentifier: PersistentIdentifier,
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        precondition(relationship.isToOneRelationship)
        let orientation = reference[0].isOwningReference()
        let foreignKeyTable = orientation ? reference[0].rhsTable : reference[0].lhsTable
        let foreignKeyColumn = orientation ? reference[0].rhsColumn : reference[0].lhsColumn
        let parentForeignKey = relatedIdentifier.primaryKey()
        _ = try connection.query(
             """
             UPDATE "\(foreignKeyTable)" SET "\(foreignKeyColumn)" = ?
             WHERE "\(pk)" = ?
             """,
             bindings: parentForeignKey, primaryKey
        )
        #if DEBUG
        let rows = try fetchToOneReference(
            relatedIdentifier.primaryKey(),
            for: property,
            into: [String: any Sendable](),
            connection: connection
        ) { collection, row in
            for column in row.columns { collection[column.name] = column.value }
        }
        logger.trace("Validation result on to-one link: \(rows)")
        #endif
    }
    
    /// Unlinks the owning many-to-one or one-to-one relationship by the old referenced identifier.
    nonisolated internal func unlinkToOneReference(
        _ relatedIdentifier: PersistentIdentifier,
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        let orientation = reference[0].isOwningReference()
        let foreignKeyTable = orientation ? reference[0].lhsTable : reference[0].rhsTable
        let foreignKeyColumn = orientation ? reference[0].lhsColumn : reference[0].rhsColumn
        let oldParentForeignKey = relatedIdentifier.primaryKey()
        if relationship.isOptional {
            _ = try connection.query(
                """
                UPDATE "\(foreignKeyTable)" SET "\(foreignKeyColumn)" = NULL
                WHERE "\(pk)" = ?
                """,
                bindings: oldParentForeignKey
            )
        } else {
            switch relationship.deleteRule {
            case .cascade:
                _ = try connection.query(
                    """
                    DELETE FROM "\(foreignKeyTable)"
                    WHERE "\(pk)" = ?
                    """,
                    bindings: oldParentForeignKey
                )
            case .nullify:
                logger.error("Non-optional relationship marked as nullify: \(property.name)")
                throw ConstraintError(.requiredRelationshipNotFound)
            case .deny:
                logger.warning("Unlink denied: \(property.name) is non-optional")
                throw ConstraintError(.requiredRelationshipNotFound)
            case .noAction:
                logger.trace("No action on unlink: \(property.name)")
                break
            @unknown default:
                fatalError(DataStoreError.unsupportedFeature.localizedDescription)
            }
        }
        #if DEBUG
        let rows = try fetchToOneReference(
            relatedIdentifier.primaryKey(),
            for: property,
            into: [String: any Sendable](),
            connection: connection
        ) { collection, row in
            for column in row.columns { collection[column.name] = column.value }
        }
        logger.trace("Validation result on to-one unlink: \(rows)")
        #endif
    }
    
    nonisolated internal func fetchReference(
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws -> any DataStoreSnapshotValue {
        let description = "\(persistentIdentifier) - \(entityName).\(property.name)"
        guard let storeIdentifier = self.persistentIdentifier.storeIdentifier else {
            throw Error.identifierNotAssociatedToStore
        }
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        if reference.count == 2 {
            let destination = reference[1].rhsTable
            let description = "\(description) -> \(reference[0].rhsTable) -> \(destination)"
            assert(
                relationship.destination == destination,
                "Destination mismatch: \(relationship.destination) != \(destination)"
            )
            let sql = """
                SELECT "\(reference[1].lhsColumn)" FROM "\(reference[0].rhsTable)"
                WHERE "\(reference[0].rhsColumn)" = ?
                """
            let rows = try connection.query(sql, bindings: primaryKey).compactMap { row in
                guard let foreignKey = row[reference[1].lhsColumn] as? String else {
                    return Optional<PersistentIdentifier>.none
                }
                return try PersistentIdentifier.identifier(
                    for: storeIdentifier,
                    entityName: relationship.destination,
                    primaryKey: foreignKey
                )
            }
            logger.debug(
                "Fetched foreign keys relationship: \(description) \(rows.count)",
                metadata: ["type": "many-to-many"]
            )
            return try ensureRelationshipValue(rows, in: relationship)
        } else if !relationship.isToOneRelationship {
            let sql = """
                SELECT "\(pk)" FROM "\(relationship.destination)"
                WHERE "\(reference[0].destinationColumn)" = ?
                """
            let rows = try connection.query(sql, bindings: primaryKey).compactMap { row in
                guard let foreignKey = row[pk] as? String else {
                    return Optional<PersistentIdentifier>.none
                }
                return try PersistentIdentifier.identifier(
                    for: storeIdentifier,
                    entityName: relationship.destination,
                    primaryKey: foreignKey
                )
            }
            logger.debug(
                "Fetched foreign keys for relationship: \(description) \(rows.count)",
                metadata: ["type": "non-owning one-to-many"]
            )
            return try ensureRelationshipValue(rows, in: relationship)
        } else {
            let statement = SQL {
                "SELECT \(quote(reference[0].lhsColumn)) AS relationship"
                "FROM \(quote(entityName))"
                "WHERE \(quote(reference[0].rhsColumn)) = ?"
                "LIMIT 1"
            }
            let rows = try connection.query(statement.sql, bindings: primaryKey).compactMap { row in
                guard let foreignKey = row["relationship"] as? String else {
                    return Optional<PersistentIdentifier>.none
                }
                return try PersistentIdentifier.identifier(
                    for: storeIdentifier,
                    entityName: relationship.destination,
                    primaryKey: foreignKey
                )
            }
            logger.debug(
                "Fetched foreign keys for relationship: \(description) \(rows.count)",
                metadata: ["type": "owning many-to-one"]
            )
            return try ensureRelationshipValue(rows, in: relationship)
        }
    }
    
    nonisolated package static func fetchSuperentities(
        for identifier: PersistentIdentifier,
        entity: String,
        connection: borrowing DatabaseConnection<Store>,
        chain: inout [PersistentIdentifier]
    ) throws {
        chain.append(identifier)
        guard let type = Schema.type(for: entity),
              let superEntity = class_getSuperclass(type) as? any PersistentModel.Type else {
            return
        }
        let superentityName = Schema.entityName(for: superEntity)
        let rows = try connection.query(
            """
            SELECT "\(pk)" FROM "\(superentityName)"
            WHERE "\(pk)" = ?
            LIMIT 1
            """,
            bindings: [identifier.primaryKey()]
        )
        if !rows.isEmpty {
            guard let storeIdentifier = identifier.storeIdentifier else {
                throw Error.identifierNotAssociatedToStore
            }
            let superentityIdentifier = try PersistentIdentifier.identifier(
                for: storeIdentifier,
                entityName: superentityName,
                primaryKey: identifier.primaryKey()
            )
            try Self.fetchSuperentities(
                for: superentityIdentifier,
                entity: superentityName,
                connection: connection,
                chain: &chain
            )
        }
    }
    
    nonisolated package static func fetchSuperentitySnapshots(
        for identifier: PersistentIdentifier,
        on entity: Schema.Entity,
        connection: borrowing DatabaseConnection<Store>,
        relatedSnapshots: inout [PersistentIdentifier: Self]
    ) throws -> (rootEntity: Schema.Entity, rootPersistentIdentifier: PersistentIdentifier) {
        guard let superentity = entity.superentity else {
            return (entity, identifier)
        }
        let superentityName = superentity.name
        guard let superType = Schema.type(for: superentityName) else {
            return (entity, identifier)
        }
        guard let row = try connection.query(
            """
            SELECT * FROM "\(superentityName)"
            WHERE "\(pk)" = ?
            LIMIT 1
            """,
            bindings: identifier.primaryKey()
        ).first else {
            return (entity, identifier)
        }
        guard let storeIdentifier = identifier.storeIdentifier else {
            throw Error.identifierNotAssociatedToStore
        }
        let superentityIdentifier = try PersistentIdentifier.identifier(
            for: storeIdentifier,
            entityName: superentityName,
            primaryKey: identifier.primaryKey()
        )
        let schemaMetadata = superType.databaseSchemaMetadata
        var properties = ContiguousArray<PropertyMetadata>()
        var values = ContiguousArray<any DataStoreSnapshotValue>()
        for property in schemaMetadata {
            if let value: any DataStoreSnapshotValue = sendable(cast: row[property.name] as Any) {
                properties.append(property)
                values.append(value)
            }
        }
        let snapshot = Self(
            persistentIdentifier: superentityIdentifier,
            type: superType,
            properties: properties,
            values: values
        )
        relatedSnapshots[superentityIdentifier] = consume snapshot
        return try Self.fetchSuperentitySnapshots(
            for: superentityIdentifier,
            on: superentity,
            connection: connection,
            relatedSnapshots: &relatedSnapshots
        )
    }
    
    nonisolated package static func fetchInheritanceDependencies(
        for persistentIdentifier: PersistentIdentifier,
        on entity: Schema.Entity,
        connection: borrowing DatabaseConnection<Store>,
        direction: FetchHierarchyDirection = .both,
        excludeExistingValues: Bool = true
    ) throws -> [String: any DataStoreSnapshotValue] {
        let chain = Self.collectClassTableInheritanceHierarchy(
            direction: direction,
            from: entity,
            shouldExcludeLeafEntity: excludeExistingValues
        )
        guard !chain.isEmpty, let root = chain.first else {
            return [:]
        }
        return try connection.fetch(
            SQL {
                "SELECT \(chain.map { "\(quote($0.name)).*" }.joined(separator: ", "))"
                From(root.name)
                for index in 1..<chain.count {
                    let left = quote(chain[index - 1].name)
                    let right = quote(chain[index].name)
                    "LEFT JOIN \(right) ON \(left).\(quote(pk)) = \(right).\(quote(pk))"
                }
                "WHERE \(quote(root.name)).\(quote(pk)) = ?"
                Limit(1)
            }.sql,
            bindings: [persistentIdentifier.primaryKey()],
            into: [String: any DataStoreSnapshotValue]()
        ) { collection, row in
            for column in row.columns {
                collection[column.name] = sendable(cast: column.value as Any)
            }
        }
    }
    
    nonisolated package static func collectClassTableInheritanceHierarchy(
        direction: FetchHierarchyDirection,
        from entity: Schema.Entity,
        shouldExcludeLeafEntity: Bool = false
    ) -> [Schema.Entity] {
        var entityInheritanceChain = [Schema.Entity]()
        func walkUp(_ currentEntity: Schema.Entity) {
            if let superentity = currentEntity.superentity {
                walkUp(superentity)
                entityInheritanceChain.append(superentity)
            }
        }
        func walkDown(_ currentEntity: Schema.Entity) {
            for subentity in currentEntity.subentities {
                entityInheritanceChain.append(subentity)
                walkDown(subentity)
            }
        }
        switch direction {
        case .ascend:
            walkUp(entity)
            if !shouldExcludeLeafEntity { entityInheritanceChain.append(entity) }
        case .descend:
            if !shouldExcludeLeafEntity { entityInheritanceChain.append(entity) }
            walkDown(entity)
        case .both:
            walkUp(entity)
            if !shouldExcludeLeafEntity { entityInheritanceChain.append(entity) }
            walkDown(entity)
        }
        return entityInheritanceChain
    }
    
    package enum FetchHierarchyDirection {
        case ascend, descend, both
    }
}

extension DatabaseSnapshot {
    private struct CodingKeys: CodingKey {
        nonisolated internal var stringValue: String
        nonisolated internal var intValue: Int?
        
        nonisolated internal init?(stringValue: String) {
            self.stringValue = stringValue
        }
        
        nonisolated internal init?(intValue: Int) {
            nil
        }
    }
    
    /// Inherited from `Decodable.init(from:)`.
    nonisolated public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: DataStoreSnapshotCodingKey.self)
        let persistentIdentifier = try container.decode(
            PersistentIdentifier.self,
            forKey: .persistentIdentifier
        )
        self.init(persistentIdentifier: persistentIdentifier, type: nil)
        let schemaMetadata = self.type.databaseSchemaMetadata
        for (index, property) in schemaMetadata.enumerated() {
            switch property.metadata {
            case let attribute as Schema.Attribute: try setValue(attribute, at: index)
            case let relationship as Schema.Relationship: try setValue(relationship, at: index)
            default: continue
            }
        }
        func setValue(_ attribute: Schema.Attribute, at index: Int) throws {
            let description = "\(entityName).\(attribute.name) as \(attribute.valueType).self"
            let key = DataStoreSnapshotCodingKey(stringValue: attribute.name)!
            let context = DecodingError.Context(codingPath: [], debugDescription: description)
            let valueType = unwrapOptionalMetatype(attribute.valueType)
            guard let valueType = valueType as? any DataStoreSnapshotValue.Type else {
                throw DecodingError.dataCorrupted(context)
            }
            guard container.contains(key) else {
                if !attribute.isOptional { throw DecodingError.keyNotFound(key, context) }
                values[index] = SQLNull()
                logger.trace("Decoded attribute property: \(description) = NULL")
                return
            }
            if try container.decodeNil(forKey: key) {
                self.self.values[index] = SQLNull()
                logger.trace("Decoded attribute property: \(description) = NULL")
            } else if #available(iOS 26.1, macOS 26.1, tvOS 26.1, visionOS 26.1, watchOS 26.1, *),
                      attribute.isTransformable {
                if let data = try? container.decode(Data.self, forKey: key) {
                    self.values[index] = data as any DataStoreSnapshotValue
                } else {
                    throw DecodingError.dataCorrupted(context)
                }
            } else if attribute is Schema.CompositeAttribute,
                      let value = try? valueType.init(from: container.superDecoder(forKey: key)) {
                self.values[index] = value
                logger.trace("Decoded composite attribute property: \(description) = \(value)")
            } else if let value = try? container.decode(valueType, forKey: key) {
                self.values[index] = value
                logger.trace("Decoded attribute property: \(description) = \(value)")
            } else {
                throw DecodingError.valueNotFound(attribute.valueType, context)
            }
        }
        func setValue(_ relationship: Schema.Relationship, at index: Int) throws {
            let description = "\(entityName).\(relationship.name) as \(relationship.valueType).self"
            let key = DataStoreSnapshotCodingKey(stringValue: relationship.name)!
            let context = DecodingError.Context(codingPath: [], debugDescription: description)
            guard container.contains(key) else {
                if !relationship.isOptional { throw DecodingError.keyNotFound(key, context) }
                self.values[index] = SQLNull()
                logger.trace("Decoded relationship property: \(description) = NULL")
                return
            }
            switch unwrapOptionalMetatype(relationship.valueType) {
            case is any PersistentModel.Type:
                if try container.decodeNil(forKey: key) {
                    self.values[index] = SQLNull()
                    logger.trace("Decoded to-one relationship property: \(description) = NULL")
                } else if let value = try? container.decode(PersistentIdentifier.self, forKey: key) {
                    self.values[index] = value
                    logger.trace("Decoded to-one relationship property: \(description) = primary key")
                } else {
                    fallthrough
                }
            case is any RelationshipCollection.Type:
                if try container.decodeNil(forKey: key) {
                    self.values[index] = SQLNull()
                    logger.trace("Decoded to-many relationship property: \(description) = NULL")
                } else if let items = try? container.decode([PersistentIdentifier].self, forKey: key) {
                    self.values[index] = items
                    logger.trace("Decoded to-many relationship property: \(description) = [primary key] ")
                } else {
                    fallthrough
                }
            default:
                throw DecodingError.valueNotFound(relationship.valueType, context)
            }
        }
    }
    
    /// Inherited from `Encodable.encode(to:)`.
    nonisolated public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: DataStoreSnapshotCodingKey.self)
        try container.encode(persistentIdentifier, forKey: .persistentIdentifier)
        for (index, key) in properties.enumerated() {
            switch true {
            case key.metadata is Schema.Attribute: try setValue(attribute: key, index: index)
            case key.metadata is Schema.Relationship: try setValue(relationship: key, index: index)
            default: fatalError("The value is not a valid attribute or relationship: \(key)")
            }
        }
        func setValue(attribute property: PropertyMetadata, index: Int) throws {
            if !property.isSelected { return }
            let wrappedValue = self.values[index]
            let codingKey = property.key
            let valueType = unwrapOptionalMetatype(Swift.type(of: wrappedValue))
            let description = "\(entityName).\(property)"
            guard let valueType = valueType as? any DataStoreSnapshotValue.Type else {
                preconditionFailure("All attributes must be defined in the schema: \(property)")
            }
            // A temporary workaround for the OS 26.1 transformable bug.
            if #available(iOS 26.1, macOS 26.1, tvOS 26.1, visionOS 26.1, watchOS 26.1, *),
               (property.metadata as! Schema.Attribute).isTransformable {
                if let value = unwrapValue(wrappedValue, as: valueType) {
                    guard value is SQLNull == false else {
                        try container.encodeNil(forKey: codingKey)
                        return
                    }
                    let archived: Data = try NSKeyedArchiver.archivedData(
                        withRootObject: value,
                        requiringSecureCoding: true
                    )
                    try container.encode(archived, forKey: codingKey)
                    return
                }
                try container.encodeNil(forKey: codingKey)
                return
            }
            switch unwrapValue(wrappedValue, as: valueType) {
            case is SQLNull:
                try container.encodeNil(forKey: codingKey)
                logger.trace("Encoded attribute property: \(description) = NULL")
            case let value?:
                try container.encode(value, forKey: codingKey)
                logger.trace("Encoded attribute property: \(description) = \(value)")
            default:
                try container.encodeNil(forKey: codingKey)
                logger.trace("Encoded attribute property: \(description) = NULL nil")
            }
        }
        func setValue(relationship property: PropertyMetadata, index: Int) throws {
            let description = "\(entityName).\(property)"
            switch values[index] {
            case is SQLNull:
                try container.encodeNil(forKey: property.key)
                logger.trace("Encoded relationship property: \(description) = NULL")
            case let value as PersistentIdentifier:
                try container.encode(value, forKey: property.key)
                logger.trace("Encoded to-one relationship property: \(description) = \(value)")
            case let values as [PersistentIdentifier]:
                try container.encode(values, forKey: property.key)
                logger.trace("Encoded to-many relationship property: \(description) = \(values)")
            default:
                try container.encodeNil(forKey: property.key)
                logger.trace("Encoded relationship property: \(description) = NULL nil")
            }
        }
        func unwrapValue<T>(_ wrappedValue: any DataStoreSnapshotValue, as valueType: T.Type) -> T?
        where T: Decodable {
            switch wrappedValue {
            case let value? as T?: value
            default: nil
            }
        }
    }
}

extension DatabaseSnapshot: Collection {
    /// Inherited from `Collection.Index`.
    public typealias Index = Int
}

extension DatabaseSnapshot: Sequence {
    /// Inherited from `Sequence.Element`.
    public typealias Element = (property: PropertyMetadata, value: any DataStoreSnapshotValue)
}

extension DatabaseSnapshot: BidirectionalCollection, MutableCollection, RandomAccessCollection {
    /// Inherited from `Collection.startIndex`.
    nonisolated public var startIndex: Int {
        properties.startIndex
    }
    
    /// Inherited from `Collection.endIndex`.
    nonisolated public var endIndex: Int {
        assert(properties.count == values.count, "Count mismatch for properties and values.")
        return properties.endIndex
    }
    
    /// Inherited from `Collection.subscript(_:)`.
    nonisolated public subscript(position: Int) -> Element {
        get {
            assert(position >= 0 && position < endIndex, "Index out of bounds: \(position)")
            return (properties[position], values[position])
        }
        set {
            self.properties[position] = newValue.property
            self.values[position] = newValue.value
        }
    }
    
    /// Inherited from `RandomAccessCollection.index(after:)`.
    @inlinable nonisolated public func index(after i: Int) -> Int { i &+ 1 }
    
    /// Inherited from `RandomAccessCollection.index(before:)`.
    @inlinable nonisolated public func index(before i: Int) -> Int { i &- 1 }
    
    /// Rebuilds indexes if the view of `properties` changed.
    @inline(__always) nonisolated private func ensureIndexes() {
        if cache.countSignature == properties.count { return }
        cache.nameToIndex.removeAll(keepingCapacity: true)
        cache.keyPathToIndex.removeAll(keepingCapacity: true)
        for (index, property) in properties.enumerated() {
            cache.nameToIndex[property.name] = index
            cache.keyPathToIndex[property.keyPath] = index
        }
        self.cache.countSignature = properties.count
    }
    
    nonisolated public func rebuildIndexes() { cache.clear(); ensureIndexes() }
    
    private final class Cache: Sendable {
        nonisolated private let _nameToIndex: Mutex<[String: Int]> = .init([:])
        nonisolated private let _keyPathToIndex: Mutex<[AnyKeyPath & Sendable: Int]> = .init([:])
        nonisolated private let _countSignature: Atomic<Int> = .init(-1)
        
        nonisolated fileprivate var nameToIndex: [String: Int] {
            get { _nameToIndex.withLock { $0 } }
            set { _nameToIndex.withLock { $0 = newValue } }
        }
        
        nonisolated fileprivate var keyPathToIndex: [AnyKeyPath & Sendable: Int] {
            get { _keyPathToIndex.withLock { $0 } }
            set { _keyPathToIndex.withLock { $0 = newValue } }
        }
        
        nonisolated fileprivate var countSignature: Int {
            get { _countSignature.load(ordering: .relaxed) }
            set { _countSignature.store(newValue, ordering: .relaxed) }
        }
        
        nonisolated fileprivate func clear() {
            nameToIndex.removeAll(keepingCapacity: true)
            keyPathToIndex.removeAll(keepingCapacity: true)
            self.countSignature = -1
        }
    }
}

extension DatabaseSnapshot {
    nonisolated public func getProperty(name: String) -> PropertyMetadata? {
        ensureIndexes()
        guard let index = self.cache.nameToIndex[name] else { return nil }
        return properties[index]
    }
    
    nonisolated public func getProperty(keyPath: AnyKeyPath & Sendable) -> PropertyMetadata? {
        ensureIndexes()
        guard let index = self.cache.keyPathToIndex[keyPath] else { return nil }
        return properties[index]
    }
}

extension DatabaseSnapshot {
    nonisolated public func getValue(name: String) -> (any DataStoreSnapshotValue)? {
        ensureIndexes()
        guard let index = self.cache.nameToIndex[name] else { return nil }
        return values[index]
    }
    
    nonisolated public func getValue<T>(name: String, as _: T.Type) -> T? {
        getValue(name: name) as? T
    }
    
    nonisolated public func getValue(keyPath: AnyKeyPath & Sendable) -> (any DataStoreSnapshotValue)? {
        ensureIndexes()
        guard let index = self.cache.keyPathToIndex[keyPath] else { return nil }
        return values[index]
    }
    
    nonisolated public func getValue<T>(keyPath: AnyKeyPath & Sendable, as _: T.Type) -> T? {
        getValue(keyPath: keyPath) as? T
    }
}

extension DatabaseSnapshot {
    @discardableResult nonisolated public mutating func setValue<T>(
        _ value: T,
        at index: Index
    ) -> Bool where T: DataStoreSnapshotValue {
        let property = self.properties[index]
        #if DEBUG
        if property.metadata.isOptional {
            guard (
                T.self is SQLNull.Type ||
                T.self is NSNull.Type ||
                T.self is Optional<T>.Type
            ) else {
                preconditionFailure("Type violation: \(T.self) is not Optional<T>")
            }
        }
        switch property.metadata {
        case let relationship as Schema.Relationship:
            if relationship.isToOneRelationship, value is PersistentIdentifier == false {
                preconditionFailure("Type violation: \(T.self) is not PersistentIdentifier")
            }
            if !relationship.isToOneRelationship, value is [PersistentIdentifier] == false {
                preconditionFailure("Type violation: \(T.self) is not Array<PersistentIdentifier>")
            }
        case let attribute as Schema.Attribute:
            if attribute.valueType is T.Type == false {
                preconditionFailure("Type violation: \(T.self) is not \(attribute.valueType)")
            }
        default:
            break
        }
        #endif
        self.values[index] = value
        return true
    }
    
    @discardableResult nonisolated public mutating func setValue<T>(
        _ value: T,
        name: String
    ) -> Bool where T: DataStoreSnapshotValue {
        ensureIndexes()
        guard let index = self.cache.nameToIndex[name] else { return false }
        return setValue(value, at: index)
    }
    
    @discardableResult nonisolated public mutating func setValue<T>(
        _ value: T,
        keyPath: AnyKeyPath & Sendable
    ) -> Bool where T: DataStoreSnapshotValue {
        ensureIndexes()
        guard let index = self.cache.keyPathToIndex[keyPath] else { return false }
        return setValue(value, at: index)
    }
}

extension DatabaseSnapshot {
    @discardableResult
    nonisolated public mutating func remove(at index: Index) -> Element? {
        let removedProperty = self.properties.remove(at: index)
        let removedValue = self.values.remove(at: index)
        assert(properties.count == values.count)
        return (removedProperty, removedValue)
    }
    
    @discardableResult
    nonisolated public mutating func removeValue(name: String) -> Element? {
        ensureIndexes()
        guard let index = self.cache.nameToIndex[name] else { return nil }
        return remove(at: index)
    }
    
    @discardableResult
    nonisolated public mutating func removeValue(keyPath: AnyKeyPath & Sendable) -> Element? {
        ensureIndexes()
        guard let index = self.cache.keyPathToIndex[keyPath] else { return nil }
        return remove(at: index)
    }
}

extension DatabaseSnapshot {
    nonisolated public subscript(name: String) -> (any DataStoreSnapshotValue)? {
        get { getValue(name: name) }
        set {
            switch newValue {
            case let value?: setValue(value, name: name)
            case nil: setValue(SQLNull(), name: name)
            }
        }
    }
    
    nonisolated public subscript(keyPath: AnyKeyPath & Sendable) -> (any DataStoreSnapshotValue)? {
        get { getValue(keyPath: keyPath) }
        set {
            switch newValue {
            case let value?: setValue(value, keyPath: keyPath)
            case nil: setValue(SQLNull(), keyPath: keyPath)
            }
        }
    }
}

extension DatabaseSnapshot {
    nonisolated package func assertRowExists(
        for primaryKey: String,
        table: String,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        let rows = try connection.query(
            "SELECT 1 FROM \(quote(table)) WHERE \(quote(pk)) = ? LIMIT 1",
            bindings: primaryKey
        )
        if rows.isEmpty {
            let lhsDescription = "\(table).\(pk) = \(primaryKey)"
            let rhsDescription = "\(entityName)._pk = \(self.primaryKey)"
            let message = "Missing parent row \(lhsDescription) while inserting for \(rhsDescription)."
            throw SQLError(.rowNotFound, message: message)
        }
    }
    
    nonisolated package func assertRelationshipValid(
        in property: PropertyMetadata,
        connection: borrowing DatabaseConnection<Store>
    ) throws {
        guard let relationship = property.metadata as? Schema.Relationship else {
            preconditionFailure("The property must be a relationship: \(property)")
        }
        guard let reference = property.reference else {
            preconditionFailure("The relationship must have a reference: \(property)")
        }
        let description = "\(entityName).\(property.name) \(persistentIdentifier)"
        let identifiers: [PersistentIdentifier]
        switch values[property.index] {
        case let value as [PersistentIdentifier]: identifiers = value
        case let value as PersistentIdentifier: identifiers = [value]
        default: identifiers = []
        }
        for identifier in identifiers {
            try assertRowExists(
                for: identifier.primaryKey(),
                table: relationship.destination,
                connection: connection
            )
            logger.notice("Pass: \(relationship.destination).\(pk) = \(identifier.primaryKey())")
        }
        if identifiers.isEmpty {
            logger.warning("No identifiers to validate: \(description)")
        }
        if relationship.isToOneRelationship {
            assert(
                reference.count == 1,
                "To-one must have exactly one reference: \(description)"
            )
        } else {
            assert(
                reference.count == 1 || reference.count == 2,
                "To-many must have 1 (foreign key) or 2 (intermediary) references: \(description)"
            )
        }
    }
}

extension DatabaseSnapshot {
    nonisolated internal var uniquenessConstraintsMetadata: Logger.Metadata {
        .init(uniqueKeysWithValues: properties
            .filter { $0.metadata.isUnique }
            .map { ("unique.\($0.name)", "\(self[$0.index])") }
        )
    }
}

extension DatabaseSnapshot {
    nonisolated public func contentDescriptions(
        including includedPropertyNames: [String]
    ) -> [String: String] {
        let included = Set(includedPropertyNames)
        return contentDescriptions { included.contains($0.name) }
    }
    
    nonisolated public func contentDescriptions(
        excluding excludedPropertyNames: [String]
    ) -> [String: String] {
        let excluded = Set(excludedPropertyNames)
        return contentDescriptions { !excluded.contains($0.name) }
    }
    
    nonisolated public func contentDescriptions(
        where shouldInclude: (PropertyMetadata) -> Bool = { _ in true }
    ) -> [String: String] {
        var result = [String: String]()
        result.reserveCapacity(properties.count)
        for (property, value) in zip(properties, values) {
            guard shouldInclude(property) else { continue }
            let name = property.name
            switch value {
            case let value as PersistentIdentifier:
                result[name] = value.primaryKey()
            case let values as [PersistentIdentifier]:
                result[name] = values.map { $0.primaryKey() }.joined(separator: ", ")
            default:
                result[name] = String(describing: value)
            }
        }
        return result
    }
}

extension DatabaseSnapshot {
    nonisolated public var contentDescriptions: [String] {
        zip(properties, values).map { (property, value) in
            switch value {
            case let value as PersistentIdentifier:
                "\(property.name): \(value.primaryKey())"
            case let values as [PersistentIdentifier]:
                "\(property.name): [\(values.map { $0.primaryKey() }.joined(separator: ", "))]"
            default:
                "\(property.name): \(value)"
            }
        }
    }
}

extension DatabaseSnapshot: CustomStringConvertible {
    /// Inherited from `CustomStringConvertible.description`.
    nonisolated public var description: String {
        "DatabaseSnapshot(\(entityName), \(persistentIdentifier))"
    }
}

extension DatabaseSnapshot: CustomDebugStringConvertible {
    /// Inherited from `CustomDebugStringConvertible.debugDescription`.
    nonisolated public var debugDescription: String {
        """
        DatabaseSnapshot(
            entity: \(entityName),
            primaryKey: \(primaryKey),
            values: \(contentDescriptions)
        )
        """
    }
}
