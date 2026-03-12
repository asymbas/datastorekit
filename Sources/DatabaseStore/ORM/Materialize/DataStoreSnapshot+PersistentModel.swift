//
//  DataStoreSnapshot+PersistentModel.swift
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
import Logging
import Foundation
import SQLiteHandle
import SwiftData

#if DEBUG

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension PersistentModel where Self: AnyObject {
    public init(
        _ snapshot: DatabaseSnapshot,
        relatedSnapshots: inout [PersistentIdentifier: DatabaseSnapshot],
        remappedIdentifiers: inout [PersistentIdentifier: PersistentIdentifier],
        modelContext: ModelContext
    ) throws {
        var snapshot = snapshot
        let editingState = modelContext.editingState
        let entityName = Schema.entityName(for: Self.self)
        self.init(backingData: Self.createBackingData())
        if snapshot.properties.isEmpty {
            snapshot.properties = .init(Self.databaseSchemaMetadata.lazy)
        }
        guard let store = DataStoreContainer.load(editingState: editingState),
              let store = store as? DatabaseStore else {
            throw DataStoreError.unsupportedFeature
        }
        if let matchedSnapshot = try store.queue.withConnection { try $0.match(snapshot: snapshot) } {
            self.persistentBackingData.persistentModelID = matchedSnapshot.persistentIdentifier
            relatedSnapshots[matchedSnapshot.persistentIdentifier] = matchedSnapshot
            if snapshot.persistentIdentifier != matchedSnapshot.persistentIdentifier {
                remappedIdentifiers[snapshot.persistentIdentifier] = matchedSnapshot.persistentIdentifier
            }
        }
        for property in snapshot.properties {
            let wrappedValue = snapshot.values[property.index]
            let description = "\(entityName).\(property) = \(wrappedValue)"
            guard let keyPath = property.keyPath as? PartialKeyPath<Self> else {
                preconditionFailure()
            }
            switch property.metadata {
            case let attribute as Schema.Attribute:
                guard let valueType = unwrapOptionalMetatype(property.valueType) as? any DataStoreSnapshotValue.Type else {
                    preconditionFailure()
                }
                setValue(attribute, at: keyPath, for: wrappedValue, as: valueType)
            case let relationship as Schema.Relationship:
                if relationship.isToOneRelationship && relationship.isOptional {
                    guard let valueType = unwrapOptionalMetatype(property.valueType) as? any PersistentModel.Type else {
                        preconditionFailure()
                    }
                    setValue(relationship, at: keyPath, for: wrappedValue, as: valueType) { _ in
                        nil
                    }
                } else if !relationship.isToOneRelationship {
                    guard let valueType = unwrapOptionalMetatype(property.valueType) as? any RelationshipCollection.Type else {
                        preconditionFailure()
                    }
                    setValue(relationship, at: keyPath, for: wrappedValue, as: valueType) { _ in
                        if relationship.isOptional {
                            nil
                        } else {
                            [any PersistentModel]()
                        }
                    }
                } else {
                    logger.debug("Relationship was not assigned a value: \(description)")
                }
            default:
                fatalError()
            }
        }
    }
}

extension PersistentModel where Self: AnyObject {
    @available(*, deprecated, message: "")
    nonisolated public init(
        with snapshot: consuming DatabaseSnapshot,
        relatedSnapshots: [PersistentIdentifier: DatabaseSnapshot]? = nil,
        modelContext: ModelContext? = nil,
        initializeEmptyValues: Bool = true
    ) {
        let entityName = Schema.entityName(for: Self.self)
        self.init(backingData: Self.createBackingData())
        logger.trace("Initializing as \(Self.self).self for entity “\(entityName)”")
        if snapshot.properties.isEmpty {
            snapshot.properties = .init(Self.databaseSchemaMetadata.lazy)
            logger.debug("Loaded empty PropertyMetadata array in \(entityName) snapshot.")
        }
        for property in snapshot.properties {
            guard let keyPath = property.keyPath as? PartialKeyPath<Self> else {
                preconditionFailure("Attribute not found: \(entityName).\(property.name)")
            }
            let wrappedValue = snapshot.values[property.index]
            switch property.metadata {
            case let attribute as Schema.Attribute:
                guard let valueType = unwrapOptionalMetatype(property.valueType) as? any DataStoreSnapshotValue.Type else {
                    preconditionFailure("Attribute does not conform to DataStoreSnapshotValue: \(entityName).\(property.name)")
                }
                setValue(attribute, at: keyPath, for: wrappedValue, as: valueType)
            case let relationship as Schema.Relationship where initializeEmptyValues:
                if relationship.isToOneRelationship && relationship.isOptional {
                    guard let valueType = unwrapOptionalMetatype(property.valueType) as? any PersistentModel.Type else {
                        preconditionFailure("Optional to-one relationship failed: \(entityName).\(property.name)")
                    }
                    setValue(relationship, at: keyPath, for: wrappedValue, as: valueType) { _ in
                        return nil
                    }
                } else if !relationship.isToOneRelationship {
                    guard let valueType = unwrapOptionalMetatype(property.valueType) as? any RelationshipCollection.Type else {
                        preconditionFailure("Optional to-one relationship failed: \(entityName).\(property.name)")
                    }
                    setValue(relationship, at: keyPath, for: wrappedValue, as: valueType) { _ in
                        if relationship.isOptional {
                            return nil
                        } else {
                            return [any PersistentModel]()
                        }
                    }
                }
            default:
                fatalError()
            }
        }
        if let modelContext {
            self.attach(with: snapshot, relatedModels: [:], modelContext: modelContext)
        }
    }
    
    nonisolated public func attach(
        with snapshot: consuming DatabaseSnapshot,
        relatedModels: [PersistentIdentifier: any PersistentModel],
        resolveMissingRelationships: Bool = true,
        modelContext: ModelContext? = nil
    ) {
        let entityName = Schema.entityName(for: Self.self)
        logger.debug("Attaching relationships to entity “\(entityName)”")
        for property in snapshot.properties where property.metadata is Schema.Relationship {
            guard let relationship = property.metadata as? Schema.Relationship,
                  let keyPath = property.keyPath as? PartialKeyPath<Self> else {
                preconditionFailure("Relationship not found: \(entityName).\(property.name)")
            }
            let wrappedValue = snapshot.values[property.index]
            switch unwrapOptionalMetatype(property.valueType) {
            case let valueType as any PersistentModel.Type:
                setValue(relationship, at: keyPath, for: wrappedValue, as: valueType) { _ in
                    return nil
                }
            case let valueType as any RelationshipCollection.Type:
                setValue(relationship, at: keyPath, for: wrappedValue, as: valueType) { persistentIdentifiers in
                    let models = [any PersistentModel]()
                    for _ in persistentIdentifiers {
                        guard let _ = unwrapArrayMetatype(valueType) as? any PersistentModel.Type else {
                            logger.error("Unable to unwrap array metatype: \(valueType)")
                            continue
                        }
                    }
                    return models
                }
            default:
                fatalError()
            }
        }
    }
}

extension PersistentModel {
    nonisolated internal func setValue<T>(
        _ property: Schema.Attribute,
        at keyPath: PartialKeyPath<Self>,
        for wrappedValue: (any DataStoreSnapshotValue)?,
        as type: T.Type
    ) where T: Codable {
        switch keyPath {
        case let keyPath as ReferenceWritableKeyPath<Self, T>:
            guard let value = (wrappedValue ?? property.defaultValue) as? T else {
                preconditionFailure("Required attribute must not be nil: \(Self.self).\(property.name)")
            }
            self.persistentBackingData.setValue(forKey: keyPath, to: value)
            logger.debug("Required attribute set: \(Self.self).\(property.name) = \(value)")
        case let keyPath as ReferenceWritableKeyPath<Self, T?>:
            guard let value = wrappedValue as? T else {
                self.persistentBackingData.setValue(forKey: keyPath, to: nil)
                logger.debug("Optional attribute set: \(Self.self).\(property.name) = nil")
                return
            }
            self.persistentBackingData.setValue(forKey: keyPath, to: value)
            logger.debug("Optional attribute set: \(Self.self).\(property.name) = \(value)")
        default:
            fatalError()
        }
    }
    
    nonisolated internal func setValue<T>(
        _ property: Schema.Relationship,
        at keyPath: PartialKeyPath<Self>,
        for wrappedValue: (any DataStoreSnapshotValue)?,
        as type: T.Type,
        model: (PersistentIdentifier) -> (any PersistentModel)?
    ) where T: PersistentModel {
        switch keyPath {
        case let keyPath as ReferenceWritableKeyPath<Self, T>:
            guard let persistentIdentifier = wrappedValue as? PersistentIdentifier else {
                fatalError("Required to-one relationship must not be nil: \(Self.self).\(property.name)")
            }
            guard let model = model(persistentIdentifier) as? T else {
                fatalError("Required to-one relationship must provide related model: \(Self.self).\(property.name)")
            }
            self.persistentBackingData.setValue(forKey: keyPath, to: model)
            logger.debug("Required to-one relationship set: \(Self.self).\(property.name)")
        case let keyPath as ReferenceWritableKeyPath<Self, T?>:
            guard let persistentIdentifier = wrappedValue as? PersistentIdentifier else {
                self.persistentBackingData.setValue(forKey: keyPath, to: nil)
                logger.debug("Optional to-one relationship: \(Self.self).\(property.name) = nil")
                return
            }
            guard let model = model(persistentIdentifier) as? T else {
                logger.warning("Optional to-one relationship must provide related model: \(Self.self).\(property.name)")
                return
            }
            self.persistentBackingData.setValue(forKey: keyPath, to: model)
            logger.debug("Optional to-one relationship set: \(Self.self).\(property.name)")
        default:
            fatalError()
        }
    }
    
    nonisolated internal func setValue<T>(
        _ property: Schema.Relationship,
        at keyPath: PartialKeyPath<Self>,
        for wrappedValue: (any DataStoreSnapshotValue)?,
        as type: T.Type,
        models: ([PersistentIdentifier]) -> [any PersistentModel]?
    ) where T: RelationshipCollection, T.PersistentElement: PersistentModel {
        switch keyPath {
        case let keyPath as ReferenceWritableKeyPath<Self, [T.PersistentElement]>:
            guard let persistentIdentifiers = wrappedValue as? [PersistentIdentifier] else {
                fatalError("Required to-many relationship must not be nil: \(Self.self).\(property.name)")
            }
            guard let models = models(persistentIdentifiers) as? [T.PersistentElement] else {
                fatalError("Required to-many relationship must provide related models: \(Self.self).\(property.name)")
            }
            self.persistentBackingData.setValue(forKey: keyPath, to: models)
            logger.debug("Required to-many relationship set: \(Self.self).\(property.name) = \(models.count) models")
        case let keyPath as ReferenceWritableKeyPath<Self, [T.PersistentElement]?>:
            guard let persistentIdentifiers = wrappedValue as? [PersistentIdentifier] else {
                self.persistentBackingData.setValue(forKey: keyPath, to: nil)
                logger.debug("Optional to-many relationship set: \(Self.self).\(property.name) = nil")
                return
            }
            guard let models = models(persistentIdentifiers) as? [T.PersistentElement] else {
                fatalError("Optional to-many relationship must provide related models: \(Self.self).\(property.name)")
            }
            self.persistentBackingData.setValue(forKey: keyPath, to: models)
            logger.debug("Optional to-many relationship set: \(Self.self).\(property.name) = \(models.count) models")
        default:
            fatalError()
        }
    }
}

extension DatabaseSnapshot {
    /// Collects all dependencies of every model by recursively traversing their relationships to create snapshots.
    public static func transform<T>(
        models: [T],
        topLevelRelationshipsOnly: Bool = false,
        modelContext: ModelContext? = nil,
        isolation: isolated Actor = #isolation
    ) throws -> [PersistentIdentifier: Self] where T: PersistentModel {
        guard !models.isEmpty else {
            return [:]
        }
        var snapshots = [PersistentIdentifier: Self]()
        var relatedBackingDatas = [PersistentIdentifier: any BackingData]()
        guard let modelContext = modelContext ?? models.first?.modelContext else {
            throw SwiftDataError.missingModelContext
        }
        let schema = modelContext.container.schema
        for model in models {
            try register(
                .init(from: model.persistentBackingData, relatedBackingDatas: &relatedBackingDatas),
                isTopLevel: true
            )
            let _ = model.persistentModelID.entityName
            let _ = model.persistentModelID.primaryKey()
        }
        nonisolated func register(
            _ snapshot: DatabaseSnapshot,
            isTopLevel: Bool = false
        ) throws {
            guard snapshots[snapshot.persistentIdentifier] == nil else {
                return
            }
            guard let entity = schema.entitiesByName[snapshot.persistentIdentifier.entityName] else {
                throw SwiftDataError.unknownSchema
            }
            snapshots[snapshot.persistentIdentifier] = snapshot
            for relationship in entity.relationships {
                guard let key = snapshot.properties.first(where: { $0.name == relationship.name }) else {
                    continue
                }
                if topLevelRelationshipsOnly && !isTopLevel,
                   relationship.isOptional || !relationship.isToOneRelationship {
                    continue
                }
                switch snapshot.values[key.index] {
                case let persistentIdentifier as PersistentIdentifier:
                    guard snapshots[persistentIdentifier] == nil else {
                        logger.debug("Snapshot of relationship has already been registered: \(key)")
                        continue
                    }
                    guard let valueType = unwrapOptionalMetatype(relationship.valueType) as? any PersistentModel.Type else {
                        fatalError("Snapshot of relationship must conform to PersistentModel: \(key)")
                    }
                    try register(getRelationship(from: persistentIdentifier, type: valueType))
                case let persistentIdentifiers as [PersistentIdentifier]:
                    guard let valueType = unwrapOptionalMetatype(relationship.valueType) as? any RelationshipCollection.Type else {
                        fatalError("Snapshot of relationship must conform to RelationshipCollection: \(key)")
                    }
                    try getRelationships(from: persistentIdentifiers, type: valueType).forEach { snapshot in
                        try register(snapshot)
                    }
                case is SQLNull:
                    logger.debug("Registering is skipped for null relationship: \(key)")
                    continue
                default:
                    fatalError("The snapshot is missing a relationship: \(entity.name).\(relationship.name)")
                }
            }
        }
        nonisolated func getRelationship<R>(
            from persistentIdentifier: PersistentIdentifier,
            type: R.Type
        ) throws -> DatabaseSnapshot where R: PersistentModel {
            var descriptor = FetchDescriptor(predicate: #Predicate<R> {
                $0.persistentModelID == persistentIdentifier
            })
            descriptor.fetchLimit = 1
            guard let model = try modelContext.fetch(descriptor).first else {
                logger.warning("Not found: \(persistentIdentifier.entityName) \(persistentIdentifier.primaryKey())")
                throw SwiftDataError.unknownSchema
            }
            let entityName = persistentIdentifier.entityName
            let primaryKey = persistentIdentifier.primaryKey()
            logger.debug("Registered snapshot of relationship: \(entityName) \(primaryKey)")
            return .init(from: model.persistentBackingData, relatedBackingDatas: &relatedBackingDatas)
        }
        nonisolated func getRelationships<R>(
            from persistentIdentifiers: [PersistentIdentifier],
            type: R.Type
        ) throws -> [Self] where R: RelationshipCollection, R.PersistentElement: PersistentModel {
            return try persistentIdentifiers.compactMap {
                guard snapshots[$0] == nil else {
                    logger.debug("Snapshot of relationship has already been registered: \($0)")
                    return nil
                }
                return try getRelationship(from: $0, type: R.PersistentElement.self)
            }
        }
        return snapshots
    }
}

extension DatabaseSnapshot {
    /// Creates typed models from snapshots collected within the same batch.
    @discardableResult nonisolated public static func rehydrate<T>(
        from snapshots: [PersistentIdentifier: Self],
        as type: T.Type
    ) throws -> [T] where T: PersistentModel {
        guard !snapshots.isEmpty else {
            logger.notice("No snapshots provided for rehydrating \(T.self) models.")
            return []
        }
        logger.debug("Starting to rehydrate \(T.self) models... (count: \(snapshots.count))")
        var relatedModels = [PersistentIdentifier: any PersistentModel]()
        for (persistentIdentifier, snapshot) in snapshots {
            guard let type = Schema.type(for: persistentIdentifier.entityName) else {
                continue
            }
            createPersistentModel(type)
            nonisolated func createPersistentModel<Model>(_ modelType: Model.Type)
            where Model: PersistentModel {
                relatedModels[persistentIdentifier] = Model.init(with: snapshot)
                logger.debug("Created \(Model.self) model for \(persistentIdentifier.primaryKey())")
            }
        }
        for (persistentIdentifier, model) in relatedModels {
            guard let snapshot = snapshots[persistentIdentifier] else {
                continue
            }
            let _ = persistentIdentifier.entityName
            model.attach(with: snapshot, relatedModels: relatedModels)
        }
        return relatedModels.values.compactMap { $0 as? T }
    }
}

#endif
