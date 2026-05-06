//
//  Schema+TypeRegistry.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Logging
private import ObjectiveC
public import DataStoreCore
public import DataStoreSupport
public import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.bootstrap")

// FIXME: Handle name collisions with entity and types that use the same name.

extension Schema {
    /// Returns the model type associated with the given entity by matching the entity's identity against registered metadata.
    nonisolated public static func type(for entity: Schema.Entity)
    -> (any (PersistentModel & SendableMetatype).Type)? {
        for entry in TypeRegistry.getValues(forTypeName: entity.name) {
            if let storedEntity = entry.metadata as? Schema.Entity, storedEntity === entity {
                return entry.type as? any (PersistentModel & SendableMetatype).Type
            }
        }
        return nil
    }
    
    @available(*, deprecated, message: "The return value may be ambiguous.")
    nonisolated public static func type(for entityName: String)
    -> (any (PersistentModel & SendableMetatype).Type)? {
        TypeRegistry.getType(forName: entityName) as? any (PersistentModel & SendableMetatype).Type
    }
    
    nonisolated public static func type(for entityName: String, in storeIdentifier: String) throws
    -> (any (PersistentModel & SendableMetatype).Type)? {
        guard let store = try DataStoreAggregate.load(for: storeIdentifier),
              let entity = store.schema.entitiesByName[entityName] else {
            return nil
        }
        return Schema.type(for: entity)
    }
    
    /// Returns the model type associated with the fully qualified type name.
    nonisolated public static func type(forQualifiedTypeName qualifiedTypeName: String)
    -> (any (PersistentModel & SendableMetatype).Type)? {
        TypeRegistry.getType(forQualifiedTypeName: qualifiedTypeName) as? any (PersistentModel & SendableMetatype).Type
    }
}

extension Schema.Entity {
    /// The model type associated to this entity.
    nonisolated public var type: (any (PersistentModel & SendableMetatype).Type)? {
        Schema.type(for: self) ?? reflectEntity(self)
    }
}

// TODO: Cast relationships to `PersistentModel` and register them implicitly.

extension TypeRegistry {
    nonisolated public static func bootstrap(schema: Schema, types: [any PersistentModel.Type] = []) {
        var visited = Set<ObjectIdentifier>()
        func register(_ type: any PersistentModel.Type, entity: Schema.Entity?) {
            guard visited.insert(ObjectIdentifier(type)).inserted else { return }
            let typeName = Schema.entityName(for: type)
            let typeAsClass: AnyClass = type as AnyObject as! AnyClass
            if TypeRegistry.getValue(forType: typeAsClass) == nil {
                let mangledTypeName = _mangledTypeName(type) ?? typeName
                TypeRegistry.register(typeAsClass, typeName: typeName, mangledTypeName: mangledTypeName, metadata: entity)
                logger.trace("TypeRegistry bootstrap registered \(typeName)")
            }
            if let superentity = entity?.superentity ?? schema.entitiesByName[typeName]?.superentity,
               let superType = types.first(where: { Schema.entityName(for: $0) == superentity.name }) ?? superentity.type {
                register(superType, entity: superentity)
            }
            if let superclass = class_getSuperclass(type),
               let superType = superclass as? any PersistentModel.Type {
                let superName = Schema.entityName(for: superType)
                register(superType, entity: schema.entitiesByName[superName])
            }
        }
        for entity in schema.entities {
            let resolvedType = types.first(where: { Schema.entityName(for: $0) == entity.name }) ?? entity.type
            guard let resolvedType else {
                preconditionFailure("Entity has an unknown type: \(entity.name)")
            }
            register(resolvedType, entity: entity)
        }
    }
}
