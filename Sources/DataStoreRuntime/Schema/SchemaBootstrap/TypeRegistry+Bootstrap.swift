//
//  TypeRegistry+Bootstrap.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
public import SwiftData

@_spi(Bootstrap)
nonisolated public func resolveEntityTypes(
    in schema: Schema,
    types suppliedTypes: [any (PersistentModel & SendableMetatype).Type],
    allowMissing: Bool
) -> [String: any (PersistentModel & SendableMetatype).Type] {
    var suppliedByName: [String: any (PersistentModel & SendableMetatype).Type] = .init(minimumCapacity: suppliedTypes.count)
    for type in suppliedTypes {
        suppliedByName[Schema.entityName(for: type)] = type
    }
    var resolved: [String: any (PersistentModel & SendableMetatype).Type] = .init()
    var visited: Set<ObjectIdentifier> = .init()
    for entity in schema.entities {
        resolve(entity)
    }
    func resolve(_ entity: Schema.Entity) {
        guard resolved[entity.name] == nil else {
            return
        }
        guard let type = suppliedByName[entity.name] ?? objectType(of: entity) ?? mangledType(of: entity) else {
            if allowMissing { return }
            preconditionFailure("No type resolved for entity: \(entity.name)")
        }
        guard visited.insert(ObjectIdentifier(type)).inserted else {
            return
        }
        resolved[entity.name] = type
        let typeAsClass: AnyClass = type as AnyObject as! AnyClass
        if TypeRegistry.getValue(forType: typeAsClass) == nil {
            let mangledTypeName = _mangledTypeName(type) ?? entity.name
            TypeRegistry.register(typeAsClass, typeName: entity.name, mangledTypeName: mangledTypeName, metadata: entity)
        }
        if let superentity = entity.superentity {
            resolve(superentity)
        }
    }
    return resolved
}
