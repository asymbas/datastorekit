//
//  MirrorSchemaEntity.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
private import Logging
public import DataStoreSupport
public import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.bootstrap")

#if false
extension Schema.Entity {
    nonisolated public var metatype: any PersistentModel.Type {
        let mirror = Mirror(reflecting: self)
        return mirror.descendant("_objectType") as! any PersistentModel.Type
    }
}
#endif

/// Returns the model's type through introspection.
///
/// - Note: This will not work from a decoded schema.
/// - Parameter entity: The entity.
/// - Returns: The model type.
nonisolated public func reflectEntity(_ entity: Schema.Entity)
-> (any (PersistentModel & SendableMetatype).Type)? {
    let mangledNameKey = "_mangledName"
    let objectTypeKey = "_objectType"
    var type: Any.Type?
    for child in Mirror(reflecting: entity).children {
        switch child.label {
        case mangledNameKey:
            if let mangledNameValue = child.value as? String {
                if DataStoreDebugging.mode == .trace {
                    logger.trace("mangledNameKey: \(mangledNameKey), mangledNameValue: \(mangledNameValue)")
                }
                type = _typeByName(mangledNameValue)
            }
        case objectTypeKey:
            if let objectTypeValue = child.value as? Any.Type {
                if DataStoreDebugging.mode == .trace {
                    logger.trace(
                        """
                        objectTypeKey - \(objectTypeKey)
                        objectTypeValue - \(objectTypeValue)
                        _getTypeName(_:qualified:) - \(_getTypeName(objectTypeValue, qualified: true))
                        _getMangledTypeName(_:) - \(_getMangledTypeName(objectTypeValue))
                        """
                    )
                }
                type = objectTypeValue
            }
        default:
            continue
        }
    }
    guard let type = type as? any (PersistentModel & SendableMetatype).Type else {
        return nil
    }
    return type
}
