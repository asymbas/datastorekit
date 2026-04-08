//
//  SchemaError.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

/// A type that describes a schema error.
public enum SchemaError: Swift.Error {
    /// The entity was not found in the schema.
    case entityNotRegistered
    /// The ORM and SQL schemas are inconsistent.
    case internalInconsistency
    /// The target entity was not found in the schema.
    case relationshipTargetEntityNotRegistered
    case propertyMetadataNotFound
}
