//
//  ModelMappingError.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

/// A type that describes a model mapping error.
public enum ModelMappingError: Swift.Error {
    /// The first result column did not provide the internal `SchemaProperty` for the primary key.
    case discriminatorKeyNotFound
    /// The `PropertyMetadata` did not have the expected `SchemaProperty` metadata.
    case metadataKindMismatch
    /// The `PropertyMetadata` mapped to `Schema.Relationship` expected `[TableReference]`.
    case relationshipMissingTableReference
}
