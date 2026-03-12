//
//  TypeMetadata.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

/// Represents metadata for a registered Swift class type.
public struct TypeMetadata: Sendable {
    nonisolated public let type: AnyClass
    nonisolated public let typeName: String
    nonisolated public let mangledTypeName: String
    nonisolated public let metadata: (any Sendable)?
    
    nonisolated internal init(
        type: AnyClass,
        typeName: String,
        mangledTypeName: String,
        metadata: (any Sendable)? = nil
    ) {
        self.type = type
        self.typeName = typeName
        self.mangledTypeName = mangledTypeName
        self.metadata = metadata
    }
}



