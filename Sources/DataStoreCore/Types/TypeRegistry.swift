//
//  TypeRegistry.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public enum TypeRegistry {
    /// A lookup key identifying a registered type by various forms.
    public enum Key {
        case type(AnyClass)
        case typeName(String)
        case mangledTypeName(String)
    }
}
