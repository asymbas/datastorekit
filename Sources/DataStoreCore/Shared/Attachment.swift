//
//  Attachment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation

public protocol ContextAttachmentKey: Sendable {
    associatedtype Value: Sendable
    nonisolated static var defaultValue: Value { get }
}

public struct ContextAttachmentValues: Sendable {
    nonisolated internal var storage: [ObjectIdentifier: any Sendable]
    
    nonisolated public init() {
        self.storage = [:]
    }
    
    nonisolated public subscript<Key>(_ key: Key.Type) -> Key.Value where Key: ContextAttachmentKey {
        get {
            if let value = storage[ObjectIdentifier(key)] as? Key.Value {
                return value
            }
            return Key.defaultValue
        }
        set {
            storage[ObjectIdentifier(key)] = newValue
        }
    }
    
    nonisolated public mutating func remove<Key>(_ key: Key.Type) where Key: ContextAttachmentKey {
        storage.removeValue(forKey: ObjectIdentifier(key))
    }
    
    nonisolated public mutating func merge(_ other: ContextAttachmentValues) {
        storage.merge(other.storage) { _, newValue in newValue }
    }
    
    nonisolated public func merged(with other: ContextAttachmentValues) -> ContextAttachmentValues {
        var values = self
        values.merge(other)
        return values
    }
}

public protocol ContextAttachmentProviding: Sendable {
    nonisolated var contextAttachments: ContextAttachmentValues { get }
}

@dynamicMemberLookup public struct ContextAttachmentAccessor<Provider>: Sendable
where Provider: ContextAttachmentProviding {
    nonisolated private let provider: Provider
    
    nonisolated public init(provider: Provider) {
        self.provider = provider
    }
    
    nonisolated public subscript<Value>(dynamicMember keyPath: KeyPath<ContextAttachmentValues, Value>) -> Value {
        provider.contextAttachments[keyPath: keyPath]
    }
    
    nonisolated public subscript<Key>(_ key: Key.Type) -> Key.Value where Key: ContextAttachmentKey {
        provider.contextAttachments[key]
    }
}
