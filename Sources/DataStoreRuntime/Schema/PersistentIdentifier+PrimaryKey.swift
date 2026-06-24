//
//  PersistentIdentifier+PrimaryKey.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Foundation
private import Synchronization
public import SwiftData

#if DEBUG
extension PersistentIdentifier: @retroactive CustomDebugStringConvertible {
    nonisolated public var debugDescription: String {
        "\(storeIdentifier ?? "nil") \(entityName) \(primaryKey())"
    }
}
#endif

// TODO: Only call this method as a fallback.

nonisolated private let encoder: Mutex<JSONEncoder> = .init(.init())

extension PersistentIdentifier {
    nonisolated public func primaryKey<T>(as type: T.Type = String.self) -> T
    where T: LosslessStringConvertible & Sendable {
        encoder.withLock { encoder in
            do {
                let data = try encoder.encode(self)
                let envelope = try JSONDecoder().decode(_PrimaryKeyEnvelope.self, from: data)
                let rawValue = envelope.implementation.primaryKey.stringValue
                if let typed = rawValue as? T { return typed }
                if let value = T(rawValue) { return value }
                fatalError("Unable to convert primary key '\(rawValue)' to \(T.self) for \(self)")
            } catch {
                fatalError("Unable to extract primary key from PersistentIdentifier: \(error)")
            }
        }
    }
    
    private struct _PrimaryKeyEnvelope: Decodable {
        nonisolated fileprivate let implementation: _PrimaryKeyImplementation
    }
    
    private struct _PrimaryKeyImplementation: Decodable {
        nonisolated fileprivate let primaryKey: _PrimaryKeyValue
    }
    
    private enum _PrimaryKeyValue: Decodable {
        case string(String)
        case int(Int64)
        case uint(UInt64)
        case double(Double)
        
        nonisolated fileprivate var stringValue: String {
            switch self {
            case .string(let value): value
            case .int(let value): .init(value)
            case .uint(let value): .init(value)
            case .double(let value): .init(value)
            }
        }
        
        nonisolated fileprivate init(from decoder: any Decoder) throws {
            let container = try decoder.singleValueContainer()
            if let value = try? container.decode(String.self) {
                self = .string(value)
            } else if let value = try? container.decode(Int64.self) {
                self = .int(value)
            } else if let value = try? container.decode(UInt64.self) {
                self = .uint(value)
            } else if let value = try? container.decode(Double.self) {
                self = .double(value)
            } else {
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription: "Unsupported primary key type for PersistentIdentifier"
                )
            }
        }
    }
}

#if false

extension PersistentIdentifier {
    nonisolated public func primaryKey<T>(as type: T.Type = String.self) -> T
    where T: LosslessStringConvertible {
        do {
            let decoded = try JSONSerialization.jsonObject(
                with: JSONEncoder().encode(self),
                options: []
            ) as? [String: Any]
            guard let implementation = decoded?["implementation"] as? [String: Any],
                  let primaryKey = implementation["primaryKey"] as? T else {
                fatalError(SwiftDataError.unknownSchema.localizedDescription)
            }
            return primaryKey
        } catch {
            fatalError("Unable to extract primary key from PersistentIdentifier: \(error)")
        }
    }
}

extension PersistentIdentifier {
    @available(*, unavailable, message: "Implementation is not stable.")
    nonisolated public func _primaryKey<T>(as type: T.Type = String.self) -> T
    where T: Codable & Equatable & Hashable & LosslessStringConvertible & Sendable {
        do {
            let data = try JSONEncoder().encode(self)
            let decoded = try JSONDecoder().decode(Format<T>.self, from: data)
            return decoded.implementation.primaryKey
        } catch {
            fatalError("Unable to extract primary key from PersistentIdentifier: \(error)")
        }
    }
    
    private struct Format<T>: Codable
    where T: Codable & Equatable & Hashable & LosslessStringConvertible & Sendable {
        nonisolated fileprivate let implementation: Key<T>
    }
    
    private struct Key<T>: Codable, Equatable, Hashable, Sendable
    where T: Codable & Equatable & Hashable & LosslessStringConvertible & Sendable {
        nonisolated public let entityName: String
        nonisolated public let isTemporary: Bool
        nonisolated public let primaryKey: T
        nonisolated public let storeIdentifier: String?
        nonisolated public let uriRepresentation: String
    }
}

#endif
