//
//  PersistentIdentifier+PrimaryKey.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Foundation
public import SwiftData

#if DEBUG
extension PersistentIdentifier: @retroactive CustomStringConvertible {
    nonisolated public var description: String {
        "\(storeIdentifier ?? "nil") \(entityName) \(primaryKey())"
    }
}
#endif

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
