//
//  SQLType.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import Foundation
import SQLite3
import System
import Synchronization

public enum SQLType: String, CaseIterable, Equatable, Hashable, Sendable {
    case null = "NULL"
    case integer = "INTEGER"
    case real = "REAL"
    case text = "TEXT"
    case blob = "BLOB"
    
    nonisolated public init?(for type: Any.Type) {
        switch unwrapOptionalMetatype(type) {
        case let type as any RawRepresentable.Type:
            if let type = Self(as: type) {
                self = type
            } else {
                return nil
            }
        case is NSNull.Type, is SQLNull.Type: self = .null
        case is Bool.Type: self = .integer
        case is any BinaryInteger.Type: self = .integer
        case is any BinaryFloatingPoint.Type: self = .real
        case is Date.Type: self = .real
        case is FilePath.Type, is URL.Type, is UUID.Type: self = .text
        case is String.Type: self = .text
        case is Data.Type: self = .blob
        case is any Codable.Type: self = .text
        default: return nil
        }
    }
    
    nonisolated public init?<T: RawRepresentable>(as type: T.Type) {
        if let type = Self(for: type.RawValue) {
            self = type
        } else {
            return nil
        }
    }
    
    nonisolated public init?<T>(equivalentRawValueType type: T.Type) {
        switch unwrapOptionalMetatype(type) {
        case is Optional<T>.Type, is NSNull.Type, is SQLNull.Type: self = .null
        case is Bool.Type, is Int.Type, is Int64.Type: self = .integer
        case is Float.Type, is Double.Type: self = .real
        case is String.Type: self = .text
        case is Data.Type: self = .blob
        default: return nil
        }
    }
}

extension SQLType: LosslessStringConvertible {
    /// Inherited from `LosslessStringConvertible.init(_:)`.
    nonisolated public init?(_ description: String) {
        switch description {
        case "NULL": self = .null
        case "INTEGER": self = .integer
        case "REAL": self = .real
        case "TEXT": self = .text
        case "BLOB": self = .blob
        default: return nil
        }
    }
}

extension SQLType: CustomStringConvertible {
    /// Inherited from `CustomStringConvertible.description`.
    nonisolated public var description: String {
        rawValue
    }
}
