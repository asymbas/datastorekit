//
//  SQLNull.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public struct SQLNull: Codable, Equatable, Hashable, Sendable {
    nonisolated public init() {}
}

extension SQLNull: LosslessStringConvertible {
    /// Inherited from `LosslessStringConvertible.init(_:)`.
    nonisolated public init?(_ description: String) {
        guard description == "NULL" else {
            return nil
        }
    }
}

extension SQLNull: CustomStringConvertible {
    /// Inherited from `CustomStringConvertible.description`.
    nonisolated public var description: String {
        "NULL"
    }
}
