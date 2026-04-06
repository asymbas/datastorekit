//
//  RecordIdentifier.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public struct RecordIdentifier: Equatable, Hashable, Identifiable, Sendable {
    nonisolated public let storeIdentifier: String
    nonisolated public let tableName: String
    nonisolated public let primaryKey: any LosslessStringConvertible & Sendable
    
    nonisolated public init(
        for storeIdentifier: String,
        tableName: String,
        primaryKey: any LosslessStringConvertible & Sendable
    ) {
        self.storeIdentifier = storeIdentifier
        self.tableName = tableName
        self.primaryKey = primaryKey
    }
    
    nonisolated public var id: String {
        storeIdentifier + ":" + tableName + ":" + primaryKey.description
    }
    
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.id == rhs.id
    }
    
    nonisolated public func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

extension RecordIdentifier: CustomStringConvertible {
    nonisolated public var description: String {
        "\(tableName):\(primaryKey)"
    }
}
