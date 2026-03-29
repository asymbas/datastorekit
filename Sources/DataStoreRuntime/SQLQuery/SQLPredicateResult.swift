//
//  SQLPredicateResult.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation
import SQLiteHandle
import SQLiteStatement
import SwiftData

public struct SQLPredicateResult: Sendable {
    nonisolated public let key: Int?
    nonisolated public let statement: SQL
    nonisolated public let properties: [PropertyMetadata]
    nonisolated public let shouldCache: Bool
    nonisolated public let requestedIdentifiers: Set<PersistentIdentifier>?
    
    nonisolated package init(
        hash: Int?,
        statement: SQL,
        properties: [PropertyMetadata],
        requestedIdentifiers:  Set<PersistentIdentifier>?
    ) {
        if let requestedIdentifiers {
            var hasher = Hasher()
            hasher.combine(requestedIdentifiers)
            self.key = hasher.finalize()
        } else {
            self.key = hash
        }
        self.statement = statement
        self.properties = properties
        self.shouldCache = hash != nil
        self.requestedIdentifiers = requestedIdentifiers
    }
}

public struct SQLPredicateTranslation: Equatable, Hashable, Identifiable, Sendable {
    nonisolated public let id: UUID
    nonisolated public var predicateDescription: String?
    nonisolated public var predicateHash: Int?
    nonisolated public var sql: String?
    nonisolated public var placeholdersCount: Int?
    nonisolated public var bindingsCount: Int?
    nonisolated public var tree: PredicateTree
    
    nonisolated public init(
        id: UUID,
        predicateDescription: String? = nil,
        predicateHash: Int? = nil,
        sql: String? = nil,
        placeholdersCount: Int? = nil,
        bindingsCount: Int? = nil,
        tree: PredicateTree? = nil
    ) {
        self.id = id
        self.predicateDescription = predicateDescription
        self.predicateHash = predicateHash
        self.sql = sql
        self.placeholdersCount = placeholdersCount
        self.bindingsCount = bindingsCount
        self.tree = tree ?? .init(id: id, path: [])
    }
}
