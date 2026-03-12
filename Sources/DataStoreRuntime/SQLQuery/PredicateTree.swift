//
//  PredicateTree.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation

public struct PredicateTree: Equatable, Hashable, Sendable {
    nonisolated public let id: UUID
    nonisolated public var path: [PredicateTree.Node]
    
    nonisolated public init(id: UUID, path: [PredicateTree.Node]) {
        self.id = id
        self.path = path
    }
    
    public struct Node: Equatable, Hashable, Sendable {
        nonisolated public var path: [PredicateExpressions.VariableID]
        nonisolated public var key: PredicateExpressions.VariableID?
        nonisolated public var expression: Any.Type?
        nonisolated public var title: String
        nonisolated public var content: [String]
        nonisolated public var level: Int
        nonisolated public var isComplete: Bool
        
        nonisolated public init(
            path: [PredicateExpressions.VariableID] = [],
            key: PredicateExpressions.VariableID? = nil,
            expression: Any.Type? = nil,
            title: String,
            content: [String] = [],
            level: Int,
            isComplete: Bool
        ) {
            self.path = path
            self.key = key
            self.expression = expression
            self.title = title
            self.content = content
            self.level = level
            self.isComplete = isComplete
        }
        
        nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.path == rhs.path &&
            lhs.key == rhs.key &&
            lhs.title == rhs.title &&
            lhs.content == rhs.content &&
            lhs.level == rhs.level &&
            lhs.isComplete == rhs.isComplete
        }
        
        nonisolated public func hash(into hasher: inout Hasher) {
            hasher.combine(path)
            hasher.combine(key)
            hasher.combine(title)
            hasher.combine(content)
            hasher.combine(level)
            hasher.combine(isComplete)
        }
    }
}
