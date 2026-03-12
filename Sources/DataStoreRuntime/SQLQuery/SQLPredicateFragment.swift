//
//  SQLPredicateFragment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Collections
import DataStoreCore
import Foundation
import Logging
import SwiftUI
import Synchronization

@preconcurrency import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.query")

#if swift(>=6.2)
nonisolated internal struct SQLPredicateFragment: ~Copyable {
    nonisolated internal static let shouldDebug: Bool = {
        DataStoreDebugging.mode == .trace
    }()
    nonisolated internal let id: String
    internal let clause: String
    internal var bindings: [Any]
    internal let key: PredicateExpressions.VariableID?
    internal let alias: String?
    internal let type: Any.Type?
    internal let entity: Schema.Entity?
    internal let property: PropertyMetadata?
    internal weak var keyPath: (AnyKeyPath & Sendable)?
    internal let kind: SQLExpressionKind?
    internal var expression: (any PredicateExpression.Type)?
    internal var tag: String
    
    internal init(
        clause: String,
        bindings: [Any] = [],
        key: PredicateExpressions.VariableID? = nil,
        alias: String? = nil,
        type: Any.Type? = nil,
        entity: Schema.Entity? = nil,
        property: PropertyMetadata? = nil,
        keyPath: (AnyKeyPath & Sendable)? = nil,
        kind: SQLExpressionKind? = nil,
        expression: (any PredicateExpression.Type)? = nil,
        tag: String? = nil,
        id: String? = nil
    ) {
        self.clause = clause
        self.bindings = bindings
        self.key = key
        self.type = type
        self.alias = alias
        self.entity = entity
        self.property = property
        self.keyPath = keyPath
        self.kind = kind
        self.expression = expression
        self.tag = tag ?? ""
        self.id = Self.randomID()
    }
}
#else
internal final class SQLPredicateFragment {
    nonisolated internal static let debug: Bool = false
    nonisolated internal let id: String
    internal let clause: String
    internal var bindings: [Any]
    internal let key: PredicateExpressions.VariableID?
    internal let alias: String?
    internal let type: Any.Type?
    internal let entity: Schema.Entity?
    internal let property: PropertyMetadata?
    internal weak var keyPath: (AnyKeyPath & Sendable)?
    internal let kind: SQLExpressionKind?
    internal var expression: (any PredicateExpression.Type)?
    internal var tag: String
    
    internal init(
        clause: String,
        bindings: [Any] = [],
        key: PredicateExpressions.VariableID? = nil,
        alias: String? = nil,
        type: Any.Type? = nil,
        entity: Schema.Entity? = nil,
        property: PropertyMetadata? = nil,
        keyPath: (AnyKeyPath & Sendable)? = nil,
        kind: SQLExpressionKind? = nil,
        expression: (any PredicateExpression.Type)? = nil,
        tag: String? = nil,
        id: String? = nil
    ) {
        self.clause = clause
        self.bindings = bindings
        self.key = key
        self.type = type
        self.alias = alias
        self.entity = entity
        self.property = property
        self.keyPath = keyPath
        self.kind = kind
        self.expression = expression
        self.tag = tag ?? ""
        self.id = Self.randomID()
    }
}
#endif

extension SQLPredicateFragment {
    /// Creates a copy of the instance and applies updates provided as arguments.
    nonisolated internal /*mutating*/ func copy(
        clause: String? = nil,
        bindings: [Any]? = nil,
        key: PredicateExpressions.VariableID? = nil,
        alias: String? = nil,
        type: Any.Type? = nil,
        entity: Schema.Entity? = nil,
        property: PropertyMetadata? = nil,
        keyPath: (AnyKeyPath & Sendable)? = nil,
        kind: SQLExpressionKind? = nil,
        expression: (any PredicateExpression.Type)? = nil,
        tag: String? = nil
    ) -> SQLPredicateFragment {
        SQLPredicateFragment(
            clause: clause ?? self.clause,
            bindings: bindings ?? self.bindings,
            key: key ?? self.key,
            alias: alias ?? self.alias,
            type: type ?? self.type,
            entity: entity ?? self.entity,
            property: property ?? self.property,
            keyPath: keyPath ?? self.keyPath,
            kind: kind ?? self.kind,
            expression: expression ?? self.expression,
            tag: tag ?? self.tag,
            id: id
        )
    }
    
    nonisolated private static func randomID(length: Int = 3) -> String {
        let characters = Array("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        return String((0..<length).compactMap { _ in characters.randomElement() })
    }
    
    internal enum SQLExpressionKind: String, Sendable {
        case scope
        /// A column reference like `"Archive"."id"`.
        case columnReference
        /// A placeholder with a bound parameter to `?`.
        case bindParameter
        /// A literal value like `123`, `'abc'`, or `TRUE` that is `SQLValue`.
        case literal
        /// Includes `NOT`, `+`, `-`.
        case unaryOperation
        /// A binary comparison like `id = ?`, `score >= 4`.
        case binaryOperation
        /// A logical operation like `a AND b` or `x OR y`.
        case logicalOperator
        /// A SQL function call like `COUNT(*)`, `MAX(score)`.
        case functionCall
        /// An `EXISTS` clause like `EXISTS (SELECT ...)`.
        case existsClause
        /// A value expression like `(a + b)` or `(CAST(x AS TEXT))`.
        case expression
        /// A `CASE` expression like `CASE WHEN ... THEN ...`.
        case caseExpression
        /// A set membership check like `id IN (1, 2, 3)`.
        case setMembership
        /// A pattern match like name `LIKE 'foo%'`.
        case patternMatch
        /// A subquery expression like `(SELECT MAX(score) FROM ...)`.
        case subquery
        /// Anything else, like `CAST(x AS TEXT)` or `COLLATE NOCASE`.
        case other
    }
    
    /// The complete path that excludes aliasing.
    nonisolated internal var label: String {
        "\(keyLabel)_\(entityLabel).\(propertyLabel)"
    }
    
    /// Used for debugging `alias`.
    nonisolated internal var aliasLabel: String {
        alias == nil ? "(nil_alias)" : "\(alias.unsafelyUnwrapped)"
    }
    
    /// Used for debugging `key`.
    nonisolated internal var keyLabel: String {
        key == nil ? "(nil_key)" : "\(key.unsafelyUnwrapped)"
    }
    
    /// Used for debugging `entity`.
    nonisolated internal var entityLabel: String {
        entity == nil ? "(nil_entity)" : "\(entity.unsafelyUnwrapped.name)"
    }
    
    /// Used for debugging `property`.
    nonisolated internal var propertyLabel: String {
        property == nil ? "(nil_property)" : "\(property.unsafelyUnwrapped.name)"
    }
    
    nonisolated internal var debugPropertyKeyPath: String {
        if let keyPath = self.property?.keyPath {
            String(describing: keyPath)
        } else {
            "nil"
        }
    }
    
    nonisolated internal var debugPropertyMetadataKeyPath: String {
        if let keyPath = (self.property?.metadata as? Schema.Relationship)?.keypath {
            String(describing: keyPath)
        } else {
            "nil"
        }
    }
    
    nonisolated internal var debugPropertyMetadataInverseKeyPath: String {
        if let keyPath = (self.property?.metadata as? Schema.Relationship)?.inverseKeyPath {
            String(describing: keyPath)
        } else {
            "nil"
        }
    }
    
    nonisolated internal var debugPropertyEnclosingKeyPath: String {
        if let keyPath = (self.property?.enclosing as? Schema.Relationship)?.keypath {
            String(describing: keyPath)
        } else {
            "nil"
        }
    }
    
    nonisolated internal var debugPropertyEnclosingInverseKeyPath: String {
        if let keyPath = (self.property?.enclosing as? Schema.Relationship)?.inverseKeyPath {
            String(describing: keyPath)
        } else {
            "nil"
        }
    }
}

extension SQLPredicateFragment {
    nonisolated public var description: String {
        guard Self.shouldDebug == true else { return label }
        return debugDescription
    }
    
    nonisolated public var debugDescription: String {
        let metadata = self.property?.metadata as? Schema.Relationship
        let enclosing = self.property?.enclosing as? Schema.Relationship
        let mainRows: [(String, String)] = [
            ("* type", type.map { "\($0)" } ?? "nil"),
            ("* kind", kind?.rawValue ?? "nil"),
            ("* label", label),
            ("* alias", alias ?? "nil"),
            ("* entity", entity?.name ?? "nil"),
            ("* property", property?.name ?? "nil")
        ]
        let metadataRows: [(String, String)] = [
            ("* name (metadata)", property?.metadata.name ?? "nil"),
            ("* inverseName", metadata?.inverseName ?? "nil"),
            ("* isRelationship", String(property?.metadata is Schema.Relationship)),
            ("* isToOneRelationship", String(metadata?.isToOneRelationship ?? false)),
            ("* keyPath", debugPropertyMetadataKeyPath),
            ("* inverseKeyPath", debugPropertyMetadataInverseKeyPath)
        ]
        let enclosingRows: [(String, String)] = [
            ("* name (enclosing)", property?.enclosing?.name ?? "nil"),
            ("* inverseName", enclosing?.inverseName ?? "nil"),
            ("* isRelationship", String(property?.enclosing is Schema.Relationship)),
            ("* isToOneRelationship", String(enclosing?.isToOneRelationship ?? false)),
            ("* keyPath", debugPropertyEnclosingKeyPath),
            ("* inverseKeyPath", debugPropertyEnclosingInverseKeyPath)
        ]
        let topKeyWidth = mainRows.map { $0.0.count }.max() ?? 0
        let topValueWidth = mainRows.map { $0.1.count }.max() ?? 0
        let metadataKeyWidth = metadataRows.map { $0.0.count }.max() ?? 0
        let metadataValueWidth = metadataRows.map { $0.1.count }.max() ?? 0
        let enclosingKeyWidth = enclosingRows.map { $0.0.count }.max() ?? 0
        let enclosingValueWidth = enclosingRows.map { $0.1.count }.max() ?? 0
        let rowCount = max(mainRows.count, metadataRows.count, enclosingRows.count)
        var lines: [String] = []
        for i in 0..<rowCount {
            let _0 = i < mainRows.count ? mainRows[i] : ("", "")
            let _1 = i < metadataRows.count ? metadataRows[i] : ("", "")
            let _2 = i < enclosingRows.count ? enclosingRows[i] : ("", "")
            let _0_Key = _0.0.padding(toLength: topKeyWidth, withPad: " ", startingAt: 0)
            let _0_Value = _0.1.padding(toLength: topValueWidth, withPad: " ", startingAt: 0)
            let _1_Key = _1.0.padding(toLength: metadataKeyWidth, withPad: " ", startingAt: 0)
            let _1_Value = _1.1.padding(toLength: metadataValueWidth, withPad: " ", startingAt: 0)
            let _2_Key = _2.0.padding(toLength: enclosingKeyWidth, withPad: " ", startingAt: 0)
            let _2_Value = _2.1.padding(toLength: enclosingValueWidth, withPad: " ", startingAt: 0)
            let row = "    \(_0_Key) : \(_0_Value)   | \(_1_Key) : \(_1_Value)   | \(_2_Key) : \(_2_Value)"
            lines.append(row)
        }
        let contentWidth = lines.map { $0.count }.max() ?? 60
        let headerText = " FRAGMENT \(id) (\(tag)) "
        let prefix = "---"
        let suffix = String(
            repeating: "-",
            count: max(0, contentWidth - (prefix.count + headerText.count))
        )
        let headerLine = prefix + headerText + suffix
        let bottomLine = String(repeating: "-", count: contentWidth)
        return """
            \n
            \(headerLine)
            \(lines.joined(separator: "\n"))
                [SQL CLAUSE] Bindings: \(bindings.map { "\($0)" }.joined(separator: ", "))
                \(clause)
            \(bottomLine)
            """
    }
}

extension SQLPredicateFragment {
    nonisolated internal static var invalid: SQLPredicateFragment {
        .init(clause: "[INVALID]", bindings: [])
    }
    
    nonisolated internal static func invalid(_ messages: Any...) -> SQLPredicateFragment {
        let combined = messages.map(String.init(describing:)).joined(separator: ", ")
        logger.error("Fragment: \(combined)")
        return .init(clause: "[" + String(describing: messages[0]) + "]", bindings: [])
    }
    
    nonisolated internal func invalid(_ messages: Any...) -> SQLPredicateFragment {
        Self.invalid(messages, self.description)
    }
}
