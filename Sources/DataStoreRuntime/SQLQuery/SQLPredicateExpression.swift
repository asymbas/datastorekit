//
//  SQLPredicateExpression.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation
import SwiftData

internal protocol SQLPredicateExpression {
    typealias Context = SQLPredicateTranslator
    typealias Fragment = SQLPredicateFragment
    nonisolated func evaluate<T>(_ context: inout Context<T>) -> Fragment
}

extension SQLPredicateExpression {
    nonisolated fileprivate static var fullTypeLabel: String {
        String(describing: Self.self)
    }
    
    nonisolated fileprivate static var baseTypeLabel: String {
        let raw = String(describing: Self.self)
        if let genericStart = raw.firstIndex(of: "<") {
            return String(raw[..<genericStart])
        }
        return raw
    }
    
    nonisolated internal
    func debugVariableIDs(_ values: [(String, PredicateExpressions.VariableID?)]) -> String {
        values.map { label, value in
            let string = String(describing: value)
            let id = string.firstMatch(of: /VariableID\(id: (\d+)\)/)?.output.1 ?? "NULL"
            return label.isEmpty ? "\(id)" : "\(label) \(id)"
        }.joined(separator: ", ")
    }
    
    nonisolated internal
    func debugVariableIDs(_ values: (String, PredicateExpressions.VariableID?)...) -> String {
        debugVariableIDs(values)
    }
    
    // TODO: Pass nested type and use `is` keyword to match case instead of String.
    
    /// Wraps the `evaluate(_:)` method for tracing purposes.
    ///
    /// - Describes the closure's metatype when entering and exiting.
    /// - Handles metadata for debugging the predicate tree.
    nonisolated public func query<T>(_ context: inout Context<T>) -> Fragment {
        let metatype = Self.self as? any PredicateExpression.Type
        let previousTag = context.tag
        let label = Self.baseTypeLabel
        context.tag = label
        defer { context.tag = previousTag }
        if let variable = (self as? PredicateExpressions.Variable<T>) {
            if context.root == nil { context.root = variable.key }
            context.key = variable.key
        }
        #if DEBUG
        let pathStart = context.path.isEmpty ? "" : " (Path: \(debugVariableIDs(context.path.map { ("", $0) })))"
        context.log(/*nil*/ .debug, "ENTERING as \(T.self).self...\(pathStart)")
        if context.shouldLogInformation {
            context.node(atTerminal: false, in: metatype, title: label, content: [Self.fullTypeLabel])
        }
        context.counter += 1
        context.level += 1
        #endif
        var fragment = evaluate(&context)
        #if DEBUG
        context.tag = label
        fragment.tag = label
        fragment.expression = Self.self as? any PredicateExpression.Type
        context.hasher.combine(ObjectIdentifier(Self.self))
        context.level -= 1
        if context.options.contains(.useVerboseLogging), context.minimumLogLevel == .trace {
            print("\(fragment.description)")
        }
        let pathEnd = context.path.isEmpty ?
        "" : " (Path: \(debugVariableIDs(context.path.map { ("", $0) })))"
        context.log(/*nil*/ .debug, "EXITING as \(T.self).self...\(pathEnd) -> \(fragment.description)")
        if context.shouldLogInformation {
            var outputTrace = [
                fragment.clause,
                {
                    let key = "\(fragment.key == nil ? "(NULL_KEY)" : "\(fragment.key!)")"
                    let entity = "\(fragment.entity?.name ?? "(NULL_ENTITY)")"
                    let property = "\(fragment.property?.metadata.name ?? "(NULL_PROPERTY)")"
                    return "\(key)_\(entity).\(property)"
                }()
            ] + (fragment.bindings.isEmpty ? [] : ["Bindings: \(fragment.bindings)"])
            if let key = fragment.key {
                if let joins = context.references[key] {
                    outputTrace = outputTrace + [joins.map(\.description).joined(separator: "\n")]
                }
                if let ctes = context.ctesMap[key] {
                    outputTrace = outputTrace + [ctes.map { "CTE: \($0.sql)" }.joined(separator: "\n")]
                }
                outputTrace = outputTrace + ["Fragment Key: \(key)"]
            }
            context.node(atTerminal: true, in: metatype, title: label, content: outputTrace)
        }
        #endif
        return fragment
    }
}

/// - Constrain `PredicateExpressions` subdomains similarly to `PredicateExpression`.
extension Predicate {
    nonisolated func evaluate<T>(_ context: inout SQLPredicateTranslator<T>)
    -> SQLPredicateFragment? where T: PersistentModel {
        guard let expression = self.expression as? SQLPredicateExpression else {
            return nil
        }
        return expression.query(&context)
    }
}
