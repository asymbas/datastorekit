//
//  PredicateExpressions+SQLHistoryExpression.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreSupport
internal import Foundation

#if swift(>=6.2)
internal import SwiftData
#else
@preconcurrency internal import SwiftData
#endif

nonisolated private func isNilLiteralValue(_ value: Any) -> Bool {
    let mirror = Mirror(reflecting: value)
    guard mirror.displayStyle == .optional else { return false }
    return mirror.children.isEmpty
}

extension PredicateExpressions.Value: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        if let value = self.value as? Bool {
            return .boolean(value)
        }
        if isNilLiteralValue(self.value) {
            return .literal(nil, isNull: true)
        }
        return .literal(sendable(cast: self.value), isNull: false)
    }
}

extension PredicateExpressions.Variable: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        .scope
    }
}

extension PredicateExpressions.KeyPath: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let root = context.fragment(for: self.root)
        guard root.isScope else {
            return .untranslatable
        }
        guard let column = context.column(for: self.keyPath) else {
            return .untranslatable
        }
        return .column(column)
    }
}

extension PredicateExpressions.ForcedUnwrap: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.fragment(for: self.inner)
    }
}

extension PredicateExpressions.NilLiteral: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        .literal(nil, isNull: true)
    }
}

extension PredicateExpressions.Equal: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = context.fragment(for: self.lhs)
        let rhs = context.fragment(for: self.rhs)
        return context.equality(lhs, rhs, negated: false)
    }
}

extension PredicateExpressions.NotEqual: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = context.fragment(for: self.lhs)
        let rhs = context.fragment(for: self.rhs)
        return context.equality(lhs, rhs, negated: true)
    }
}

extension PredicateExpressions.Comparison: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = context.fragment(for: self.lhs)
        let rhs = context.fragment(for: self.rhs)
        let resolved: HistoryComparisonOperator
        switch self.op {
        case .lessThan: resolved = .lessThan
        case .lessThanOrEqual: resolved = .lessThanOrEqual
        case .greaterThan: resolved = .greaterThan
        case .greaterThanOrEqual: resolved = .greaterThanOrEqual
        @unknown default: return .untranslatable
        }
        return context.comparison(lhs, rhs, op: resolved)
    }
}

extension PredicateExpressions.Conjunction: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = context.fragment(for: self.lhs)
        let rhs = context.fragment(for: self.rhs)
        return .init(
            clause: "(\(lhs.clause) AND \(rhs.clause))",
            bindings: lhs.bindings + rhs.bindings,
            interval: lhs.interval.intersection(rhs.interval),
            isComplete: lhs.isComplete && rhs.isComplete
        )
    }
}

extension PredicateExpressions.Disjunction: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = context.fragment(for: self.lhs)
        let rhs = context.fragment(for: self.rhs)
        return .init(
            clause: "(\(lhs.clause) OR \(rhs.clause))",
            bindings: lhs.bindings + rhs.bindings,
            interval: lhs.interval.union(rhs.interval),
            isComplete: lhs.isComplete && rhs.isComplete
        )
    }
}

extension PredicateExpressions.Negation: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let wrapped = context.fragment(for: self.wrapped)
        return .init(
            clause: "(NOT \(wrapped.clause))",
            bindings: wrapped.bindings,
            interval: .unbounded,
            isComplete: wrapped.isComplete
        )
    }
}

extension PredicateExpressions.SequenceContains: SQLHistoryExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let sequence = context.fragment(for: self.sequence)
        let element = context.fragment(for: self.element)
        return context.membership(sequence: sequence, element: element)
    }
}
