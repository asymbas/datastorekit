//
//  SQLHistoryTranslator.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreSupport
private import SQLSupport
internal import Foundation

#if swift(>=6.2)
internal import SwiftData
#else
@preconcurrency internal import SwiftData
#endif

nonisolated internal struct SQLHistoryTranslator<T>: ~Copyable, Sendable where T: HistoryTransaction {
    internal init() {}
    
    internal mutating func translate(_ descriptor: HistoryDescriptor<T>) throws -> SQLHistoryTranslation {
        let sortOrder: SortOrder
        if #available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *) {
            sortOrder = Self.resolveSortOrder(descriptor.sortBy) ?? .forward
        } else {
            sortOrder = .forward
        }
        let fetchLimit = Self.resolveFetchLimit(descriptor)
        guard let predicate = descriptor.predicate else {
            return .init(
                whereClause: nil,
                bindings: [],
                hasPredicate: false,
                evaluatesPredicateInSQL: false,
                lowerBoundTimestamp: nil,
                upperBoundTimestamp: nil,
                sortOrder: sortOrder,
                fetchLimit: fetchLimit
            )
        }
        guard let expression = predicate.expression as? any SQLHistoryExpression else {
            return .init(
                whereClause: nil,
                bindings: [],
                hasPredicate: true,
                evaluatesPredicateInSQL: false,
                lowerBoundTimestamp: nil,
                upperBoundTimestamp: nil,
                sortOrder: sortOrder,
                fetchLimit: fetchLimit
            )
        }
        let fragment = expression.query(&self)
        return .init(
            whereClause: fragment.isComplete ? fragment.clause : nil,
            bindings: fragment.isComplete ? fragment.bindings : [],
            hasPredicate: true,
            evaluatesPredicateInSQL: fragment.isComplete,
            lowerBoundTimestamp: fragment.interval.lowerBound,
            upperBoundTimestamp: fragment.interval.upperBound,
            sortOrder: sortOrder,
            fetchLimit: fetchLimit
        )
    }
}

nonisolated extension SQLHistoryTranslator {
    internal mutating func fragment(for child: Any) -> SQLHistoryFragment {
        guard let expression = child as? any SQLHistoryExpression else {
            return .untranslatable
        }
        return expression.query(&self)
    }
    
    internal func column(for keyPath: AnyKeyPath & Sendable) -> SQLHistoryColumn? {
        historyColumnsByKeyPath[keyPath]
    }
    
    internal func comparison(
        _ lhs: SQLHistoryFragment,
        _ rhs: SQLHistoryFragment,
        op: HistoryComparisonOperator
    ) -> SQLHistoryFragment {
        guard lhs.isComplete, rhs.isComplete else {
            return .untranslatable
        }
        if let column = lhs.column, rhs.isLiteral {
            guard rhs.isNull == false, let bound = column.bind(rhs.literal) else {
                return .untranslatable
            }
            return .predicate(
                clause: "(\(column.columnName) \(op.sqlOperator) ?)",
                bindings: [bound],
                interval: Self.interval(for: column, value: rhs.literal, op: op),
                isComplete: column.producesExactComparison
            )
        }
        if let column = rhs.column, lhs.isLiteral {
            guard lhs.isNull == false, let bound = column.bind(lhs.literal) else {
                return .untranslatable
            }
            return .predicate(
                clause: "(? \(op.sqlOperator) \(column.columnName))",
                bindings: [bound],
                interval: Self.interval(for: column, value: lhs.literal, op: op.flipped),
                isComplete: column.producesExactComparison
            )
        }
        if let lhsColumn = lhs.column, let rhsColumn = rhs.column {
            return .predicate(
                clause: "(\(lhsColumn.columnName) \(op.sqlOperator) \(rhsColumn.columnName))",
                bindings: [],
                interval: .unbounded,
                isComplete: true
            )
        }
        if lhs.isLiteral, rhs.isLiteral {
            guard let lhsValue = encodeGenericValue(lhs), let rhsValue = encodeGenericValue(rhs) else {
                return .untranslatable
            }
            return .predicate(
                clause: "(? \(op.sqlOperator) ?)",
                bindings: [lhsValue, rhsValue],
                interval: .unbounded,
                isComplete: true
            )
        }
        return .untranslatable
    }
    
    internal func equality(
        _ lhs: SQLHistoryFragment,
        _ rhs: SQLHistoryFragment,
        negated: Bool
    ) -> SQLHistoryFragment {
        guard lhs.isComplete, rhs.isComplete else {
            return .untranslatable
        }
        if let column = lhs.column, rhs.isLiteral {
            return columnEquality(column: column, literal: rhs, negated: negated)
        }
        if let column = rhs.column, lhs.isLiteral {
            return columnEquality(column: column, literal: lhs, negated: negated)
        }
        if let lhsColumn = lhs.column, let rhsColumn = rhs.column {
            let sqlOperator = negated ? "<>" : "="
            return .predicate(
                clause: "(\(lhsColumn.columnName) \(sqlOperator) \(rhsColumn.columnName))",
                bindings: [],
                interval: .unbounded,
                isComplete: true
            )
        }
        if lhs.isLiteral, rhs.isLiteral {
            guard let lhsValue = encodeGenericValue(lhs), let rhsValue = encodeGenericValue(rhs) else {
                return .untranslatable
            }
            let sqlOperator = negated ? "<>" : "="
            return .predicate(
                clause: "(? \(sqlOperator) ?)",
                bindings: [lhsValue, rhsValue],
                interval: .unbounded,
                isComplete: true
            )
        }
        return .untranslatable
    }
    
    internal func membership(sequence: SQLHistoryFragment, element: SQLHistoryFragment) -> SQLHistoryFragment {
        guard sequence.isComplete, element.isComplete else {
            return .untranslatable
        }
        guard let column = element.column, sequence.isLiteral, let literal = sequence.literal else {
            return .untranslatable
        }
        let mirror = Mirror(reflecting: literal)
        guard mirror.displayStyle == .collection else {
            return .untranslatable
        }
        let values = mirror.children.map(\.value)
        guard values.isEmpty == false else {
            return .boolean(false)
        }
        var bindings: [any Sendable] = []
        bindings.reserveCapacity(values.count)
        var interval: TimestampInterval?
        for value in values {
            let candidate = value
            guard let bound = column.bind(sendable(cast: candidate)) else {
                return .untranslatable
            }
            bindings.append(bound)
            if column.isTimestamp, let microseconds = column.microseconds(from: sendable(cast: candidate)) {
                let point = TimestampInterval(lowerBound: microseconds, upperBound: microseconds)
                interval = interval.map { $0.union(point) } ?? point
            }
        }
        let placeholders = Array(repeating: "?", count: bindings.count).joined(separator: ", ")
        return .predicate(
            clause: "(\(column.columnName) IN (\(placeholders)))",
            bindings: bindings,
            interval: interval ?? .unbounded,
            isComplete: column.producesExactComparison
        )
    }
    
    private func columnEquality(
        column: SQLHistoryColumn,
        literal: SQLHistoryFragment,
        negated: Bool
    ) -> SQLHistoryFragment {
        if literal.isNull {
            let clause = negated ? "(\(column.columnName) IS NOT NULL)" : "(\(column.columnName) IS NULL)"
            return .predicate(clause: clause, bindings: [], interval: .unbounded, isComplete: true)
        }
        guard let bound = column.bind(literal.literal) else {
            return .untranslatable
        }
        let sqlOperator = negated ? "<>" : "="
        let interval: TimestampInterval
        if negated == false, column.isTimestamp, let microseconds = column.microseconds(from: literal.literal) {
            interval = .init(lowerBound: microseconds, upperBound: microseconds)
        } else {
            interval = .unbounded
        }
        return .predicate(
            clause: "(\(column.columnName) \(sqlOperator) ?)",
            bindings: [bound],
            interval: interval,
            isComplete: column.producesExactComparison
        )
    }
    
    private func encodeGenericValue(_ fragment: SQLHistoryFragment) -> SQLValue? {
        if fragment.isNull { return SQLValue.null }
        guard let literal = fragment.literal else { return SQLValue.null }
        return SQLValue(any: literal)
    }
    
    internal static func interval(
        for column: SQLHistoryColumn,
        value: (any Sendable)?,
        op: HistoryComparisonOperator
    ) -> TimestampInterval {
        guard column.isTimestamp, let microseconds = column.microseconds(from: value) else {
            return .unbounded
        }
        switch op {
        case .lessThan, .lessThanOrEqual: return .init(lowerBound: nil, upperBound: microseconds)
        case .greaterThan, .greaterThanOrEqual: return .init(lowerBound: microseconds, upperBound: nil)
        }
    }
    
    private static func resolveSortOrder(_ descriptors: [SortDescriptor<T>]) -> SortOrder? {
        for descriptor in descriptors {
            guard let keyPath: AnyKeyPath & Sendable = sendable(cast: descriptor.keyPath as Any),
                  let column = historyColumnsByKeyPath[keyPath] else {
                continue
            }
            if column.isTimestamp { return descriptor.order }
        }
        return nil
    }
    
    private static func resolveFetchLimit(_ descriptor: HistoryDescriptor<T>) -> Int? {
        let limit = descriptor.fetchLimit
        guard limit > 0 else { return nil }
        return limit > UInt64(Int.max) ? Int.max : Int(limit)
    }
}

nonisolated internal struct SQLHistoryTranslation: Sendable {
    internal let whereClause: String?
    internal let bindings: [any Sendable]
    internal let hasPredicate: Bool
    internal let evaluatesPredicateInSQL: Bool
    internal let lowerBoundTimestamp: Int64?
    internal let upperBoundTimestamp: Int64?
    internal let sortOrder: SortOrder?
    internal let fetchLimit: Int?
    
    internal init(
        whereClause: String?,
        bindings: [any Sendable],
        hasPredicate: Bool,
        evaluatesPredicateInSQL: Bool,
        lowerBoundTimestamp: Int64?,
        upperBoundTimestamp: Int64?,
        sortOrder: SortOrder?,
        fetchLimit: Int?
    ) {
        self.whereClause = whereClause
        self.bindings = bindings
        self.hasPredicate = hasPredicate
        self.evaluatesPredicateInSQL = evaluatesPredicateInSQL
        self.lowerBoundTimestamp = lowerBoundTimestamp
        self.upperBoundTimestamp = upperBoundTimestamp
        self.sortOrder = sortOrder
        self.fetchLimit = fetchLimit
    }
    
    internal func includesArchiveYear(_ year: Int, calendar: Calendar) -> Bool {
        let interval = TimestampInterval(lowerBound: lowerBoundTimestamp, upperBound: upperBoundTimestamp)
        guard let start = calendar.date(from: DateComponents(year: year, month: 1, day: 1)),
              let end = calendar.date(from: DateComponents(year: year + 1, month: 1, day: 1)) else {
            return true
        }
        let startMicroseconds = Int64(start.timeIntervalSince1970 * 1_000_000)
        let endMicroseconds = Int64(end.timeIntervalSince1970 * 1_000_000)
        return interval.overlaps(start: startMicroseconds, endExclusive: endMicroseconds)
    }
}
