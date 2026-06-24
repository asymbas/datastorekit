//
//  HistoryComparisonOperator.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

internal import DataStoreRuntime
internal import Foundation
internal import SQLSupport

nonisolated internal enum HistoryComparisonOperator: Sendable {
    case lessThan
    case lessThanOrEqual
    case greaterThan
    case greaterThanOrEqual
    
    internal var sqlOperator: String {
        switch self {
        case .lessThan: "<"
        case .lessThanOrEqual: "<="
        case .greaterThan: ">"
        case .greaterThanOrEqual: ">="
        }
    }
    
    internal var flipped: Self {
        switch self {
        case .lessThan: .greaterThan
        case .lessThanOrEqual: .greaterThanOrEqual
        case .greaterThan: .lessThan
        case .greaterThanOrEqual: .lessThanOrEqual
        }
    }
}

nonisolated internal enum SQLHistoryColumn: Sendable {
    case timestampDate
    case timestampRaw
    case token
    case author
    case storeIdentifier
    
    internal var columnName: String {
        switch self {
        case .timestampDate, .timestampRaw, .token: HistoryTable.timestamp.rawValue
        case .author: HistoryTable.author.rawValue
        case .storeIdentifier: HistoryTable.storeIdentifier.rawValue
        }
    }
    
    internal var isTimestamp: Bool {
        switch self {
        case .timestampDate, .timestampRaw, .token: true
        case .author, .storeIdentifier: false
        }
    }
    
    internal var producesExactComparison: Bool {
        switch self {
        case .timestampDate: false
        case .timestampRaw, .token, .author, .storeIdentifier: true
        }
    }
    
    internal func bind(_ literal: (any Sendable)?) -> SQLValue? {
        switch self {
        case .timestampDate:
            guard let date = literal as? Date else { return nil }
            return .integer(Int64(date.timeIntervalSince1970 * 1_000_000))
        case .timestampRaw:
            if let value = literal as? Int64 { return .integer(value) }
            if let value = literal as? Int { return .integer(Int64(value)) }
            return nil
        case .token:
            guard let token = literal as? DatabaseHistoryToken else { return nil }
            return .integer(Int64(token.id))
        case .author, .storeIdentifier:
            guard let value = literal as? String else { return nil }
            return .text(value)
        }
    }
    
    internal func microseconds(from literal: (any Sendable)?) -> Int64? {
        switch self {
        case .timestampDate:
            guard let date = literal as? Date else { return nil }
            return Int64(date.timeIntervalSince1970 * 1_000_000)
        case .timestampRaw:
            if let value = literal as? Int64 { return value }
            if let value = literal as? Int { return Int64(value) }
            return nil
        case .token:
            guard let token = literal as? DatabaseHistoryToken else { return nil }
            return Int64(token.id)
        case .author, .storeIdentifier:
            return nil
        }
    }
}

nonisolated internal let historyColumnsByKeyPath: [AnyKeyPath & Sendable: SQLHistoryColumn] = [
    \DatabaseHistoryTransaction.timestamp: .timestampDate,
     \DatabaseHistoryTransaction.transactionIdentifier: .timestampRaw,
     \DatabaseHistoryTransaction.id: .timestampRaw,
     \DatabaseHistoryTransaction.token: .token,
     \DatabaseHistoryTransaction.author: .author,
     \DatabaseHistoryTransaction.storeIdentifier: .storeIdentifier
]
