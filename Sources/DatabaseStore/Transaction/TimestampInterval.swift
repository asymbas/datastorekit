//
//  TimestampInterval.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

nonisolated internal struct TimestampInterval: Sendable {
    internal let lowerBound: Int64?
    internal let upperBound: Int64?
    
    internal static var unbounded: Self {
        .init(lowerBound: nil, upperBound: nil)
    }
    
    internal init(lowerBound: Int64?, upperBound: Int64?) {
        self.lowerBound = lowerBound
        self.upperBound = upperBound
    }
    
    internal func intersection(_ other: Self) -> Self {
        let lower: Int64?
        switch (lowerBound, other.lowerBound) {
        case let (first?, second?): lower = Swift.max(first, second)
        case let (first?, nil): lower = first
        case let (nil, second?): lower = second
        case (nil, nil): lower = nil
        }
        let upper: Int64?
        switch (upperBound, other.upperBound) {
        case let (first?, second?): upper = Swift.min(first, second)
        case let (first?, nil): upper = first
        case let (nil, second?): upper = second
        case (nil, nil): upper = nil
        }
        return .init(lowerBound: lower, upperBound: upper)
    }
    
    internal func union(_ other: Self) -> Self {
        let lower: Int64?
        switch (lowerBound, other.lowerBound) {
        case let (first?, second?): lower = Swift.min(first, second)
        default: lower = nil
        }
        let upper: Int64?
        switch (upperBound, other.upperBound) {
        case let (first?, second?): upper = Swift.max(first, second)
        default: upper = nil
        }
        return .init(lowerBound: lower, upperBound: upper)
    }
    
    internal func overlaps(start: Int64, endExclusive: Int64) -> Bool {
        if let upperBound, upperBound < start { return false }
        if let lowerBound, lowerBound >= endExclusive { return false }
        return true
    }
}
