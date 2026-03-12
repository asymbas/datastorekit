//
//  DataStoreCache.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public enum CacheEntryLimit: Equatable, Sendable {
    case bounded(maxCount: Int)
    case unbounded
}

public enum CacheCostLimit: Equatable, Sendable {
    case bounded(maxTotal: UInt64)
    case unbounded
}

public enum CacheEvictionPolicy: Equatable, Sendable {
    case leastRecentlyUsed
    case firstInFirstOut
    case leastFrequentlyUsed
}

public enum CacheExpiry: Equatable, Sendable {
    case none
    case expireAfterWrite(seconds: UInt64)
    case expireAfterAccess(seconds: UInt64)
}

public enum CacheValidationPolicy: Equatable, Sendable {
    case none
    case globalGeneration
    case entityGeneration
}

public struct CacheLayerPolicy: Equatable, Sendable {
    nonisolated public let limit: CacheEntryLimit
    nonisolated public let eviction: CacheEvictionPolicy
    nonisolated public let expiry: CacheExpiry
    nonisolated public let validation: CacheValidationPolicy
    nonisolated public let costLimit: CacheCostLimit
    
    nonisolated public init(
        limit: CacheEntryLimit,
        eviction: CacheEvictionPolicy = .leastRecentlyUsed,
        expiry: CacheExpiry = .none,
        validation: CacheValidationPolicy = .entityGeneration,
        costLimit: CacheCostLimit = .unbounded
    ) {
        self.limit = limit
        self.eviction = eviction
        self.expiry = expiry
        self.validation = validation
        self.costLimit = costLimit
    }
}

public struct CachePolicy: Equatable, Sendable {
    nonisolated public static var `default`: Self { .init() }
    
    nonisolated public static var unbounded: Self {
        .init(
            predicateResults: .init(
                limit: .unbounded
            )
        )
    }
    
    nonisolated public var predicateResults: CacheLayerPolicy
    
    nonisolated public init(
        predicateResults: CacheLayerPolicy = .init(
            limit: .bounded(maxCount: 512),
            eviction: .leastRecentlyUsed,
            expiry: .none,
            validation: .entityGeneration,
            costLimit: .unbounded
        )
    ) {
        self.predicateResults = predicateResults
    }
}
