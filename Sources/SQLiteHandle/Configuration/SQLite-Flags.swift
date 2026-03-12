//
//  SQLite-Flags.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import SQLite3

extension SQLite {
    public struct Flags: Equatable, Hashable, OptionSet, Sendable {
        public typealias RawValue = Int32
        nonisolated public let rawValue: RawValue
        
        nonisolated public init(rawValue: RawValue) {
            self.rawValue = rawValue
        }
        
        nonisolated public init(sqlite rawValue: RawValue) {
            self.rawValue = rawValue
        }
        
        nonisolated public static func combine(_ options: [Self]) -> RawValue {
            options.reduce(0) { $0 | $1.rawValue }
        }
    }
}

extension SQLite.Flags: CaseIterable {
    nonisolated public static var readOnly: Self { .init(rawValue: SQLITE_OPEN_READONLY) }
    nonisolated public static var readWrite: Self { .init(rawValue: SQLITE_OPEN_READWRITE) }
    nonisolated public static var create: Self { .init(rawValue: SQLITE_OPEN_CREATE) }
    nonisolated public static var uri: Self { .init(rawValue: SQLITE_OPEN_URI) }
    nonisolated public static var memory: Self { .init(rawValue: SQLITE_OPEN_MEMORY) }
    nonisolated public static var noMutex: Self { .init(rawValue: SQLITE_OPEN_NOMUTEX) }
    nonisolated public static var fullMutex: Self { .init(rawValue: SQLITE_OPEN_FULLMUTEX) }
    nonisolated public static var sharedCache: Self { .init(rawValue: SQLITE_OPEN_SHAREDCACHE) }
    nonisolated public static var privateCache: Self { .init(rawValue: SQLITE_OPEN_PRIVATECACHE) }
    nonisolated public static var noFollow: Self { .init(rawValue: SQLITE_OPEN_NOFOLLOW) }
    
    nonisolated public static var allCases: Set<Self> {
        [
            .readOnly,
            .readWrite,
            .create,
            .uri,
            .memory,
            .noMutex,
            .fullMutex,
            .sharedCache,
            .privateCache,
            .noFollow
        ]
    }
}
