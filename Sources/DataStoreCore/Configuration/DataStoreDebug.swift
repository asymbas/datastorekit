//
//  DataStoreDebug.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import Synchronization

public enum DataStoreDebugging: UInt8, AtomicRepresentable, Equatable, Hashable, Sendable {
    case `default` = 0
    case trace
    
    nonisolated private static let _mode: Atomic<Self> = .init(.default)
    
    nonisolated public package(set) static var mode: Self {
        get {
            #if DEBUG
            _mode.load(ordering: .relaxed)
            #else
            .default
            #endif
        }
        set {
            #if DEBUG
            _mode.store(newValue, ordering: .sequentiallyConsistent)
            #endif
        }
    }
    
    nonisolated package static
    func execute(body: @autoclosure @escaping @Sendable () throws -> Void) rethrows {
        #if DEBUG
        if Self.mode == .trace { try body() }
        #endif
    }
}
