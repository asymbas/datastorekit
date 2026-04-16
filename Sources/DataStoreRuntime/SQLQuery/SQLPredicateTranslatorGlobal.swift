//
//  SQLPredicateTranslatorGlobal.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Synchronization

nonisolated private let _tags: AtomicLazyReference<Storage> = .init()

private final class Storage: Sendable {
    nonisolated internal let expressions: Set<String>
    
    nonisolated init(expressions: Set<String>) {
        self.expressions = expressions
    }
}

extension SQLPredicateTranslatorOptions {
    nonisolated internal static var tags: Set<String>? {
        _tags.load()?.expressions
    }
    
    nonisolated public static func tags(_ expressions: String...) {
        _ = _tags.storeIfNil(.init(expressions: .init(expressions)))
    }
}
