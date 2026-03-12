//
//  DataStoreObservable.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation
import Observation

public protocol DataStoreObservable: AnyObject, Observable, Sendable {
    nonisolated var onTransactionFailure: @Sendable ([ConstraintViolation]) -> Void { get }
    @MainActor var translations: [SQLPredicateTranslation] { get set }
    @MainActor func resolveTranslation(_ translation: SQLPredicateTranslation)
}

extension DataStoreObservable {
    nonisolated public var onTransactionFailure: @Sendable ([ConstraintViolation]) -> Void {
        { _ in }
    }
    
    @MainActor public func resolveTranslation(_ translation: SQLPredicateTranslation) {
        if let index = self.translations.lastIndex(where: { $0.id == translation.id }) {
            self.translations[index] = translation
        } else {
            translations.append(translation)
        }
    }
}

extension DataStoreObservable {
    nonisolated package func insertPredicateTreeNode(
        _ translatorID: UUID,
        predicateDescription: String? = nil,
        predicateHash: Int? = nil,
        key: PredicateExpressions.VariableID? = nil,
        expression: Any.Type? = nil,
        title: String = "",
        content: String...,
        level: Int = 0,
        isComplete: Bool = true
    ) {
        Task { @MainActor in
            var translations = self.translations
            if translations.lastIndex(where: { $0.id == translatorID }) == nil {
                translations.append(.init(
                    id: translatorID,
                    predicateDescription: predicateDescription,
                    predicateHash: predicateHash
                ))
            }
            guard let index = translations.lastIndex(where: { $0.id == translatorID }) else {
                self.translations = translations
                return
            }
            if let predicateDescription {
                translations[index].predicateDescription = predicateDescription
            }
            if let predicateHash {
                translations[index].predicateHash = predicateHash
            }
            translations[index].tree.path.append(.init(
                path: [],
                key: key,
                expression: expression,
                title: title,
                content: content,
                level: level,
                isComplete: isComplete
            ))
            if translations.count > 1000 {
                translations.removeFirst(translations.count - 1000)
            }
            self.translations = translations
        }
    }
}
