//
//  SQLPredicateTranslator+DatabaseConfiguration.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreRuntime

extension SQLPredicateTranslator {
    nonisolated public init(configuration: DatabaseConfiguration) {
        var options =
        if let options = configuration.configurations[.predicate],
           let options = options as? SQLPredicateTranslatorOptions {
            options
        } else {
            SQLPredicateTranslatorOptions()
        }
        if !configuration.options.contains(.disablePredicateCaching) {
            options.insert(.isCachingPredicates)
        }
        if !configuration.options.contains(.disableKeyPathVariants) {
            options.insert(.allowKeyPathVariantsForPropertyLookup)
        }
        let translator = SQLPredicateTranslator(
            schema: configuration.schema ?? .init(),
            attachment: configuration.attachment as? DataStoreObservable,
            options: options,
            minimumLogLevel: DataStoreDebugging.mode == .trace ? .trace : .notice,
            tags: nil
        )
        self = translator
        self.evaluateEphemeralProperty = { evaluate in
            guard let registry = configuration.store?.manager.registry(for: evaluate.editingState) else {
                throw Self.Error.cannotEvaluateEphemeralProperties
            }
            let snapshots = registry.step(from: evaluate.entityName) { backingData in
                backingData.compareField(evaluate.value, at: evaluate.propertyIndex)
            }
            return .init(uniqueKeysWithValues: snapshots.map { ($0.persistentIdentifier, $0) })
        }
    }
}
