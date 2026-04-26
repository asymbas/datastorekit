//
//  SQLPredicateTranslator+DatabaseConfiguration.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
public import DataStoreRuntime

extension SQLPredicateTranslator {
    nonisolated public init(configuration: DatabaseConfiguration) {
        var options =
        if let options = configuration.configurations[.predicate] {
            options as! SQLPredicateTranslatorOptions
        } else {
            SQLPredicateTranslatorOptions()
        }
        #if DEBUG
        let translator = SQLPredicateTranslator(
            schema: configuration.schema ?? .init(),
            attachment: configuration.attachment as? DataStoreObservable,
            options: options,
            minimumLogLevel: DataStoreDebugging.mode == .trace ? .trace : .notice,
            tags: nil
        )
        #else
        let translator = SQLPredicateTranslator(
            schema: configuration.schema ?? .init(),
            attachment: nil,
            options: options,
            minimumLogLevel: .warning,
            tags: []
        )
        #endif
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
