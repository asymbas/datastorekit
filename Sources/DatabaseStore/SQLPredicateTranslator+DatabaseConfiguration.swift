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
        if configuration.options.contains(.useVerboseLogging) {
            options.insert(.useVerboseLogging)
        }
        let translator = SQLPredicateTranslator(
            schema: configuration.schema.unsafelyUnwrapped,
            attachment: configuration.attachment as? DataStoreObservable,
            options: options,
            minimumLogLevel: DataStoreDebugging.mode == .trace ? .trace : .notice,
            tags: nil
        )
        self = translator
    }
}
