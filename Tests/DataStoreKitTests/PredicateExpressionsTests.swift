//
//  PredicateExpressionsTests.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreKit
import Foundation
import Logging
import Testing
import SwiftData

@Suite("PredicateExpressions")
struct PredicateExpressionsTests {
    private let schema: Schema
    private let configuration: DatabaseConfiguration
    private let modelContainer: ModelContainer
    
    init() throws {
        _ = logging
        self.schema = Schema([Base.self])
        var configuration = DatabaseConfiguration.transient(
            types: [Base.self],
            schema: schema,
            options: .disableSnapshotCaching
        )
        configuration.configurations[.predicate] = SQLPredicateTranslatorOptions([
            .generateSQLStatement,
            .useDetailedLogging,
            .useVerboseLogging,
            .logAllPredicateExpressions
        ])
        self.configuration = configuration
        self.modelContainer = try ModelContainer(for: schema, configurations: [configuration])
    }
    
    @Model class Base {
        @Attribute(.unique) var id: String
        
        init(id: String) {
            self.id = id
        }
    }
    
    private func seed(into modelContext: ModelContext, models: [any PersistentModel]) throws {
        for model in models {
            modelContext.insert(model)
        }
        try modelContext.save()
    }
    
    @Test("PredicateExpressions.Value")
    func value() async throws {
        let descriptor = FetchDescriptor(predicate: #Predicate<Base> { _ in true })
        var translator = SQLPredicateTranslator<Base>(configuration: configuration)
        let translation = try translator.translate(descriptor)
        #expect(
            translation.statement.bindings.contains(where: { $0 is SQLValue && $0 as! SQLValue == .true }),
            "Bindings"
        )
    }
    
    @Test("PredicateExpressions.Equal")
    func variable() async throws {
        let id = UUID().uuidString
        let modelContext = ModelContext(modelContainer)
        try seed(into: modelContext, models: [Base(id: id)])
        let descriptor = FetchDescriptor(predicate: #Predicate<Base> {
            $0.id == id
        })
        var translator = SQLPredicateTranslator<Base>.init(schema: schema)
        let translation = try translator.translate(descriptor)
        #expect(translation.statement.bindings.contains(where: { ($0 as? SQLValue) == SQLValue(any: id) }))
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 1)
    }
}
