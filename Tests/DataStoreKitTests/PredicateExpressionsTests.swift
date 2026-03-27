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
import SQLiteStatement
import SwiftData

@Suite("PredicateExpressions")
struct PredicateExpressionsTests {
    private let schema: Schema
    private let configuration: DatabaseConfiguration
    private let modelContainer: ModelContainer
    
    init() throws {
        _ = logging
        let types: [any PersistentModel.Type] = [
            Base.self,
            RawValue.self,
            EntitySQLPassthrough.self
        ]
        self.schema = Schema(types)
        var configuration = DatabaseConfiguration.transient(
            types: types,
            schema: schema,
            options: [.disableSnapshotCaching, .useDetailedLogging, .useVerboseLogging]
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
    
    @Model class RawValue {
        @Attribute(.unique) var id: String
        @Attribute var color: Color
        @Attribute var colors: Set<Color>
        @Attribute var shape: Shape
        @Attribute var shapes: Set<Shape>
        @Attribute var style: Style
        
        init(
            id: String = UUID().uuidString,
            color: Color = .red,
            colors: Set<Color> = [],
            shape: Shape = .rectangle,
            shapes: Set<Shape> = [],
            style: Style = []
        ) {
            self.id = id
            self.color = color
            self.colors = colors
            self.shape = shape
            self.shapes = shapes
            self.style = style
        }
        
        enum Color: String, CaseIterable, Codable, Equatable, Hashable {
            case red
            case green
            case blue
        }
        
        struct Shape: CaseIterable, Codable, Equatable, Hashable, RawRepresentable {
            static let allCases: Set<Self> = [.rectangle, .square, .circle, .triangle]
            static let rectangle: Self = .init(rawValue: 0)
            static let square: Self = .init(rawValue: 1)
            static let circle: Self = .init(rawValue: 2)
            static let triangle: Self = .init(rawValue: 3)
            let rawValue: UInt
            
            init(rawValue: Self.RawValue) {
                self.rawValue = rawValue
            }
        }
        
        struct Style: CaseIterable, Codable, OptionSet {
            static let allCases: [Self] = [.fill, .stroke]
            static let fill: Self = .init(rawValue: 0)
            static let stroke: Self = .init(rawValue: 1)
            let rawValue: Int32
            
            init(rawValue: Self.RawValue) {
                self.rawValue = rawValue
            }
        }
    }
    
    @Model class EntitySQLPassthrough: SQLPassthrough {
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
        var translator = SQLPredicateTranslator<Base>(schema: schema)
        let translation = try translator.translate(descriptor)
        #expect(translation.statement.bindings.contains(where: { ($0 as? SQLValue) == SQLValue(any: id) }))
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 1)
    }
    
    @Test("Use struct and enum with constant or case raw values")
    func rawValue() async throws {
        let testStructShape = RawValue.Shape.allCases.randomElement()!
        let testEnumColor = RawValue.Color.allCases.randomElement()!
        let testStructShapes = Array(RawValue.Shape.allCases)
            .shuffled()
            .prefix(Int.random(in: 0...RawValue.Shape.allCases.count))
        let testEnumColors = Array(RawValue.Color.allCases)
            .shuffled()
            .prefix(Int.random(in: 0...RawValue.Color.allCases.count))
        let modelContext = ModelContext(modelContainer)
        try seed(into: modelContext, models: [
            RawValue(id: "test-struct", shape: testStructShape, shapes: Set(testStructShapes)),
            RawValue(id: "test-enum", color: testEnumColor, colors: Set(testEnumColors))
        ])
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-struct" && $0.shape == testStructShape
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
            #expect(models.count == 1)
        }
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-struct" && $0.shape.rawValue == testStructShape.rawValue
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
            #expect(models.count == 1)
        }
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-struct" && $0.shapes.contains(testStructShape)
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
        }
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-struct" && $0.shapes.contains(where: { shape in
                    shape.rawValue == testStructShape.rawValue
                })
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
        }
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-enum" && $0.color == testEnumColor
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
            #expect(models.count == 1)
        }
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-enum" && $0.color.rawValue == testEnumColor.rawValue
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
            #expect(models.count == 1)
        }
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-enum" && $0.colors.contains(testEnumColor)
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
        }
        do {
            let descriptor = FetchDescriptor(predicate: #Predicate<RawValue> {
                $0.id == "test-enum" && $0.colors.contains(where: { shape in
                    shape.rawValue == testEnumColor.rawValue
                })
            })
            var translator = SQLPredicateTranslator<RawValue>(schema: schema)
            let translation = try translator.translate(descriptor)
            let models = try modelContext.fetch(descriptor)
        }
    }
    
    @Test("SQLPassthrough")
    func sqlPassthrough() async throws {
        let id = UUID().uuidString
        let modelContext = ModelContext(modelContainer)
        try seed(into: modelContext, models: [EntitySQLPassthrough(id: id)])
        let sql = SQL {
            "\"id\" = '\(id)'"
        }
        let descriptor = FetchDescriptor(predicate: #Predicate<EntitySQLPassthrough> {
            $0.sql == sql
        })
        var translator = SQLPredicateTranslator<EntitySQLPassthrough>(schema: schema)
        let translation = try translator.translate(descriptor)
        #expect(translation.statement.sql.contains(id))
        #expect(translation.statement.bindings.isEmpty)
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 1)
    }
}
