//
//  InheritanceTests.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreKit
import Foundation
import SwiftData
import Testing

@available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
fileprivate struct InheritanceSchema: VersionedSchema {
    static let versionIdentifier: Schema.Version = .init(0, 0, 0)
    static let models: [any PersistentModel.Type] = [Entity.self, Person.self]
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    @Model class Entity {
        var id: String
        var type: String
        
        init(id: String, type: String) {
            self.id = id
            self.type = type
        }
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    @Model class Person: Entity {
        var name: String
        
        init(id: String, type: String, name: String) {
            self.name = name
            super.init(id: id, type: type)
        }
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    fileprivate static func seed(into modelContext: ModelContext) throws {
        let entity = Entity(id: "entity", type: "general")
        let person1 = Person(id: "person-0", type: "general", name: "Anferne Pineda")
        let person2 = Person(id: "person-1", type: "other", name: "Asymbas")
        modelContext.insert(entity)
        modelContext.insert(person1)
        modelContext.insert(person2)
        try modelContext.save()
    }
}

@Suite("Inheritance", .serialized)
struct InheritanceTests {
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    fileprivate typealias Entity = InheritanceSchema.Entity
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    fileprivate typealias Person = InheritanceSchema.Person
    private let modelContext: ModelContext
    private let modelContainer: ModelContainer
    private let configuration: DatabaseConfiguration
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    init() throws {
        _ = logging
        let schema = Schema([Entity.self, Person.self])
        var configuration = DatabaseConfiguration.transient(
            types: [Entity.self, Person.self],
            schema: schema,
            options: .disableSnapshotCaching
        )
        configuration.configurations[.predicate] = SQLPredicateTranslatorOptions([
            .useDetailedLogging,
            .useVerboseLogging,
            .logAllPredicateExpressions
        ])
        self.configuration = configuration

        self.modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        self.modelContext = ModelContext(modelContainer)
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func fetchAllFromRootType() throws {
        try InheritanceSchema.seed(into: modelContext)
        let models = try modelContext.fetch(FetchDescriptor<Entity>())
        #expect(models.count == 3)
        #expect(Set(models.map(\.id)) == Set(["entity", "person-0", "person-1"]))
        #expect(Set(models.compactMap { ($0 as? Person)?.id }) == Set(["person-0", "person-1"]))
        #expect(Set(models.filter { $0 as? Person == nil }.map(\.id)) == Set(["entity"]))
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func conditionalCastPredicateExpression() throws {
        try InheritanceSchema.seed(into: modelContext)
        let descriptor = FetchDescriptor<Entity>(predicate: #Predicate<Entity> { entity in
            (entity as? Person) != nil
        })
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 2)
        #expect(Set(models.map(\.id)) == Set(["person-0", "person-1"]))
        #expect(models.allSatisfy { $0 is Person })
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func typeCheckPredicateExpression() throws {
        try InheritanceSchema.seed(into: modelContext)
        let descriptor = FetchDescriptor<Entity>(predicate: #Predicate<Entity> { entity in
            entity is Person
        })
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 2)
        #expect(Set(models.map(\.id)) == Set(["person-0", "person-1"]))
        #expect(models.allSatisfy { $0 is Person })
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func typeCheckMatchesConditionalCast() throws {
        try InheritanceSchema.seed(into: modelContext)
        let typeCheckDescriptor = FetchDescriptor<Entity>(predicate: #Predicate<Entity> { entity in
            entity is Person
        })
        let conditionalCastDescriptor = FetchDescriptor<Entity>(predicate: #Predicate<Entity> { entity in
            (entity as? Person) != nil
        })
        let typeCheckModels = try modelContext.fetch(typeCheckDescriptor)
        let conditionalCastModels = try modelContext.fetch(conditionalCastDescriptor)
        #expect(Set(typeCheckModels.map(\.id)) == Set(["person-0", "person-1"]))
        #expect(Set(conditionalCastModels.map(\.id)) == Set(["person-0", "person-1"]))
        #expect(Set(typeCheckModels.map(\.persistentModelID)) == Set(conditionalCastModels.map(\.persistentModelID)))
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func subclassPredicateByOwnProperty() throws {
        try InheritanceSchema.seed(into: modelContext)
        let descriptor = FetchDescriptor<Person>(predicate: #Predicate<Person> { person in
            person.name == "Anferne Pineda"
        })
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 1)
        #expect(models[0].name == "Anferne Pineda")
        #expect(models[0].id == "person-0")
        #expect(models[0].type == "general")
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func subclassPredicateByInheritedProperty() throws {
        try InheritanceSchema.seed(into: modelContext)
        let idDescriptor = FetchDescriptor<Person>(predicate: #Predicate<Person> { person in
            person.id == "person-1"
        })
        let testDescriptor = FetchDescriptor<Person>(predicate: #Predicate<Person> { person in
            person.type == "general"
        })
        let idModels = try modelContext.fetch(idDescriptor)
        let typeModels = try modelContext.fetch(testDescriptor)
        #expect(idModels.count == 1)
        #expect(idModels[0].name == "Asymbas")
        #expect(idModels[0].id == "person-1")
        #expect(idModels[0].type == "other")
        #expect(typeModels.count == 1)
        #expect(typeModels[0].name == "Anferne Pineda")
        #expect(typeModels[0].id == "person-0")
        #expect(typeModels[0].type == "general")
    }
}
