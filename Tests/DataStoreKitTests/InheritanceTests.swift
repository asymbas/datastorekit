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

@Suite(.serialized)
struct InheritanceTests {
    private let modelContext: ModelContext
    private let modelContainer: ModelContainer
    private let configuration: DatabaseConfiguration
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    init() throws {
        _ = logging
        let schema = Schema(versionedSchema: Sample.self)
        var configuration = DatabaseConfiguration.transient(
            types: Sample.models,
            schema: schema,
            options: .disableSnapshotCaching
        )
        configuration.configurations[.predicate] = SQLPredicateTranslatorOptions([
            .useDetailedLogging,
            .useVerboseLogging,
            .logAllPredicateExpressions,
            .preferStandardOutput
        ])
        self.configuration = configuration
        self.modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        self.modelContext = ModelContext(modelContainer)
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func fetchAllFromRootType() throws {
        try Sample.seed(into: modelContext)
        let models = try modelContext.fetch(FetchDescriptor<Sample.Entity>())
        #expect(models.count == 3)
        #expect(Set(models.map(\.id)) == Set(["entity", "person-0", "person-1"]))
        #expect(Set(models.compactMap { ($0 as? Sample.Person)?.id }) == Set(["person-0", "person-1"]))
        #expect(Set(models.filter { $0 as? Sample.Person == nil }.map(\.id)) == Set(["entity"]))
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func conditionalCastPredicateExpression() throws {
        try Sample.seed(into: modelContext)
        let descriptor = FetchDescriptor(predicate: #Predicate<Sample.Entity> { entity in
            (entity as? Sample.Person) != nil
        })
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 2)
        #expect(Set(models.map(\.id)) == Set(["person-0", "person-1"]))
        #expect(models.allSatisfy { $0 is Sample.Person })
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func typeCheckPredicateExpression() throws {
        try Sample.seed(into: modelContext)
        let descriptor = FetchDescriptor(predicate: #Predicate<Sample.Entity> { entity in
            entity is Sample.Person
        })
        let models = try modelContext.fetch(descriptor)
        #expect(models.count == 2)
        #expect(Set(models.map(\.id)) == Set(["person-0", "person-1"]))
        #expect(models.allSatisfy { $0 is Sample.Person })
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func typeCheckMatchesConditionalCast() throws {
        try Sample.seed(into: modelContext)
        let typeCheckDescriptor = FetchDescriptor(predicate: #Predicate<Sample.Entity> { entity in
            entity is Sample.Person
        })
        let conditionalCastDescriptor = FetchDescriptor(predicate: #Predicate<Sample.Entity> { entity in
            (entity as? Sample.Person) != nil
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
        try Sample.seed(into: modelContext)
        let descriptor = FetchDescriptor(predicate: #Predicate<Sample.Person> { person in
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
        try Sample.seed(into: modelContext)
        let idDescriptor = FetchDescriptor(predicate: #Predicate<Sample.Person> { person in
            person.id == "person-1"
        })
        let testDescriptor = FetchDescriptor(predicate: #Predicate<Sample.Person> { person in
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
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    struct Sample: VersionedSchema {
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
        static func seed(into modelContext: ModelContext) throws {
            let entity = Entity(id: "entity", type: "general")
            let person1 = Person(id: "person-0", type: "general", name: "Anferne Pineda")
            let person2 = Person(id: "person-1", type: "other", name: "Asymbas")
            modelContext.insert(entity)
            modelContext.insert(person1)
            modelContext.insert(person2)
            try modelContext.save()
        }
    }
}

@Suite(.serialized)
struct InheritanceChainBranchTests {
    private let modelContext: ModelContext
    private let modelContainer: ModelContainer
    private let configuration: DatabaseConfiguration
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    init() throws {
        let schema = Schema(versionedSchema: Sample.self)
        var configuration = DatabaseConfiguration.transient(
            types: Sample.models,
            schema: schema,
            options: .disableSnapshotCaching
        )
        configuration.configurations[.predicate] = SQLPredicateTranslatorOptions([
            .useDetailedLogging,
            .useVerboseLogging,
            .logAllPredicateExpressions,
            .preferStandardOutput
        ])
        self.configuration = configuration
        self.modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        self.modelContext = ModelContext(modelContainer)
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    static func branchedInheritanceTypeNames(for model: Sample.BranchEntity) -> [String] {
        var types: [String] = ["BranchEntity"]
        if model is Sample.AlphaBranch { types.append("AlphaBranch") }
        if model is Sample.BetaBranch { types.append("BetaBranch") }
        if model is Sample.AlphaOneBranch { types.append("AlphaOneBranch") }
        if model is Sample.AlphaTwoBranch { types.append("AlphaTwoBranch") }
        if model is Sample.BetaOneBranch { types.append("BetaOneBranch") }
        if model is Sample.BetaTwoBranch { types.append("BetaTwoBranch") }
        if model is Sample.AlphaOneLeaf { types.append("AlphaOneLeaf") }
        if model is Sample.AlphaTwoLeaf { types.append("AlphaTwoLeaf") }
        if model is Sample.BetaOneLeaf { types.append("BetaOneLeaf") }
        if model is Sample.BetaTwoLeaf { types.append("BetaTwoLeaf") }
        return types
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func branchedInheritanceChecksAllSubtreesAndConcreteTypes() throws {
        try Sample.seed(into: modelContext)
        let models = try modelContext.fetch(FetchDescriptor<Sample.BranchEntity>())
        #expect(models.count == 11)
        let modelsByID = Dictionary(uniqueKeysWithValues: models.map { ($0.id, $0) })
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["root"])) == [
            "BranchEntity"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["alpha"])) == [
            "BranchEntity",
            "AlphaBranch"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["beta"])) == [
            "BranchEntity",
            "BetaBranch"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["alpha-1"])) == [
            "BranchEntity",
            "AlphaBranch",
            "AlphaOneBranch"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["alpha-2"])) == [
            "BranchEntity",
            "AlphaBranch",
            "AlphaTwoBranch"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["beta-1"])) == [
            "BranchEntity",
            "BetaBranch",
            "BetaOneBranch"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["beta-2"])) == [
            "BranchEntity",
            "BetaBranch",
            "BetaTwoBranch"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["alpha-1-leaf"])) == [
            "BranchEntity",
            "AlphaBranch",
            "AlphaOneBranch",
            "AlphaOneLeaf"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["alpha-2-leaf"])) == [
            "BranchEntity",
            "AlphaBranch",
            "AlphaTwoBranch",
            "AlphaTwoLeaf"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["beta-1-leaf"])) == [
            "BranchEntity",
            "BetaBranch",
            "BetaOneBranch",
            "BetaOneLeaf"
        ])
        #expect(Self.branchedInheritanceTypeNames(for: try #require(modelsByID["beta-2-leaf"])) == [
            "BranchEntity",
            "BetaBranch",
            "BetaTwoBranch",
            "BetaTwoLeaf"
        ])
        #expect(try modelContext.fetch(FetchDescriptor<Sample.AlphaBranch>()).count == 5)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.BetaBranch>()).count == 5)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.AlphaOneBranch>()).count == 2)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.AlphaTwoBranch>()).count == 2)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.BetaOneBranch>()).count == 2)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.BetaTwoBranch>()).count == 2)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.AlphaOneLeaf>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.AlphaTwoLeaf>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.BetaOneLeaf>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.BetaTwoLeaf>()).count == 1)
        let alphaDescriptor = FetchDescriptor(predicate: #Predicate<Sample.BranchEntity> { entity in
            entity is Sample.AlphaBranch
        })
        let betaDescriptor = FetchDescriptor(predicate: #Predicate<Sample.BranchEntity> { entity in
            entity is Sample.BetaBranch
        })
        let alphaOneDescriptor = FetchDescriptor(predicate: #Predicate<Sample.BranchEntity> { entity in
            entity is Sample.AlphaOneBranch
        })
        let alphaOneLeafDescriptor = FetchDescriptor(predicate: #Predicate<Sample.BranchEntity> { entity in
            entity is Sample.AlphaOneLeaf
        })
        #expect(try modelContext.fetch(alphaDescriptor).count == 5)
        #expect(try modelContext.fetch(betaDescriptor).count == 5)
        #expect(try modelContext.fetch(alphaOneDescriptor).count == 2)
        #expect(try modelContext.fetch(alphaOneLeafDescriptor).count == 1)
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    struct Sample: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(0, 0, 0)
        static let models: [any PersistentModel.Type] = [
            BranchEntity.self,
            AlphaBranch.self,
            BetaBranch.self,
            AlphaOneBranch.self,
            AlphaTwoBranch.self,
            BetaOneBranch.self,
            BetaTwoBranch.self,
            AlphaOneLeaf.self,
            AlphaTwoLeaf.self,
            BetaOneLeaf.self,
            BetaTwoLeaf.self
        ]
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class BranchEntity {
            var id: String
            init(id: String) { self.id = id }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class AlphaBranch: BranchEntity {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class BetaBranch: BranchEntity {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class AlphaOneBranch: AlphaBranch {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class AlphaTwoBranch: AlphaBranch {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class BetaOneBranch: BetaBranch {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class BetaTwoBranch: BetaBranch {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class AlphaOneLeaf: AlphaOneBranch {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class AlphaTwoLeaf: AlphaTwoBranch {
            override init(id: String) { super.init(id: id) }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class BetaOneLeaf: BetaOneBranch {
            override init(id: String) {
                super.init(id: id)
            }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class BetaTwoLeaf: BetaTwoBranch {
            override init(id: String) {
                super.init(id: id)
            }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        static func seed(into modelContext: ModelContext) throws {
            modelContext.insert(BranchEntity(id: "root"))
            modelContext.insert(AlphaBranch(id: "alpha"))
            modelContext.insert(BetaBranch(id: "beta"))
            modelContext.insert(AlphaOneBranch(id: "alpha-1"))
            modelContext.insert(AlphaTwoBranch(id: "alpha-2"))
            modelContext.insert(BetaOneBranch(id: "beta-1"))
            modelContext.insert(BetaTwoBranch(id: "beta-2"))
            modelContext.insert(AlphaOneLeaf(id: "alpha-1-leaf"))
            modelContext.insert(AlphaTwoLeaf(id: "alpha-2-leaf"))
            modelContext.insert(BetaOneLeaf(id: "beta-1-leaf"))
            modelContext.insert(BetaTwoLeaf(id: "beta-2-leaf"))
            try modelContext.save()
        }
    }
}

@Suite(.serialized)
struct InheritanceChainBranchSpreadTests {
    private let modelContext: ModelContext
    private let modelContainer: ModelContainer
    private let configuration: DatabaseConfiguration
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    init() throws {
        let schema = Schema(versionedSchema: Sample.self)
        var configuration = DatabaseConfiguration.transient(
            types: Sample.models,
            schema: schema,
            options: .disableSnapshotCaching
        )
        configuration.configurations[.predicate] = SQLPredicateTranslatorOptions([
            .useDetailedLogging,
            .useVerboseLogging,
            .logAllPredicateExpressions,
            .preferStandardOutput
        ])
        self.configuration = configuration
        self.modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        self.modelContext = ModelContext(modelContainer)
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    static func alphabetParentByID() -> [String: String] {
        [
            "b": "a",
            "c": "a",
            "d": "b",
            "e": "b",
            "f": "b",
            "g": "c",
            "h": "c",
            "i": "c",
            "j": "d",
            "k": "d",
            "l": "d",
            "m": "d",
            "n": "e",
            "o": "e",
            "p": "e",
            "q": "e",
            "r": "f",
            "s": "f",
            "t": "f",
            "u": "f",
            "v": "g",
            "w": "g",
            "x": "g",
            "y": "g",
            "z": "h"
        ]
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    static func alphabetExpectedTypeNames(for id: String) -> [String] {
        let parents = Self.alphabetParentByID()
        var chain: [String] = [id]
        var current = id
        while let parent = parents[current] {
            chain.insert(parent, at: 0)
            current = parent
        }
        return chain.map { $0.uppercased() }
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    static func alphabetTypeNames(for model: Sample.A) -> [String] {
        var types: [String] = ["A"]
        if model is Sample.B { types.append("B") }
        if model is Sample.C { types.append("C") }
        if model is Sample.D { types.append("D") }
        if model is Sample.E { types.append("E") }
        if model is Sample.F { types.append("F") }
        if model is Sample.G { types.append("G") }
        if model is Sample.H { types.append("H") }
        if model is Sample.I { types.append("I") }
        if model is Sample.J { types.append("J") }
        if model is Sample.K { types.append("K") }
        if model is Sample.L { types.append("L") }
        if model is Sample.M { types.append("M") }
        if model is Sample.N { types.append("N") }
        if model is Sample.O { types.append("O") }
        if model is Sample.P { types.append("P") }
        if model is Sample.Q { types.append("Q") }
        if model is Sample.R { types.append("R") }
        if model is Sample.S { types.append("S") }
        if model is Sample.T { types.append("T") }
        if model is Sample.U { types.append("U") }
        if model is Sample.V { types.append("V") }
        if model is Sample.W { types.append("W") }
        if model is Sample.X { types.append("X") }
        if model is Sample.Y { types.append("Y") }
        if model is Sample.Z { types.append("Z") }
        return types
    }
    
    @Test
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func alphabetInheritanceBreadthFirstTreeChecksAllPaths() throws {
        try Sample.seed(into: modelContext)
        let models = try modelContext.fetch(FetchDescriptor<Sample.A>())
        #expect(models.count == 26)
        let modelsByID = Dictionary(uniqueKeysWithValues: models.map { ($0.id, $0) })
        for id in Array("abcdefghijklmnopqrstuvwxyz").map({ String($0) }) {
            let lhs = Self.alphabetTypeNames(for: try #require(modelsByID[id]))
            let rhs = Self.alphabetExpectedTypeNames(for: id)
            #expect(lhs == rhs)
        }
        #expect(try modelContext.fetch(FetchDescriptor<Sample.A>()).count == 26)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.B>()).count == 16)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.C>()).count == 9)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.D>()).count == 5)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.E>()).count == 5)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.F>()).count == 5)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.G>()).count == 5)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.H>()).count == 2)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.I>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.J>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.K>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.L>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.M>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.N>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.O>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.P>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.Q>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.R>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.S>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.T>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.U>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.V>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.W>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.X>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.Y>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor<Sample.Z>()).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor(predicate: #Predicate<Sample.A> { $0 is Sample.B })).count == 16)
        #expect(try modelContext.fetch(FetchDescriptor(predicate: #Predicate<Sample.A> { $0 is Sample.C })).count == 9)
        #expect(try modelContext.fetch(FetchDescriptor(predicate: #Predicate<Sample.A> { $0 is Sample.D })).count == 5)
        #expect(try modelContext.fetch(FetchDescriptor(predicate: #Predicate<Sample.A> { $0 is Sample.H })).count == 2)
        #expect(try modelContext.fetch(FetchDescriptor(predicate: #Predicate<Sample.A> { $0 is Sample.I })).count == 1)
        #expect(try modelContext.fetch(FetchDescriptor(predicate: #Predicate<Sample.A> { $0 is Sample.Z })).count == 1)
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    struct Sample: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(0, 0, 0)
        static let models: [any PersistentModel.Type] = [
            A.self,
            B.self,
            C.self,
            D.self,
            E.self,
            F.self,
            G.self,
            H.self,
            I.self,
            J.self,
            K.self,
            L.self,
            M.self,
            N.self,
            O.self,
            P.self,
            Q.self,
            R.self,
            S.self,
            T.self,
            U.self,
            V.self,
            W.self,
            X.self,
            Y.self,
            Z.self
        ]
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class A {
            var id: String
            init(id: String) { self.id = id }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class B: A { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class C: A { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class D: B { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class E: B { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class F: B { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class G: C { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class H: C { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class I: C { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class J: D { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class K: D { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class L: D { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class M: D { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class N: E { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class O: E { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class P: E { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class Q: E { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class R: F { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class S: F { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class T: F { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class U: F { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class V: G { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class W: G { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class X: G { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class Y: G { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class Z: H { override init(id: String) { super.init(id: id) } }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        static func seed(into modelContext: ModelContext) throws {
            modelContext.insert(A(id: "a"))
            modelContext.insert(B(id: "b"))
            modelContext.insert(C(id: "c"))
            modelContext.insert(D(id: "d"))
            modelContext.insert(E(id: "e"))
            modelContext.insert(F(id: "f"))
            modelContext.insert(G(id: "g"))
            modelContext.insert(H(id: "h"))
            modelContext.insert(I(id: "i"))
            modelContext.insert(J(id: "j"))
            modelContext.insert(K(id: "k"))
            modelContext.insert(L(id: "l"))
            modelContext.insert(M(id: "m"))
            modelContext.insert(N(id: "n"))
            modelContext.insert(O(id: "o"))
            modelContext.insert(P(id: "p"))
            modelContext.insert(Q(id: "q"))
            modelContext.insert(R(id: "r"))
            modelContext.insert(S(id: "s"))
            modelContext.insert(T(id: "t"))
            modelContext.insert(U(id: "u"))
            modelContext.insert(V(id: "v"))
            modelContext.insert(W(id: "w"))
            modelContext.insert(X(id: "x"))
            modelContext.insert(Y(id: "y"))
            modelContext.insert(Z(id: "z"))
            try modelContext.save()
        }
    }
}
