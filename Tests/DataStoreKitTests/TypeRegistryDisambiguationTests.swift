//
//  TypeRegistryDisambiguationTests.swift
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
struct TypeRegistryDisambiguationTests {
    enum NamespaceA {
        @Model class Entity { init() {} }
        
        struct Sample: VersionedSchema {
            static let versionIdentifier: Schema.Version = .init(0, 0, 0)
            static let models: [any PersistentModel.Type] = [Entity.self]
        }
    }
    
    enum NamespaceB {
        @Model class Entity { init() {} }
        
        struct Sample: VersionedSchema {
            static let versionIdentifier: Schema.Version = .init(0, 0, 0)
            static let models: [any PersistentModel.Type] = [Entity.self]
        }
    }
    
    @Test
    func bothColocatedTypesCoexistInTypeRegistry() throws {
        let schemaA = Schema(versionedSchema: NamespaceA.Sample.self)
        let schemaB = Schema(versionedSchema: NamespaceB.Sample.self)
        TypeRegistry.bootstrap(schema: schemaA, types: NamespaceA.Sample.models)
        TypeRegistry.bootstrap(schema: schemaB, types: NamespaceB.Sample.models)
        let qualifiedNames = Set(TypeRegistry.getValues(forTypeName: "Entity").map(\.qualifiedTypeName))
        #expect(qualifiedNames.contains(String(reflecting: NamespaceA.Entity.self)))
        #expect(qualifiedNames.contains(String(reflecting: NamespaceB.Entity.self)))
    }
    
    @Test
    func entityLookupReturnsCorrectTypeForEachSchema() throws {
        let schemaA = Schema(versionedSchema: NamespaceA.Sample.self)
        let schemaB = Schema(versionedSchema: NamespaceB.Sample.self)
        TypeRegistry.bootstrap(schema: schemaA, types: NamespaceA.Sample.models)
        TypeRegistry.bootstrap(schema: schemaB, types: NamespaceB.Sample.models)
        let entityA = try #require(schemaA.entitiesByName["Entity"])
        let entityB = try #require(schemaB.entitiesByName["Entity"])
        let typeA = try #require(Schema.type(for: entityA))
        let typeB = try #require(Schema.type(for: entityB))
        #expect(ObjectIdentifier(typeA) == ObjectIdentifier(NamespaceA.Entity.self))
        #expect(ObjectIdentifier(typeB) == ObjectIdentifier(NamespaceB.Entity.self))
        #expect(ObjectIdentifier(typeA) != ObjectIdentifier(typeB))
    }
    
    @Test
    func qualifiedNameLookupIsUnambiguous() throws {
        let schemaA = Schema(versionedSchema: NamespaceA.Sample.self)
        let schemaB = Schema(versionedSchema: NamespaceB.Sample.self)
        TypeRegistry.bootstrap(schema: schemaA, types: NamespaceA.Sample.models)
        TypeRegistry.bootstrap(schema: schemaB, types: NamespaceB.Sample.models)
        let qualifiedA = String(reflecting: NamespaceA.Entity.self)
        let qualifiedB = String(reflecting: NamespaceB.Entity.self)
        let typeA: AnyClass = try #require(TypeRegistry.getType(forQualifiedTypeName: qualifiedA))
        let typeB: AnyClass = try #require(TypeRegistry.getType(forQualifiedTypeName: qualifiedB))
        #expect(ObjectIdentifier(typeA) == ObjectIdentifier(NamespaceA.Entity.self))
        #expect(ObjectIdentifier(typeB) == ObjectIdentifier(NamespaceB.Entity.self))
    }
}
