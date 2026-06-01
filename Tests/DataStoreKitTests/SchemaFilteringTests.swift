//
//  SchemaFilteringTests.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

@testable import DataStoreKit
import Foundation
import SwiftData
import Testing

@Suite(.bootstrap, .serialized)
struct SchemaFilteringTests {
    @Model class Known {
        var name: String
        init(name: String) { self.name = name }
    }
    
    private func encodedSchema(for types: [any PersistentModel.Type]) throws -> Data {
        TypeRegistry.removeAll()
        let schema = Schema(types)
        TypeRegistry.bootstrap(schema: schema, types: types)
        return try JSONEncoder().encode(schema)
    }
    
    @Test(.disabled("This will crash as expected."))
    func deliberatelyCrashOnUnresolvableValueTypeName() throws {
        TypeRegistry.removeAll()
        let schema = Schema([Known.self])
        TypeRegistry.bootstrap(schema: schema, types: [Known.self])
        let data = try JSONEncoder().encode(schema)
        var root = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        var entities = try #require(root["entities"] as? [[String: Any]])
        var entity = entities[0]
        entity["attributes"] = [
            [
                "name": "field",
                "valueTypeName": "99FakeModule5GhostC",
                "isOptional": false,
                "originalName": "",
                "options": [],
                "hashModifier": NSNull()
            ]
        ]
        entity["storedAttributes"] = entity["attributes"]
        entities[0] = entity
        root["entities"] = entities
        let injected = try JSONSerialization.data(withJSONObject: root)
        _ = try JSONDecoder().decode(Schema.self, from: injected)
    }
    
    @Test
    func encodedSchemaExposesValueTypeName() throws {
        let data = try encodedSchema(for: [Known.self])
        let root = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        let entities = try #require(root["entities"] as? [[String: Any]])
        let attributes = try #require(entities[0]["attributes"] as? [[String: Any]])
        let valueTypeName = try #require(attributes.first?["valueTypeName"] as? String)
        #expect(valueTypeName == "SS")
    }
    
    @Test
    func keepsEntitiesWithResolvableValueTypeNames() throws {
        let data = try encodedSchema(for: [Known.self])
        let filtered = try Schema.filteringUnregisteredEntities(in: data)
        let root = try #require(try JSONSerialization.jsonObject(with: filtered) as? [String: Any])
        let entities = try #require(root["entities"] as? [[String: Any]])
        #expect(entities.count == 1)
    }
    
    @Test
    func dropsEntitiesWithUnresolvableValueTypeName() throws {
        let data = try encodedSchema(for: [Known.self])
        var root = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        var entities = try #require(root["entities"] as? [[String: Any]])
        var entity = entities[0]
        entity["attributes"] = [["name": "field", "valueTypeName": "99FakeModule5GhostC"]]
        entities[0] = entity
        root["entities"] = entities
        let injected = try JSONSerialization.data(withJSONObject: root)
        let filtered = try Schema.filteringUnregisteredEntities(in: injected)
        let resultRoot = try #require(try JSONSerialization.jsonObject(with: filtered) as? [String: Any])
        let resultEntities = try #require(resultRoot["entities"] as? [[String: Any]])
        #expect(resultEntities.isEmpty)
    }
    
    @Test
    func dropsOnlyUnresolvableEntitiesWhenMixed() throws {
        let data = try encodedSchema(for: [Known.self])
        var root = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        var entities = try #require(root["entities"] as? [[String: Any]])
        var ghost = entities[0]
        ghost["name"] = "Ghost"
        ghost["attributes"] = [["name": "field", "valueTypeName": "99FakeModule5GhostC"]]
        entities.append(ghost)
        root["entities"] = entities
        let injected = try JSONSerialization.data(withJSONObject: root)
        let filtered = try Schema.filteringUnregisteredEntities(in: injected)
        let resultRoot = try #require(try JSONSerialization.jsonObject(with: filtered) as? [String: Any])
        let resultEntities = try #require(resultRoot["entities"] as? [[String: Any]])
        #expect(resultEntities.count == 1)
        #expect(resultEntities[0]["name"] as? String == "Known")
    }
    
    @Test
    func dropsEntityWhenNestedValueTypeNameIsUnresolvable() throws {
        let data = try encodedSchema(for: [Known.self])
        var root = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        var entities = try #require(root["entities"] as? [[String: Any]])
        var entity = entities[0]
        entity["storedAttributes"] = [["name": "field", "valueTypeName": "99FakeModule5GhostC"]]
        entities[0] = entity
        root["entities"] = entities
        let injected = try JSONSerialization.data(withJSONObject: root)
        let filtered = try Schema.filteringUnregisteredEntities(in: injected)
        let resultRoot = try #require(try JSONSerialization.jsonObject(with: filtered) as? [String: Any])
        let resultEntities = try #require(resultRoot["entities"] as? [[String: Any]])
        #expect(resultEntities.isEmpty)
    }
    
    @Test
    func decodingSucceedsAfterFiltering() throws {
        let data = try encodedSchema(for: [Known.self])
        var root = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        var entities = try #require(root["entities"] as? [[String: Any]])
        var ghost = entities[0]
        ghost["name"] = "Ghost"
        ghost["attributes"] = [["name": "field", "valueTypeName": "99FakeModule5GhostC"]]
        entities.append(ghost)
        root["entities"] = entities
        let injected = try JSONSerialization.data(withJSONObject: root)
        let filtered = try Schema.filteringUnregisteredEntities(in: injected)
        let decoded = try JSONDecoder().decode(Schema.self, from: filtered)
        #expect(decoded.entitiesByName["Known"] != nil)
        #expect(decoded.entitiesByName["Ghost"] == nil)
    }
    
    @Test
    func returnsOriginalDataWhenNoChangesNeeded() throws {
        let data = try encodedSchema(for: [Known.self])
        let filtered = try Schema.filteringUnregisteredEntities(in: data)
        #expect(filtered == data)
    }
    
    @Test
    func returnsOriginalDataWhenStructureIsUnrecognized() throws {
        let unrelated = try JSONSerialization.data(withJSONObject: ["foo": "bar"])
        let filtered = try Schema.filteringUnregisteredEntities(in: unrelated)
        #expect(filtered == unrelated)
    }
}
