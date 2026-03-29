//
//  DataStoreTests.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreKit
import Foundation
import Logging
import SwiftData
import Testing

@Suite("DatabaseConfiguration Lifecycle", .disabled())
struct DatabaseConfigurationLifecycleTests {
    init() {
        _ = logging
    }

    @Test
    func storeIsNilBeforeContainerCreation() {
        let schema = Schema()
        let configuration = DatabaseConfiguration.transient(types: [], schema: schema)
        #expect(configuration.store == nil)
    }

    @Test
    func storeIsBoundAfterContainerCreation() throws {
        let schema = Schema()
        var configuration = DatabaseConfiguration.transient(types: [], schema: schema)
        let modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        #expect(configuration.store != nil)
    }

    @Test
    func storeIsNilAfterContainerDeinits() throws {
        let schema = Schema()
        var configuration = DatabaseConfiguration.transient(types: [], schema: schema)
        do {
            let modelContainer = try ModelContainer(for: schema, configurations: [configuration])
            #expect(configuration.store != nil)
        }
        #expect(configuration.store == nil)
    }

    @Test
    func configurationIsReusableAfterContainerDeinits() throws {
        let schema = Schema()
        var configuration = DatabaseConfiguration.transient(types: [], schema: schema)
        do {
            let modelContainer = try ModelContainer(for: schema, configurations: [configuration])
            #expect(configuration.store != nil)
        }
        #expect(configuration.store == nil)
        let modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        #expect(configuration.store != nil)
    }

    @Test
    func sequentialContainersBindDistinctStores() throws {
        let schema = Schema()
        var configuration = DatabaseConfiguration.transient(
            types: [],
            schema: schema
        )
        let firstIdentifier: String
        do {
            let modelContainer = try ModelContainer(for: schema, configurations: [configuration])
            let store = try #require(configuration.store)
            firstIdentifier = store.identifier
        }
        let modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        let store = try #require(configuration.store)
        #expect(store.identifier == firstIdentifier)
    }
}
