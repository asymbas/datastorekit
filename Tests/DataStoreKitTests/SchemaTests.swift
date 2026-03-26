//
//  SchemaTests.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreKit
import Testing
import SwiftData

@Suite("Schema")
struct SchemaTests {
    @Model class EntityImplicitSchemaProperty {
        var id: String
        
        init(id: String) {
            self.id = id
        }
    }
    
    
    
    @Test func test() async throws {
        
    }
}
