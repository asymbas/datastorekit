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
    @Model class ImplicitEntity {
        var id: String
        var relationship: RelationshipEntity?
        
        init(id: String) {
            self.id = id
        }
    }
    
    @Model class ExplicitEntity {
        @Attribute var id: String
        @Relationship(deleteRule: .cascade, inverse: \RelationshipEntity.explicit)
        var relationship: RelationshipEntity?
        
        init(id: String) {
            self.id = id
        }
    }
    
    @Model class RelationshipEntity {
        var id: String
        @Relationship var implicit: ImplicitEntity?
        @Relationship var explicit: ExplicitEntity?
        
        init(id: String) {
            self.id = id
        }
    }
    
    @Test func test() async throws {
        
    }
}
