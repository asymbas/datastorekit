//
//  DatabaseFetchRequest.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import SwiftData

public struct DatabaseFetchRequest<T>: FetchRequest where T: PersistentModel {
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var editingState: DatabaseEditingState
    
    nonisolated public init(
        descriptor: FetchDescriptor<T>,
        editingState: DatabaseEditingState
    ) {
        self.descriptor = descriptor
        self.editingState = editingState
    }
}

public struct DatabaseFetchResult<T, Snapshot>: FetchResult
where T: PersistentModel, Snapshot: DataStoreSnapshot {
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var fetchedSnapshots: [Snapshot]
    nonisolated public var relatedSnapshots: [PersistentIdentifier: Snapshot]
    
    nonisolated public init(
        descriptor: FetchDescriptor<T>,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier : Snapshot]
    ) {
        self.descriptor = descriptor
        self.fetchedSnapshots = fetchedSnapshots
        self.relatedSnapshots = relatedSnapshots
    }
}
