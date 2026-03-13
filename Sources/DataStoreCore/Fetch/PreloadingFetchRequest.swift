//
//  PreloadingFetchRequest.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import SwiftData

public struct PreloadFetchRequest<T>: FetchRequest where T: PersistentModel {
    nonisolated public var isUnchecked: Bool
    nonisolated public var modifier: (any Hashable & Sendable)?
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var editingState: DatabaseEditingState
    
    nonisolated public init(
        isUnchecked: Bool,
        modifier: (any Hashable & Sendable)?,
        descriptor: FetchDescriptor<T>,
        editingState: DatabaseEditingState
    ) {
        self.isUnchecked = isUnchecked
        self.modifier = modifier
        self.descriptor = descriptor
        self.editingState = editingState
    }
}

public struct PreloadFetchResult<T, Snapshot>: FetchResult
where T: PersistentModel, Snapshot: DataStoreSnapshot {
    /// A flag that will unsafely return the fetch result without validating the match.
    nonisolated package private(set) var isUnchecked: Bool
    nonisolated public var key: Int?
    nonisolated public var editingState: (any EditingStateProviding)?
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var fetchedSnapshots: [Snapshot]
    nonisolated public var relatedSnapshots: [PersistentIdentifier: Snapshot]
    
    nonisolated private init(
        isUnchecked: Bool,
        key: Int?,
        editingState: (any EditingStateProviding)?,
        descriptor: FetchDescriptor<T>,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier : Snapshot]
    ) {
        self.isUnchecked = isUnchecked
        self.key = key
        self.editingState = editingState
        self.descriptor = descriptor
        self.fetchedSnapshots = fetchedSnapshots
        self.relatedSnapshots = relatedSnapshots
    }
    
    nonisolated package init(
        request: PreloadFetchRequest<T>,
        forKey key: Int?,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier : Snapshot]
    ) {
        self.init(
            isUnchecked: request.isUnchecked,
            key: key,
            editingState: request.editingState,
            descriptor: request.descriptor,
            fetchedSnapshots: fetchedSnapshots,
            relatedSnapshots: relatedSnapshots
        )
    }
    
    nonisolated public init(
        descriptor: FetchDescriptor<T>,
        fetchedSnapshots: [Snapshot],
        relatedSnapshots: [PersistentIdentifier : Snapshot]
    ) {
        self.init(
            isUnchecked: false,
            key: -1,
            editingState: nil,
            descriptor: descriptor,
            fetchedSnapshots: fetchedSnapshots,
            relatedSnapshots: relatedSnapshots
        )
    }
    
    nonisolated package func convert<Result>(into result: Result.Type) -> Result
    where Result: FetchResult, Self.ModelType == Result.ModelType, Self.SnapshotType == Result.SnapshotType {
        .init(descriptor: descriptor, fetchedSnapshots: fetchedSnapshots, relatedSnapshots: relatedSnapshots)
    }
}
