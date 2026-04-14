//
//  PreloadingFetchRequest.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import SwiftData

package struct PreloadFetchKey: Equatable, Hashable, Sendable {
    nonisolated package let editingStateID: EditingState.ID
    nonisolated package let modifier: String?
    nonisolated package var key: Int?
    
    nonisolated package init(
        editingStateID: EditingState.ID,
        modifier: String?,
        key: Int?
    ) {
        self.editingStateID = editingStateID
        self.modifier = modifier
        self.key = key
    }
    
    nonisolated package init<each Value: Hashable & Sendable>(
        _ editingStateID: EditingState.ID,
        _ modifier: String?,
        _ key: repeat each Value
    ) {
        self.editingStateID = editingStateID
        self.modifier = modifier
        var hasher = Hasher()
        hasher.combine(editingStateID)
        repeat hasher.combine(each key)
        self.key = hasher.finalize()
    }
    
    nonisolated package static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.editingStateID == rhs.editingStateID && lhs.modifier == rhs.modifier
    }
    
    nonisolated package func hash(into hasher: inout Hasher) {
        hasher.combine(editingStateID)
        hasher.combine(modifier)
    }
}

public struct PreloadFetchRequest<T>: FetchRequest where T: PersistentModel {
    nonisolated public var isUnchecked: Bool
    nonisolated public var modifier: String?
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var editingState: DatabaseEditingState
    
    nonisolated public init(
        isUnchecked: Bool,
        modifier: String?,
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
            key: nil,
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
