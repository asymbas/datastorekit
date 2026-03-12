//
//  FetchResultProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import SwiftData

public protocol FetchResult<ModelType, SnapshotType>: Sendable, SendableMetatype
where ModelType: PersistentModel, SnapshotType: DataStoreSnapshot {
    associatedtype ModelType
    associatedtype SnapshotType
    nonisolated var descriptor: FetchDescriptor<ModelType> { get }
    nonisolated var fetchedSnapshots: [SnapshotType] { get }
    nonisolated var relatedSnapshots: [PersistentIdentifier: SnapshotType] { get }
    
    nonisolated init(
        descriptor: FetchDescriptor<ModelType>,
        fetchedSnapshots: [SnapshotType],
        relatedSnapshots: [PersistentIdentifier: SnapshotType]
    )
}

extension DataStoreFetchResult: FetchResult {}

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
        Result(descriptor: descriptor, fetchedSnapshots: fetchedSnapshots, relatedSnapshots: relatedSnapshots)
    }
}
