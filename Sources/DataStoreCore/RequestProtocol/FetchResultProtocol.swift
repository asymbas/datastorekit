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
