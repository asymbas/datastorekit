//
//  SaveChangesRequestProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import SwiftData

public protocol SaveChangesRequest<SnapshotType>: Sendable, SendableMetatype {
    associatedtype SnapshotType: DataStoreSnapshot
    associatedtype EditingStateType: EditingStateProviding
    var editingState: EditingStateType { get }
    var inserted: [SnapshotType] { get }
    var updated: [SnapshotType] { get }
    var deleted: [SnapshotType] { get }
}

extension DataStoreSaveChangesRequest: SaveChangesRequest {}

public struct DatabaseSaveChangesRequest<Snapshot, EditingState>: SaveChangesRequest
where Snapshot: DataStoreSnapshot, EditingState: EditingStateProviding {
    public typealias SnapshotType = Snapshot
    public typealias EditingStateType = EditingState
    nonisolated public var editingState: EditingState
    nonisolated public var inserted: [Snapshot]
    nonisolated public var updated: [Snapshot]
    nonisolated public var deleted: [Snapshot]
    
    nonisolated public init(
        editingState: EditingState,
        inserted: [Snapshot],
        updated: [Snapshot],
        deleted: [Snapshot]
    ) {
        self.editingState = editingState
        self.inserted = inserted
        self.updated = updated
        self.deleted = deleted
    }
}
